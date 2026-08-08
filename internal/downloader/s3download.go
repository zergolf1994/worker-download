package downloader

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"worker-download/internal/db/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// DownloadFromS3 downloads a file from S3-compatible storage
func DownloadFromS3(ctx context.Context, storage *models.Storage, objectPath, outputPath string, onProgress func(downloaded, total int64)) error {
	if storage.S3 == nil {
		return fmt.Errorf("storage has no S3 config")
	}

	s3Cfg := storage.S3
	endpoint := strings.TrimRight(*s3Cfg.Endpoint, "/")
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = "https://" + endpoint
	}

	if strings.HasSuffix(endpoint, "/"+s3Cfg.Bucket) {
		endpoint = endpoint[:len(endpoint)-len(s3Cfg.Bucket)-1]
	}

	objectKey := objectPath
	if s3Cfg.Prefix != "" && !strings.HasPrefix(objectPath, s3Cfg.Prefix) {
		objectKey = strings.TrimRight(s3Cfg.Prefix, "/") + "/" + objectPath
	}

	region := s3Cfg.Region
	if region == "" {
		region = "auto"
	}

	log.Printf("📥 S3 Download: endpoint=%s bucket=%s key=%s", endpoint, s3Cfg.Bucket, objectKey)

	client := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: &endpoint,
		Credentials: credentials.NewStaticCredentialsProvider(
			s3Cfg.AccessKeyID,
			s3Cfg.SecretAccessKey,
			"",
		),
		UsePathStyle: s3Cfg.ForcePathStyle,
	})

	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	partialPath := outputPath + ".part"
	_ = os.Remove(partialPath)

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		lastErr = nil
		_ = os.Remove(partialPath)

		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s3Cfg.Bucket),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			lastErr = fmt.Errorf("S3 GetObject: %v", err)
			log.Printf("⚠️ S3 attempt %d/3: %v", attempt, lastErr)
			continue
		}

		totalSize := result.ContentLength
		if totalSize != nil && *totalSize > 0 {
			log.Printf("📦 File size: %.2f MB", float64(*totalSize)/1024/1024)
		}

		out, err := os.Create(partialPath)
		if err != nil {
			result.Body.Close()
			return fmt.Errorf("create partial output file: %w", err)
		}

		var written int64
		buf := make([]byte, 256*1024)
		lastLog := int64(0)
		lastProgress := int64(0)
		for {
			n, readErr := result.Body.Read(buf)
			if n > 0 {
				if _, wErr := out.Write(buf[:n]); wErr != nil {
					out.Close()
					result.Body.Close()
					os.Remove(partialPath)
					return fmt.Errorf("write error: %w", wErr)
				}
				written += int64(n)

				if onProgress != nil && totalSize != nil && *totalSize > 0 && written-lastProgress >= 1*1024*1024 {
					onProgress(written, *totalSize)
					lastProgress = written
				}

				if written-lastLog >= 10*1024*1024 {
					if totalSize != nil && *totalSize > 0 {
						pct := float64(written) / float64(*totalSize) * 100
						log.Printf("📥 S3: %.2f / %.2f MB (%.1f%%)", float64(written)/1024/1024, float64(*totalSize)/1024/1024, pct)
					} else {
						log.Printf("📥 S3: %.2f MB downloaded", float64(written)/1024/1024)
					}
					lastLog = written
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				out.Close()
				result.Body.Close()
				os.Remove(partialPath)
				if totalSize != nil && *totalSize > 0 {
					lastErr = fmt.Errorf("%w: S3 read stopped at %d of %d bytes: %v", ErrIncompleteDownload, written, *totalSize, readErr)
				} else {
					lastErr = fmt.Errorf("S3 read error after %d bytes: %w", written, readErr)
				}
				log.Printf("⚠️ S3 attempt %d/3 read error: %v", attempt, readErr)
				break
			}
		}
		closeErr := out.Close()
		result.Body.Close()

		if lastErr != nil {
			os.Remove(partialPath)
			continue
		}
		if closeErr != nil {
			lastErr = fmt.Errorf("close partial output: %w", closeErr)
			os.Remove(partialPath)
			continue
		}
		if totalSize != nil && *totalSize > 0 && written != *totalSize {
			lastErr = fmt.Errorf("%w: S3 returned %d of %d bytes", ErrIncompleteDownload, written, *totalSize)
			os.Remove(partialPath)
			continue
		}
		if err := os.Rename(partialPath, outputPath); err != nil {
			// Unix replaces atomically. Windows may reject an existing target, so
			// fall back to removing only after the complete partial is on disk.
			if removeErr := os.Remove(outputPath); removeErr != nil && !os.IsNotExist(removeErr) {
				os.Remove(partialPath)
				return fmt.Errorf("remove stale output file: %w", removeErr)
			}
			if retryErr := os.Rename(partialPath, outputPath); retryErr != nil {
				os.Remove(partialPath)
				return fmt.Errorf("commit S3 download: %w", retryErr)
			}
		}

		log.Printf("✅ Downloaded %.2f MB from S3", float64(written)/1024/1024)
		return nil
	}
	return fmt.Errorf("S3 download failed after 3 attempts: %w", lastErr)
}

// DeleteFromS3 deletes a file from S3-compatible storage
func DeleteFromS3(storage *models.Storage, objectPath string) error {
	if storage.S3 == nil {
		return fmt.Errorf("storage has no S3 config")
	}

	s3Cfg := storage.S3
	endpoint := strings.TrimRight(*s3Cfg.Endpoint, "/")
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = "https://" + endpoint
	}

	if strings.HasSuffix(endpoint, "/"+s3Cfg.Bucket) {
		endpoint = endpoint[:len(endpoint)-len(s3Cfg.Bucket)-1]
	}

	objectKey := objectPath
	if s3Cfg.Prefix != "" && !strings.HasPrefix(objectPath, s3Cfg.Prefix) {
		objectKey = strings.TrimRight(s3Cfg.Prefix, "/") + "/" + objectPath
	}

	region := s3Cfg.Region
	if region == "" {
		region = "auto"
	}

	client := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: &endpoint,
		Credentials: credentials.NewStaticCredentialsProvider(
			s3Cfg.AccessKeyID,
			s3Cfg.SecretAccessKey,
			"",
		),
		UsePathStyle: s3Cfg.ForcePathStyle,
	})

	_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s3Cfg.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("S3 DeleteObject failed: %w", err)
	}

	log.Printf("🗑️  Deleted from S3: bucket=%s key=%s", s3Cfg.Bucket, objectKey)
	return nil
}
