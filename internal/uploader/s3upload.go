package uploader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"worker-download/internal/db/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// multipartThreshold is the file size above which multipart upload is used.
	multipartThreshold = 100 * 1024 * 1024 // 100 MB
	// partSize is the size of each part in multipart upload.
	partSize = 50 * 1024 * 1024 // 50 MB
)

// UploadToS3 uploads a local file to S3-compatible storage.
// objectKey is the full key (e.g. "{fileID}/file_original.mp4").
// onProgress is called periodically with (uploaded bytes, total bytes).
func UploadToS3(ctx context.Context, storage *models.Storage, localPath, objectKey string, onProgress func(uploaded, total int64)) error {
	if storage.S3 == nil {
		return fmt.Errorf("storage has no S3 config")
	}
	if storage.S3.Endpoint == nil || strings.TrimSpace(*storage.S3.Endpoint) == "" {
		return fmt.Errorf("storage has no S3 endpoint")
	}

	s3Cfg := storage.S3
	endpoint := strings.TrimRight(*s3Cfg.Endpoint, "/")
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = "https://" + endpoint
	}
	if strings.HasSuffix(endpoint, "/"+s3Cfg.Bucket) {
		endpoint = endpoint[:len(endpoint)-len(s3Cfg.Bucket)-1]
	}

	// Prepend prefix if configured
	fullKey := objectKey
	if s3Cfg.Prefix != "" && !strings.HasPrefix(objectKey, s3Cfg.Prefix) {
		fullKey = strings.TrimRight(s3Cfg.Prefix, "/") + "/" + objectKey
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

	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("stat local file: %w", err)
	}
	totalSize := fileInfo.Size()

	log.Printf("📤 S3 Upload: endpoint=%s bucket=%s key=%s size=%.2fMB",
		endpoint, s3Cfg.Bucket, fullKey, float64(totalSize)/1024/1024)

	if totalSize <= multipartThreshold {
		return uploadSinglePart(ctx, client, s3Cfg.Bucket, fullKey, localPath, totalSize, onProgress)
	}
	return uploadMultipart(ctx, client, s3Cfg.Bucket, fullKey, localPath, totalSize, onProgress)
}

// uploadSinglePart uploads a file in a single PutObject call.
func uploadSinglePart(ctx context.Context, client *s3.Client, bucket, key, localPath string, totalSize int64, onProgress func(uploaded, total int64)) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(totalSize),
		ContentType:   aws.String("video/mp4"),
	})
	if err != nil {
		return fmt.Errorf("S3 PutObject: %w", err)
	}

	if onProgress != nil {
		onProgress(totalSize, totalSize)
	}
	log.Printf("✅ S3 single-part upload complete: %.2f MB", float64(totalSize)/1024/1024)
	return nil
}

// uploadMultipart uploads a file using S3 multipart upload.
func uploadMultipart(ctx context.Context, client *s3.Client, bucket, key, localPath string, totalSize int64, onProgress func(uploaded, total int64)) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	// Initiate multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		return fmt.Errorf("S3 CreateMultipartUpload: %w", err)
	}
	uploadID := *createResp.UploadId

	// Upload parts
	var completedParts []types.CompletedPart
	var uploaded int64
	partNum := int32(1)
	buf := make([]byte, partSize)

	for {
		n, readErr := io.ReadFull(f, buf)
		if n == 0 && readErr != nil {
			break
		}

		partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNum),
			Body:          bytes.NewReader(buf[:n]),
			ContentLength: aws.Int64(int64(n)),
		})
		if err != nil {
			// Abort on failure
			client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: aws.String(uploadID),
			})
			return fmt.Errorf("S3 UploadPart %d: %w", partNum, err)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       partResp.ETag,
			PartNumber: aws.Int32(partNum),
		})

		uploaded += int64(n)
		if onProgress != nil {
			onProgress(uploaded, totalSize)
		}
		log.Printf("📤 S3 part %d: %.2f / %.2f MB (%.1f%%)",
			partNum, float64(uploaded)/1024/1024, float64(totalSize)/1024/1024,
			float64(uploaded)/float64(totalSize)*100)

		partNum++
		if readErr != nil {
			break // EOF or short read (last part)
		}
	}

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("S3 CompleteMultipartUpload: %w", err)
	}

	log.Printf("✅ S3 multipart upload complete: %d parts, %.2f MB", partNum-1, float64(totalSize)/1024/1024)
	return nil
}
