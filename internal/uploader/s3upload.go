package uploader

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"worker-download/internal/config"
	"worker-download/internal/db/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsretry "github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// multipartThreshold is the file size above which multipart upload is used.
	multipartThreshold = 100 * 1024 * 1024 // 100 MB
	// partSize is the size of each part in multipart upload.
	partSize      = 100 * 1024 * 1024 // 100 MB
	s3MaxAttempts = 5
)

type multipartClient interface {
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

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
		Retryer: awsretry.NewStandard(func(options *awsretry.StandardOptions) {
			options.MaxAttempts = s3MaxAttempts
			options.MaxBackoff = 20 * time.Second
		}),
	})

	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("stat local file: %w", err)
	}
	totalSize := fileInfo.Size()

	log.Printf("📤 S3 Upload: endpoint=%s bucket=%s key=%s size=%.2fMB",
		endpoint, s3Cfg.Bucket, fullKey, float64(totalSize)/1024/1024)

	if totalSize <= multipartThreshold {
		err = uploadSinglePart(ctx, client, s3Cfg.Bucket, fullKey, localPath, totalSize, contentTypeFor(localPath), onProgress)
	} else {
		err = uploadMultipart(ctx, client, s3Cfg.Bucket, fullKey, localPath, totalSize,
			contentTypeFor(localPath), partSize, config.AppConfig.S3UploadConcurrency, onProgress)
	}
	if err != nil {
		return err
	}
	return verifyS3Object(ctx, client, s3Cfg.Bucket, fullKey, totalSize)
}

func contentTypeFor(filePath string) string {
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".mp4":
		return "video/mp4"
	case ".zip":
		return "application/zip"
	case ".vtt":
		return "text/vtt; charset=utf-8"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".log", ".txt":
		return "text/plain; charset=utf-8"
	}
	if detected := mime.TypeByExtension(strings.ToLower(filepath.Ext(filePath))); detected != "" {
		return detected
	}
	return "application/octet-stream"
}

func verifyS3Object(ctx context.Context, client *s3.Client, bucket, key string, expectedSize int64) error {
	result, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("S3 HeadObject: %w", err)
	}
	actual := aws.ToInt64(result.ContentLength)
	if result.ContentLength == nil || actual != expectedSize {
		return fmt.Errorf("S3 size mismatch: expected %d, got %d", expectedSize, actual)
	}
	log.Printf("✅ S3 object verified: %s (%d bytes)", key, expectedSize)
	return nil
}

// uploadSinglePart uploads a file in a single PutObject call.
func uploadSinglePart(ctx context.Context, client *s3.Client, bucket, key, localPath string, totalSize int64, contentType string, onProgress func(uploaded, total int64)) error {
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
		ContentType:   aws.String(contentType),
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
func uploadMultipart(ctx context.Context, client multipartClient, bucket, key, localPath string, totalSize int64, contentType string, partBytes int64, concurrency int, onProgress func(uploaded, total int64)) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("S3 CreateMultipartUpload: %w", err)
	}
	if createResp.UploadId == nil || *createResp.UploadId == "" {
		return fmt.Errorf("S3 CreateMultipartUpload returned no upload ID")
	}
	uploadID := *createResp.UploadId
	abort := func() {
		abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, abortErr := client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		}); abortErr != nil {
			log.Printf("⚠️  S3 abort multipart failed: %v", abortErr)
		}
	}
	if partBytes <= 0 {
		abort()
		return fmt.Errorf("multipart part size must be positive")
	}
	type partJob struct {
		index  int
		number int32
		offset int64
		size   int64
	}
	partCount := int((totalSize + partBytes - 1) / partBytes)
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > partCount {
		concurrency = partCount
	}
	jobs := make(chan partJob, partCount)
	for index := 0; index < partCount; index++ {
		offset := int64(index) * partBytes
		size := partBytes
		if remaining := totalSize - offset; remaining < size {
			size = remaining
		}
		jobs <- partJob{index: index, number: int32(index + 1), offset: offset, size: size}
	}
	close(jobs)
	uploadCtx, cancelUploads := context.WithCancel(ctx)
	defer cancelUploads()
	completedParts := make([]types.CompletedPart, partCount)
	var uploaded int64
	var progressMu sync.Mutex
	var firstErr error
	var errOnce sync.Once
	var workers sync.WaitGroup
	log.Printf("📤 S3 multipart: %d part(s), %.0f MB each, concurrency=%d", partCount, float64(partBytes)/1024/1024, concurrency)
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for part := range jobs {
				if uploadCtx.Err() != nil {
					return
				}
				startedAt := time.Now()
				partResp, uploadErr := client.UploadPart(uploadCtx, &s3.UploadPartInput{
					Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
					PartNumber: aws.Int32(part.number), Body: io.NewSectionReader(f, part.offset, part.size), ContentLength: aws.Int64(part.size),
				})
				if uploadErr != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("S3 UploadPart %d: %w", part.number, uploadErr)
						cancelUploads()
					})
					return
				}
				completedParts[part.index] = types.CompletedPart{ETag: partResp.ETag, PartNumber: aws.Int32(part.number)}
				progressMu.Lock()
				uploaded += part.size
				if onProgress != nil {
					onProgress(uploaded, totalSize)
				}
				progressMu.Unlock()
				duration := time.Since(startedAt).Seconds()
				if duration < 0.001 {
					duration = 0.001
				}
				log.Printf("📤 S3 part %d/%d: %.2f MB in %.2fs (%.2f MB/s)", part.number, partCount, float64(part.size)/1024/1024, duration, float64(part.size)/1024/1024/duration)
			}
		}()
	}
	workers.Wait()
	if firstErr != nil {
		abort()
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		abort()
		return err
	}
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		abort()
		return fmt.Errorf("S3 CompleteMultipartUpload: %w", err)
	}

	log.Printf("✅ S3 multipart upload complete: %d parts, %.2f MB", partCount, float64(totalSize)/1024/1024)
	return nil
}
