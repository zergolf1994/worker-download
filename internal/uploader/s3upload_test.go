package uploader

import (
	"context"
	"strings"
	"testing"

	"worker-download/internal/db/models"
)

func TestUploadToS3RejectsMissingEndpoint(t *testing.T) {
	storage := &models.Storage{S3: &models.StorageS3Config{Bucket: "bucket"}}
	err := UploadToS3(context.Background(), storage, "unused.mp4", "file-id/file_original.mp4", nil)
	if err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("UploadToS3() error = %v, want missing endpoint error", err)
	}
}
