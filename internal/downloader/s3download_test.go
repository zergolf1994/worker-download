package downloader

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"worker-download/internal/db/models"
)

func TestDownloadFromS3CommitsCompleteObjectAtomically(t *testing.T) {
	body := bytes.Repeat([]byte("video-data"), 1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(outputPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	err := DownloadFromS3(context.Background(), testS3Storage(server.URL), "object.mp4", outputPath, nil)
	if err != nil {
		t.Fatalf("DownloadFromS3() error = %v", err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("committed output does not match S3 object")
	}
	if _, err := os.Stat(outputPath + ".part"); !os.IsNotExist(err) {
		t.Fatalf("partial file remains after success: %v", err)
	}
}

func TestDownloadFromS3RejectsTruncatedObjectAndRemovesPartial(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 100))
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "source.mp4")
	err := DownloadFromS3(context.Background(), testS3Storage(server.URL), "object.mp4", outputPath, nil)
	if !errors.Is(err, ErrIncompleteDownload) {
		t.Fatalf("DownloadFromS3() error = %v, want ErrIncompleteDownload", err)
	}
	if requests.Load() != 3 {
		t.Fatalf("request count = %d, want 3", requests.Load())
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output file exists after truncated download: %v", err)
	}
	if _, err := os.Stat(outputPath + ".part"); !os.IsNotExist(err) {
		t.Fatalf("partial file remains after truncated download: %v", err)
	}
}

func testS3Storage(endpoint string) *models.Storage {
	return &models.Storage{S3: &models.StorageS3Config{
		Endpoint:        &endpoint,
		Region:          "auto",
		Bucket:          "bucket",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		ForcePathStyle:  true,
	}}
}
