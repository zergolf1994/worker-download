package downloader

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestValidateVideoFileMarksRejectedMediaInvalid(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}

	path := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(path, []byte("not a video"), 0o600); err != nil {
		t.Fatalf("write invalid source: %v", err)
	}

	err := ValidateVideoFile(path)
	if !errors.Is(err, ErrInvalidVideo) {
		t.Fatalf("ValidateVideoFile() error = %v, want ErrInvalidVideo", err)
	}
}

func TestValidateVideoFileDoesNotMarkToolFailureInvalid(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	err := ValidateVideoFile(filepath.Join(t.TempDir(), "source.mp4"))
	if err == nil {
		t.Fatal("ValidateVideoFile() error = nil, want tool error")
	}
	if errors.Is(err, ErrInvalidVideo) {
		t.Fatalf("ValidateVideoFile() error = %v, must not be ErrInvalidVideo", err)
	}
}
