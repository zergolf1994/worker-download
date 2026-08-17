package download

import (
	"testing"
	"time"
)

func TestS3VideoObjectKey(t *testing.T) {
	got := s3VideoObjectKey("file-id", "file_original.mp4")
	want := "file-id/file_original.mp4"
	if got != want {
		t.Fatalf("s3VideoObjectKey() = %q, want %q", got, want)
	}
}

func TestS3TempObjectKey(t *testing.T) {
	now := time.Date(2026, time.August, 17, 12, 0, 0, 0, time.UTC)
	got := s3TempObjectKey(now, "file-id", "file_original.mp4")
	want := "2026-08-17/file-id_file_original.mp4"
	if got != want {
		t.Fatalf("s3TempObjectKey() = %q, want %q", got, want)
	}
}
