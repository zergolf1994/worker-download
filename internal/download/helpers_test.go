package download

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReusableSourceFileRemovesInvalidVideo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.mp4")
	if err := os.WriteFile(path, bytes.Repeat([]byte("not-video"), 2048), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, reusable := reusableSourceFile(path); reusable {
		t.Fatal("reusableSourceFile() accepted invalid video")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("invalid cached source was not removed: %v", err)
	}
}
