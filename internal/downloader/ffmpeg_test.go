package downloader

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAACStereoArgs(t *testing.T) {
	want := []string{"-c:a", "aac", "-b:a", "128k", "-ac", "2", "-ar", "48000"}
	if got := aacStereoArgs("128k"); !reflect.DeepEqual(got, want) {
		t.Fatalf("aacStereoArgs() = %v, want %v", got, want)
	}
}

func TestH264CmdNormalizesAudioToStereo48K(t *testing.T) {
	cmd := h264Cmd(context.Background(), "source.mkv", "output.mp4", false, encoderCPU)
	want := []string{"-c:a", "aac", "-b:a", "128k", "-ac", "2", "-ar", "48000"}
	if !containsArgs(cmd.Args, want) {
		t.Fatalf("h264Cmd args = %v, want sequence %v", cmd.Args, want)
	}
}

func containsArgs(args, sequence []string) bool {
	for i := 0; i+len(sequence) <= len(args); i++ {
		if reflect.DeepEqual(args[i:i+len(sequence)], sequence) {
			return true
		}
	}
	return false
}

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
