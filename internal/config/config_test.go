package config

import "testing"

func TestGetMediaLayoutEnv(t *testing.T) {
	t.Run("defaults to muxed", func(t *testing.T) {
		t.Setenv("MEDIA_LAYOUT", "")
		if got := getMediaLayoutEnv(); got != "muxed" {
			t.Fatalf("got %q, want muxed", got)
		}
	})
	t.Run("accepts separated", func(t *testing.T) {
		t.Setenv("MEDIA_LAYOUT", "separated")
		if got := getMediaLayoutEnv(); got != "separated" {
			t.Fatalf("got %q, want separated", got)
		}
	})
	t.Run("invalid falls back to muxed", func(t *testing.T) {
		t.Setenv("MEDIA_LAYOUT", "invalid")
		if got := getMediaLayoutEnv(); got != "muxed" {
			t.Fatalf("got %q, want muxed", got)
		}
	})
}
