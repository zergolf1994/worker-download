package downloader

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIsWebVTTSubtitleCodec(t *testing.T) {
	for _, codec := range []string{"subrip", "ass", "ssa", "webvtt", "mov_text", "ttml"} {
		if !IsWebVTTSubtitleCodec(codec) {
			t.Errorf("expected %s to be supported", codec)
		}
	}
	for _, codec := range []string{"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle", "xsub", ""} {
		if IsWebVTTSubtitleCodec(codec) {
			t.Errorf("expected %s to be skipped", codec)
		}
	}
}

func TestSplitMediaProducesVideoOnlyAndOneAssetPerAudioTrack(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}

	dir := t.TempDir()
	input := filepath.Join(dir, "source.mkv")
	cmd := exec.Command("ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=black:s=320x180:r=25:d=1",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=1",
		"-f", "lavfi", "-i", "sine=frequency=880:duration=1",
		"-map", "0:v:0", "-map", "1:a:0", "-map", "2:a:0",
		"-c:v", "libx264", "-c:a", "aac", "-shortest", input)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create fixture: %v: %s", err, output)
	}

	assets, err := SplitMedia(context.Background(), input, filepath.Join(dir, "out"), nil)
	if err != nil {
		t.Fatalf("SplitMedia: %v", err)
	}
	if len(assets) != 3 {
		t.Fatalf("got %d assets, want 3", len(assets))
	}
	if assets[0].Kind != SplitAssetVideo || assets[1].FileName != "audio_1.m4a" || assets[2].FileName != "audio_2.m4a" {
		t.Fatalf("unexpected assets: %+v", assets)
	}

	probe := exec.Command("ffprobe", "-v", "error", "-select_streams", "a",
		"-show_entries", "stream=index", "-of", "csv=p=0", assets[0].Path)
	output, err := probe.Output()
	if err != nil {
		t.Fatalf("probe video-only output: %v", err)
	}
	if strings.TrimSpace(string(output)) != "" {
		t.Fatalf("video-only output contains audio streams: %s", output)
	}
}
