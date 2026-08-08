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
	"strings"
	"testing"
)

func TestDownloadHLSSegmentsWithSharedByteRangeResource(t *testing.T) {
	resource := make([]byte, 188*3)
	for i := 0; i < 3; i++ {
		segment := resource[i*188 : (i+1)*188]
		segment[0] = 0x47
		for j := 1; j < len(segment); j++ {
			segment[j] = byte(i + 1)
		}
	}

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/stream.m3u8":
			_, _ = w.Write([]byte(`#EXTM3U
#EXT-X-VERSION:4
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:6.0,
#EXT-X-BYTERANGE:188@0
media.ts
#EXTINF:6.0,
#EXT-X-BYTERANGE:188
media.ts
#EXTINF:6.0,
#EXT-X-BYTERANGE:188
media.ts
#EXT-X-ENDLIST
`))
		case "/media.ts":
			rangeValue := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
			startText, endText, ok := strings.Cut(rangeValue, "-")
			if !ok {
				http.Error(w, "missing range", http.StatusBadRequest)
				return
			}
			start, startErr := strconv.Atoi(startText)
			end, endErr := strconv.Atoi(endText)
			if startErr != nil || endErr != nil || start < 0 || end >= len(resource) || start > end {
				http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Range", "bytes "+rangeValue+"/"+strconv.Itoa(len(resource)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(resource[start : end+1])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result, err := DownloadHLSSegments(context.Background(), server.URL+"/stream.m3u8", t.TempDir(), &DownloadProgress{})
	if err != nil {
		t.Fatalf("DownloadHLSSegments() error = %v", err)
	}
	if result.SegmentCount != 3 || len(result.SegmentFiles) != 3 {
		t.Fatalf("downloaded %d segments with %d files, want 3", result.SegmentCount, len(result.SegmentFiles))
	}
	if result.TotalSize != int64(len(resource)) {
		t.Fatalf("TotalSize = %d, want %d", result.TotalSize, len(resource))
	}
	for i, path := range result.SegmentFiles {
		got, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("ReadFile(%q) error = %v", path, readErr)
		}
		want := resource[i*188 : (i+1)*188]
		if !bytes.Equal(got, want) {
			t.Errorf("segment file %d does not contain its requested byte range", i)
		}
	}
}

func TestDownloadFileRequestsOnlySegmentByteRange(t *testing.T) {
	resource := bytes.Repeat([]byte("0123456789"), 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=100-199" {
			t.Errorf("Range header = %q, want %q", got, "bytes=100-199")
		}
		w.Header().Set("Content-Range", "bytes 100-199/1000")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(resource[100:200])
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "segment.ts")
	if err := os.WriteFile(outputPath, bytes.Repeat([]byte("stale"), 1024), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	segment := MediaSegment{
		URL:       server.URL + "/media.ts",
		ByteRange: &ByteRange{Offset: 100, Length: 100},
	}
	status, err := downloadFile(context.Background(), segment, outputPath)
	if err != nil {
		t.Fatalf("downloadFile() error = %v", err)
	}
	if status != http.StatusPartialContent {
		t.Fatalf("downloadFile() status = %d, want 206", status)
	}

	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.Equal(got, resource[100:200]) {
		t.Fatalf("downloaded bytes do not match requested range")
	}
}

func TestDownloadFileRejectsServerIgnoringRangeWithoutWriting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 1024*1024))
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "segment.ts")
	if err := os.WriteFile(outputPath, bytes.Repeat([]byte("stale"), 1024), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	segment := MediaSegment{
		URL:       server.URL + "/media.ts",
		ByteRange: &ByteRange{Offset: 0, Length: 100},
	}
	_, err := downloadFile(context.Background(), segment, outputPath)
	if !errors.Is(err, ErrByteRangeNotHonored) {
		t.Fatalf("downloadFile() error = %v, want ErrByteRangeNotHonored", err)
	}
	if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
		t.Fatalf("output file exists after rejected full-resource response: %v", statErr)
	}
	if _, statErr := os.Stat(outputPath + ".part"); !os.IsNotExist(statErr) {
		t.Fatalf("partial file exists after rejected full-resource response: %v", statErr)
	}
}

func TestDownloadFileCapsOversizedRangeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-99/1000")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 1000))
	}))
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "segment.ts")
	segment := MediaSegment{
		URL:       server.URL + "/media.ts",
		ByteRange: &ByteRange{Offset: 0, Length: 100},
	}
	_, err := downloadFile(context.Background(), segment, outputPath)
	if !errors.Is(err, ErrIncompleteDownload) {
		t.Fatalf("downloadFile() error = %v, want ErrIncompleteDownload", err)
	}
	if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
		t.Fatalf("output file exists after oversized response: %v", statErr)
	}
	if _, statErr := os.Stat(outputPath + ".part"); !os.IsNotExist(statErr) {
		t.Fatalf("partial file exists after oversized response: %v", statErr)
	}
}
