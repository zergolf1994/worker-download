package downloader

import "testing"

func TestParseSegmentContentByteRanges(t *testing.T) {
	playlist := `#EXTM3U
#EXT-X-VERSION:4
#EXTINF:6.0,
#EXT-X-BYTERANGE:654616@0
media.ts
#EXTINF:6.0,
#EXT-X-BYTERANGE:784336
media.ts
#EXTINF:6.0,
#EXT-X-BYTERANGE:681876@1438952
media.ts
`

	segments, err := parseSegmentContent(playlist, "https://cdn.example/video/stream.m3u8?token=abc")
	if err != nil {
		t.Fatalf("parseSegmentContent() error = %v", err)
	}
	if len(segments) != 3 {
		t.Fatalf("len(segments) = %d, want 3", len(segments))
	}

	wantOffsets := []int64{0, 654616, 1438952}
	wantLengths := []int64{654616, 784336, 681876}
	for i, segment := range segments {
		if segment.URL != "https://cdn.example/video/media.ts?token=abc" {
			t.Errorf("segments[%d].URL = %q", i, segment.URL)
		}
		if segment.ByteRange == nil {
			t.Fatalf("segments[%d].ByteRange is nil", i)
		}
		if segment.ByteRange.Offset != wantOffsets[i] || segment.ByteRange.Length != wantLengths[i] {
			t.Errorf("segments[%d].ByteRange = %+v, want offset=%d length=%d", i, segment.ByteRange, wantOffsets[i], wantLengths[i])
		}
	}
}

func TestParseSegmentContentRejectsImplicitOffsetForDifferentResource(t *testing.T) {
	playlist := `#EXTM3U
#EXT-X-BYTERANGE:100@0
first.ts
#EXT-X-BYTERANGE:100
second.ts
`

	if _, err := parseSegmentContent(playlist, "https://cdn.example/stream.m3u8"); err == nil {
		t.Fatal("parseSegmentContent() error = nil, want implicit-offset error")
	}
}
