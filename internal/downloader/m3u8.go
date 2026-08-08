package downloader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// StreamInfo represents a single stream variant from master playlist
type StreamInfo struct {
	URL        string
	Resolution string
	Width      int
	Height     int
	Bandwidth  int
}

// ByteRange identifies the byte window occupied by one media segment in a
// shared resource. HLS expresses this as #EXT-X-BYTERANGE:<length>[@<offset>].
type ByteRange struct {
	Length int64
	Offset int64
}

// MediaSegment is one entry in an HLS media playlist.
type MediaSegment struct {
	URL       string
	ByteRange *ByteRange
}

func (segment MediaSegment) String() string {
	return segment.URL
}

// ParseMasterPlaylist fetches and parses the M3U8 master playlist
func ParseMasterPlaylist(ctx context.Context, m3u8URL string) ([]StreamInfo, error) {
	var lastErr error

	for attempt := 1; attempt <= 5; attempt++ {
		resp, err := httpGet(ctx, m3u8URL)
		if err != nil {
			lastErr = fmt.Errorf("failed to fetch playlist: %w", err)
			if attempt < 5 {
				backoff := RetryDelay * time.Duration(1<<(attempt-1))
				log.Printf("⚠️  Playlist fetch failed (attempt %d/5): %v - retrying in %v", attempt, err, backoff)
				time.Sleep(backoff)
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			lastErr = fmt.Errorf("playlist returned status 429")
			if attempt < 5 {
				backoff := RateLimitBackoff * time.Duration(attempt)
				log.Printf("⏳ Playlist rate limited (attempt %d/5): retrying in %v", attempt, backoff)
				time.Sleep(backoff)
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("playlist returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read playlist: %w", err)
		}

		return parseM3U8Content(string(body), m3u8URL)
	}

	return nil, fmt.Errorf("failed to parse master playlist after 5 attempts: %w", lastErr)
}

func parseM3U8Content(content string, baseURL string) ([]StreamInfo, error) {
	streams := []StreamInfo{}

	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	baseDir := base.Scheme + "://" + base.Host + base.Path[:strings.LastIndex(base.Path, "/")+1]

	resRegex := regexp.MustCompile(`RESOLUTION=(\d+)x(\d+)`)
	bwRegex := regexp.MustCompile(`BANDWIDTH=(\d+)`)

	scanner := bufio.NewScanner(strings.NewReader(content))
	var currentStream *StreamInfo
	hasExtInf := false // ตรวจว่าเป็น segment playlist โดยตรง

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "#EXT-X-STREAM-INF:") {
			currentStream = &StreamInfo{}

			if matches := resRegex.FindStringSubmatch(line); len(matches) == 3 {
				currentStream.Width, _ = strconv.Atoi(matches[1])
				currentStream.Height, _ = strconv.Atoi(matches[2])
				currentStream.Resolution = fmt.Sprintf("%dx%d", currentStream.Width, currentStream.Height)
			}

			if matches := bwRegex.FindStringSubmatch(line); len(matches) == 2 {
				currentStream.Bandwidth, _ = strconv.Atoi(matches[1])
			}

		} else if strings.HasPrefix(line, "#EXTINF:") {
			hasExtInf = true

		} else if currentStream != nil && !strings.HasPrefix(line, "#") && line != "" {
			if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
				currentStream.URL = line
			} else if strings.HasPrefix(line, "//") {
				// protocol-relative URL → inherit scheme from base
				currentStream.URL = base.Scheme + ":" + line
			} else {
				currentStream.URL = baseDir + line
			}
			streams = append(streams, *currentStream)
			currentStream = nil
		}
	}

	if len(streams) == 0 {
		// ถ้าไม่มี stream variants แต่มี #EXTINF → URL นี้คือ segment playlist โดยตรง
		if hasExtInf {
			log.Printf("ℹ️  No master streams found — treating URL as segment playlist directly: %s", baseURL)
			return []StreamInfo{{URL: baseURL, Resolution: "unknown"}}, nil
		}
		return nil, fmt.Errorf("no streams found in playlist")
	}

	sort.Slice(streams, func(i, j int) bool {
		pixelsI := streams[i].Width * streams[i].Height
		pixelsJ := streams[j].Width * streams[j].Height
		return pixelsI > pixelsJ
	})

	return streams, nil
}

// SelectHighestResolution returns the stream with highest resolution
func SelectHighestResolution(streams []StreamInfo) StreamInfo {
	if len(streams) == 0 {
		return StreamInfo{}
	}
	return streams[0]
}

// ParseSegmentPlaylist fetches and parses a segment playlist
func ParseSegmentPlaylist(ctx context.Context, playlistURL string) ([]MediaSegment, error) {
	segments, _, err := ParseSegmentPlaylistWithContent(ctx, playlistURL)
	return segments, err
}

// ParseSegmentPlaylistWithContent fetches and parses a segment playlist, returning content too
func ParseSegmentPlaylistWithContent(ctx context.Context, playlistURL string) ([]MediaSegment, string, error) {
	var lastErr error

	for attempt := 1; attempt <= 5; attempt++ {
		resp, err := httpGet(ctx, playlistURL)
		if err != nil {
			lastErr = fmt.Errorf("failed to fetch segment playlist: %w", err)
			if attempt < 5 {
				backoff := RetryDelay * time.Duration(1<<(attempt-1))
				log.Printf("⚠️  Segment playlist fetch failed (attempt %d/5): %v - retrying in %v", attempt, err, backoff)
				time.Sleep(backoff)
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			lastErr = fmt.Errorf("segment playlist returned status 429")
			if attempt < 5 {
				backoff := RateLimitBackoff * time.Duration(attempt)
				log.Printf("⏳ Segment playlist rate limited (attempt %d/5): retrying in %v", attempt, backoff)
				time.Sleep(backoff)
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, "", fmt.Errorf("segment playlist returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, "", fmt.Errorf("failed to read segment playlist: %w", err)
		}

		content := string(body)
		segments, err := parseSegmentContent(content, playlistURL)
		return segments, content, err
	}

	return nil, "", fmt.Errorf("failed to fetch segment playlist after 5 attempts: %w", lastErr)
}

func parseSegmentContent(content string, baseURL string) ([]MediaSegment, error) {
	segments := []MediaSegment{}

	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	baseDir := base.Scheme + "://" + base.Host + base.Path[:strings.LastIndex(base.Path, "/")+1]

	scanner := bufio.NewScanner(strings.NewReader(content))
	var pendingRange *ByteRange
	var previousRangeEnd int64
	var previousRangeURL string
	hasPreviousRange := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-BYTERANGE:") {
			if pendingRange != nil {
				return nil, fmt.Errorf("byte range tag is not followed by a media URI")
			}

			value := strings.TrimSpace(strings.TrimPrefix(line, "#EXT-X-BYTERANGE:"))
			lengthText, offsetText, hasOffset := strings.Cut(value, "@")
			length, err := strconv.ParseInt(strings.TrimSpace(lengthText), 10, 64)
			if err != nil || length <= 0 || length == math.MaxInt64 {
				return nil, fmt.Errorf("invalid byte range length %q", lengthText)
			}

			offset := int64(-1)
			if hasOffset {
				offset, err = strconv.ParseInt(strings.TrimSpace(offsetText), 10, 64)
				if err != nil || offset < 0 {
					return nil, fmt.Errorf("invalid byte range offset %q", offsetText)
				}
			}
			if offset >= 0 && offset > math.MaxInt64-(length-1) {
				return nil, fmt.Errorf("byte range %q overflows int64", value)
			}

			pendingRange = &ByteRange{Length: length, Offset: offset}
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}

		var segmentURL string
		if strings.HasPrefix(line, "http://") || strings.HasPrefix(line, "https://") {
			segmentURL = line
		} else if strings.HasPrefix(line, "//") {
			// protocol-relative URL → inherit scheme from base
			segmentURL = base.Scheme + ":" + line
		} else {
			segmentURL = baseDir + line
			if base.RawQuery != "" && !strings.Contains(line, "?") {
				segmentURL += "?" + base.RawQuery
			}
		}
		segment := MediaSegment{URL: segmentURL}
		if pendingRange != nil {
			byteRange := *pendingRange
			if byteRange.Offset < 0 {
				if !hasPreviousRange || previousRangeURL != segmentURL {
					return nil, fmt.Errorf("byte range for %s omits offset without a previous range on the same resource", segmentURL)
				}
				byteRange.Offset = previousRangeEnd
			}
			if byteRange.Offset > math.MaxInt64-(byteRange.Length-1) {
				return nil, fmt.Errorf("byte range for %s overflows int64", segmentURL)
			}

			segment.ByteRange = &byteRange
			previousRangeEnd = byteRange.Offset + byteRange.Length
			previousRangeURL = segmentURL
			hasPreviousRange = true
			pendingRange = nil
		} else {
			hasPreviousRange = false
			previousRangeURL = ""
		}
		segments = append(segments, segment)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan segment playlist: %w", err)
	}
	if pendingRange != nil {
		return nil, fmt.Errorf("byte range tag is not followed by a media URI")
	}

	if len(segments) == 0 {
		return nil, fmt.Errorf("no segments found in playlist")
	}

	if len(segments) > 0 {
		log.Printf("🔗 First segment URL: %s", segments[0])
	}

	return segments, nil
}
