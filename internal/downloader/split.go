package downloader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	SplitAssetVideo    = "video"
	SplitAssetAudio    = "audio"
	SplitAssetSubtitle = "subtitle"
)

// SplitAsset is one independently stored playback asset produced from a
// source container. Bitmap subtitles are intentionally excluded.
type SplitAsset struct {
	Kind        string
	Path        string
	FileName    string
	MimeType    string
	SourceIndex int
	SourceCodec string
	Codec       string
	Language    string
	Title       string
	Default     bool
	Forced      bool
	Channels    int
	SampleRate  int
	Bitrate     int64
	Duration    float64
	Size        int64
	Width       int
	Height      int
}

type probedMedia struct {
	Duration float64        `json:"-"`
	Streams  []probedStream `json:"streams"`
	Format   struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

type probedStream struct {
	Index      int    `json:"index"`
	CodecType  string `json:"codec_type"`
	CodecName  string `json:"codec_name"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	Channels   int    `json:"channels"`
	SampleRate string `json:"sample_rate"`
	BitRate    string `json:"bit_rate"`
	Tags       struct {
		Language string `json:"language"`
		Title    string `json:"title"`
	} `json:"tags"`
	Disposition struct {
		Default     int `json:"default"`
		Forced      int `json:"forced"`
		AttachedPic int `json:"attached_pic"`
	} `json:"disposition"`
}

var webVTTSubtitleCodecs = map[string]bool{
	"subrip":             true,
	"ass":                true,
	"ssa":                true,
	"webvtt":             true,
	"mov_text":           true,
	"text":               true,
	"ttml":               true,
	"hdmv_text_subtitle": true,
}

func IsWebVTTSubtitleCodec(codec string) bool {
	return webVTTSubtitleCodecs[strings.ToLower(strings.TrimSpace(codec))]
}

// SplitMedia creates a H264 video-only original, one AAC stereo M4A per audio
// stream, and one WebVTT file per supported text subtitle. It deliberately
// skips bitmap subtitles such as PGS/DVD/DVB/XSub.
func SplitMedia(ctx context.Context, inputPath, outputDir string, onProgress func(percent int)) ([]SplitAsset, error) {
	info, err := probeAllStreams(inputPath)
	if err != nil {
		return nil, err
	}

	var video *probedStream
	var audios []probedStream
	var subtitles []probedStream
	for i := range info.Streams {
		stream := info.Streams[i]
		switch stream.CodecType {
		case "video":
			if video == nil && stream.Disposition.AttachedPic == 0 {
				copy := stream
				video = &copy
			}
		case "audio":
			audios = append(audios, stream)
		case "subtitle":
			if IsWebVTTSubtitleCodec(stream.CodecName) {
				subtitles = append(subtitles, stream)
			} else {
				log.Printf("⏭️  Skipping unsupported/image subtitle stream %d (codec=%s)", stream.Index, stream.CodecName)
			}
		}
	}
	if video == nil {
		return nil, fmt.Errorf("no video stream found")
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create split output: %w", err)
	}

	totalSteps := 1 + len(audios) + len(subtitles)
	stepProgress := func(step int) func(int) {
		return func(percent int) {
			if onProgress == nil {
				return
			}
			overall := ((step * 100) + percent) / totalSteps
			onProgress(overall)
		}
	}

	videoPath := filepath.Join(outputDir, "file_original.mp4")
	if err := ensureH264VideoOnly(ctx, inputPath, videoPath, info.Duration, stepProgress(0)); err != nil {
		return nil, fmt.Errorf("extract video: %w", err)
	}
	videoStat, err := os.Stat(videoPath)
	if err != nil {
		return nil, fmt.Errorf("stat video output: %w", err)
	}
	assets := []SplitAsset{{
		Kind: SplitAssetVideo, Path: videoPath, FileName: "file_original.mp4",
		MimeType: "video/mp4", SourceIndex: video.Index, SourceCodec: video.CodecName,
		Codec: "h264", Duration: info.Duration, Size: videoStat.Size(), Width: video.Width, Height: video.Height,
	}}

	hasDefaultAudio := false
	for _, stream := range audios {
		if stream.Disposition.Default != 0 {
			hasDefaultAudio = true
			break
		}
	}
	step := 1
	for index, stream := range audios {
		fileName := fmt.Sprintf("audio_%d.m4a", index+1)
		outputPath := filepath.Join(outputDir, fileName)
		args := []string{"-y", "-i", inputPath, "-map", fmt.Sprintf("0:%d", stream.Index), "-map_metadata", "-1", "-vn", "-sn", "-dn"}
		args = append(args, aacStereoArgs("192k")...)
		args = append(args, "-movflags", "+faststart", outputPath)
		if err := runFFmpegArgs(ctx, args, info.Duration, stepProgress(step)); err != nil {
			return nil, fmt.Errorf("extract audio stream %d: %w", stream.Index, err)
		}
		stat, err := os.Stat(outputPath)
		if err != nil {
			return nil, fmt.Errorf("stat audio output: %w", err)
		}
		assets = append(assets, SplitAsset{
			Kind: SplitAssetAudio, Path: outputPath, FileName: fileName, MimeType: "audio/mp4",
			SourceIndex: stream.Index, SourceCodec: stream.CodecName, Codec: "aac",
			Language: normalizedLanguage(stream.Tags.Language), Title: stream.Tags.Title,
			Default: stream.Disposition.Default != 0 || (!hasDefaultAudio && index == 0), Forced: stream.Disposition.Forced != 0,
			Channels: 2, SampleRate: 48000, Bitrate: 192000, Duration: info.Duration, Size: stat.Size(),
		})
		step++
	}

	for index, stream := range subtitles {
		fileName := fmt.Sprintf("subtitle_%d.vtt", index+1)
		outputPath := filepath.Join(outputDir, fileName)
		codecArgs := []string{"-c:s", "webvtt"}
		if stream.CodecName == "webvtt" {
			codecArgs = []string{"-c:s", "copy"}
		}
		args := []string{"-y", "-i", inputPath, "-map", fmt.Sprintf("0:%d", stream.Index), "-map_metadata", "-1", "-vn", "-an", "-dn"}
		args = append(args, codecArgs...)
		args = append(args, outputPath)
		if err := runFFmpegArgs(ctx, args, info.Duration, stepProgress(step)); err != nil {
			return nil, fmt.Errorf("extract subtitle stream %d: %w", stream.Index, err)
		}
		stat, err := os.Stat(outputPath)
		if err != nil {
			return nil, fmt.Errorf("stat subtitle output: %w", err)
		}
		assets = append(assets, SplitAsset{
			Kind: SplitAssetSubtitle, Path: outputPath, FileName: fileName, MimeType: "text/vtt",
			SourceIndex: stream.Index, SourceCodec: stream.CodecName, Codec: "webvtt",
			Language: normalizedLanguage(stream.Tags.Language), Title: stream.Tags.Title,
			Default: stream.Disposition.Default != 0, Forced: stream.Disposition.Forced != 0,
			Duration: info.Duration, Size: stat.Size(),
		})
		step++
	}

	if onProgress != nil {
		onProgress(100)
	}
	return assets, nil
}

func probeAllStreams(inputPath string) (*probedMedia, error) {
	cmd := exec.Command("ffprobe", "-v", "error", "-show_entries",
		"stream=index,codec_type,codec_name,width,height,channels,sample_rate,bit_rate:stream_tags=language,title:stream_disposition=default,forced,attached_pic:format=duration",
		"-of", "json", inputPath)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe streams: %w", err)
	}
	var raw probedMedia
	if err := json.Unmarshal(output, &raw); err != nil {
		return nil, fmt.Errorf("decode ffprobe streams: %w", err)
	}
	duration, err := strconv.ParseFloat(raw.Format.Duration, 64)
	if err != nil || duration <= 0 {
		return nil, fmt.Errorf("invalid media duration %q", raw.Format.Duration)
	}
	raw.Duration = duration
	return &raw, nil
}

func ensureH264VideoOnly(ctx context.Context, inputPath, outputPath string, duration float64, onProgress func(int)) error {
	codecInfo, err := DetectCodecs(inputPath)
	if err == nil && strings.EqualFold(codecInfo.VideoCodec, "h264") {
		args := []string{"-y", "-i", inputPath, "-map", "0:v:0", "-c:v", "copy", "-an", "-sn", "-dn", "-movflags", "+faststart", outputPath}
		return runFFmpegArgs(ctx, args, duration, onProgress)
	}
	return runFFmpegWithProgress(h264Cmd(ctx, inputPath, outputPath, true, detectH264Encoder()), duration, onProgress)
}

func runFFmpegArgs(ctx context.Context, args []string, duration float64, onProgress func(int)) error {
	progressArgs := []string{"-hide_banner", "-loglevel", "error", "-nostats", "-stats_period", "0.5", "-progress", "pipe:1"}
	progressArgs = append(progressArgs, args...)
	return runFFmpegWithProgress(exec.CommandContext(ctx, "ffmpeg", progressArgs...), duration, onProgress)
}

func normalizedLanguage(language string) string {
	language = strings.ToLower(strings.TrimSpace(language))
	if language == "" {
		return "und"
	}
	return language
}
