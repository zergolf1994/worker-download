package downloader

import (
	"log"
	"os/exec"
	"strings"
	"sync"
)

const (
	encoderCPU   = "libx264"
	encoderNVENC = "h264_nvenc"
)

var encoderState struct {
	sync.Mutex
	detected bool
	encoder  string
}

// detectH264Encoder verifies that NVENC can actually encode a frame. Merely
// finding h264_nvenc in `ffmpeg -encoders` is insufficient when the NVIDIA
// runtime library or driver is missing.
func detectH264Encoder() string {
	encoderState.Lock()
	defer encoderState.Unlock()
	if encoderState.detected {
		return encoderState.encoder
	}
	encoderState.detected = true
	encoderState.encoder = encoderCPU
	args := []string{
		"-hide_banner", "-loglevel", "error", "-f", "lavfi",
		"-i", "color=c=black:s=256x256:d=1:r=25", "-pix_fmt", "yuv420p",
		"-c:v", encoderNVENC, "-frames:v", "1", "-f", "null", "-",
	}
	if output, err := exec.Command("ffmpeg", args...).CombinedOutput(); err == nil {
		encoderState.encoder = encoderNVENC
		log.Printf("🎮 GPU detected: NVIDIA NVENC enabled for video re-encode")
	} else {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = err.Error()
		}
		log.Printf("💻 NVENC unavailable — using CPU (libx264): %s", message)
	}
	return encoderState.encoder
}

func disableNVENC() {
	encoderState.Lock()
	encoderState.detected = true
	encoderState.encoder = encoderCPU
	encoderState.Unlock()
}

func videoEncoderArgs(encoder string) []string {
	if encoder == encoderNVENC {
		return []string{
			"-c:v", encoderNVENC,
			"-preset", "p5",
			"-tune", "hq",
			"-rc", "vbr",
			"-cq", "23",
			"-b:v", "0",
			"-profile:v", "high",
			"-spatial-aq", "1",
		}
	}
	return []string{
		"-c:v", encoderCPU,
		"-preset", "fast",
		"-profile:v", "high",
		"-level", "4.0",
		"-crf", "23",
	}
}
