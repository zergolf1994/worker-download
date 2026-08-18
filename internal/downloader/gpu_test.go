package downloader

import "testing"

func TestVideoEncoderArgs(t *testing.T) {
	nvenc := videoEncoderArgs(encoderNVENC)
	if !containsPair(nvenc, "-c:v", "h264_nvenc") || !containsPair(nvenc, "-cq", "23") {
		t.Fatalf("unexpected NVENC args: %v", nvenc)
	}
	cpu := videoEncoderArgs(encoderCPU)
	if !containsPair(cpu, "-c:v", "libx264") || !containsPair(cpu, "-crf", "23") {
		t.Fatalf("unexpected CPU args: %v", cpu)
	}
}

func containsPair(values []string, key, value string) bool {
	for i := 0; i+1 < len(values); i++ {
		if values[i] == key && values[i+1] == value {
			return true
		}
	}
	return false
}
