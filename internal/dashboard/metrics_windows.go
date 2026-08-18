//go:build windows

package dashboard

import "runtime"

type platformSampler struct{}

func newPlatformSampler(_ string) platformSampler { return platformSampler{} }
func (s *platformSampler) sample() (CPUInfo, MemoryInfo, DiskInfo) {
	return CPUInfo{Cores: runtime.NumCPU()}, MemoryInfo{}, DiskInfo{}
}
