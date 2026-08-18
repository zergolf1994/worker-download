//go:build !windows

package dashboard

import (
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var partitionDevice = regexp.MustCompile(`^(?:sd|hd|vd|xvd)[a-z]+\d+$`)
var nvmePartition = regexp.MustCompile(`^(?:nvme\d+n\d+|mmcblk\d+)p\d+$`)

type ioCounters struct{ read, write uint64 }
type cpuCounters struct{ idle, total uint64 }
type platformSampler struct {
	storagePath string
	lastCPU     cpuCounters
	lastIO      ioCounters
	lastAt      time.Time
}

func newPlatformSampler(storagePath string) platformSampler {
	if storagePath == "" {
		storagePath = "."
	}
	return platformSampler{storagePath: storagePath, lastCPU: readCPU(), lastIO: readDiskIO(), lastAt: time.Now()}
}

func (s *platformSampler) sample() (CPUInfo, MemoryInfo, DiskInfo) {
	now := time.Now()
	cpuNow := readCPU()
	ioNow := readDiskIO()
	elapsed := now.Sub(s.lastAt).Seconds()
	cpu := CPUInfo{Cores: runtime.NumCPU()}
	if delta := cpuNow.total - s.lastCPU.total; delta > 0 {
		cpu.Percent = clamp(100 * (1 - float64(cpuNow.idle-s.lastCPU.idle)/float64(delta)))
	}
	memory := readMemory()
	total, used, _ := diskUsage(s.storagePath)
	disk := DiskInfo{Total: total, Used: used}
	if total > 0 {
		disk.Percent = float64(used) / float64(total) * 100
	}
	if elapsed > 0 {
		if ioNow.read >= s.lastIO.read {
			disk.ReadBytes = float64(ioNow.read-s.lastIO.read) / elapsed
		}
		if ioNow.write >= s.lastIO.write {
			disk.WriteBytes = float64(ioNow.write-s.lastIO.write) / elapsed
		}
	}
	s.lastCPU, s.lastIO, s.lastAt = cpuNow, ioNow, now
	return cpu, memory, disk
}

func readCPU() cpuCounters {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuCounters{}
	}
	fields := strings.Fields(strings.SplitN(string(data), "\n", 2)[0])
	values := make([]uint64, 0, len(fields)-1)
	for _, field := range fields[1:] {
		value, _ := strconv.ParseUint(field, 10, 64)
		values = append(values, value)
	}
	var total uint64
	for _, value := range values {
		total += value
	}
	var idle uint64
	if len(values) > 3 {
		idle = values[3]
	}
	if len(values) > 4 {
		idle += values[4]
	}
	return cpuCounters{idle: idle, total: total}
}

func readMemory() MemoryInfo {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return MemoryInfo{}
	}
	var total, available int64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "MemTotal:":
			total = value * 1024
		case "MemAvailable:":
			available = value * 1024
		}
	}
	used := total - available
	info := MemoryInfo{Total: total, Used: used}
	if total > 0 {
		info.Percent = float64(used) / float64(total) * 100
	}
	return info
}

func readDiskIO() ioCounters {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return ioCounters{}
	}
	type row struct {
		name        string
		read, write uint64
	}
	var all, virtual []row
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") || strings.HasPrefix(name, "sr") || partitionDevice.MatchString(name) || nvmePartition.MatchString(name) {
			continue
		}
		reads, e1 := strconv.ParseUint(fields[5], 10, 64)
		writes, e2 := strconv.ParseUint(fields[9], 10, 64)
		if e1 != nil || e2 != nil {
			continue
		}
		r := row{name: name, read: reads * 512, write: writes * 512}
		all = append(all, r)
		if strings.HasPrefix(name, "md") || strings.HasPrefix(name, "dm-") {
			virtual = append(virtual, r)
		}
	}
	selected := all
	if len(virtual) > 0 {
		selected = virtual
	}
	var result ioCounters
	for _, row := range selected {
		result.read += row.read
		result.write += row.write
	}
	return result
}

func diskUsage(path string) (total, used, free int64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		if err := syscall.Statfs("/", &stat); err != nil {
			return 0, 0, 0
		}
	}
	total = int64(stat.Blocks) * int64(stat.Bsize)
	free = int64(stat.Bavail) * int64(stat.Bsize)
	used = total - free
	return
}
