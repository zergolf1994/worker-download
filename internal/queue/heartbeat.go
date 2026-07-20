package queue

import (
	"context"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"worker-download/internal/config"
	"worker-download/internal/core/enums"
	"worker-download/internal/db/models"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── Heartbeat ────────────────────────────────────────────────
//
// Upserts this worker into the workers collection every minute, keyed by
// workerId ("download_hostname@n"). The enqueuer (vdohide-service) counts
// capacity from workers with enable=true + fresh heartbeatAt, so a worker
// that stops beating stops receiving new jobs automatically.

const (
	heartbeatInterval = 1 * time.Minute
	// disk >= 90% → paused + enable=false: keeps beating (visible in admin)
	// but the enqueuer stops counting it as capacity
	diskPauseThreshold = 90.0
)

// StartHeartbeat sends heartbeats until ctx is cancelled, then marks the
// worker offline. Run in a goroutine.
func StartHeartbeat(ctx context.Context, workerID string) {
	log.Printf("💓 Starting heartbeat (every %s, workerId=%s)", heartbeatInterval, workerID)

	workerType, hostname := parseWorkerID(workerID)
	ip := getOutboundIP()
	pid := os.Getpid()

	beat := func() {
		hbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Slot truth lives in video_process, not this counter (see worker model
		// on the Node side) — but report it anyway for the admin dashboard.
		activeJobs, _ := models.VideoProcessModel.CountDocuments(hbCtx, bson.M{
			"workerId":    workerID,
			"status":      enums.ProcessStatusProcessing,
			"processType": workerType,
		})

		status := enums.WorkerStatusIdle
		if activeJobs > 0 {
			status = enums.WorkerStatusBusy
		}

		sys := gatherSystemInfo(config.AppConfig.StoragePath)

		// disk เต็ม → โชว์ paused ให้แอดมินเห็น (worker มี claim-gate ของตัวเอง
		// กันหยิบงานตอน disk ใกล้เต็มอยู่แล้ว) — ไม่แตะ enable
		if sys.DiskTotal > 0 {
			diskPct := float64(sys.DiskUsed) / float64(sys.DiskTotal) * 100
			if diskPct >= diskPauseThreshold {
				status = enums.WorkerStatusPaused
				log.Printf("⚠️ Heartbeat: disk usage %.1f%% >= %.0f%% — paused", diskPct, diskPauseThreshold)
			}
		}

		now := time.Now()
		update := bson.M{
			"$set": bson.M{
				"hostname":    hostname,
				"ip":          ip,
				"pid":         pid,
				"type":        workerType,
				"status":      status,
				"activeJobs":  activeJobs,
				"maxJobs":     1, // 1 worker = 1 job at a time
				"system":      sys,
				"heartbeatAt": now,
				"updatedAt":   now,
			},
			// enable = ฟิลด์ที่แอดมินคุมเอง — worker ตั้ง default true แค่ตอน
			// สร้าง doc ครั้งแรกเท่านั้น ไม่เขียนทับทุก heartbeat (บางเซิร์ฟเวอร์
			// อาจถูกปิดไว้ ต้องคงค่าที่ตั้งไว้)
			"$setOnInsert": bson.M{
				"_id":       uuid.New().String(),
				"enable":    true,
				"createdAt": now,
			},
		}
		opts := options.Update().SetUpsert(true)
		if _, err := models.WorkerModel.Col().UpdateOne(hbCtx, bson.M{"workerId": workerID}, update, opts); err != nil {
			log.Printf("⚠️ Heartbeat failed: %v", err)
		}
	}

	beat() // first beat immediately

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			markOffline(workerID)
			return
		case <-ticker.C:
			beat()
		}
	}
}

// markOffline flags the worker offline on graceful shutdown so the admin
// sees it immediately instead of waiting for the heartbeat TTL to expire.
func markOffline(workerID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ไม่แตะ enable (แอดมินคุมเอง) — status=offline + heartbeat หมดอายุ (3 นาที)
	// ก็พอให้ enqueuer เลิกนับ worker นี้ และ restart กลับมาไม่ค้าง disabled
	now := time.Now()
	_, err := models.WorkerModel.Col().UpdateOne(ctx,
		bson.M{"workerId": workerID},
		bson.M{"$set": bson.M{
			"status":    enums.WorkerStatusOffline,
			"updatedAt": now,
		}},
	)
	if err != nil {
		log.Printf("⚠️ Failed to mark worker offline: %v", err)
	} else {
		log.Printf("💤 Worker marked offline (workerId=%s)", workerID)
	}
}

// parseWorkerID splits "type_hostname@n" into worker type and hostname.
// Supports legacy "hostname@n" (type defaults to download).
func parseWorkerID(wID string) (workerType, hostname string) {
	prefix := strings.SplitN(wID, "@", 2)[0]

	workerType = enums.ProcessTypeDownload
	hostname = prefix
	if idx := strings.Index(prefix, "_"); idx >= 0 {
		workerType = prefix[:idx]
		hostname = prefix[idx+1:]
	}
	return
}

// getOutboundIP returns the preferred outbound IP of this machine.
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return "unknown"
		}
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
		return "unknown"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

// DiskUsage exposes disk stats to other packages (download's safety check).
func DiskUsage(path string) (total, used, free int64) {
	return getDiskUsage(path)
}

// gatherSystemInfo collects disk/memory/CPU metrics (per-OS implementations
// in sysinfo_*.go). Disk is measured at the storage path — that's the disk
// that fills up, not necessarily "/".
func gatherSystemInfo(storagePath string) *models.WorkerSystemInfo {
	info := &models.WorkerSystemInfo{}
	info.DiskTotal, info.DiskUsed, info.DiskFree = getDiskUsage(storagePath)
	info.MemTotal, info.MemUsed = getMemoryUsage()
	info.CPUPercent = getCPUPercent()
	return info
}
