package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"worker-download/internal/config"
	"worker-download/internal/core/utils"
	"worker-download/internal/dashboard"
	"worker-download/internal/db/database"
	"worker-download/internal/download"
	"worker-download/internal/queue"
)

// version ถูกฝังตอน build โดย GitHub Actions: -ldflags="-X main.version=v1.x.x"
var version = "dev"

func main() {
	config.Load()
	workerID := utils.GenerateWorkerID()
	log.Printf("🚀 Starting Worker Download %s [Worker: %s]", version, workerID)

	// log ทั่วไปออก stdout ให้ systemd/journald เก็บและหมุนให้
	// (journalctl -u worker-download -f) — ของเดิมเรียก logger.Init ซึ่ง
	// log.SetOutput ทับ stdout ทำให้ journal ว่างเปล่า และไฟล์ที่หมุนไว้
	// ก็ไม่มีใครลบ ส่วน log รายงาน (logs/process/{slug}.log) ยังเขียนอยู่
	// เพราะต้องอัพขึ้น S3 ตอนงานจบ

	// ── MongoDB ───────────────────────────────────────────────
	if err := database.Connect(); err != nil {
		log.Printf("❌ Failed to connect to MongoDB: %v", err)
		time.Sleep(5 * time.Second) // ให้ log ถูก flush / กัน restart-loop รัวๆ
		os.Exit(1)
	}
	defer database.Disconnect()

	// ── Heartbeat ─────────────────────────────────────────────
	// ctx ยกเลิกเมื่อโดน SIGINT/SIGTERM → heartbeat mark ตัวเอง offline ก่อนจบ
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		queue.StartHeartbeat(ctx, workerID)
	}()

	if dashboard.ShouldStart(workerID) {
		go dashboard.Start(ctx, config.AppConfig.DashboardPort, workerID, config.AppConfig.StoragePath)
	} else {
		log.Printf("📺 Download monitor owned by worker @1 (this worker: %s)", workerID)
	}

	// ── Job loop (blocking จนโดน SIGINT/SIGTERM) ──────────────
	// shutdown ระหว่างทำงาน → loop จะ Release งานคืนคิวให้เอง
	queue.RunLoop(ctx, workerID, download.Run)

	log.Println("🛑 Shutting down...")

	// รอ heartbeat ปิดตัว (mark offline) ให้เสร็จก่อน disconnect DB
	select {
	case <-hbDone:
	case <-time.After(10 * time.Second):
		log.Println("⚠️ Heartbeat shutdown timed out")
	}
}
