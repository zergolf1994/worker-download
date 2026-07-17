package download

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"worker-download/internal/db/models"
	"worker-download/internal/queue"
	"worker-download/internal/uploader"
)

// ─── Process log → S3 ────────────────────────────────────────
// จบงานแต่ละไฟล์ → อัพ logs/process/<slug>.log ขึ้น S3 temp ที่
// logs/download/<slug>.log แล้วลบไฟล์ออกจากเครื่อง (ไม่บันทึกอะไรลง DB)
//
// อัพเฉพาะตอนงาน "ถึงจุดจบ" (สำเร็จ / ยกเลิก / fail ครั้งสุดท้าย) —
// ระหว่างรอ retry เก็บไฟล์ไว้ให้ attempt ถัดไป append ต่อ log จะได้ครบทุกรอบ
// อัพไม่สำเร็จ = เก็บไฟล์ไว้ (CleanOldLogs กวาดของเก่า 7 วันเป็น backstop)

func finalizeProcessLog(jobCtx context.Context, process *models.VideoProcess, runErr error) {
	slug := derefStr(process.Slug)
	if slug == "" {
		return
	}
	logPath := filepath.Join("logs", "process", fmt.Sprintf("%s.log", slug))
	if _, err := os.Stat(logPath); err != nil {
		return // ไม่มีไฟล์ log — ไม่มีอะไรต้องทำ
	}

	// admin cancel: jobCtx ตายก็จริง แต่ถือเป็น "จบงาน" — ต้องอัพ log
	cancelled := errors.Is(runErr, queue.ErrJobCancelled) ||
		errors.Is(context.Cause(jobCtx), queue.ErrJobCancelled)

	if !cancelled {
		// shutdown กลางงาน → loop จะ Release คืนคิว = ยังไม่จบ
		if jobCtx.Err() != nil || !isTerminal(process, runErr) {
			return // งานยังไม่จบจริง (รอ retry / requeue) — เก็บ log ไว้ต่อ
		}
	}

	// ใช้ ctx สดเสมอ — jobCtx อาจตายแล้ว (เคส cancel) แต่การอัพ log ต้องสำเร็จ
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s3Storage, err := resolveS3TempStorage(ctx)
	if err != nil {
		log.Printf("⚠️  [%s] Log upload skipped (no S3 temp): %v", slug, err)
		return
	}

	key := fmt.Sprintf("logs/download/%s.log", slug)
	if err := uploader.UploadToS3(ctx, s3Storage, logPath, key, nil); err != nil {
		log.Printf("⚠️  [%s] Log upload failed (keeping local): %v", slug, err)
		return
	}

	if err := os.Remove(logPath); err != nil {
		log.Printf("⚠️  [%s] Log uploaded but local remove failed: %v", slug, err)
		return
	}
	log.Printf("🗂️  [%s] Process log → %s (local removed)", slug, key)
}

// isTerminal — งานนี้จะไม่ถูกรันซ้ำอีกแล้วใช่ไหม (mirror logic ของ queue loop)
func isTerminal(process *models.VideoProcess, runErr error) bool {
	if runErr == nil || errors.Is(runErr, queue.ErrJobCancelled) {
		return true
	}
	if errors.Is(runErr, queue.ErrJobRequeue) || errors.Is(runErr, context.Canceled) {
		return false // คืนคิว — จะมีรอบต่อไป
	}
	// error ปกติ: terminal เมื่อ attempt นี้เป็นครั้งสุดท้าย (ตรงกับ RetryOrFail)
	attempt := 1
	if process.RetryCount != nil {
		attempt = *process.RetryCount + 1
	}
	return attempt >= queue.MaxRetries
}
