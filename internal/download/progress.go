package download

import (
	"context"
	"fmt"
	"log"
	"time"

	"worker-download/internal/core/enums"
	"worker-download/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
)

// ─── Progress → log (ไม่แตะ DB) ──────────────────────────────
// % ระหว่างทำงานออก log อย่างเดียว (เข้า per-process log ของ slug นั้น)
// throttle ทุก 10% — ไฟล์ใหญ่ๆ จะได้ไม่ spam log เป็นแสนบรรทัด

// pctLogger64 — callback แบบ bytes (download/upload)
func pctLogger64(slug, step string) func(done, total int64) {
	last := -10.0
	return func(done, total int64) {
		if total <= 0 {
			return
		}
		pct := float64(done) / float64(total) * 100
		if pct-last >= 10 || (pct >= 100 && last < 100) {
			last = pct
			log.Printf("⏳ [%s] %s %.0f%% (%.1f/%.1f MB)", slug, step, pct, float64(done)/1048576, float64(total)/1048576)
		}
	}
}

// pctLoggerInt — callback แบบ % ตรงๆ (ffmpeg merge/encode)
func pctLoggerInt(slug, step string) func(pct int) {
	last := -10
	return func(pct int) {
		if pct-last >= 10 || (pct >= 100 && last < 100) {
			last = pct
			log.Printf("⏳ [%s] %s %d%%", slug, step, pct)
		}
	}
}

// segLogger — callback แบบ segment (HLS)
func segLogger(slug string) func(current, total int) {
	last := -10.0
	return func(current, total int) {
		if total <= 0 {
			return
		}
		pct := float64(current) / float64(total) * 100
		if pct-last >= 10 || current == total {
			last = pct
			log.Printf("⏳ [%s] download %d/%d segments (%.0f%%)", slug, current, total, pct)
		}
	}
}

// ─── Step boundary updates ───────────────────────────────────
// เขียน DB เฉพาะตอนเริ่ม/จบแต่ละ step เท่านั้น — ไม่เขียน % เรียลไทม์
// (ตัด write รัวๆ ระหว่าง download/merge/upload ออกทั้งหมด)
// overallPercent จึงเดินเป็นขั้น: 0 → 33 (download) → 66 (merge) → 100 (upload)

func startStep(ctx context.Context, processID, step string) {
	now := time.Now()
	models.VideoProcessModel.UpdateByID(ctx, processID, bson.M{"$set": bson.M{
		fmt.Sprintf("timeline.%s.status", step):    enums.StepStatusProcessing,
		fmt.Sprintf("timeline.%s.percent", step):   0,
		fmt.Sprintf("timeline.%s.startedAt", step): now,
		"updatedAt": now,
	}})
}

func completeStep(ctx context.Context, processID, step string) {
	now := time.Now()
	var overall float64
	switch step {
	case "download":
		overall = 33
	case "merge":
		overall = 66
	case "upload":
		overall = 100
	}
	models.VideoProcessModel.UpdateByID(ctx, processID, bson.M{"$set": bson.M{
		fmt.Sprintf("timeline.%s.status", step):  enums.StepStatusCompleted,
		fmt.Sprintf("timeline.%s.percent", step): 100,
		fmt.Sprintf("timeline.%s.endedAt", step): now,
		"overallPercent":                         overall, "updatedAt": now,
	}})
}
