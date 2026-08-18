package download

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"worker-download/internal/core/enums"
	"worker-download/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
)

// Progress is persisted every whole percent for the realtime dashboard, while
// process logs stay at 10% milestones so large files do not flood journald.

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

func trackedBytes(ctx context.Context, processID, slug, step string) func(done, total int64) {
	logger := pctLogger64(slug, step)
	var mu sync.Mutex
	lastPersisted := -1
	return func(done, total int64) {
		if total <= 0 {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		logger(done, total)
		persistPercent(ctx, processID, step, percent(done, total), &lastPersisted)
	}
}

func trackedSegments(ctx context.Context, processID, slug string) func(current, total int) {
	logger := segLogger(slug)
	var mu sync.Mutex
	lastPersisted := -1
	return func(current, total int) {
		if total <= 0 {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		logger(current, total)
		persistPercent(ctx, processID, "download", percent(int64(current), int64(total)), &lastPersisted)
	}
}

func trackedPercent(ctx context.Context, processID, slug, step string) func(value int) {
	logger := pctLoggerInt(slug, step)
	var mu sync.Mutex
	lastPersisted := -1
	return func(value int) {
		mu.Lock()
		defer mu.Unlock()
		logger(value)
		persistPercent(ctx, processID, step, float64(value), &lastPersisted)
	}
}

func percent(done, total int64) float64 {
	if total <= 0 {
		return 0
	}
	value := float64(done) / float64(total) * 100
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func persistPercent(ctx context.Context, processID, step string, value float64, lastPersisted *int) {
	whole := int(math.Floor(value))
	if value >= 100 {
		whole = 100
	}
	if whole <= *lastPersisted {
		return
	}
	*lastPersisted = whole
	overall := stepOverall(step, float64(whole))
	models.VideoProcessModel.UpdateByID(ctx, processID, bson.M{"$set": bson.M{
		fmt.Sprintf("timeline.%s.percent", step): float64(whole),
		"overallPercent":                         overall,
		"updatedAt":                              time.Now(),
	}})
}

func stepOverall(step string, value float64) float64 {
	switch step {
	case "download":
		return value * 0.33
	case "merge":
		return 33 + value*0.33
	case "upload":
		return 66 + value*0.34
	default:
		return value
	}
}

// ─── Step boundary updates ───────────────────────────────────
// Step boundaries remain explicit so retries always reset the active step.

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
