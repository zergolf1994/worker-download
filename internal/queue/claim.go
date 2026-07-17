package queue

import (
	"context"
	"errors"
	"time"

	"worker-download/internal/core/enums"
	"worker-download/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── Claim ────────────────────────────────────────────────────
//
// The enqueuer (vdohide-service) inserts pending jobs into video_process.
// Workers never scan the files collection — they only claim from here.
//
// Claim is atomic: FindOneAndUpdate flips pending → processing and stamps
// workerId + claimedAt in one operation, so two workers can never grab the
// same job. Sort must match the index {processType, status, priority: -1,
// createdAt: 1} — highest priority first, then oldest.

// Claim atomically claims the next pending job for this worker.
// Returns (nil, nil) when the queue is empty.
func Claim(ctx context.Context, workerID string) (*models.VideoProcess, error) {
	now := time.Now()
	job, err := models.VideoProcessModel.FindOneAndUpdate(ctx,
		bson.M{
			"processType": enums.ProcessTypeDownload,
			"status":      enums.ProcessStatusPending,
			// งานที่รอ retry (backoff) ยังไม่ถึงเวลา — ข้ามไว้ก่อน
			"$or": []bson.M{
				{"nextRetryAt": bson.M{"$exists": false}},
				{"nextRetryAt": bson.M{"$lte": now}},
			},
		},
		bson.M{
			"$set": bson.M{
				"status":    enums.ProcessStatusProcessing,
				"workerId":  workerID,
				"claimedAt": now,
			},
		},
		options.FindOneAndUpdate().
			SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "createdAt", Value: 1}}).
			SetReturnDocument(options.After),
	)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil // queue empty — not an error
		}
		return nil, err
	}
	return job, nil
}

// ResumeOwn returns this worker's own processing job, if any — used on
// startup to resume work interrupted by a crash/restart instead of
// claiming a new job while an old one still holds a slot.
func ResumeOwn(ctx context.Context, workerID string) (*models.VideoProcess, error) {
	job, err := models.VideoProcessModel.FindOne(ctx, bson.M{
		"processType": enums.ProcessTypeDownload,
		"status":      enums.ProcessStatusProcessing,
		"workerId":    workerID,
	})
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}

// ─── Job lifecycle ────────────────────────────────────────────

// Complete marks a job as completed (terminal). The partial unique index
// only covers pending/processing, so the same file can be re-enqueued later.
func Complete(ctx context.Context, jobID string) error {
	_, err := models.VideoProcessModel.FindByIDAndUpdate(ctx, jobID, bson.M{
		"$set": bson.M{
			"status":         enums.ProcessStatusCompleted,
			"overallPercent": 100.0,
		},
	})
	return err
}

// MaxRetries — a job fails this many times before going terminal.
const MaxRetries = 3

// Sentinel errors a JobHandler can return to control settling:
var (
	// ErrJobCancelled — admin set status=cancelled mid-run; leave the doc
	// alone (don't overwrite with completed/failed).
	ErrJobCancelled = errors.New("job cancelled")
	// ErrJobRequeue — failure is not the job's fault (e.g. disk full);
	// Release back to pending WITHOUT counting a retry.
	ErrJobRequeue = errors.New("job requeue")
)

// retryBackoff returns the wait before attempt n runs again (1m, 2m, ...).
func retryBackoff(attempt int) time.Duration {
	return time.Duration(1<<(attempt-1)) * time.Minute
}

// RetryOrFail settles a failed run. Under MaxRetries the SAME doc goes back
// to pending with a backoff (nextRetryAt) — no new doc, retryCount is the
// single source of truth. At MaxRetries the job goes terminal AND the file
// is flipped waiting → error, which stops the enqueuer from ever re-picking
// it (the enqueuer only scans waiting files). Returns retried=true if the
// job was requeued.
func RetryOrFail(ctx context.Context, job *models.VideoProcess, errMsg string, category string) (retried bool, err error) {
	attempt := 1
	if job.RetryCount != nil {
		attempt = *job.RetryCount + 1
	}

	if attempt < MaxRetries {
		_, err = models.VideoProcessModel.FindByIDAndUpdate(ctx, job.ID, bson.M{
			"$set": bson.M{
				"status":        enums.ProcessStatusPending,
				"error":         errMsg,
				"errorCategory": category,
				"nextRetryAt":   time.Now().Add(retryBackoff(attempt)),
			},
			"$inc":   bson.M{"retryCount": 1},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		})
		return true, err
	}

	// terminal — mark job failed, then flip the file out of waiting so the
	// enqueuer stops re-enqueuing it forever (needs admin action to retry)
	_, err = models.VideoProcessModel.FindByIDAndUpdate(ctx, job.ID, bson.M{
		"$set": bson.M{
			"status":        enums.ProcessStatusFailed,
			"error":         errMsg,
			"errorCategory": category,
		},
		"$inc": bson.M{"retryCount": 1},
	})
	if err != nil {
		return false, err
	}

	if job.FileID != nil {
		now := time.Now()
		_, fErr := models.FileModel.Col().UpdateOne(ctx,
			bson.M{"_id": *job.FileID, "status": enums.FileStatusWaiting},
			bson.M{"$set": bson.M{"status": enums.FileStatusError, "updatedAt": now}},
		)
		if fErr != nil {
			return false, fErr
		}
		// ไฟล์โคลนรอผลจากต้นฉบับอยู่ — ต้นฉบับตายถาวร โคลนต้องตายตาม
		_, _ = models.FileModel.Col().UpdateMany(ctx, bson.M{
			"clonedFrom":         *job.FileID,
			"type":               enums.FileTypeVideo,
			"status":             bson.M{"$in": []string{enums.FileStatusWaiting, enums.FileStatusProcessing}},
			"metadata.trashedAt": bson.M{"$exists": false},
			"metadata.deletedAt": bson.M{"$exists": false},
		}, bson.M{"$set": bson.M{"status": enums.FileStatusError, "updatedAt": now}})
	}
	return false, nil
}

// Release returns a claimed job to the queue (processing → pending),
// clearing ownership. Called on graceful shutdown so another worker can
// pick the job up immediately instead of waiting for the reaper.
func Release(ctx context.Context, jobID string) error {
	_, err := models.VideoProcessModel.FindOneAndUpdate(ctx,
		bson.M{
			"_id":    jobID,
			"status": enums.ProcessStatusProcessing,
		},
		bson.M{
			"$set":   bson.M{"status": enums.ProcessStatusPending},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		},
	)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		return nil // already completed/reaped — nothing to release
	}
	return err
}
