package download

import (
	"context"
	goerrors "errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"worker-download/internal/config"
	"worker-download/internal/core/enums"
	"worker-download/internal/core/utils"
	"worker-download/internal/db/models"
	"worker-download/internal/downloader"
	"worker-download/internal/queue"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func newUUID() string { return uuid.New().String() }

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// reusableSourceFile returns an existing source only when it is large enough
// and ffprobe confirms that its container has a video stream. Invalid retry
// leftovers are removed so the caller downloads a clean copy.
func reusableSourceFile(path string) (os.FileInfo, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, false
	}
	if info.Size() <= 10*1024 {
		_ = os.Remove(path)
		return nil, false
	}
	if err := downloader.ValidateVideoFile(path); err != nil {
		if goerrors.Is(err, downloader.ErrInvalidVideo) {
			log.Printf("Invalid cached source %s: %v; removing before retry", path, err)
			_ = os.Remove(path)
			return nil, false
		}
		log.Printf("Could not validate cached source %s: %v", path, err)
		return info, true
	}
	return info, true
}

// ─── Storage Resolution ───────────────────────────────────────
// Prefer durable S3 storage/video. S3 temp remains the processing path when no
// directly playable S3 storage is available; local is the machine fallback.

// resolveS3VideoStorage finds directly playable, durable S3 storage. originUrl
// is required because storage-node/nginx-vod reads the uploaded MP4 through it.
func resolveS3VideoStorage(ctx context.Context) (*models.Storage, error) {
	filter := bson.M{
		"enable":    true,
		"status":    enums.StorageStatusOnline,
		"type":      enums.StorageTypeS3,
		"originUrl": bson.M{"$type": "string", "$ne": ""},
		"accepts":   bson.M{"$all": []string{enums.StorageAcceptStorage, enums.StorageAcceptVideo}},
		"$or": []bson.M{
			{"drainState": "idle"},
			{"drainState": bson.M{"$exists": false}},
		},
	}
	storage, err := models.StorageModel.FindOne(ctx, filter, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("no S3 video storage available")
	}
	if storage.OriginURL == nil || strings.TrimSpace(*storage.OriginURL) == "" {
		return nil, fmt.Errorf("S3 video storage has no origin URL")
	}
	return storage, nil
}

// resolveS3TempStorage finds an S3 storage that accepts ["temp", "video"].
func resolveS3TempStorage(ctx context.Context) (*models.Storage, error) {
	filter := bson.M{
		"enable":  true,
		"status":  enums.StorageStatusOnline,
		"type":    enums.StorageTypeS3,
		"accepts": bson.M{"$all": []string{enums.StorageAcceptTemp, enums.StorageAcceptVideo}},
	}
	storage, err := models.StorageModel.FindOne(ctx, filter, options.FindOne().SetSort(bson.M{"capacity.percentage": 1}))
	if err != nil {
		return nil, fmt.Errorf("no S3 temp storage available")
	}
	return storage, nil
}

// ─── Scraper URL ──────────────────────────────────────────────

func getScraperURL(ctx context.Context) string {
	if url := config.AppConfig.ScraperURL; url != "" {
		return url
	}
	setting, err := models.SettingModel.FindOne(ctx, bson.M{"name": enums.SettingURLScraping})
	if err == nil {
		if v, ok := setting.Value.(string); ok {
			return v
		}
	}
	return ""
}

// ─── isCancelled ─────────────────────────────────────────────
// Admin cancel: sets video_process.status = cancelled mid-run; the worker
// polls between heavy steps and aborts.

func isCancelled(ctx context.Context, processID string) bool {
	p, err := models.VideoProcessModel.FindByID(ctx, processID)
	if err != nil {
		return false // doc gone ≠ cancelled; let the run finish and settle
	}
	return derefStr(p.Status) == enums.ProcessStatusCancelled
}

// ─── wrapDownloadErr ─────────────────────────────────────────

// wrapDownloadErr labels a download failure and marks truncated sources as
// permanent so the loop fails them on the first attempt.
//
// Retrying a truncated file is worse than useless: the partial source is kept
// on disk for retries, so the next attempt skips the download and goes
// straight back to encoding the same broken input.
func wrapDownloadErr(stage string, err error, downloadDir string) error {
	if goerrors.Is(err, downloader.ErrIncompleteDownload) {
		downloader.Cleanup(downloadDir) // อย่าเก็บไฟล์ครึ่งๆ ไว้ให้รอบหน้าหยิบไปใช้
		return fmt.Errorf("%s: %v: %w", stage, err, queue.ErrPermanent)
	}
	return fmt.Errorf("%s: %w", stage, err)
}

// ─── Clone media to cloned files ─────────────────────────────

func cloneMediaToClonedFiles(ctx context.Context, sourceFileID string, media models.Media, slug string) {
	cursor, err := models.FileModel.FindRaw(ctx, bson.M{
		"clonedFrom":         sourceFileID,
		"type":               enums.FileTypeVideo,
		"metadata.trashedAt": bson.M{"$exists": false},
		"metadata.deletedAt": bson.M{"$exists": false},
	})
	if err != nil {
		return
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var clonedFile models.File
		if err := cursor.Decode(&clonedFile); err != nil {
			continue
		}

		filter := bson.M{"fileId": clonedFile.ID, "type": media.Type}
		if media.Resolution != nil {
			filter["resolution"] = *media.Resolution
		}
		existCount, _ := models.MediaModel.CountDocuments(ctx, filter)
		if existCount > 0 {
			continue
		}

		now := time.Now()
		slug11 := utils.RandomString(11, true)
		clonedMedia := models.Media{
			ID:         newUUID(),
			Type:       media.Type,
			FileName:   media.FileName,
			MimeType:   media.MimeType,
			Resolution: media.Resolution,
			StorageID:  media.StorageID,
			Slug:       slug11,
			FileID:     &clonedFile.ID,
			Metadata:   media.Metadata,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		clonedFrom := sourceFileID
		clonedMedia.ClonedFrom = &clonedFrom

		if _, err := models.MediaModel.Create(ctx, &clonedMedia); err != nil {
			log.Printf("⚠️  [%s] Failed to clone media to %s: %v", slug, clonedFile.ID, err)
			continue
		}
		log.Printf("📋 [%s] Cloned media → file %s", slug, clonedFile.ID)
	}
}

// ─── DetermineHighestResolution ───────────────────────────────

// DetermineHighestResolution maps pixel height to the highest standard tier (95% tolerance).
func DetermineHighestResolution(height int) int {
	threshold := func(t int) int { return t * 95 / 100 }
	if height >= threshold(1080) {
		return 1080
	}
	if height >= threshold(720) {
		return 720
	}
	if height >= threshold(480) {
		return 480
	}
	return 360
}

// softDeleteUploadIngest marks upload ingests as no longer eligible for processing.
func softDeleteUploadIngest(ctx context.Context, fileID, slug string) {
	now := time.Now()
	result, err := models.IngestModel.Col().UpdateMany(ctx, bson.M{
		"fileId":     fileID,
		"sourceType": enums.IngestSourceTypeUpload,
		"deletedAt":  bson.M{"$exists": false},
	}, bson.M{"$set": bson.M{"deletedAt": now, "updatedAt": now}})
	if err != nil {
		log.Printf("⚠️  [%s] soft-delete upload ingest: %v", slug, err)
		return
	}
	if result.ModifiedCount > 0 {
		log.Printf("🗑️  [%s] Soft-deleted upload ingest (%d)", slug, result.ModifiedCount)
	}
}

// copyFileLocal copies src to dst.
func copyFileLocal(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
