package download

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"worker-download/internal/config"
	"worker-download/internal/core/enums"
	"worker-download/internal/core/utils"
	"worker-download/internal/db/models"

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

// ─── Storage Resolution ───────────────────────────────────────
// เส้นหลักคือ S3 temp/video — fallback เดียวคือ local storage ของเครื่องเอง
// (ตั้งผ่าน STORAGE_ID/STORAGE_PATH ใน env) SCP/SSH ถูกถอดออกแล้ว

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

// softDeleteUploadIngest marks upload ingests as deleted after successful processing.
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
