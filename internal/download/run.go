package download

import (
	"context"
	goerrors "errors"
	"fmt"
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
	"worker-download/internal/uploader"

	"go.mongodb.org/mongo-driver/bson"
)

// Run executes one download job (ported from server-download runProcess).
//
// Settling is the queue loop's responsibility — this function only returns:
//   - nil                  → loop marks completed
//   - queue.ErrJobCancelled → admin cancelled; loop leaves the doc alone
//   - queue.ErrJobRequeue   → not the job's fault (disk full); released, no retry count
//   - any other error       → loop retries with backoff, then terminal fail
func Run(ctx context.Context, process *models.VideoProcess) error {
	err := run(ctx, process)
	// หลัง process logger ปิดแล้ว (ไฟล์ flush ครบ) ค่อยจัดการ log ของงานนี้
	finalizeProcessLog(ctx, process, err)
	return err
}

func run(ctx context.Context, process *models.VideoProcess) error {
	fileID := derefStr(process.FileID)
	slug := derefStr(process.Slug)

	// Use exe dir for download temp files
	exePath, _ := os.Executable()
	baseDir := filepath.Dir(exePath)
	if strings.Contains(exePath, "go-build") {
		baseDir, _ = os.Getwd()
	}
	downloadDir := filepath.Join(baseDir, "download", slug)

	// โดน cancel กลางคัน (watcher จุดระเบิด ctx) — เก็บกวาด temp เสมอ ไม่ว่าตายตรงไหน
	defer func() {
		if goerrors.Is(context.Cause(ctx), queue.ErrJobCancelled) {
			downloader.Cleanup(downloadDir)
		}
	}()

	processLogger := utils.NewProcessLogger(slug)
	defer processLogger.Close()

	// Safety net: check disk space before doing heavy work
	if total, used, _ := queue.DiskUsage(config.AppConfig.StoragePath); total > 0 {
		pct := float64(used) / float64(total) * 100
		if pct >= 90 {
			log.Printf("⚠️ [%s] Disk usage %.1f%% >= 90%% — requeueing", slug, pct)
			return fmt.Errorf("disk usage %.1f%% >= 90%%: %w", pct, queue.ErrJobRequeue)
		}
	}

	file, err := models.FileModel.FindByID(ctx, fileID)
	if err != nil {
		return fmt.Errorf("file not found: %w", err)
	}

	// sourceType: from process record first, then file metadata, then auto-detect
	sourceType := derefStr(process.SourceType)
	if sourceType == "" && file.Metadata != nil && file.Metadata.SourceType != nil {
		sourceType = *file.Metadata.SourceType
	}
	if sourceType == "" {
		ingestCount, _ := models.IngestModel.CountDocuments(ctx, bson.M{"fileId": file.ID})
		if ingestCount > 0 {
			sourceType = enums.IngestSourceTypeUpload
		}
	}

	log.Printf("📋 [%s] Source type: %s", slug, sourceType)
	utils.LogMain("📥 [%s] START (source: %s)", slug, sourceType)

	fileName := models.FileNameOriginal
	resolution := enums.ResolutionOriginal

	var mp4Path string
	var fileSize int64
	isDirectMP4 := false

	// ─── STEP 1: DOWNLOAD ─────────────────────────────────────

	switch sourceType {
	case enums.IngestSourceTypeUpload:
		isDirectMP4 = true
		os.MkdirAll(downloadDir, 0755)
		mp4Path = filepath.Join(downloadDir, "source.mp4")

		// Skip download if source already exists (retry after encode failure)
		if info, ok := reusableSourceFile(mp4Path); ok {
			log.Printf("♻️ [%s] Source file exists (%.2f MB) — skipping download", slug, float64(info.Size())/1024/1024)
			completeStep(ctx, process.ID, "download")
		} else {
			startStep(ctx, process.ID, "download")

			// Find ingest record (non-deleted upload ingest)
			ingest, err := models.IngestModel.FindOne(ctx, bson.M{
				"fileId": file.ID, "sourceType": enums.IngestSourceTypeUpload,
				"deletedAt": bson.M{"$exists": false},
			})
			if err != nil {
				return fmt.Errorf("ingest record not found")
			}

			ingestPathVal := derefStr(ingest.Path)
			if ingestPathVal == "" {
				return fmt.Errorf("ingest has no path")
			}

			// Find ingest storage
			var ingestStorage models.Storage
			storageLoaded := false
			if ingest.StorageID != nil && *ingest.StorageID != "" {
				if s, err := models.StorageModel.FindByID(ctx, *ingest.StorageID); err == nil {
					ingestStorage = *s
					storageLoaded = true
				}
			}

			if storageLoaded && ingestStorage.Type == enums.StorageTypeS3 {
				// Download from S3
				if err := downloader.DownloadFromS3(ctx, &ingestStorage, ingestPathVal, mp4Path, pctLogger64(slug, "download")); err != nil {
					if isCancelled(ctx, process.ID) {
						downloader.Cleanup(downloadDir)
						return queue.ErrJobCancelled
					}
					return fmt.Errorf("S3 download: %w", err)
				}
			} else {
				// Local filesystem
				basePath := ""
				if storageLoaded && ingestStorage.Local != nil && ingestStorage.Local.Path != "" {
					basePath = ingestStorage.Local.Path
				} else {
					basePath = config.AppConfig.StoragePath
				}

				localSrc := ingestPathVal
				if basePath != "" {
					localSrc = filepath.Join(basePath, ingestPathVal)
				}
				log.Printf("📂 [%s] Local ingest file: %s", slug, localSrc)
				if _, err := os.Stat(localSrc); err != nil {
					return fmt.Errorf("ingest file not found: %s", localSrc)
				}
				// Copy to download dir so we can work on it
				if err := copyFileLocal(localSrc, mp4Path); err != nil {
					return fmt.Errorf("copy ingest: %w", err)
				}
			}
			if ingest.Size > 0 {
				info, err := os.Stat(mp4Path)
				if err != nil {
					downloader.Cleanup(downloadDir)
					return fmt.Errorf("stat downloaded upload: %w", err)
				}
				if info.Size() != ingest.Size {
					softDeleteUploadIngest(ctx, file.ID, slug)
					downloader.Cleanup(downloadDir)
					return fmt.Errorf("upload size mismatch: got %d of %d bytes: %w", info.Size(), ingest.Size, queue.ErrPermanent)
				}
			}
			completeStep(ctx, process.ID, "download")
		}

	case enums.IngestSourceTypeGDrive:
		isDirectMP4 = true
		os.MkdirAll(downloadDir, 0755)
		mp4Path = filepath.Join(downloadDir, "source.mp4")

		// Skip download if source already exists (retry after encode failure)
		if info, ok := reusableSourceFile(mp4Path); ok {
			log.Printf("♻️ [%s] Source file exists (%.2f MB) — skipping download", slug, float64(info.Size())/1024/1024)
			completeStep(ctx, process.ID, "download")
		} else {
			startStep(ctx, process.ID, "download")

			source := ""
			if file.Metadata != nil && file.Metadata.Source != nil {
				source = *file.Metadata.Source
			}
			if source == "" {
				return fmt.Errorf("no Google Drive file ID")
			}

			fileSpaceId := ""
			if file.SpaceID != nil {
				fileSpaceId = *file.SpaceID
			}
			if err := downloader.DownloadFromGDrive(ctx, source, mp4Path, models.OAuthModel.Col(), fileSpaceId, pctLogger64(slug, "download")); err != nil {
				if isCancelled(ctx, process.ID) {
					log.Printf("⏹️ [%s] Cancelled during GDrive download", slug)
					downloader.Cleanup(downloadDir)
					return queue.ErrJobCancelled
				}
				return wrapDownloadErr("GDrive download", err, downloadDir)
			}

			if isCancelled(ctx, process.ID) {
				log.Printf("⏹️ [%s] Cancelled after GDrive download", slug)
				downloader.Cleanup(downloadDir)
				return queue.ErrJobCancelled
			}
			completeStep(ctx, process.ID, "download")
		}

	case "direct":
		source := ""
		if file.Metadata != nil && file.Metadata.Source != nil {
			source = *file.Metadata.Source
		}
		if source == "" {
			return fmt.Errorf("no direct URL source")
		}

		startStep(ctx, process.ID, "download")
		os.MkdirAll(downloadDir, 0755)

		if !downloader.IsDirectVideoURL(source) {
			// HLS / m3u8 path
			result, err := downloader.DownloadHLSSegments(ctx, source, downloadDir, &downloader.DownloadProgress{
				OnProgress: segLogger(slug),
			})
			if err != nil {
				return fmt.Errorf("download: %w", err)
			}
			models.VideoProcessModel.UpdateOne(ctx, bson.M{"_id": process.ID}, bson.M{"$set": bson.M{"resolution": result.ResolutionFull}})
			completeStep(ctx, process.ID, "download")
			log.Printf("✅ [%s] HLS download complete (%d segments)", slug, result.SegmentCount)

			if isCancelled(ctx, process.ID) {
				downloader.Cleanup(downloadDir)
				return queue.ErrJobCancelled
			}

			log.Printf("🔒 [%s] Waiting for processing lock...", slug)
			procLock := utils.AcquireProcessingLock("processing")
			defer procLock.Release()

			startStep(ctx, process.ID, "merge")
			mp4Path = filepath.Join(downloadDir, fileName)
			mergeRes, err := downloader.MergeToMP4(ctx, result.SegmentFiles, mp4Path, pctLoggerInt(slug, "merge"))
			if err != nil {
				downloader.Cleanup(downloadDir)
				if downloader.IsDiskFullError(err) {
					return fmt.Errorf("disk full: %v: %w", err, queue.ErrJobRequeue)
				}
				return fmt.Errorf("merge: %w", err)
			}
			completeStep(ctx, process.ID, "merge")
			fileSize = mergeRes.FileSize
			log.Printf("✅ [%s] Merge complete (%.2f MB)", slug, float64(fileSize)/1024/1024)
		} else {
			// Direct video file path (mp4, mkv, webm, etc.)
			isDirectMP4 = true
			mp4Path = filepath.Join(downloadDir, "source.mp4")

			// Skip download if source already exists (retry after encode failure)
			if info, ok := reusableSourceFile(mp4Path); ok {
				log.Printf("♻️ [%s] Source file exists (%.2f MB) — skipping download", slug, float64(info.Size())/1024/1024)
				completeStep(ctx, process.ID, "download")
			} else {
				if err := downloader.DownloadDirectFile(ctx, source, mp4Path, pctLogger64(slug, "download")); err != nil {
					if isCancelled(ctx, process.ID) {
						downloader.Cleanup(downloadDir)
						return queue.ErrJobCancelled
					}
					return wrapDownloadErr("direct download", err, downloadDir)
				}
				completeStep(ctx, process.ID, "download")
			}
		}

		if isCancelled(ctx, process.ID) {
			downloader.Cleanup(downloadDir)
			return queue.ErrJobCancelled
		}

	default: // HLS / remote / scraper sources
		m3u8URL := ""
		if file.Metadata != nil && file.Metadata.Playlist != nil {
			m3u8URL = *file.Metadata.Playlist
		}
		if m3u8URL == "" {
			source := ""
			if file.Metadata != nil && file.Metadata.Source != nil {
				source = *file.Metadata.Source
			}
			if source == "" {
				return fmt.Errorf("no m3u8 URL or source")
			}
			scraperURL := getScraperURL(ctx)
			if scraperURL == "" {
				return fmt.Errorf("no scraper URL configured")
			}
			var err error
			m3u8URL, _, err = downloader.FetchM3U8FromScraper(scraperURL, source)
			if err != nil {
				return fmt.Errorf("scraper: %w", err)
			}
		}

		startStep(ctx, process.ID, "download")
		os.MkdirAll(downloadDir, 0755)

		result, err := downloader.DownloadHLSSegments(ctx, m3u8URL, downloadDir, &downloader.DownloadProgress{
			OnProgress: segLogger(slug),
		})
		if err != nil {
			return fmt.Errorf("download: %w", err)
		}
		models.VideoProcessModel.UpdateOne(ctx, bson.M{"_id": process.ID}, bson.M{"$set": bson.M{"resolution": result.ResolutionFull}})
		completeStep(ctx, process.ID, "download")
		log.Printf("✅ [%s] Download complete (%d segments)", slug, result.SegmentCount)

		if isCancelled(ctx, process.ID) {
			downloader.Cleanup(downloadDir)
			return queue.ErrJobCancelled
		}

		// MERGE
		log.Printf("🔒 [%s] Waiting for processing lock...", slug)
		procLock := utils.AcquireProcessingLock("processing")
		defer procLock.Release()

		startStep(ctx, process.ID, "merge")
		mp4Path = filepath.Join(downloadDir, fileName)
		mergeRes, err := downloader.MergeToMP4(ctx, result.SegmentFiles, mp4Path, pctLoggerInt(slug, "merge"))
		if err != nil {
			downloader.Cleanup(downloadDir)
			if downloader.IsDiskFullError(err) {
				return fmt.Errorf("disk full: %v: %w", err, queue.ErrJobRequeue)
			}
			return fmt.Errorf("merge: %w", err)
		}
		completeStep(ctx, process.ID, "merge")
		fileSize = mergeRes.FileSize
		log.Printf("✅ [%s] Merge complete (%.2f MB)", slug, float64(fileSize)/1024/1024)
	}

	// Check cancellation before heavy processing
	if isCancelled(ctx, process.ID) {
		log.Printf("⏹️ [%s] Cancelled before processing", slug)
		downloader.Cleanup(downloadDir)
		return queue.ErrJobCancelled
	}

	// For direct MP4: ensure h264 + faststart
	if isDirectMP4 {
		if err := downloader.ValidateVideoFile(mp4Path); err != nil {
			downloader.Cleanup(downloadDir)
			if !goerrors.Is(err, downloader.ErrInvalidVideo) {
				return fmt.Errorf("validate source video: %w", err)
			}
			if sourceType == enums.IngestSourceTypeUpload {
				softDeleteUploadIngest(ctx, file.ID, slug)
			}
			return fmt.Errorf("invalid source video: %v: %w", err, queue.ErrPermanent)
		}

		log.Printf("🔒 [%s] Waiting for processing lock...", slug)
		procLock := utils.AcquireProcessingLock("processing")
		defer procLock.Release()

		startStep(ctx, process.ID, "merge")
		utils.LogMain("🔄 [%s] ENCODE", slug)
		faststartPath := filepath.Join(downloadDir, fileName)
		if err := downloader.EnsureH264Faststart(ctx, mp4Path, faststartPath, pctLoggerInt(slug, "merge")); err != nil {
			if isCancelled(ctx, process.ID) {
				downloader.Cleanup(downloadDir)
				return queue.ErrJobCancelled
			}
			if downloader.IsDiskFullError(err) {
				downloader.Cleanup(downloadDir)
				return fmt.Errorf("disk full: %v: %w", err, queue.ErrJobRequeue)
			}
			// Don't cleanup on encode failure — keep source.mp4 for retry
			return fmt.Errorf("H264 encode failed: %w", err)
		}
		if mp4Path != faststartPath {
			os.Remove(mp4Path)
		}
		mp4Path = faststartPath
		if info, err := os.Stat(mp4Path); err == nil {
			fileSize = info.Size()
		}
		// download ถูกปิดไปตั้งแต่โหลดเสร็จแล้ว — ของเดิมมาปิดตรงนี้ ทำให้
		// ระหว่าง encode (เป็นชั่วโมงได้) หน้า admin ยังเห็น download ค้าง 0%
		completeStep(ctx, process.ID, "merge")
	}

	if isCancelled(ctx, process.ID) {
		log.Printf("⏹️ [%s] Cancelled after processing", slug)
		downloader.Cleanup(downloadDir)
		return queue.ErrJobCancelled
	}

	// ─── Probe video info ──────────────────────────────────────
	var videoWidth, videoHeight, videoDuration int64
	vi, probeErr := downloader.ProbeVideoInfo(mp4Path)
	if probeErr != nil {
		log.Printf("⚠️  [%s] Video probe failed: %v — metadata.highest will not be set", slug, probeErr)
	}
	if vi != nil {
		videoWidth, videoHeight, videoDuration = vi.Width, vi.Height, vi.Duration
		log.Printf("📐 [%s] Probed: %dx%d, dur=%ds", slug, videoWidth, videoHeight, videoDuration)
	}

	// ─── STEP 3: UPLOAD ───────────────────────────────────────
	// If STORAGE_ID is set (self-hosted), use local storage directly.
	// Otherwise try S3 temp storage first, fallback to local/SCP.
	uploadedToS3 := false
	var s3Storage *models.Storage

	localStorageID := config.AppConfig.StorageId
	if localStorageID == "" {
		var s3Err error
		s3Storage, s3Err = resolveS3TempStorage(ctx)
		if s3Err != nil {
			s3Storage = nil
		}
	}

	var storage *models.Storage

	startStep(ctx, process.ID, "upload")

	// key แบ่งตามวันที่ — bucket temp ไม่รก + ตั้ง R2 lifecycle ลบตาม prefix ได้
	// ⚠ objectKey ตัวนี้ตัวเดียวใช้ทั้งอัพโหลดและเขียนลง ingest.path — ห้ามประกอบแยก
	//   (ปลายทาง HLS ต้องอ่าน path จาก ingest doc เท่านั้น)
	objectKey := fmt.Sprintf("%s/%s_%s", time.Now().Format("2006-01-02"), file.ID, fileName)

	if s3Storage != nil {
		// ─── S3 TEMP UPLOAD ──────────────────────────────────
		log.Printf("📦 [%s] S3 temp storage resolved: %s", slug, s3Storage.Name)
		utils.LogMain("📤 [%s] UPLOAD → S3 %s", slug, s3Storage.Name)

		if err := uploader.UploadToS3(ctx, s3Storage, mp4Path, objectKey, pctLogger64(slug, "upload")); err != nil {
			log.Printf("⚠️  [%s] S3 upload failed: %v — trying fallback", slug, err)
		} else {
			uploadedToS3 = true
			storage = s3Storage
			log.Printf("✅ [%s] S3 upload complete", slug)
		}
	}

	if !uploadedToS3 {
		// ─── FALLBACK: LOCAL (self-hosted) ───────────────────
		// S3 temp/video เป็นเส้นหลัก — fallback มีแค่เครื่องที่ตั้ง STORAGE_ID
		// (SCP ถูกถอดออกแล้ว)
		localStoragePath := config.AppConfig.StoragePath

		if localStoragePath == "" || localStorageID == "" {
			downloader.Cleanup(downloadDir)
			return fmt.Errorf("no storage available for upload (S3 temp unreachable, no local STORAGE_ID)")
		}

		utils.LogMain("📤 [%s] UPLOAD → local storage (fallback)", slug)
		log.Printf("📤 [%s] Moving to local storage...", slug)
		if _, err := uploader.MoveFilesLocal(localStoragePath, file.ID, mp4Path, fileName, &uploader.LocalUploadProgress{
			OnProgress: pctLogger64(slug, "upload"),
		}); err != nil {
			return fmt.Errorf("local move: %w", err)
		}
		storage = &models.Storage{ID: localStorageID}
	}

	completeStep(ctx, process.ID, "upload")
	log.Printf("✅ [%s] Upload complete", slug)

	// ─── COMPLETE ─────────────────────────────────────────────
	now := time.Now()

	// Compute video metadata updates
	shortSide := videoHeight
	if videoWidth > 0 && videoWidth < videoHeight {
		shortSide = videoWidth
	}

	if uploadedToS3 {
		// ─── S3 PATH: Create ingest + set ready_original ────
		ingestPath := objectKey // ต้องตรงกับ key ที่อัพจริงเสมอ
		mimeType := "video/mp4"
		ingest := models.Ingest{
			ID:         newUUID(),
			FileID:     &file.ID,
			StorageID:  &storage.ID,
			FileName:   fileName,
			Status:     "completed",
			Size:       fileSize,
			MimeType:   &mimeType,
			Path:       &ingestPath,
			SourceType: enums.IngestSourceTypeProcessed,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		models.IngestModel.Create(ctx, &ingest)
		log.Printf("✅ [%s] Created ingest record (S3 temp → HLS service)", slug)

		// Update file → ready_original
		updateFields := bson.M{"status": enums.FileStatusReadyOriginal, "updatedAt": now}
		if shortSide > 0 {
			updateFields["metadata.highest"] = DetermineHighestResolution(int(shortSide))
		}
		if videoDuration > 0 {
			updateFields["metadata.duration"] = videoDuration
		}
		if fileSize > 0 {
			updateFields["metadata.size"] = fileSize
		}
		models.FileModel.UpdateByID(ctx, file.ID, bson.M{"$set": updateFields})

		// Update cloned files → ready_original (no media clone needed)
		cloneUpdate := bson.M{"status": enums.FileStatusReadyOriginal, "updatedAt": now}
		if shortSide > 0 {
			cloneUpdate["metadata.highest"] = DetermineHighestResolution(int(shortSide))
		}
		if videoDuration > 0 {
			cloneUpdate["metadata.duration"] = videoDuration
		}
		if fileSize > 0 {
			cloneUpdate["metadata.size"] = fileSize
		}
		cloneResult, _ := models.FileModel.UpdateMany(ctx, bson.M{
			"clonedFrom":         file.ID,
			"type":               enums.FileTypeVideo,
			"status":             bson.M{"$in": []string{enums.FileStatusWaiting, enums.FileStatusProcessing, enums.FileStatusError}},
			"metadata.trashedAt": bson.M{"$exists": false},
			"metadata.deletedAt": bson.M{"$exists": false},
		}, bson.M{"$set": cloneUpdate})
		if cloneResult != nil && cloneResult.ModifiedCount > 0 {
			log.Printf("📋 [%s] Updated %d cloned files → ready_original", slug, cloneResult.ModifiedCount)
		}
	} else {
		// ─── FALLBACK PATH: Create media + set ready ─────────
		mediaSlug := utils.RandomString(11, true)
		mimeType := "video/mp4"
		resPtr := &resolution

		media := models.Media{
			ID:         newUUID(),
			Type:       enums.MediaTypeVideo,
			FileName:   &fileName,
			MimeType:   &mimeType,
			Resolution: resPtr,
			StorageID:  &storage.ID,
			Slug:       mediaSlug,
			FileID:     &file.ID,
			Metadata: &models.MediaMetadata{
				Size:     fileSize,
				Width:    int(videoWidth),
				Height:   int(videoHeight),
				Duration: float64(videoDuration),
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		models.MediaModel.Create(ctx, &media)
		log.Printf("✅ [%s] Created media record (fallback)", slug)

		cloneMediaToClonedFiles(ctx, file.ID, media, slug)

		// Update file → ready
		updateFields := bson.M{"status": enums.FileStatusReady, "updatedAt": now}
		if shortSide > 0 {
			updateFields["metadata.highest"] = DetermineHighestResolution(int(shortSide))
		}
		if videoDuration > 0 {
			updateFields["metadata.duration"] = videoDuration
		}
		if fileSize > 0 {
			updateFields["metadata.size"] = fileSize
		}
		models.FileModel.UpdateByID(ctx, file.ID, bson.M{"$set": updateFields})

		// Update cloned files → ready
		cloneUpdate := bson.M{"status": enums.FileStatusReady, "updatedAt": now}
		if shortSide > 0 {
			cloneUpdate["metadata.highest"] = DetermineHighestResolution(int(shortSide))
		}
		if videoDuration > 0 {
			cloneUpdate["metadata.duration"] = videoDuration
		}
		if fileSize > 0 {
			cloneUpdate["metadata.size"] = fileSize
		}
		cloneResult, _ := models.FileModel.UpdateMany(ctx, bson.M{
			"clonedFrom":         file.ID,
			"type":               enums.FileTypeVideo,
			"status":             bson.M{"$in": []string{enums.FileStatusWaiting, enums.FileStatusProcessing, enums.FileStatusError}},
			"metadata.trashedAt": bson.M{"$exists": false},
			"metadata.deletedAt": bson.M{"$exists": false},
		}, bson.M{"$set": cloneUpdate})
		if cloneResult != nil && cloneResult.ModifiedCount > 0 {
			log.Printf("📋 [%s] Updated %d cloned files → ready", slug, cloneResult.ModifiedCount)
		}
	}

	// Soft-delete upload ingest after everything is saved.
	if sourceType == enums.IngestSourceTypeUpload {
		if !uploadedToS3 {
			ingest, err := models.IngestModel.FindOne(ctx, bson.M{
				"fileId": file.ID, "sourceType": enums.IngestSourceTypeUpload,
				"deletedAt": bson.M{"$exists": false},
			})
			if err == nil && ingest.StorageID != nil {
				iStor, err := models.StorageModel.FindByID(ctx, *ingest.StorageID)
				if err == nil && iStor.Type == enums.StorageTypeS3 {
					if delErr := downloader.DeleteFromS3(iStor, derefStr(ingest.Path)); delErr != nil {
						log.Printf("⚠️  [%s] Failed to delete S3 upload source: %v", slug, delErr)
					} else {
						log.Printf("🗑️  [%s] Deleted S3 upload source: %s", slug, derefStr(ingest.Path))
					}
				}
			}
		}
		softDeleteUploadIngest(ctx, file.ID, slug)
	}

	// Cleanup temp files — job status is settled by the queue loop.
	downloader.Cleanup(downloadDir)

	utils.LogMain("✅ [%s] COMPLETE!", slug)
	log.Printf("✅ [%s] COMPLETE!", slug)
	return nil
}
