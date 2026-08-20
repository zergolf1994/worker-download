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

type uploadDestination uint8

const (
	uploadDestinationNone uploadDestination = iota
	uploadDestinationLocal
	uploadDestinationS3Video
	uploadDestinationS3Temp
)

func s3VideoObjectKey(fileID, fileName string) string {
	return fileID + "/" + fileName
}

func s3TempObjectKey(now time.Time, fileID, fileName string) string {
	return fmt.Sprintf("%s/%s_%s", now.Format("2006-01-02"), fileID, fileName)
}

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
	var splitAssets []downloader.SplitAsset

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
				if err := downloader.DownloadFromS3(ctx, &ingestStorage, ingestPathVal, mp4Path, trackedBytes(ctx, process.ID, slug, "download")); err != nil {
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
				if err := copyFileLocal(localSrc, mp4Path, trackedBytes(ctx, process.ID, slug, "download")); err != nil {
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
			if err := downloader.DownloadFromGDrive(ctx, source, mp4Path, models.OAuthModel.Col(), fileSpaceId, trackedBytes(ctx, process.ID, slug, "download")); err != nil {
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
				OnProgress: trackedSegments(ctx, process.ID, slug),
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
			mergeRes, err := downloader.MergeToMP4(ctx, result.SegmentFiles, mp4Path, trackedPercent(ctx, process.ID, slug, "merge"))
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
				if err := downloader.DownloadDirectFile(ctx, source, mp4Path, trackedBytes(ctx, process.ID, slug, "download")); err != nil {
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
			OnProgress: trackedSegments(ctx, process.ID, slug),
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
		mergeRes, err := downloader.MergeToMP4(ctx, result.SegmentFiles, mp4Path, trackedPercent(ctx, process.ID, slug, "merge"))
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

	// Separated layout: preserve every audio/text-subtitle stream before the
	// legacy normalizer selects only the default audio stream.
	if config.AppConfig.MediaLayout == "separated" {
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

		if isDirectMP4 {
			log.Printf("🔒 [%s] Waiting for processing lock...", slug)
			procLock := utils.AcquireProcessingLock("processing")
			defer procLock.Release()
		}

		startStep(ctx, process.ID, "merge")
		utils.LogMain("🧩 [%s] SPLIT video/audio/subtitle", slug)
		splitDir := filepath.Join(downloadDir, "separated")
		assets, err := downloader.SplitMedia(ctx, mp4Path, splitDir, trackedPercent(ctx, process.ID, slug, "merge"))
		if err != nil {
			if isCancelled(ctx, process.ID) {
				downloader.Cleanup(downloadDir)
				return queue.ErrJobCancelled
			}
			if downloader.IsDiskFullError(err) {
				downloader.Cleanup(downloadDir)
				return fmt.Errorf("disk full: %v: %w", err, queue.ErrJobRequeue)
			}
			return fmt.Errorf("split media: %w", err)
		}
		splitAssets = assets
		videoPath := ""
		for _, asset := range splitAssets {
			if asset.Kind == downloader.SplitAssetVideo {
				videoPath = asset.Path
				fileSize = asset.Size
				break
			}
		}
		if videoPath == "" {
			return fmt.Errorf("split media produced no video asset")
		}
		if mp4Path != videoPath {
			os.Remove(mp4Path)
		}
		mp4Path = videoPath
		completeStep(ctx, process.ID, "merge")
	} else if isDirectMP4 {
		// Legacy muxed layout: ensure h264 + faststart while retaining the
		// default audio stream inside file_original.mp4.
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
		if err := downloader.EnsureH264Faststart(ctx, mp4Path, faststartPath, trackedPercent(ctx, process.ID, slug, "merge")); err != nil {
			if isCancelled(ctx, process.ID) {
				downloader.Cleanup(downloadDir)
				return queue.ErrJobCancelled
			}
			if downloader.IsDiskFullError(err) {
				downloader.Cleanup(downloadDir)
				return fmt.Errorf("disk full: %v: %w", err, queue.ErrJobRequeue)
			}
			return fmt.Errorf("H264 encode failed: %w", err)
		}
		if mp4Path != faststartPath {
			os.Remove(mp4Path)
		}
		mp4Path = faststartPath
		if info, err := os.Stat(mp4Path); err == nil {
			fileSize = info.Size()
		}
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

	if len(splitAssets) > 0 {
		return completeSeparatedMedia(ctx, process, file, sourceType, slug, downloadDir, splitAssets, videoWidth, videoHeight, videoDuration)
	}

	// ─── STEP 3: UPLOAD ───────────────────────────────────────
	// Prefer durable S3 storage/video and create media directly. If none is
	// available, preserve the existing local or S3-temp processing path.
	destination := uploadDestinationNone
	var storage *models.Storage
	var objectKey string
	localStorageID := config.AppConfig.StorageId

	startStep(ctx, process.ID, "upload")

	if s3VideoStorage, err := resolveS3VideoStorage(ctx); err == nil {
		objectKey = s3VideoObjectKey(file.ID, fileName)
		log.Printf("📦 [%s] S3 video storage resolved: %s", slug, s3VideoStorage.Name)
		utils.LogMain("📤 [%s] UPLOAD → S3 video %s", slug, s3VideoStorage.Name)
		if err := uploader.UploadToS3(ctx, s3VideoStorage, mp4Path, objectKey, trackedBytes(ctx, process.ID, slug, "upload")); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				downloader.Cleanup(downloadDir)
				return cause
			}
			log.Printf("⚠️  [%s] Direct S3 video upload failed: %v — trying fallback", slug, err)
		} else {
			destination = uploadDestinationS3Video
			storage = s3VideoStorage
			log.Printf("✅ [%s] Direct S3 video upload complete", slug)
		}
	}

	if destination == uploadDestinationNone && localStorageID != "" {
		// ─── FALLBACK: LOCAL (self-hosted) ───────────────────
		localStoragePath := config.AppConfig.StoragePath
		if localStoragePath == "" {
			return fmt.Errorf("local STORAGE_ID is set but STORAGE_PATH is empty")
		}
		utils.LogMain("📤 [%s] UPLOAD → local storage (fallback)", slug)
		log.Printf("📤 [%s] Moving to local storage...", slug)
		if _, err := uploader.MoveFilesLocal(localStoragePath, file.ID, mp4Path, fileName, &uploader.LocalUploadProgress{
			OnProgress: trackedBytes(ctx, process.ID, slug, "upload"),
		}); err != nil {
			return fmt.Errorf("local move: %w", err)
		}
		storage = &models.Storage{ID: localStorageID}
		destination = uploadDestinationLocal
	}

	if destination == uploadDestinationNone {
		// ─── FALLBACK: S3 TEMP → HLS SERVICE ─────────────────
		s3TempStorage, err := resolveS3TempStorage(ctx)
		if err != nil {
			downloader.Cleanup(downloadDir)
			return fmt.Errorf("no storage available for upload (no S3 video, local, or S3 temp storage)")
		}
		// The exact key written here is also persisted in ingest.path.
		objectKey = s3TempObjectKey(time.Now(), file.ID, fileName)
		log.Printf("📦 [%s] S3 temp storage resolved: %s", slug, s3TempStorage.Name)
		utils.LogMain("📤 [%s] UPLOAD → S3 temp %s", slug, s3TempStorage.Name)
		if err := uploader.UploadToS3(ctx, s3TempStorage, mp4Path, objectKey, trackedBytes(ctx, process.ID, slug, "upload")); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				downloader.Cleanup(downloadDir)
				return cause
			}
			return fmt.Errorf("S3 temp upload: %w", err)
		}
		storage = s3TempStorage
		destination = uploadDestinationS3Temp
		log.Printf("✅ [%s] S3 temp upload complete", slug)
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

	if destination == uploadDestinationS3Temp {
		// ─── S3 PATH: Create ingest + set ready_original ────
		ingestPath := objectKey // ต้องตรงกับ key ที่อัพจริงเสมอ
		mimeType := "video/mp4"
		mediaType, installTarget := enums.MediaTypeVideo, "local"
		layout := config.AppConfig.MediaLayout
		mediaMetadata := &models.MediaMetadata{Size: fileSize, Width: int(videoWidth), Height: int(videoHeight), Duration: float64(videoDuration), MediaLayout: &layout}
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
			MediaType:  &mediaType, Resolution: &resolution, MediaMetadata: mediaMetadata,
			InstallTarget: &installTarget,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		existingIngest, findErr := models.IngestModel.FindOne(ctx, bson.M{
			"fileId":     file.ID,
			"storageId":  storage.ID,
			"sourceType": enums.IngestSourceTypeProcessed,
			"path":       ingestPath,
			"deletedAt":  nil,
		})
		if findErr != nil || existingIngest == nil {
			if _, err := models.IngestModel.Create(ctx, &ingest); err != nil {
				return fmt.Errorf("create processed ingest: %w", err)
			}
		} else {
			log.Printf("♻️ [%s] Reusing processed ingest %s", slug, existingIngest.ID)
		}
		log.Printf("✅ [%s] Processed ingest ready (S3 temp → HLS service)", slug)

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
		if _, err := models.FileModel.UpdateByID(ctx, file.ID, bson.M{"$set": updateFields}); err != nil {
			return fmt.Errorf("update file ready_original: %w", err)
		}

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
		// ─── DIRECT STORAGE PATH: Create media + set ready ───
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
		mediaRecord := media
		existingMedia, findErr := models.MediaModel.FindOne(ctx, bson.M{
			"fileId":     file.ID,
			"storageId":  storage.ID,
			"type":       enums.MediaTypeVideo,
			"resolution": resolution,
			"deletedAt":  nil,
		})
		if findErr == nil && existingMedia != nil {
			mediaRecord = *existingMedia
			log.Printf("♻️ [%s] Reusing original media %s", slug, existingMedia.ID)
		} else if _, err := models.MediaModel.Create(ctx, &media); err != nil {
			return fmt.Errorf("create original media: %w", err)
		}
		log.Printf("✅ [%s] Original media ready on storage %s", slug, storage.ID)

		cloneMediaToClonedFiles(ctx, file.ID, mediaRecord, slug)

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
		if _, err := models.FileModel.UpdateByID(ctx, file.ID, bson.M{"$set": updateFields}); err != nil {
			return fmt.Errorf("update file ready: %w", err)
		}

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
		if destination == uploadDestinationLocal {
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
