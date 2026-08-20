package download

import (
	"context"
	"fmt"
	"log"
	"time"

	"worker-download/internal/config"
	"worker-download/internal/core/enums"
	"worker-download/internal/core/utils"
	"worker-download/internal/db/models"
	"worker-download/internal/downloader"
	"worker-download/internal/uploader"

	"go.mongodb.org/mongo-driver/bson"
)

func completeSeparatedMedia(
	ctx context.Context,
	process *models.VideoProcess,
	file *models.File,
	sourceType, slug, downloadDir string,
	assets []downloader.SplitAsset,
	videoWidth, videoHeight, videoDuration int64,
) error {
	if len(assets) == 0 {
		return fmt.Errorf("separated layout has no assets")
	}

	startStep(ctx, process.ID, "upload")
	var storage *models.Storage
	destination := uploadDestinationNone
	paths := make(map[string]string, len(assets))

	if s3Storage, err := resolveS3VideoStorage(ctx); err == nil {
		storage = s3Storage
		total := totalAssetSize(assets)
		var completed int64
		trackUpload := trackedBytes(ctx, process.ID, slug, "upload")
		for _, asset := range assets {
			key := s3VideoObjectKey(file.ID, asset.FileName)
			base := completed
			err := uploader.UploadToS3(ctx, s3Storage, asset.Path, key, func(done, _ int64) {
				trackUpload(base+done, total)
			})
			if err != nil {
				if cause := context.Cause(ctx); cause != nil {
					return cause
				}
				// Keys are deterministic. Keep completed uploads and retry safely;
				// do not mix one separated set across different storages.
				return fmt.Errorf("upload separated asset %s: %w", asset.FileName, err)
			}
			paths[asset.FileName] = key
			completed += asset.Size
		}
		destination = uploadDestinationS3Video
	} else if config.AppConfig.StorageId != "" {
		storage = &models.Storage{ID: config.AppConfig.StorageId}
		for _, asset := range assets {
			if _, err := uploader.MoveFilesLocal(config.AppConfig.StoragePath, file.ID, asset.Path, asset.FileName, nil); err != nil {
				return fmt.Errorf("install separated asset %s: %w", asset.FileName, err)
			}
			paths[asset.FileName] = file.ID + "/" + asset.FileName
		}
		destination = uploadDestinationLocal
	} else {
		return fmt.Errorf("separated layout requires permanent S3 video storage or local STORAGE_ID; temp transfer is not supported")
	}

	completeStep(ctx, process.ID, "upload")
	now := time.Now()
	created := make([]models.Media, 0, len(assets))
	for _, asset := range assets {
		media, err := upsertSeparatedMedia(ctx, file.ID, storage.ID, paths[asset.FileName], asset, now)
		if err != nil {
			return err
		}
		created = append(created, *media)
		cloneMediaToClonedFiles(ctx, file.ID, *media, slug)
	}

	audioCount, subtitleCount := countTrackAssets(assets)
	shortSide := videoHeight
	if videoWidth > 0 && videoWidth < videoHeight {
		shortSide = videoWidth
	}
	update := bson.M{
		"status":                      enums.FileStatusReady,
		"updatedAt":                   now,
		"metadata.mediaLayout":        "separated",
		"metadata.audioTrackCount":    audioCount,
		"metadata.subtitleTrackCount": subtitleCount,
		"metadata.size":               totalAssetSize(assets),
	}
	if shortSide > 0 {
		update["metadata.highest"] = DetermineHighestResolution(int(shortSide))
	}
	if videoDuration > 0 {
		update["metadata.duration"] = videoDuration
	}
	if _, err := models.FileModel.UpdateByID(ctx, file.ID, bson.M{"$set": update}); err != nil {
		return fmt.Errorf("update separated file: %w", err)
	}
	_, _ = models.FileModel.UpdateMany(ctx, bson.M{
		"clonedFrom":         file.ID,
		"type":               enums.FileTypeVideo,
		"metadata.trashedAt": bson.M{"$exists": false},
		"metadata.deletedAt": bson.M{"$exists": false},
	}, bson.M{"$set": update})

	if sourceType == enums.IngestSourceTypeUpload {
		if destination == uploadDestinationLocal {
			deleteUploadSourceFromS3(ctx, file.ID, slug)
		}
		softDeleteUploadIngest(ctx, file.ID, slug)
	}

	downloader.Cleanup(downloadDir)
	utils.LogMain("✅ [%s] COMPLETE separated (%d media)", slug, len(created))
	return nil
}

func upsertSeparatedMedia(ctx context.Context, fileID, storageID, objectPath string, asset downloader.SplitAsset, now time.Time) (*models.Media, error) {
	filter := bson.M{
		"fileId": fileID, "storageId": storageID, "type": asset.Kind,
		"fileName": asset.FileName, "deletedAt": nil,
	}
	media, err := models.MediaModel.FindOne(ctx, filter)
	if err == nil && media != nil {
		return media, nil
	}

	fileName, mimeType, path := asset.FileName, asset.MimeType, objectPath
	sourceCodec, codec := asset.SourceCodec, asset.Codec
	language, title := asset.Language, asset.Title
	defaultTrack, forced := asset.Default, asset.Forced
	mediaLayout := "separated"
	sourceIndex, channels, sampleRate, bitrate := asset.SourceIndex, asset.Channels, asset.SampleRate, asset.Bitrate
	metadata := &models.MediaMetadata{
		Size: asset.Size, Width: asset.Width, Height: asset.Height, Duration: asset.Duration,
		SourceIndex: &sourceIndex, SourceCodec: &sourceCodec, Codec: &codec,
		Language: &language, Title: &title, IsDefault: &defaultTrack, IsForced: &forced,
		MediaLayout: &mediaLayout,
	}
	if asset.Kind == downloader.SplitAssetAudio {
		metadata.Channels = &channels
		metadata.SampleRate = &sampleRate
		metadata.Bitrate = &bitrate
	}
	media = &models.Media{
		ID: newUUID(), Type: asset.Kind, FileName: &fileName, MimeType: &mimeType,
		StorageID: &storageID, Slug: utils.RandomString(11, true), Path: &path,
		FileID: &fileID, Metadata: metadata, CreatedAt: now, UpdatedAt: now,
	}
	if asset.Kind == downloader.SplitAssetVideo {
		resolution := enums.ResolutionOriginal
		media.Resolution = &resolution
	}
	if _, err := models.MediaModel.Create(ctx, media); err != nil {
		return nil, fmt.Errorf("create %s media %s: %w", asset.Kind, asset.FileName, err)
	}
	return media, nil
}

func countTrackAssets(assets []downloader.SplitAsset) (audio, subtitle int) {
	for _, asset := range assets {
		switch asset.Kind {
		case downloader.SplitAssetAudio:
			audio++
		case downloader.SplitAssetSubtitle:
			subtitle++
		}
	}
	return
}

func totalAssetSize(assets []downloader.SplitAsset) int64 {
	var total int64
	for _, asset := range assets {
		total += asset.Size
	}
	return total
}

func deleteUploadSourceFromS3(ctx context.Context, fileID, slug string) {
	ingest, err := models.IngestModel.FindOne(ctx, bson.M{
		"fileId": fileID, "sourceType": enums.IngestSourceTypeUpload,
		"deletedAt": bson.M{"$exists": false},
	})
	if err != nil || ingest.StorageID == nil {
		return
	}
	storage, err := models.StorageModel.FindByID(ctx, *ingest.StorageID)
	if err != nil || storage.Type != enums.StorageTypeS3 {
		return
	}
	if err := downloader.DeleteFromS3(storage, derefStr(ingest.Path)); err != nil {
		log.Printf("⚠️  [%s] Failed to delete S3 upload source: %v", slug, err)
	}
}
