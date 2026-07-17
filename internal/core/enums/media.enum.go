package enums

// ─── Media Types ─────────────────────────────────────────────────────
// Must match MediaType in vdohide-service (file.enum.ts).

const (
	MediaTypeVideo     = "video"
	MediaTypeAudio     = "audio"
	MediaTypeSubtitle  = "subtitle"
	MediaTypeThumbnail = "thumbnail"
	MediaTypeImage     = "image"
	MediaTypeDocument  = "document"
	MediaTypeOther     = "other"
)

// ─── Ingest Source Types ─────────────────────────────────────────────
// "processed" = created by the download worker (original ready for HLS).

const (
	IngestSourceTypeUpload    = "upload"
	IngestSourceTypeRemote    = "remote"
	IngestSourceTypeGDrive    = "gdrive"
	IngestSourceTypeS3Import  = "s3_import"
	IngestSourceTypeProcessed = "processed"
)

// ─── Resolution ──────────────────────────────────────────────────────

const ResolutionOriginal = "original"
