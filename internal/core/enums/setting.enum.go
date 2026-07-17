package enums

// ─── Setting Keys ────────────────────────────────────────────────────

const (
	// download_config = {enabled, sort, filter, slotRate} — shared with the
	// vdohide-service enqueuer; worker reads only .enabled as a kill switch
	SettingDownloadConfig = "download_config"
	SettingURLScraping    = "url_scraping"
)
