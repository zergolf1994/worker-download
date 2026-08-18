package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// AppConfig holds the application configuration loaded from environment variables.
var AppConfig Config

// Config represents the application configuration.
type Config struct {
	Port     string
	MongoURI string

	StorageId   string
	StoragePath string
	ScraperURL  string

	// Number of multipart S3 parts uploaded in parallel.
	S3UploadConcurrency int

	LogPath string // Path to rotating log file (env: LOG_PATH)
}

// Load reads configuration from environment variables (and .env file).
func Load() {
	// Load .env file if present (ignore error if not found)
	godotenv.Load()

	AppConfig = Config{
		MongoURI:            getEnv("DATABASE_URL", "mongodb://localhost:27017"),
		StorageId:           getEnv("STORAGE_ID", ""),
		StoragePath:         getEnv("STORAGE_PATH", "./files"),
		ScraperURL:          getEnv("SCRAPER_URL", ""),
		S3UploadConcurrency: getIntEnv("S3_UPLOAD_CONCURRENCY", 3, 1, 8),
		LogPath:             getEnv("LOG_PATH", "logs/worker-download.log"),
	}
}

func getIntEnv(key string, fallback, minValue, maxValue int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
