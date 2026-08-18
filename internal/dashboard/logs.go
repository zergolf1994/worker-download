package dashboard

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"worker-download/internal/core/enums"
	"worker-download/internal/db/models"
)

const maxLogResponse = int64(512 * 1024)

var safeSlug = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func serveProcessLog(group string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		jobID := strings.TrimPrefix(r.URL.Path, "/logs/")
		if jobID == "" || strings.Contains(jobID, "/") {
			http.Error(w, "invalid job id", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		process, err := models.VideoProcessModel.FindByID(ctx, jobID)
		if err != nil || process == nil || process.ProcessType != enums.ProcessTypeDownload || !belongsToWorkerGroup(process.WorkerID, group) {
			http.NotFound(w, r)
			return
		}
		slug := ""
		if process.Slug != nil {
			slug = *process.Slug
		}
		logPath, ok := processLogPath(slug)
		if !ok {
			http.NotFound(w, r)
			return
		}
		content, truncated, err := readLogTail(logPath, maxLogResponse)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "log is no longer available locally", http.StatusGone)
				return
			}
			http.Error(w, "failed to read log", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if truncated {
			w.Header().Set("X-Log-Truncated", "true")
			_, _ = fmt.Fprintln(w, "--- showing the latest 512 KiB ---")
		}
		_, _ = w.Write(content)
	}
}

func belongsToWorkerGroup(workerID *string, group string) bool {
	if workerID == nil || *workerID == "" || group == "" {
		return false
	}
	pattern := `^` + regexp.QuoteMeta(group) + `(?:@\d+)?$`
	matched, _ := regexp.MatchString(pattern, *workerID)
	return matched
}

func processLogPath(slug string) (string, bool) {
	if !safeSlug.MatchString(slug) || filepath.Base(slug) != slug {
		return "", false
	}
	return filepath.Join("logs", "process", slug+".log"), true
}

func readLogTail(path string, limit int64) ([]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, false, err
	}
	start := int64(0)
	truncated := info.Size() > limit
	if truncated {
		start = info.Size() - limit
		if _, err := file.Seek(start, io.SeekStart); err != nil {
			return nil, false, err
		}
	}
	content, err := io.ReadAll(io.LimitReader(file, limit))
	return content, truncated, err
}
