package dashboard

import (
	"context"
	"regexp"
	"sort"
	"time"

	"worker-download/internal/core/enums"
	"worker-download/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Snapshot struct {
	Timestamp time.Time     `json:"timestamp"`
	System    SystemMetrics `json:"system"`
	Jobs      []JobView     `json:"jobs"`
}

type JobView struct {
	ID             string     `json:"id"`
	FileID         string     `json:"fileId"`
	Slug           string     `json:"slug"`
	WorkerID       string     `json:"workerId"`
	SourceType     string     `json:"sourceType"`
	OverallPercent float64    `json:"overallPercent"`
	ClaimedAt      *time.Time `json:"claimedAt,omitempty"`
	Steps          []StepView `json:"steps"`
}

type StepView struct {
	Key     string  `json:"key"`
	Label   string  `json:"label"`
	Status  string  `json:"status"`
	Percent float64 `json:"percent"`
}

func loadJobs(ctx context.Context, workerGroup string) []JobView {
	filter := bson.M{"processType": enums.ProcessTypeDownload, "status": enums.ProcessStatusProcessing}
	if workerGroup != "" {
		filter["workerId"] = bson.M{"$regex": "^" + regexp.QuoteMeta(workerGroup) + `(?:@\d+)?$`}
	}
	cursor, err := models.VideoProcessModel.Col().Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "claimedAt", Value: 1}}))
	if err != nil {
		return []JobView{}
	}
	defer cursor.Close(ctx)
	var processes []models.VideoProcess
	if err := cursor.All(ctx, &processes); err != nil {
		return []JobView{}
	}
	jobs := make([]JobView, 0, len(processes))
	for _, process := range processes {
		job := JobView{ID: process.ID, ClaimedAt: process.ClaimedAt}
		if process.FileID != nil {
			job.FileID = *process.FileID
		}
		if process.Slug != nil {
			job.Slug = *process.Slug
		}
		if process.WorkerID != nil {
			job.WorkerID = *process.WorkerID
		}
		if process.SourceType != nil {
			job.SourceType = *process.SourceType
		}
		if process.OverallPercent != nil {
			job.OverallPercent = clamp(*process.OverallPercent)
		}
		job.Steps = timelineSteps(process.Timeline)
		jobs = append(jobs, job)
	}
	return jobs
}

func timelineSteps(raw interface{}) []StepView {
	doc := toMap(raw)
	steps := make([]StepView, 0, 3)
	for _, key := range []string{"download", "merge", "upload"} {
		stepDoc := toMap(doc[key])
		if len(stepDoc) == 0 {
			continue
		}
		status, _ := stepDoc["status"].(string)
		value := numeric(stepDoc["percent"])
		if status == enums.StepStatusCompleted {
			value = 100
		}
		steps = append(steps, StepView{Key: key, Label: stepLabel(key), Status: status, Percent: clamp(value)})
	}
	sort.SliceStable(steps, func(i, j int) bool { return stepOrder(steps[i].Key) < stepOrder(steps[j].Key) })
	return steps
}

func toMap(value interface{}) map[string]interface{} {
	switch v := value.(type) {
	case bson.M:
		return v
	case map[string]interface{}:
		return v
	case bson.D:
		m := make(map[string]interface{}, len(v))
		for _, item := range v {
			m[item.Key] = item.Value
		}
		return m
	default:
		return map[string]interface{}{}
	}
}

func numeric(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}

func clamp(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func stepOrder(key string) int {
	switch key {
	case "download":
		return 0
	case "merge":
		return 1
	case "upload":
		return 2
	default:
		return 99
	}
}

func stepLabel(key string) string {
	switch key {
	case "download":
		return "Download source"
	case "merge":
		return "Merge / optimize"
	case "upload":
		return "Upload / install"
	default:
		return key
	}
}
