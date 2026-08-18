package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type hub struct {
	mu          sync.RWMutex
	clients     map[chan []byte]struct{}
	latest      []byte
	workerGroup string
	metrics     *metricSampler
}

func newHub(workerGroup, storagePath string) *hub {
	return &hub{clients: make(map[chan []byte]struct{}), workerGroup: workerGroup, metrics: newMetricSampler(storagePath)}
}

func (h *hub) run(ctx context.Context) {
	h.collectAndBroadcast(ctx)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.collectAndBroadcast(ctx)
		}
	}
}

func (h *hub) collectAndBroadcast(parent context.Context) {
	// nvidia-smi/dmon can take about a second. Do not let that consume the
	// Mongo timeout or active jobs would intermittently disappear from the UI.
	system := h.metrics.sample()
	ctx, cancel := context.WithTimeout(parent, 900*time.Millisecond)
	defer cancel()
	snapshot := Snapshot{Timestamp: time.Now(), System: system, Jobs: loadJobs(ctx, h.workerGroup)}
	b, err := json.Marshal(snapshot)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.latest = b
	for ch := range h.clients {
		select {
		case ch <- b:
		default:
		}
	}
	h.mu.Unlock()
}

func (h *hub) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	ch := make(chan []byte, 2)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	latest := h.latest
	h.mu.Unlock()
	defer func() { h.mu.Lock(); delete(h.clients, ch); h.mu.Unlock() }()
	if len(latest) > 0 {
		_, _ = fmt.Fprintf(w, "data: %s\n\n", latest)
		flusher.Flush()
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}
