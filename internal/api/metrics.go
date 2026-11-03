package api

import (
	"net/http"
	"strings"

	"github.com/pelavarthi/distributed-kv-go/internal/metrics"
)

// SetMetrics turns on request metrics and structured request
// logging for this server, and serves the snapshot at
// GET /metrics. Call it once, before serving.
func (s *Server) SetMetrics(m *metrics.Metrics) {
	s.metrics = m
	s.mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, m.Snapshot())
	})
}

// statusWriter remembers the status code a handler wrote, so
// ServeHTTP can record it. Handlers that never call WriteHeader
// implicitly send 200, which is the default here.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// routePattern collapses a request path to its route, so
// /kv/hello and /kv/world both count as /kv/:key instead of
// one counter per key.
func routePattern(path string) string {
	if strings.HasPrefix(path, "/kv/") {
		return "/kv/:key"
	}
	return path
}
