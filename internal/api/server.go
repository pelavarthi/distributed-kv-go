// Package api exposes the store over HTTP: one key per path, JSON
// bodies, and plain status codes (200, 201, 204, 400, 404, 405).
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

type putRequest struct {
	Value      string `json:"value"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

type keyResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// Server is an http.Handler around a store. When log is set, every
// mutation is appended to the log before it touches the store.
type Server struct {
	store *store.Store
	log   *wal.Log
	mux   *http.ServeMux
}

func New(s *store.Store) *Server {
	return NewWithLog(s, nil)
}

// NewWithLog is New with a write-ahead log for durability.
func NewWithLog(s *store.Store, log *wal.Log) *Server {
	srv := &Server{store: s, log: log, mux: http.NewServeMux()}
	srv.mux.HandleFunc("/health", srv.handleHealth)
	srv.mux.HandleFunc("/kv", srv.handleList)
	srv.mux.HandleFunc("/kv/", srv.handleKey)
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"keys": s.store.Keys()})
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		value, ok := s.store.Get(key)
		if !ok {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		writeJSON(w, http.StatusOK, keyResponse{Key: key, Value: value})
	case http.MethodDelete:
		if s.log != nil {
			if err := s.log.AppendDelete(key); err != nil {
				writeError(w, http.StatusInternalServerError, "failed to write log")
				return
			}
		}
		if !s.store.Delete(key) {
			writeError(w, http.StatusNotFound, "key not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.TTLSeconds < 0 {
		writeError(w, http.StatusBadRequest, "ttl_seconds must not be negative")
		return
	}
	_, existed := s.store.Get(key)
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if s.log != nil {
		if err := s.log.AppendSet(key, req.Value, ttl); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to write log")
			return
		}
	}
	s.store.Set(key, req.Value, ttl)
	status := http.StatusOK
	if !existed {
		status = http.StatusCreated
	}
	writeJSON(w, status, keyResponse{Key: key, Value: req.Value})
}
