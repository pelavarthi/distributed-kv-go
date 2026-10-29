// Package api exposes the store over HTTP: one key per path, JSON
// bodies, and plain status codes (200, 201, 204, 400, 404, 405).
package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/replication"
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
// When repl is set, writes go through replication instead: only
// the leader accepts them, and a write is applied only after a
// majority of nodes hold it. Reads still serve from the local
// store, which holds every committed write.
type Server struct {
	store  *store.Store
	log    *wal.Log
	mux    *http.ServeMux
	repl   *replication.Replicator
	leader func() (bool, int) // isLeader, term
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

// NewWithReplication is NewWithLog with a replicator. The leader
// function reports whether this node currently leads and its term;
// the replicator's apply function, set by the caller, is what
// writes committed entries to the log and store.
func NewWithReplication(s *store.Store, log *wal.Log, repl *replication.Replicator, leader func() (bool, int)) *Server {
	srv := NewWithLog(s, log)
	srv.repl = repl
	srv.leader = leader
	srv.mux.HandleFunc("/cluster/replicate", srv.handleReplicate)
	return srv
}

// HandleReplicate serves POST /cluster/replicate for followers.
// It is also registered by the server binary alongside the other
// cluster endpoints, so a replicator can be reached directly.
func (s *Server) HandleReplicate(w http.ResponseWriter, r *http.Request) {
	s.handleReplicate(w, r)
}

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil {
		writeError(w, http.StatusNotFound, "replication is not configured")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req replication.AppendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	writeJSON(w, http.StatusOK, s.repl.Receive(req))
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
		if s.repl != nil {
			s.handleReplicatedDelete(w, key)
			return
		}
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

// checkLeader reports whether this node may accept a write. A
// follower answers 503 so a client can retry on the leader.
func (s *Server) checkLeader(w http.ResponseWriter) (int, bool) {
	isLeader, term := true, 0
	if s.leader != nil {
		isLeader, term = s.leader()
	}
	if !isLeader {
		writeError(w, http.StatusServiceUnavailable, "not leader")
		return 0, false
	}
	return term, true
}

func (s *Server) handleReplicatedDelete(w http.ResponseWriter, key string) {
	if _, ok := s.store.Get(key); !ok {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	term, ok := s.checkLeader(w)
	if !ok {
		return
	}
	index := s.repl.Append(replication.OpDelete, key, "", time.Time{}, false, term)
	if !s.repl.Replicate(index) {
		writeError(w, http.StatusServiceUnavailable, "could not reach a majority of followers")
		return
	}
	s.repl.Commit(index)
	s.repl.SyncCommit()
	w.WriteHeader(http.StatusNoContent)
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
	if s.repl != nil {
		term, ok := s.checkLeader(w)
		if !ok {
			return
		}
		var expiresAt time.Time
		hasTTL := req.TTLSeconds > 0
		if hasTTL {
			expiresAt = time.Now().Add(time.Duration(req.TTLSeconds) * time.Second)
		}
		index := s.repl.Append(replication.OpSet, key, req.Value, expiresAt, hasTTL, term)
		if !s.repl.Replicate(index) {
			writeError(w, http.StatusServiceUnavailable, "could not reach a majority of followers")
			return
		}
		s.repl.Commit(index)
		s.repl.SyncCommit()
		status := http.StatusOK
		if !existed {
			status = http.StatusCreated
		}
		writeJSON(w, status, keyResponse{Key: key, Value: req.Value})
		return
	}
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
