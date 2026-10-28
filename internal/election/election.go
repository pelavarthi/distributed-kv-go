// Package election picks one leader from the cluster's membership
// list. It is deliberately simple: the alive member with the highest
// id wins, the way the bully algorithm ends up, but without vote
// messages. The membership list already carries liveness from the
// heartbeat chunk, so every node that sees the same list picks the
// same leader, and when the leader goes quiet the next-highest
// alive member takes over on the next refresh.
//
// This is enough to decide who coordinates the next chunk's
// replication, but it is not a consensus election: two nodes with
// different membership views can briefly disagree, so a real system
// would still need terms and votes (as in Raft) before letting the
// leader commit writes.
package election

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/pelavarthi/distributed-kv-go/internal/cluster"
)

// Election tracks the current leader for one node.
type Election struct {
	selfID string

	mu     sync.RWMutex
	leader string
	term   int // counts how many times leadership has changed
}

// New builds an election for a node. No leader is known yet; the
// first Refresh picks one.
func New(selfID string) *Election {
	return &Election{selfID: selfID}
}

// Refresh recomputes the leader from the membership list and
// reports whether it changed. Only alive members can lead. The
// term goes up by one on every change, including the first pick,
// so callers can tell a new leader took over even if they missed
// the previous one.
func (e *Election) Refresh(members []cluster.Member) (leader string, changed bool) {
	pick := ""
	for _, m := range members {
		if m.Alive && m.ID != "" && m.ID > pick {
			pick = m.ID
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if pick == "" || pick == e.leader {
		return e.leader, false
	}
	e.leader = pick
	e.term++
	return e.leader, true
}

// Leader returns the current leader id and term. An empty id means
// no leader has been picked yet.
func (e *Election) Leader() (string, int) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leader, e.term
}

// IsLeader reports whether this node is the current leader.
func (e *Election) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leader != "" && e.leader == e.selfID
}

type leaderResponse struct {
	Leader string `json:"leader"`
	Term   int    `json:"term"`
}

// HandleLeader serves GET /cluster/leader. It refreshes from the
// given membership function on every request, so the answer always
// reflects the latest heartbeats.
func (e *Election) HandleLeader(members func() []cluster.Member) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		leader, _ := e.Refresh(members())
		_, term := e.Leader()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(leaderResponse{Leader: leader, Term: term})
	}
}
