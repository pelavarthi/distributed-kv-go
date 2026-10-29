// Package replication copies the leader's log to its followers and
// only applies a write once a majority of nodes hold it. The leader
// appends a write to its own log, sends it to every alive follower,
// and advances the commit index when enough followers acknowledge
// it. A follower checks that the entry before the new ones matches
// its own log, replaces any conflicting suffix, and applies entries
// up to the leader's commit index, in order.
//
// A write that cannot reach a majority is left uncommitted in the
// log and never applied, so a lone leader cannot pretend a write
// is durable. This chunk sends only the newest entry, so a follower
// that missed earlier entries rejects the append until it catches
// up; that catch-up is the next chunk's job.
package replication

import (
	"sync"
	"time"
)

const (
	OpSet    = "set"
	OpDelete = "delete"
)

// Entry is one replicated write. Index starts at 1 and never
// reuses a number, and Term is the leader's term when the entry
// was appended, so a conflicting entry at the same index can be
// told apart from the one it replaces.
type Entry struct {
	Index     uint64    `json:"index"`
	Term      int       `json:"term"`
	Op        string    `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	HasTTL    bool      `json:"has_ttl,omitempty"`
}

// AppendRequest is what the leader sends a follower.
type AppendRequest struct {
	Term         int     `json:"term"`
	Leader       string  `json:"leader"`
	PrevIndex    uint64  `json:"prev_index"`
	PrevTerm     int     `json:"prev_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// AppendResponse is what a follower sends back. MatchIndex is the
// highest index the follower now holds, whether or not it applied it.
type AppendResponse struct {
	Success    bool   `json:"success"`
	MatchIndex uint64 `json:"match_index"`
	Term       int    `json:"term"`
}

// Sender delivers one append request to a follower. The server
// wires this to an HTTP POST; tests substitute a fake.
type Sender func(addr string, req AppendRequest) (AppendResponse, error)

// Apply is called, in log order, for every entry as it commits.
// The server wires it to writing the WAL and applying the store.
type Apply func(e Entry)

// Replicator holds one node's log and commit state.
type Replicator struct {
	selfID string

	mu          sync.Mutex
	term        int
	entries     []Entry // index i lives at entries[i-1]
	commitIndex uint64
	lastApplied uint64
	apply       Apply
	sender      Sender
	peers       func() []string // alive follower addresses
}

// New builds a replicator with no entries and no commit yet.
func New(selfID string) *Replicator {
	return &Replicator{selfID: selfID}
}

// SetApply registers the function run for each committed entry.
func (r *Replicator) SetApply(apply Apply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apply = apply
}

// SetSender registers how append requests reach followers.
func (r *Replicator) SetSender(sender Sender) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sender = sender
}

// SetPeers registers how the current alive followers are found.
func (r *Replicator) SetPeers(peers func() []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = peers
}

// Append adds an entry to this node's log and returns its index.
// It does not commit or apply anything on its own.
func (r *Replicator) Append(op, key, value string, expiresAt time.Time, hasTTL bool, term int) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if term > r.term {
		r.term = term
	}
	e := Entry{
		Index:     uint64(len(r.entries)) + 1,
		Term:      term,
		Op:        op,
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
		HasTTL:    hasTTL,
	}
	r.entries = append(r.entries, e)
	return e.Index
}

// LastIndex returns the highest log index, or 0 for an empty log.
func (r *Replicator) LastIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return uint64(len(r.entries))
}

// CommitIndex returns the highest committed index.
func (r *Replicator) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// Term returns the highest term this node has seen.
func (r *Replicator) Term() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.term
}

// entryAt returns the entry at index, or false if the log is shorter.
func (r *Replicator) entryAt(index uint64) (Entry, bool) {
	if index == 0 || index > uint64(len(r.entries)) {
		return Entry{}, false
	}
	return r.entries[index-1], true
}

// applyUpTo applies every unapplied entry up to index, in order.
// The caller must hold the lock.
func (r *Replicator) applyUpTo(index uint64) {
	apply := r.apply
	for r.lastApplied < index {
		e, ok := r.entryAt(r.lastApplied + 1)
		if !ok {
			return
		}
		r.lastApplied++
		if apply != nil {
			// Apply under the lock so entries can never be
			// applied out of order by a concurrent caller.
			apply(e)
		}
	}
}

// Commit advances the commit index to index and applies every
// newly committed entry. It never moves the index backwards.
func (r *Replicator) Commit(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index > r.commitIndex && index <= uint64(len(r.entries)) {
		r.commitIndex = index
	}
	r.applyUpTo(r.commitIndex)
}

// Replicate sends the entry at index to every alive follower and
// reports whether a majority of nodes now hold it. The leader
// itself counts as one vote, so a single-node cluster always
// commits. A follower that already holds the entry, or holds a
// later one, counts as an acknowledgement.
func (r *Replicator) Replicate(index uint64) bool {
	r.mu.Lock()
	entry, ok := r.entryAt(index)
	if !ok {
		r.mu.Unlock()
		return false
	}
	sender := r.sender
	peersFn := r.peers
	prevIndex := index - 1
	prevTerm := 0
	if prevIndex > 0 {
		if prev, ok := r.entryAt(prevIndex); ok {
			prevTerm = prev.Term
		}
	}
	term := r.term
	commit := r.commitIndex
	r.mu.Unlock()

	var peers []string
	if peersFn != nil {
		peers = peersFn()
	}
	if sender == nil || len(peers) == 0 {
		return true
	}

	req := AppendRequest{
		Term:         term,
		Leader:       r.selfID,
		PrevIndex:    prevIndex,
		PrevTerm:     prevTerm,
		Entries:      []Entry{entry},
		LeaderCommit: commit,
	}
	acks := 1 // the leader itself
	for _, addr := range peers {
		resp, err := sender(addr, req)
		if err != nil {
			continue
		}
		if resp.Success && resp.MatchIndex >= index {
			acks++
		}
	}
	// Majority of the whole cluster, leader included.
	return acks*2 > len(peers)+1
}

// SyncCommit tells followers the current commit index, with no
// new entries. The leader calls this after committing, because a
// follower that only received the entry does not know it is safe
// to apply yet; without this step it would hold the entry forever
// without applying it.
func (r *Replicator) SyncCommit() {
	r.mu.Lock()
	sender := r.sender
	peersFn := r.peers
	lastIndex := uint64(len(r.entries))
	lastTerm := 0
	if lastIndex > 0 {
		lastTerm = r.entries[lastIndex-1].Term
	}
	req := AppendRequest{
		Term:         r.term,
		Leader:       r.selfID,
		PrevIndex:    lastIndex,
		PrevTerm:     lastTerm,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	if sender == nil || peersFn == nil {
		return
	}
	for _, addr := range peersFn() {
		_, _ = sender(addr, req)
	}
}

// Receive handles one append request from a leader. It returns
// success and the highest index this node now holds.
func (r *Replicator) Receive(req AppendRequest) AppendResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Term < r.term {
		return AppendResponse{Success: false, MatchIndex: uint64(len(r.entries)), Term: r.term}
	}
	if req.Term > r.term {
		r.term = req.Term
	}
	if req.PrevIndex > 0 {
		prev, ok := r.entryAt(req.PrevIndex)
		if !ok || prev.Term != req.PrevTerm {
			return AppendResponse{Success: false, MatchIndex: uint64(len(r.entries)), Term: r.term}
		}
	}
	for _, e := range req.Entries {
		if existing, ok := r.entryAt(e.Index); ok {
			if existing.Term == e.Term {
				continue
			}
			// Conflicting entry: drop it and everything after.
			r.entries = r.entries[:e.Index-1]
		}
		if e.Index != uint64(len(r.entries))+1 {
			// Gap in the log; the follower needs entries it
			// missed before this one can be appended.
			return AppendResponse{Success: false, MatchIndex: uint64(len(r.entries)), Term: r.term}
		}
		r.entries = append(r.entries, e)
	}
	if req.LeaderCommit > r.commitIndex {
		last := uint64(len(r.entries))
		if req.LeaderCommit < last {
			last = req.LeaderCommit
		}
		r.commitIndex = last
		r.applyUpTo(r.commitIndex)
	}
	return AppendResponse{Success: true, MatchIndex: uint64(len(r.entries)), Term: r.term}
}

// Status is the replication state for monitoring endpoints.
type Status struct {
	Leader      string `json:"leader"`
	Term        int    `json:"term"`
	LastIndex   uint64 `json:"last_index"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
}

// Status returns a snapshot of this node's replication state.
func (r *Replicator) Status(leader string) Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		Leader:      leader,
		Term:        r.term,
		LastIndex:   uint64(len(r.entries)),
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
	}
}
