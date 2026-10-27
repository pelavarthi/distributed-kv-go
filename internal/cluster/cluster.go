// Package cluster tracks which nodes are in the cluster and whether
// they are still alive. Each node sends a small heartbeat to its
// peers on a timer; a peer that stops answering is marked dead,
// and one that starts answering again is marked alive again.
// Nothing here replicates data yet — that comes in a later chunk —
// it only answers "who is here, and who has gone quiet?"
package cluster

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Member is one node in the cluster. Addr is its base URL, used as
// the map key, because a peer's id is only known once it first
// sends a heartbeat.
type Member struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Alive    bool      `json:"alive"`
	LastSeen time.Time `json:"last_seen"`
}

type heartbeatRequest struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Cluster is the membership view for one node.
type Cluster struct {
	selfID   string
	selfAddr string
	interval time.Duration
	timeout  time.Duration

	mu      sync.RWMutex
	members map[string]*Member // keyed by addr
	http    *http.Client

	stopOnce sync.Once
	stop     chan struct{}
}

// New builds a cluster for this node. Peers are known addresses
// that have not been heard from yet, so they start out not alive.
// A peer's id is filled in when its first heartbeat arrives.
func New(selfID, selfAddr string, peerAddrs []string, interval, timeout time.Duration) *Cluster {
	if interval <= 0 {
		interval = time.Second
	}
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	c := &Cluster{
		selfID:   selfID,
		selfAddr: strings.TrimSuffix(selfAddr, "/"),
		interval: interval,
		timeout:  timeout,
		members:  map[string]*Member{},
		http:     &http.Client{Timeout: 2 * time.Second},
		stop:     make(chan struct{}),
	}
	c.members[c.selfAddr] = &Member{ID: selfID, Addr: c.selfAddr, Alive: true, LastSeen: time.Now()}
	for _, addr := range peerAddrs {
		c.AddPeer(addr)
	}
	return c
}

// Self returns this node's own member entry.
func (c *Cluster) Self() Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.members[c.selfAddr]
}

// AddPeer records a peer address. Adding the same address twice,
// or this node's own address, is a no-op.
func (c *Cluster) AddPeer(addr string) {
	addr = strings.TrimSuffix(addr, "/")
	if addr == "" || addr == c.selfAddr {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.members[addr]; !ok {
		c.members[addr] = &Member{ID: addr, Addr: addr}
	}
}

// RemovePeer forgets a peer. Removing this node itself is ignored.
func (c *Cluster) RemovePeer(addr string) {
	addr = strings.TrimSuffix(addr, "/")
	if addr == c.selfAddr {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.members, addr)
}

// ReceiveHeartbeat records a heartbeat from a peer, adding it if
// it was not known yet, and marking it alive either way.
func (c *Cluster) ReceiveHeartbeat(id, addr string) {
	addr = strings.TrimSuffix(addr, "/")
	if addr == "" || addr == c.selfAddr {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.members[addr]
	if !ok {
		m = &Member{Addr: addr}
		c.members[addr] = m
	}
	m.ID = id
	m.Alive = true
	m.LastSeen = time.Now()
}

// IsAlive reports whether the peer at addr is currently alive.
func (c *Cluster) IsAlive(addr string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.members[strings.TrimSuffix(addr, "/")]
	return ok && m.Alive
}

// Members returns a copy of the membership list, sorted by address
// so the output is stable.
func (c *Cluster) Members() []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Member, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

// Sweep marks peers that have not been heard from within the
// timeout as dead. A peer never heard from stays not alive; a
// peer that resumes heartbeating is revived by ReceiveHeartbeat.
func (c *Cluster) Sweep() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for addr, m := range c.members {
		if addr == c.selfAddr {
			continue
		}
		if !m.LastSeen.IsZero() && now.Sub(m.LastSeen) > c.timeout {
			m.Alive = false
		}
	}
}

func (c *Cluster) peerAddrs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var addrs []string
	for addr := range c.members {
		if addr != c.selfAddr {
			addrs = append(addrs, addr)
		}
	}
	sort.Strings(addrs)
	return addrs
}

func (c *Cluster) sendHeartbeat(addr string) {
	body, _ := json.Marshal(heartbeatRequest{ID: c.selfID, Addr: c.selfAddr})
	req, err := http.NewRequest(http.MethodPost, addr+"/cluster/heartbeat", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

// Start begins sending heartbeats and sweeping for dead peers on
// the configured interval. It is safe to call only once.
func (c *Cluster) Start() {
	ticker := time.NewTicker(c.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-ticker.C:
				for _, addr := range c.peerAddrs() {
					c.sendHeartbeat(addr)
				}
				c.Sweep()
			}
		}
	}()
}

// Stop ends the heartbeat loop. Sending stops, but the last
// membership list is kept for inspection.
func (c *Cluster) Stop() {
	c.stopOnce.Do(func() { close(c.stop) })
}

// HandleHeartbeat is the HTTP endpoint peers post heartbeats to.
func (c *Cluster) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.Addr == "" {
		http.Error(w, "id and addr are required", http.StatusBadRequest)
		return
	}
	c.ReceiveHeartbeat(req.ID, req.Addr)
	w.WriteHeader(http.StatusNoContent)
}

// HandleMembers lists the current membership as JSON.
func (c *Cluster) HandleMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string][]Member{"members": c.Members()})
}
