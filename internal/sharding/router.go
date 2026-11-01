package sharding

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Router answers "which node owns this key?" for the live
// membership list, and forwards a request to that node when it
// is not this one. The ring is rebuilt only when the membership
// list changes, so repeated lookups are cheap and the ring stays
// stable while the cluster is quiet.
type Router struct {
	self     string
	replicas int
	members  func() []string
	client   *http.Client

	mu       sync.Mutex
	ring     *Ring
	lastSeen []string
}

// NewRouter builds a router for this node. members must return
// the current alive member addresses, including this node's own
// address; a node missing from that list owns nothing, so it
// forwards everything instead of serving keys it does not own.
func NewRouter(self string, replicas int, members func() []string) *Router {
	if replicas <= 0 {
		replicas = DefaultReplicas
	}
	return &Router{
		self:     strings.TrimSuffix(self, "/"),
		replicas: replicas,
		members:  members,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (rt *Router) refresh() {
	if rt.members == nil {
		return
	}
	addrs := append([]string(nil), rt.members()...)
	sort.Strings(addrs)
	if slices.Equal(addrs, rt.lastSeen) {
		return
	}
	ring := New(rt.replicas)
	for _, addr := range addrs {
		ring.Add(addr)
	}
	rt.ring = ring
	rt.lastSeen = addrs
}

// Owner reports which node owns key, or false if the ring is
// empty.
func (rt *Router) Owner(key string) (string, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.refresh()
	if rt.ring == nil {
		return "", false
	}
	return rt.ring.Owner(key)
}

// IsLocal reports whether this node owns key.
func (rt *Router) IsLocal(key string) bool {
	owner, ok := rt.Owner(key)
	return ok && owner == rt.self
}

// Nodes returns the nodes currently on the ring.
func (rt *Router) Nodes() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.refresh()
	if rt.ring == nil {
		return nil
	}
	return rt.ring.Nodes()
}

// Forward proxies the request to the key's owner and reports
// whether it did. It returns false when this node owns the key,
// or when no owner can be found, and the caller then handles
// the request locally as before.
func (rt *Router) Forward(w http.ResponseWriter, r *http.Request, key string) bool {
	owner, ok := rt.Owner(key)
	if !ok || owner == rt.self {
		return false
	}
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}
	url := owner + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	for name, values := range r.Header {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	resp, err := rt.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	for name, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	return true
}

// HandleShards lists the nodes on the ring, for inspection.
func (rt *Router) HandleShards(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"nodes":    rt.Nodes(),
		"self":     rt.self,
		"replicas": rt.replicas,
	})
}

// HandleRoute answers which node owns the key in ?key=.
func (rt *Router) HandleRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	owner, ok := rt.Owner(key)
	if !ok {
		http.Error(w, "no nodes in ring", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"key":   key,
		"owner": owner,
		"local": owner == rt.self,
	})
}
