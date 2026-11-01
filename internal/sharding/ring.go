// Package sharding splits the keyspace across nodes with a
// consistent hash ring, so adding or removing a node only moves
// the keys that hash onto its part of the ring instead of
// reshuffling everything. Each node gets many virtual points on
// the ring, which keeps the split roughly even even when the
// nodes are few: with one point per node, a single node could
// own most of the ring by luck of the hash.
package sharding

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// DefaultReplicas is how many virtual points each node gets on
// the ring when the caller does not choose a number.
const DefaultReplicas = 100

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// Ring is a consistent hash ring. Node names are addresses here,
// the same strings the cluster package uses for members, so a
// ring can be rebuilt straight from the membership list.
type Ring struct {
	replicas int

	mu      sync.RWMutex
	points  []uint64          // sorted virtual-point hashes
	owner   map[uint64]string // virtual-point hash -> node
	members map[string]bool
}

// New builds an empty ring. A non-positive replica count means
// the default.
func New(replicas int) *Ring {
	if replicas <= 0 {
		replicas = DefaultReplicas
	}
	return &Ring{
		replicas: replicas,
		owner:    map[uint64]string{},
		members:  map[string]bool{},
	}
}

// Add puts a node on the ring. Adding the same node twice is a
// no-op, so rebuilding a ring from a membership list is safe.
func (r *Ring) Add(node string) {
	if node == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.members[node] {
		return
	}
	r.members[node] = true
	for i := 0; i < r.replicas; i++ {
		point := hashKey(fmt.Sprintf("%s#%d", node, i))
		r.points = append(r.points, point)
		r.owner[point] = node
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
}

// Remove takes a node off the ring. Its keys fall to the next
// node clockwise, and keys owned by other nodes do not move.
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.members[node] {
		return
	}
	delete(r.members, node)
	kept := r.points[:0]
	for _, point := range r.points {
		if r.owner[point] == node {
			delete(r.owner, point)
			continue
		}
		kept = append(kept, point)
	}
	r.points = kept
}

// Nodes returns the nodes on the ring, sorted for stable output.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.members))
	for node := range r.members {
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

// Owner returns the node that owns key: the first node clockwise
// from the key's hash, wrapping around the end of the ring. It
// reports false when the ring is empty.
func (r *Ring) Owner(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", false
	}
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= hashKey(key) })
	if i == len(r.points) {
		i = 0
	}
	return r.owner[r.points[i]], true
}

// Replicas returns up to n distinct nodes for key, starting
// with its owner and walking clockwise. This is the set of
// nodes a key would be stored on with n-way replication.
func (r *Ring) Replicas(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	if n > len(r.members) {
		n = len(r.members)
	}
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= hashKey(key) })
	var out []string
	seen := map[string]bool{}
	for i := 0; len(out) < n && i < len(r.points); i++ {
		node := r.owner[r.points[(start+i)%len(r.points)]]
		if seen[node] {
			continue
		}
		seen[node] = true
		out = append(out, node)
	}
	return out
}
