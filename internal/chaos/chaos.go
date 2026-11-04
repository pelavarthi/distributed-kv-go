// Package chaos is a small fault injector for partition tests.
// It wraps a replication sender and drops every request headed
// for a cut-off node, the way a real network partition drops
// packets, without needing real servers or firewall rules. Cut a
// node, run a write, and see which side of the partition can
// still commit; heal it, and the cluster has to converge again.
package chaos

import (
	"errors"
	"sync"

	"github.com/pelavarthi/distributed-kv-go/internal/replication"
)

var errPartitioned = errors.New("partitioned: node is cut off")

// Partition tracks which nodes are currently cut off.
type Partition struct {
	mu  sync.Mutex
	cut map[string]bool
}

// New builds a partition with nothing cut off.
func New() *Partition {
	return &Partition{cut: map[string]bool{}}
}

// Cut drops all traffic to addr until Heal is called.
func (p *Partition) Cut(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut[addr] = true
}

// Heal lets traffic to addr through again.
func (p *Partition) Heal(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cut, addr)
}

// IsCut reports whether addr is currently cut off.
func (p *Partition) IsCut(addr string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cut[addr]
}

// Wrap returns a sender that behaves like next, except requests
// to cut-off nodes fail before they are sent. The error is a
// plain delivery failure, so the caller cannot tell a partition
// from a crashed node, which is exactly the point: the
// replication code must stay safe either way.
func (p *Partition) Wrap(next replication.Sender) replication.Sender {
	return func(addr string, req replication.AppendRequest) (replication.AppendResponse, error) {
		if p.IsCut(addr) {
			return replication.AppendResponse{}, errPartitioned
		}
		return next(addr, req)
	}
}
