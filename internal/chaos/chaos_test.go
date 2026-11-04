package chaos_test

import (
	"testing"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/chaos"
	"github.com/pelavarthi/distributed-kv-go/internal/replication"
)

// TestPartitionDropsAndHeals checks the injector itself: a cut
// node gets errors, everyone else still gets through, and healing
// restores delivery without rebuilding anything.
func TestPartitionDropsAndHeals(t *testing.T) {
	p := chaos.New()
	calls := 0
	next := func(addr string, req replication.AppendRequest) (replication.AppendResponse, error) {
		calls++
		return replication.AppendResponse{Success: true, MatchIndex: 1}, nil
	}
	sender := p.Wrap(next)

	if _, err := sender("node2", replication.AppendRequest{}); err != nil {
		t.Fatalf("send before cut failed: %v", err)
	}
	p.Cut("node2")
	if _, err := sender("node2", replication.AppendRequest{}); err == nil {
		t.Fatal("send to cut node succeeded, want failure")
	}
	if _, err := sender("node3", replication.AppendRequest{}); err != nil {
		t.Fatalf("send to uncut node failed: %v", err)
	}
	p.Heal("node2")
	if _, err := sender("node2", replication.AppendRequest{}); err != nil {
		t.Fatalf("send after heal failed: %v", err)
	}
	if calls != 3 {
		t.Fatalf("delivered calls = %d, want 3", calls)
	}
}

// TestPartitionScenario runs a whole partition through real
// replicators: cut the leader off from both followers, confirm
// its write cannot commit, heal one follower, and confirm the
// majority side commits and the healed follower applies it.
func TestPartitionScenario(t *testing.T) {
	followers := map[string]*replication.Replicator{}
	applied := map[string][]string{}
	for _, id := range []string{"node2", "node3"} {
		f := replication.New(id)
		f.SetApply(func(e replication.Entry) {
			applied[id] = append(applied[id], e.Value)
		})
		followers[id] = f
	}

	partition := chaos.New()
	leader := replication.New("node1")
	leader.SetApply(func(e replication.Entry) {})
	leader.SetClusterSize(func() int { return 3 })
	alive := []string{"node2", "node3"}
	leader.SetPeers(func() []string { return alive })
	leader.SetSender(partition.Wrap(func(addr string, req replication.AppendRequest) (replication.AppendResponse, error) {
		return followers[addr].Receive(req), nil
	}))

	// Partition: both followers cut, and the leader's view of
	// alive followers shrinks to nothing, as the server wires it.
	partition.Cut("node2")
	partition.Cut("node3")
	alive = nil
	idx := leader.Append(replication.OpSet, "a", "alone", time.Time{}, false, 1)
	if leader.Replicate(idx) {
		t.Fatal("cut-off leader committed, want failure")
	}

	// Heal one follower: 2 of 3 is a majority again.
	partition.Heal("node2")
	alive = []string{"node2"}
	if !leader.Replicate(idx) {
		t.Fatal("majority leader failed to replicate after heal, want success")
	}
	leader.Commit(idx)
	leader.SyncCommit()
	if len(applied["node2"]) != 1 || applied["node2"][0] != "alone" {
		t.Fatalf("healed follower applied = %v, want [alone]", applied["node2"])
	}
	if len(applied["node3"]) != 0 {
		t.Fatalf("still-cut follower applied = %v, want none", applied["node3"])
	}
}
