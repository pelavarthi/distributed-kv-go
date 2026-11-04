package replication

import (
	"errors"
	"testing"
	"time"
)

// TestPartitionedMinorityCannotCommit reproduces the split-brain
// bug this chunk fixes. In a 3-node cluster, a leader cut off from
// both followers sees no alive peers. Before the fix, Replicate
// treated "no peers to ask" as success and committed on its own,
// so the minority side and the majority side could each commit a
// different value for the same key. Counted against the whole
// cluster, one node out of three is not a majority.
func TestPartitionedMinorityCannotCommit(t *testing.T) {
	leader := New("node1")
	var applied []string
	leader.SetApply(func(e Entry) { applied = append(applied, e.Key) })
	leader.SetClusterSize(func() int { return 3 })
	// The server only passes alive followers here, and after a
	// partition there are none left.
	leader.SetPeers(func() []string { return nil })
	leader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		return AppendResponse{}, errors.New("partitioned")
	})

	idx := leader.Append(OpSet, "a", "minority", time.Time{}, false, 1)
	if leader.Replicate(idx) {
		t.Fatal("partitioned minority leader committed, want failure")
	}
	// The server only commits after Replicate succeeds, so the
	// entry must sit uncommitted and unapplied.
	if leader.CommitIndex() != 0 {
		t.Fatalf("commit index = %d, want 0", leader.CommitIndex())
	}
	if len(applied) != 0 {
		t.Fatalf("applied = %v, want none", applied)
	}
}

// TestPartitionedMajorityStillCommits covers the other side of
// the same partition: the leader with one of two followers alive
// is 2 of 3, a majority, so its write must still go through even
// though the third node is unreachable.
func TestPartitionedMajorityStillCommits(t *testing.T) {
	follower := New("node2")
	var applied []string
	follower.SetApply(func(e Entry) { applied = append(applied, e.Value) })

	leader := New("node1")
	leader.SetApply(func(e Entry) {})
	leader.SetClusterSize(func() int { return 3 })
	leader.SetPeers(func() []string { return []string{"node2"} })
	leader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		return follower.Receive(req), nil
	})

	idx := leader.Append(OpSet, "a", "majority", time.Time{}, false, 1)
	if !leader.Replicate(idx) {
		t.Fatal("majority leader failed to replicate, want success")
	}
	leader.Commit(idx)
	leader.SyncCommit()
	if len(applied) != 1 || applied[0] != "majority" {
		t.Fatalf("follower applied = %v, want [majority]", applied)
	}
}

// TestSingleNodeStillCommitsWithClusterSize makes sure the fix
// does not break the one case where committing alone is correct:
// a cluster that really has only one node.
func TestSingleNodeStillCommitsWithClusterSize(t *testing.T) {
	leader := New("node1")
	leader.SetClusterSize(func() int { return 1 })
	leader.SetPeers(func() []string { return nil })

	idx := leader.Append(OpSet, "a", "1", time.Time{}, false, 1)
	if !leader.Replicate(idx) {
		t.Fatal("single-node replicate failed, want success")
	}
}

// TestHealAndCatchUpAfterPartition finishes the partition story:
// the cut-off follower comes back, the leader catches it up, and
// it ends up with the majority's value, not a value the minority
// side wrote while it was alone.
func TestHealAndCatchUpAfterPartition(t *testing.T) {
	follower := New("node3")
	var applied []string
	follower.SetApply(func(e Entry) { applied = append(applied, e.Value) })
	// While partitioned, this node was its own minority leader
	// and appended a conflicting value it never committed.
	// The healed majority leader runs at a newer term, so its
	// entry replaces the stale one instead of being rejected.
	follower.Append(OpSet, "a", "stale", time.Time{}, false, 1)

	leader := New("node1")
	leader.SetApply(func(e Entry) {})
	leader.SetClusterSize(func() int { return 3 })
	leader.SetPeers(func() []string { return []string{"node3"} })
	leader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		return follower.Receive(req), nil
	})
	leader.Append(OpSet, "a", "majority", time.Time{}, false, 2)

	if leader.CatchUp() != 1 {
		t.Fatal("catch-up after heal did not report the follower synced")
	}
	leader.Commit(1)
	leader.SyncCommit()
	if len(applied) != 1 || applied[0] != "majority" {
		t.Fatalf("follower applied = %v, want [majority]", applied)
	}
}
