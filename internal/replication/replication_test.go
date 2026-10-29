package replication

import (
	"errors"
	"testing"
	"time"
)

func TestAppendAndCommitAppliesInOrder(t *testing.T) {
	r := New("node1")
	var applied []string
	r.SetApply(func(e Entry) { applied = append(applied, e.Key) })

	i1 := r.Append(OpSet, "a", "1", time.Time{}, false, 1)
	i2 := r.Append(OpSet, "b", "2", time.Time{}, false, 1)
	if i1 != 1 || i2 != 2 {
		t.Fatalf("indexes = %d, %d, want 1, 2", i1, i2)
	}
	if len(applied) != 0 {
		t.Fatalf("applied before commit = %v, want none", applied)
	}
	r.Commit(2)
	if len(applied) != 2 || applied[0] != "a" || applied[1] != "b" {
		t.Fatalf("applied = %v, want [a b]", applied)
	}
	if r.CommitIndex() != 2 {
		t.Fatalf("commit index = %d, want 2", r.CommitIndex())
	}
	// Commit never moves backwards.
	r.Commit(1)
	if r.CommitIndex() != 2 {
		t.Fatalf("commit index after backward commit = %d, want 2", r.CommitIndex())
	}
}

func TestReceiveRejectsStalePrevAndAppliesToLeaderCommit(t *testing.T) {
	follower := New("node2")
	var applied []string
	follower.SetApply(func(e Entry) { applied = append(applied, e.Key) })

	// Follower's log does not contain the leader's previous entry.
	resp := follower.Receive(AppendRequest{
		Term: 1, Leader: "node1", PrevIndex: 1, PrevTerm: 1,
		Entries: []Entry{{Index: 2, Term: 1, Op: OpSet, Key: "b", Value: "2"}},
	})
	if resp.Success {
		t.Fatal("receive with missing prev entry succeeded, want failure")
	}
	if len(applied) != 0 {
		t.Fatalf("applied = %v, want none", applied)
	}

	// Now the entries arrive in order, but the leader has only
	// committed the first one, so only it is applied.
	resp = follower.Receive(AppendRequest{
		Term: 1, Leader: "node1", PrevIndex: 0,
		Entries: []Entry{
			{Index: 1, Term: 1, Op: OpSet, Key: "a", Value: "1"},
			{Index: 2, Term: 1, Op: OpSet, Key: "b", Value: "2"},
		},
		LeaderCommit: 1,
	})
	if !resp.Success || resp.MatchIndex != 2 {
		t.Fatalf("receive = %+v, want success with match 2", resp)
	}
	if len(applied) != 1 || applied[0] != "a" {
		t.Fatalf("applied = %v, want [a]", applied)
	}
	if follower.CommitIndex() != 1 {
		t.Fatalf("commit index = %d, want 1", follower.CommitIndex())
	}
}

func TestReceiveReplacesConflictingEntry(t *testing.T) {
	follower := New("node2")
	follower.Append(OpSet, "a", "old", time.Time{}, false, 1)
	var applied []string
	follower.SetApply(func(e Entry) { applied = append(applied, e.Value) })
	resp := follower.Receive(AppendRequest{
		Term: 2, Leader: "node1", PrevIndex: 0,
		Entries:      []Entry{{Index: 1, Term: 2, Op: OpSet, Key: "a", Value: "new"}},
		LeaderCommit: 1,
	})
	if !resp.Success {
		t.Fatalf("receive = %+v, want success", resp)
	}
	if follower.LastIndex() != 1 {
		t.Fatalf("last index = %d, want 1", follower.LastIndex())
	}
	if len(applied) != 1 || applied[0] != "new" {
		t.Fatalf("applied = %v, want [new]", applied)
	}
}

func TestReceiveRejectsStaleTerm(t *testing.T) {
	follower := New("node2")
	follower.Receive(AppendRequest{Term: 3, Leader: "node1", PrevIndex: 0})
	resp := follower.Receive(AppendRequest{
		Term: 2, Leader: "node1", PrevIndex: 0,
		Entries: []Entry{{Index: 1, Term: 2, Op: OpSet, Key: "a", Value: "1"}},
	})
	if resp.Success {
		t.Fatal("receive with stale term succeeded, want failure")
	}
	if follower.LastIndex() != 0 {
		t.Fatalf("last index = %d, want 0", follower.LastIndex())
	}
}

func TestReplicateNeedsMajority(t *testing.T) {
	leader := New("node1")
	leader.SetPeers(func() []string { return []string{"node2", "node3", "node4"} })

	// Only one of three followers acknowledges: leader + 1 is
	// 2 of 4, not a majority.
	leader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		if addr == "node2" {
			return AppendResponse{Success: true, MatchIndex: req.Entries[0].Index}, nil
		}
		return AppendResponse{}, errors.New("unreachable")
	})
	idx := leader.Append(OpSet, "a", "1", time.Time{}, false, 1)
	if leader.Replicate(idx) {
		t.Fatal("replicate with 2 of 4 nodes succeeded, want failure")
	}
	if leader.CommitIndex() != 0 {
		t.Fatalf("commit index = %d, want 0", leader.CommitIndex())
	}

	// Two of three followers acknowledge: leader + 2 is 3 of 4.
	leader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		if addr == "node4" {
			return AppendResponse{}, errors.New("unreachable")
		}
		return AppendResponse{Success: true, MatchIndex: req.Entries[0].Index}, nil
	})
	if !leader.Replicate(idx) {
		t.Fatal("replicate with 3 of 4 nodes failed, want success")
	}
	leader.Commit(idx)
	if leader.CommitIndex() != 1 {
		t.Fatalf("commit index = %d, want 1", leader.CommitIndex())
	}
}

func TestReplicateSingleNodeAlwaysCommits(t *testing.T) {
	leader := New("node1")
	leader.SetPeers(func() []string { return nil })
	idx := leader.Append(OpSet, "a", "1", time.Time{}, false, 1)
	if !leader.Replicate(idx) {
		t.Fatal("single-node replicate failed, want success")
	}
}
