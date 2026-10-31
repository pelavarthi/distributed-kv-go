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

func TestReplicateCatchesUpFollowerThatMissedEntries(t *testing.T) {
	follower := New("node2")
	var applied []string
	follower.SetApply(func(e Entry) { applied = append(applied, e.Key) })

	leader := New("node1")
	leader.SetApply(func(e Entry) {})
	leader.SetPeers(func() []string { return []string{"node2"} })
	down := true
	leader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		if down {
			return AppendResponse{}, errors.New("follower is down")
		}
		return follower.Receive(req), nil
	})

	i1 := leader.Append(OpSet, "a", "1", time.Time{}, false, 1)
	i2 := leader.Append(OpSet, "b", "2", time.Time{}, false, 1)
	i3 := leader.Append(OpSet, "c", "3", time.Time{}, false, 1)
	// While the follower is down, nothing replicates.
	if leader.Replicate(i1) || leader.Replicate(i2) || leader.Replicate(i3) {
		t.Fatal("replicate to a down follower succeeded, want failure")
	}
	if follower.LastIndex() != 0 {
		t.Fatalf("follower last index = %d, want 0", follower.LastIndex())
	}

	// The follower returns, and the next write trips over the gap:
	// Replicate must resend the missing entries, not just the new one.
	down = false
	if !leader.Replicate(i3) {
		t.Fatal("replicate after follower returned failed, want success")
	}
	if follower.LastIndex() != 3 {
		t.Fatalf("follower last index = %d, want 3", follower.LastIndex())
	}
	leader.Commit(3)
	leader.SyncCommit()
	if len(applied) != 3 || applied[2] != "c" {
		t.Fatalf("follower applied = %v, want [a b c]", applied)
	}
}

func TestCatchUpReplacesConflictingTail(t *testing.T) {
	follower := New("node2")
	var applied []string
	follower.SetApply(func(e Entry) { applied = append(applied, e.Value) })
	follower.Append(OpSet, "a", "old", time.Time{}, false, 1)
	follower.Append(OpSet, "b", "stale", time.Time{}, false, 1)

	leader := New("node1")
	leader.SetPeers(func() []string { return []string{"node2"} })
	leader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		return follower.Receive(req), nil
	})
	leader.Append(OpSet, "a", "new", time.Time{}, false, 2)
	leader.Append(OpSet, "b", "fresh", time.Time{}, false, 2)

	if leader.CatchUp() != 1 {
		t.Fatal("catch-up did not report the follower synced")
	}
	if follower.LastIndex() != 2 {
		t.Fatalf("follower last index = %d, want 2", follower.LastIndex())
	}
	leader.Commit(2)
	leader.SyncCommit()
	// Applying the commit must surface the leader's values,
	// not the follower's stale ones.
	if len(applied) != 2 || applied[0] != "new" || applied[1] != "fresh" {
		t.Fatalf("follower applied = %v, want [new fresh]", applied)
	}
}

func TestRedriveCommitsTailAfterFailover(t *testing.T) {
	// The old leader sent two entries to a follower but only
	// committed the first before dying. That follower is now the
	// new leader; its log still holds the second entry.
	newLeader := New("node2")
	var applied []string
	newLeader.SetApply(func(e Entry) { applied = append(applied, e.Key) })
	newLeader.Append(OpSet, "a", "1", time.Time{}, false, 1)
	newLeader.Append(OpSet, "b", "2", time.Time{}, false, 1)
	newLeader.Commit(1)
	if len(applied) != 1 {
		t.Fatalf("applied before redrive = %v, want [a]", applied)
	}

	other := New("node3")
	other.SetApply(func(e Entry) {})
	other.Receive(AppendRequest{Term: 1, Leader: "node1", PrevIndex: 0,
		Entries: []Entry{
			{Index: 1, Term: 1, Op: OpSet, Key: "a", Value: "1"},
			{Index: 2, Term: 1, Op: OpSet, Key: "b", Value: "2"},
		},
		LeaderCommit: 1,
	})
	newLeader.SetPeers(func() []string { return []string{"node3", "node4"} })
	newLeader.SetSender(func(addr string, req AppendRequest) (AppendResponse, error) {
		if addr == "node3" {
			return other.Receive(req), nil
		}
		return AppendResponse{}, errors.New("node4 is down")
	})
	// Two of three nodes hold entry 2, so it can commit now.
	if newLeader.Redrive() != 2 {
		t.Fatalf("commit index after redrive = %d, want 2", newLeader.CommitIndex())
	}
	if len(applied) != 2 || applied[1] != "b" {
		t.Fatalf("applied after redrive = %v, want [a b]", applied)
	}
}

func TestNoteTermOnlyMovesForward(t *testing.T) {
	r := New("node1")
	r.Append(OpSet, "a", "1", time.Time{}, false, 2)
	r.NoteTerm(1)
	if r.Term() != 2 {
		t.Fatalf("term after stale note = %d, want 2", r.Term())
	}
	r.NoteTerm(3)
	if r.Term() != 3 {
		t.Fatalf("term after newer note = %d, want 3", r.Term())
	}
}
