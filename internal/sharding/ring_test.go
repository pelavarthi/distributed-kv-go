package sharding

import (
	"fmt"
	"testing"
)

func TestEmptyRingHasNoOwner(t *testing.T) {
	r := New(10)
	if _, ok := r.Owner("anything"); ok {
		t.Fatal("empty ring should have no owner")
	}
	if got := r.Replicas("anything", 2); got != nil {
		t.Fatalf("empty ring replicas = %v, want nil", got)
	}
}

func TestSingleNodeOwnsEverything(t *testing.T) {
	r := New(10)
	r.Add("node1")
	for i := 0; i < 50; i++ {
		owner, ok := r.Owner(fmt.Sprintf("key-%d", i))
		if !ok || owner != "node1" {
			t.Fatalf("key-%d owner = %q, %v, want node1, true", i, owner, ok)
		}
	}
}

func TestKeysSpreadAcrossNodes(t *testing.T) {
	r := New(DefaultReplicas)
	for _, n := range []string{"node1", "node2", "node3"} {
		r.Add(n)
	}
	counts := map[string]int{}
	for i := 0; i < 3000; i++ {
		owner, _ := r.Owner(fmt.Sprintf("key-%d", i))
		counts[owner]++
	}
	for node, count := range counts {
		if count < 600 || count > 1500 {
			t.Fatalf("node %s owns %d of 3000 keys, want a roughly even share", node, count)
		}
	}
}

func TestAddingNodeOnlyMovesItsOwnKeys(t *testing.T) {
	r := New(DefaultReplicas)
	for _, n := range []string{"node1", "node2", "node3"} {
		r.Add(n)
	}
	before := map[string]string{}
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("key-%d", i)
		owner, _ := r.Owner(key)
		before[key] = owner
	}
	r.Add("node4")
	moved := 0
	for key, old := range before {
		owner, _ := r.Owner(key)
		if owner != old {
			moved++
			if owner != "node4" {
				t.Fatalf("key %s moved from %s to %s, want only node4 to gain keys", key, old, owner)
			}
		}
	}
	if moved == 0 || moved > 1500 {
		t.Fatalf("adding node4 moved %d of 3000 keys, want roughly a quarter", moved)
	}
}

func TestRemovingNodeMovesOnlyItsKeys(t *testing.T) {
	r := New(DefaultReplicas)
	for _, n := range []string{"node1", "node2", "node3"} {
		r.Add(n)
	}
	before := map[string]string{}
	for i := 0; i < 3000; i++ {
		key := fmt.Sprintf("key-%d", i)
		owner, _ := r.Owner(key)
		before[key] = owner
	}
	r.Remove("node2")
	for key, old := range before {
		owner, _ := r.Owner(key)
		if old != "node2" && owner != old {
			t.Fatalf("key %s moved from %s to %s on removing an unrelated node", key, old, owner)
		}
		if owner == "node2" {
			t.Fatalf("key %s still owned by removed node2", key)
		}
	}
}

func TestReplicasAreDistinctAndStartWithOwner(t *testing.T) {
	r := New(DefaultReplicas)
	for _, n := range []string{"node1", "node2", "node3"} {
		r.Add(n)
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		owner, _ := r.Owner(key)
		got := r.Replicas(key, 2)
		if len(got) != 2 || got[0] != owner || got[1] == owner {
			t.Fatalf("key %s replicas = %v, want [owner %s, another node]", key, got, owner)
		}
		if got := r.Replicas(key, 99); len(got) != 3 {
			t.Fatalf("replicas capped at node count = %v, want 3", got)
		}
	}
}

func TestSameRingGivesSameOwner(t *testing.T) {
	build := func() *Ring {
		r := New(50)
		r.Add("node2")
		r.Add("node1")
		return r
	}
	a, b := build(), build()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		oa, _ := a.Owner(key)
		ob, _ := b.Owner(key)
		if oa != ob {
			t.Fatalf("key %s owner differs across identical rings: %s vs %s", key, oa, ob)
		}
	}
}
