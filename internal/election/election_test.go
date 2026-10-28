package election

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pelavarthi/distributed-kv-go/internal/cluster"
)

func members(ids ...string) []cluster.Member {
	out := make([]cluster.Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, cluster.Member{ID: id, Addr: "http://" + id, Alive: true})
	}
	return out
}

func TestHighestAliveMemberWins(t *testing.T) {
	e := New("node1")
	leader, changed := e.Refresh(members("node1", "node3", "node2"))
	if !changed || leader != "node3" {
		t.Fatalf("want leader node3 with a change, got %q changed=%v", leader, changed)
	}
	if got, term := e.Leader(); got != "node3" || term != 1 {
		t.Fatalf("want leader node3 term 1, got %q term %d", got, term)
	}
	if e.IsLeader() {
		t.Fatal("node1 should not be leader while node3 is alive")
	}
}

func TestDeadLeaderFallsToNextHighest(t *testing.T) {
	e := New("node1")
	e.Refresh(members("node1", "node2", "node3"))
	dead := members("node1", "node2")
	dead = append(dead, cluster.Member{ID: "node3", Addr: "http://node3"})
	leader, changed := e.Refresh(dead)
	if !changed || leader != "node2" {
		t.Fatalf("want leader node2 after node3 dies, got %q changed=%v", leader, changed)
	}
	if _, term := e.Leader(); term != 2 {
		t.Fatalf("want term 2 after a leadership change, got %d", term)
	}
}

func TestRevivedLeaderRetakesLeadership(t *testing.T) {
	e := New("node1")
	e.Refresh(members("node1", "node2", "node3"))
	e.Refresh(members("node1", "node2"))
	leader, changed := e.Refresh(members("node1", "node2", "node3"))
	if !changed || leader != "node3" {
		t.Fatalf("want node3 back as leader, got %q changed=%v", leader, changed)
	}
	if _, term := e.Leader(); term != 3 {
		t.Fatalf("want term 3 after two changes, got %d", term)
	}
}

func TestSameLeaderDoesNotBumpTerm(t *testing.T) {
	e := New("node1")
	e.Refresh(members("node1", "node2"))
	e.Refresh(members("node1", "node2"))
	if _, term := e.Leader(); term != 1 {
		t.Fatalf("want term to stay 1, got %d", term)
	}
}

func TestSelfBecomesLeaderWhenHighest(t *testing.T) {
	e := New("node3")
	e.Refresh(members("node1", "node2", "node3"))
	if !e.IsLeader() {
		t.Fatal("highest alive node should see itself as leader")
	}
}

func TestNoAliveMembersMeansNoLeader(t *testing.T) {
	e := New("node1")
	leader, changed := e.Refresh(nil)
	if changed || leader != "" {
		t.Fatalf("want no leader and no change, got %q changed=%v", leader, changed)
	}
	if _, term := e.Leader(); term != 0 {
		t.Fatalf("want term 0 with no leader, got %d", term)
	}
}

func TestHandleLeader(t *testing.T) {
	e := New("node1")
	current := members("node1", "node2")
	handler := e.HandleLeader(func() []cluster.Member { return current })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/cluster/leader", nil)
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/cluster/leader", nil)
	handler(rec, req)
	var out struct {
		Leader string `json:"leader"`
		Term   int    `json:"term"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Leader != "node2" || out.Term != 1 {
		t.Fatalf("want leader node2 term 1, got %+v", out)
	}

	// The handler refreshes on every request, so a dead leader is
	// reflected without any extra call.
	current = members("node1")
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/cluster/leader", nil)
	handler(rec, req)
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Leader != "node1" || out.Term != 2 {
		t.Fatalf("want leader node1 term 2 after node2 dies, got %+v", out)
	}
}
