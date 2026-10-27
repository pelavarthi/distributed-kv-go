package cluster

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewListsSelfAlive(t *testing.T) {
	c := New("node1", "http://localhost:8080", []string{"http://localhost:8081"}, time.Second, 3*time.Second)
	members := c.Members()
	if len(members) != 2 {
		t.Fatalf("want 2 members, got %d", len(members))
	}
	if !c.IsAlive("http://localhost:8080") {
		t.Fatal("self should be alive")
	}
	if c.IsAlive("http://localhost:8081") {
		t.Fatal("peer not yet heard from should not be alive")
	}
}

func TestReceiveHeartbeatAddsAndRevivesPeer(t *testing.T) {
	c := New("node1", "http://localhost:8080", nil, time.Second, 100*time.Millisecond)
	c.ReceiveHeartbeat("node2", "http://localhost:8081")
	if !c.IsAlive("http://localhost:8081") {
		t.Fatal("peer should be alive after a heartbeat")
	}
	// Age the heartbeat past the timeout, sweep, then revive it.
	c.mu.Lock()
	c.members["http://localhost:8081"].LastSeen = time.Now().Add(-time.Second)
	c.mu.Unlock()
	c.Sweep()
	if c.IsAlive("http://localhost:8081") {
		t.Fatal("stale peer should be dead after a sweep")
	}
	c.ReceiveHeartbeat("node2", "http://localhost:8081")
	if !c.IsAlive("http://localhost:8081") {
		t.Fatal("peer should be alive again after a new heartbeat")
	}
}

func TestAddAndRemovePeer(t *testing.T) {
	c := New("node1", "http://localhost:8080", nil, time.Second, 3*time.Second)
	c.AddPeer("http://localhost:8081")
	c.AddPeer("http://localhost:8081")
	if len(c.Members()) != 2 {
		t.Fatalf("duplicate add should not create a second entry, got %d members", len(c.Members()))
	}
	c.RemovePeer("http://localhost:8081")
	if len(c.Members()) != 1 {
		t.Fatalf("want 1 member after removal, got %d", len(c.Members()))
	}
	c.RemovePeer("http://localhost:8080")
	if len(c.Members()) != 1 {
		t.Fatal("removing self should be ignored")
	}
}

func TestHandleHeartbeatRejectsBadRequests(t *testing.T) {
	c := New("node1", "http://localhost:8080", nil, time.Second, 3*time.Second)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cluster/heartbeat", nil)
	c.HandleHeartbeat(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/cluster/heartbeat", nil)
	c.HandleHeartbeat(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for an empty heartbeat, got %d", rec.Code)
	}
}

// TestHeartbeatOverHTTP runs two real clusters against each other:
// one starts sending, the other sees it alive, then the sender stops
// and the other marks it dead after the timeout.
func TestHeartbeatOverHTTP(t *testing.T) {
	a := New("node-a", "", nil, 20*time.Millisecond, 150*time.Millisecond)
	b := New("node-b", "", nil, 20*time.Millisecond, 150*time.Millisecond)

	muxA := http.NewServeMux()
	muxA.HandleFunc("/cluster/heartbeat", a.HandleHeartbeat)
	srvA := httptest.NewServer(muxA)
	defer srvA.Close()
	muxB := http.NewServeMux()
	muxB.HandleFunc("/cluster/heartbeat", b.HandleHeartbeat)
	srvB := httptest.NewServer(muxB)
	defer srvB.Close()

	a.selfAddr = srvA.URL
	b.selfAddr = srvB.URL
	a.members = map[string]*Member{srvA.URL: {ID: "node-a", Addr: srvA.URL, Alive: true, LastSeen: time.Now()}}
	b.members = map[string]*Member{srvB.URL: {ID: "node-b", Addr: srvB.URL, Alive: true, LastSeen: time.Now()}}
	a.AddPeer(srvB.URL)
	b.AddPeer(srvA.URL)
	a.Start()
	b.Start()
	defer a.Stop()
	defer b.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for !b.IsAlive(srvA.URL) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !b.IsAlive(srvA.URL) {
		t.Fatal("b should see a alive after heartbeats arrive")
	}
	a.Stop()
	deadline = time.Now().Add(2 * time.Second)
	for b.IsAlive(srvA.URL) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.IsAlive(srvA.URL) {
		t.Fatal("b should mark a dead after it stops heartbeating")
	}
}

func TestHandleMembersReturnsJSON(t *testing.T) {
	c := New("node1", "http://localhost:8080", nil, time.Second, 3*time.Second)
	c.ReceiveHeartbeat("node2", "http://localhost:8081")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cluster/members", nil)
	c.HandleMembers(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if got := rec.Body.String(); got == "" || !contains(got, "node2") {
		t.Fatalf("members response should name node2, got %q", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
