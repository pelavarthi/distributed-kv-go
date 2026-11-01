package sharding

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRouterForwardsToOwner(t *testing.T) {
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(`{"from":"owner"}`))
	}))
	defer owner.Close()

	rt := NewRouter("http://local", 10, func() []string { return []string{owner.URL} })
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/kv/some-key", nil)
	if !rt.Forward(rec, req, "some-key") {
		t.Fatal("router should have forwarded to the owner")
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("forwarded status = %d, want 418", rec.Code)
	}
	if body, _ := io.ReadAll(rec.Result().Body); string(body) != `{"from":"owner"}` {
		t.Fatalf("forwarded body = %s, want owner response", body)
	}
}

func TestRouterDoesNotForwardLocalKeys(t *testing.T) {
	rt := NewRouter("node1", 10, func() []string { return []string{"node1"} })
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/kv/k", nil)
	if rt.Forward(rec, req, "k") {
		t.Fatal("router should not forward a key this node owns")
	}
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("local forward wrote status %d body %q, want untouched recorder", rec.Code, rec.Body.String())
	}
}

func TestRouterTracksMembershipChanges(t *testing.T) {
	members := []string{"node1"}
	rt := NewRouter("node1", 10, func() []string { return members })
	if got := rt.Nodes(); len(got) != 1 || got[0] != "node1" {
		t.Fatalf("nodes = %v, want [node1]", got)
	}
	members = []string{"node1", "node2"}
	if got := rt.Nodes(); len(got) != 2 {
		t.Fatalf("nodes after join = %v, want 2 nodes", got)
	}
}

func TestHandleRouteAndShards(t *testing.T) {
	rt := NewRouter("node1", 10, func() []string { return []string{"node1", "node2"} })

	rec := httptest.NewRecorder()
	rt.HandleRoute(rec, httptest.NewRequest(http.MethodGet, "/cluster/route?key=hello", nil))
	var route struct {
		Key   string `json:"key"`
		Owner string `json:"owner"`
		Local bool   `json:"local"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&route); err != nil {
		t.Fatalf("route response did not decode: %v", err)
	}
	if route.Key != "hello" || route.Owner == "" || route.Local != (route.Owner == "node1") {
		t.Fatalf("route response = %+v, want key hello with a real owner", route)
	}

	rec = httptest.NewRecorder()
	rt.HandleShards(rec, httptest.NewRequest(http.MethodGet, "/cluster/shards", nil))
	if !strings.Contains(rec.Body.String(), "node2") {
		t.Fatalf("shards response = %s, want node2 listed", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	rt.HandleRoute(rec, httptest.NewRequest(http.MethodGet, "/cluster/route", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("route without key = %d, want 400", rec.Code)
	}
}
