package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pelavarthi/distributed-kv-go/internal/metrics"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func TestMetricsEndpoint(t *testing.T) {
	s := store.New()
	t.Cleanup(s.Close)
	srv := New(s)
	srv.SetMetrics(metrics.New())
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/name", strings.NewReader(`{"value":"pranav"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201", resp.StatusCode)
	}
	resp, err = http.Get(ts.URL + "/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET missing = %d, want 404", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var snap metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap.Requests["PUT /kv/:key 201"] != 1 {
		t.Fatalf("PUT count = %d, want 1", snap.Requests["PUT /kv/:key 201"])
	}
	if snap.Requests["GET /kv/:key 404"] != 1 {
		t.Fatalf("GET 404 count = %d, want 1", snap.Requests["GET /kv/:key 404"])
	}
	if snap.Latencies["PUT /kv/:key"].Count != 1 {
		t.Fatalf("PUT latency count = %d, want 1", snap.Latencies["PUT /kv/:key"].Count)
	}
	if _, ok := snap.Requests["PUT /kv/name 201"]; ok {
		t.Fatal("metrics used the raw key as a route, want /kv/:key")
	}
}
