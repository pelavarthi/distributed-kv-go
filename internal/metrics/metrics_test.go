package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestRecordRequestAndSnapshot(t *testing.T) {
	m := New()
	m.RecordRequest("PUT", "/kv/:key", 201, 2*time.Millisecond)
	m.RecordRequest("PUT", "/kv/:key", 201, 4*time.Millisecond)
	m.RecordRequest("GET", "/kv/:key", 404, time.Millisecond)

	snap := m.Snapshot()
	if snap.Requests["PUT /kv/:key 201"] != 2 {
		t.Fatalf("PUT count = %d, want 2", snap.Requests["PUT /kv/:key 201"])
	}
	if snap.Requests["GET /kv/:key 404"] != 1 {
		t.Fatalf("GET 404 count = %d, want 1", snap.Requests["GET /kv/:key 404"])
	}
	lat := snap.Latencies["PUT /kv/:key"]
	if lat.Count != 2 || lat.AvgMs != 3 {
		t.Fatalf("PUT latency = %+v, want count 2 avg 3ms", lat)
	}
}

func TestCountersAndObserve(t *testing.T) {
	m := New()
	m.Inc("replication_commits")
	m.Inc("replication_commits")
	m.Observe("replication", 10*time.Millisecond)

	snap := m.Snapshot()
	if snap.Counters["replication_commits"] != 2 {
		t.Fatalf("counter = %d, want 2", snap.Counters["replication_commits"])
	}
	if snap.Latencies["replication"].Count != 1 {
		t.Fatalf("observed count = %d, want 1", snap.Latencies["replication"].Count)
	}
}

func TestConcurrentRecording(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.RecordRequest("GET", "/kv/:key", 200, time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if got := m.Snapshot().Requests["GET /kv/:key 200"]; got != 1000 {
		t.Fatalf("concurrent count = %d, want 1000", got)
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	m := New()
	m.RecordRequest("GET", "/health", 200, time.Millisecond)
	snap := m.Snapshot()
	m.RecordRequest("GET", "/health", 200, time.Millisecond)
	if snap.Requests["GET /health 200"] != 1 {
		t.Fatalf("earlier snapshot changed to %d, want 1", snap.Requests["GET /health 200"])
	}
}
