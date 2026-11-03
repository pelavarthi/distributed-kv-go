// Package metrics counts what the server does and how long it
// takes, so a slow or failing node is visible without reading
// through raw logs. Everything is kept in memory and served as
// JSON from GET /metrics: request counts by method, route, and
// status, average latencies per route, and a few named counters
// for events that are not HTTP requests, such as replication
// commits, which later chunks can record.
package metrics

import (
	"strconv"
	"sync"
	"time"
)

// Latency summarizes the durations seen for one operation.
type Latency struct {
	Count   int64   `json:"count"`
	TotalMs float64 `json:"total_ms"`
	AvgMs   float64 `json:"avg_ms"`
}

// Snapshot is a point-in-time copy of all metrics. It is built
// under the lock and then handed out, so callers never see a
// half-updated map.
type Snapshot struct {
	Requests  map[string]int64   `json:"requests"`
	Latencies map[string]Latency `json:"latencies"`
	Counters  map[string]int64   `json:"counters"`
}

// Metrics holds the counters. A single mutex guards all of them:
// request rates here are low enough that one lock is simpler
// than a lock per counter, and simpler wins.
type Metrics struct {
	mu        sync.Mutex
	requests  map[string]int64
	latencies map[string]*latencyAccum
	counters  map[string]int64
}

type latencyAccum struct {
	count int64
	total time.Duration
}

// New returns an empty Metrics.
func New() *Metrics {
	return &Metrics{
		requests:  make(map[string]int64),
		latencies: make(map[string]*latencyAccum),
		counters:  make(map[string]int64),
	}
}

// RecordRequest counts one finished HTTP request and folds its
// duration into that route's latency. Route is a pattern like
// "/kv/:key", not the raw path, so one key with a million
// requests does not create a million counters.
func (m *Metrics) RecordRequest(method, route string, status int, d time.Duration) {
	key := method + " " + route
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[key+" "+strconv.Itoa(status)]++
	acc := m.latencies[key]
	if acc == nil {
		acc = &latencyAccum{}
		m.latencies[key] = acc
	}
	acc.count++
	acc.total += d
}

// Inc bumps a named counter by one, for events that are not
// HTTP requests.
func (m *Metrics) Inc(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name]++
}

// Observe folds a duration for a named non-request operation
// into its latency summary.
func (m *Metrics) Observe(name string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	acc := m.latencies[name]
	if acc == nil {
		acc = &latencyAccum{}
		m.latencies[name] = acc
	}
	acc.count++
	acc.total += d
}

// Snapshot returns a copy of every metric collected so far.
func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := Snapshot{
		Requests:  make(map[string]int64, len(m.requests)),
		Latencies: make(map[string]Latency, len(m.latencies)),
		Counters:  make(map[string]int64, len(m.counters)),
	}
	for k, v := range m.requests {
		snap.Requests[k] = v
	}
	for k, acc := range m.latencies {
		snap.Latencies[k] = Latency{
			Count:   acc.count,
			TotalMs: float64(acc.total.Microseconds()) / 1000,
			AvgMs:   float64(acc.total.Microseconds()) / 1000 / float64(acc.count),
		}
	}
	for k, v := range m.counters {
		snap.Counters[k] = v
	}
	return snap
}
