package api

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// HTTP benchmarks. These run against the in-memory server with
// no WAL, so they measure the HTTP + store path on its own; the
// WAL benchmark in internal/wal covers the durable-write cost.
// Run with: go test ./internal/api/ -bench . -benchtime 10000x

func BenchmarkHTTPGet(b *testing.B) {
	srv := newTestServer(b)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/kv/bench")
	if err != nil {
		b.Fatal(err)
	}
	resp.Body.Close()

	// Seed one key outside the timed loop.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/bench", strings.NewReader(`{"value":"v"}`))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		b.Fatal(err)
	}
	resp.Body.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Get(srv.URL + "/kv/bench")
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("GET = %d, want 200", resp.StatusCode)
		}
	}
}

func BenchmarkHTTPGetParallel(b *testing.B) {
	srv := newTestServer(b)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/bench", strings.NewReader(`{"value":"v"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		b.Fatal(err)
	}
	resp.Body.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := http.Get(srv.URL + "/kv/bench")
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

func BenchmarkHTTPPut(b *testing.B) {
	srv := newTestServer(b)
	defer srv.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+fmt.Sprintf("/kv/key-%d", i%1000),
			strings.NewReader(`{"value":"v"}`))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			b.Fatalf("PUT = %d, want 200 or 201", resp.StatusCode)
		}
	}
}
