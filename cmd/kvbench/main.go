// Command kvbench hammers a running server with concurrent
// reads and writes and prints throughput and latency
// percentiles, so a tuning change can be judged by a number
// instead of a feeling.
//
// Usage:
//
//	go run ./cmd/kvbench -n 20000 -c 20 -reads 80
//	go run ./cmd/kvbench -addr http://localhost:8081 -n 5000 -c 5 -reads 0
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type result struct {
	latencies []time.Duration
	errors    int
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p / 100)
	return sorted[i]
}

func worker(addr string, n, reads, keys int, out chan<- result, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	client := &http.Client{Timeout: 10 * time.Second}
	res := result{latencies: make([]time.Duration, 0, n)}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("bench-key-%d", rng.Intn(keys))
		start := time.Now()
		var err error
		if rng.Intn(100) < reads {
			var resp *http.Response
			resp, err = client.Get(addr + "/kv/" + key)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
					err = fmt.Errorf("GET %d", resp.StatusCode)
				}
			}
		} else {
			body := bytes.NewReader([]byte(`{"value":"bench-value"}`))
			req, _ := http.NewRequest(http.MethodPut, addr+"/kv/"+key, body)
			var resp *http.Response
			resp, err = client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
					err = fmt.Errorf("PUT %d", resp.StatusCode)
				}
			}
		}
		if err != nil {
			res.errors++
			continue
		}
		res.latencies = append(res.latencies, time.Since(start))
	}
	out <- res
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "server address")
	total := flag.Int("n", 10000, "total requests")
	concurrency := flag.Int("c", 10, "concurrent workers")
	reads := flag.Int("reads", 80, "percent of requests that are reads (0-100)")
	keys := flag.Int("keys", 1000, "number of distinct keys to spread requests over")
	flag.Parse()

	if *concurrency < 1 || *total < 1 || *reads < 0 || *reads > 100 || *keys < 1 {
		fmt.Fprintln(os.Stderr, "bad flags: need -n >= 1, -c >= 1, -reads 0-100, -keys >= 1")
		os.Exit(2)
	}

	// Seed the key space first, so reads mostly hit existing keys.
	client := &http.Client{Timeout: 10 * time.Second}
	for i := 0; i < *keys; i++ {
		req, _ := http.NewRequest(http.MethodPut,
			fmt.Sprintf("%s/kv/bench-key-%d", *addr, i),
			bytes.NewReader([]byte(`{"value":"bench-value"}`)))
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "seeding failed: %v\n", err)
			os.Exit(1)
		}
		resp.Body.Close()
	}

	perWorker := *total / *concurrency
	extra := *total % *concurrency
	results := make(chan result, *concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *concurrency; w++ {
		n := perWorker
		if w < extra {
			n++
		}
		wg.Add(1)
		go worker(*addr, n, *reads, *keys, results, &wg)
	}
	wg.Wait()
	close(results)
	elapsed := time.Since(start)

	var latencies []time.Duration
	errors := 0
	for res := range results {
		latencies = append(latencies, res.latencies...)
		errors += res.errors
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var totalMs time.Duration
	for _, d := range latencies {
		totalMs += d
	}
	avg := time.Duration(0)
	if len(latencies) > 0 {
		avg = totalMs / time.Duration(len(latencies))
	}
	fmt.Printf("requests: %d  errors: %d  time: %s\n", *total, errors, elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f req/s\n", float64(len(latencies))/elapsed.Seconds())
	fmt.Printf("latency  avg: %s  p50: %s  p95: %s  p99: %s\n",
		avg, percentile(latencies, 50), percentile(latencies, 95), percentile(latencies, 99))
}
