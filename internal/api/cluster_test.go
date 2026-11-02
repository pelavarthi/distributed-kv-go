package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/pelavarthi/distributed-kv-go/internal/grpckv"
	"github.com/pelavarthi/distributed-kv-go/internal/replication"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

// node is one member of a test cluster: a store, a replicator,
// and an HTTP server wired the way cmd/server wires them, minus
// membership heartbeats, which the test replaces with a fixed
// leader so the test does not wait on timers.
type node struct {
	store *store.Store
	repl  *replication.Replicator
	srv   *httptest.Server
}

func newNode(t *testing.T, id string, isLeader bool) *node {
	t.Helper()
	s := store.New()
	t.Cleanup(s.Close)
	repl := replication.New(id)
	repl.SetApply(applyToStore(s))
	leaderFn := func() (bool, int) { return isLeader, 1 }
	srv := httptest.NewServer(NewWithReplication(s, nil, repl, leaderFn))
	t.Cleanup(srv.Close)
	return &node{store: s, repl: repl, srv: srv}
}

func httpSender(t *testing.T) replication.Sender {
	t.Helper()
	return func(addr string, req replication.AppendRequest) (replication.AppendResponse, error) {
		body, _ := json.Marshal(req)
		resp, err := http.Post(addr+"/cluster/replicate", "application/json", bytes.NewReader(body))
		if err != nil {
			return replication.AppendResponse{}, err
		}
		defer resp.Body.Close()
		var out replication.AppendResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return replication.AppendResponse{}, err
		}
		return out, nil
	}
}

func put(t *testing.T, base, key, value string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, base+"/kv/"+key, strings.NewReader(`{"value":"`+value+`"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func get(t *testing.T, base, key string) (string, int) {
	t.Helper()
	resp, err := http.Get(base + "/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	var out keyResponse
	if err := decode(resp, &out); err != nil {
		t.Fatal(err)
	}
	return out.Value, resp.StatusCode
}

func deleteKey(t *testing.T, base, key string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, base+"/kv/"+key, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestThreeNodeClusterEndToEnd runs a write through a real
// three-node cluster: it must be refused by a follower, commit
// through the leader, show up on both followers, survive one
// follower going down, and be readable over gRPC on the leader.
func TestThreeNodeClusterEndToEnd(t *testing.T) {
	leader := newNode(t, "node1", true)
	followerA := newNode(t, "node2", false)
	followerB := newNode(t, "node3", false)

	var leaderPeers []string
	leader.repl.SetPeers(func() []string { return leaderPeers })
	leader.repl.SetSender(httpSender(t))
	leaderPeers = []string{followerA.srv.URL, followerB.srv.URL}

	if status := put(t, followerA.srv.URL, "alpha", "1"); status != http.StatusServiceUnavailable {
		t.Fatalf("PUT to follower = %d, want 503", status)
	}

	if status := put(t, leader.srv.URL, "alpha", "1"); status != http.StatusCreated {
		t.Fatalf("PUT to leader = %d, want 201", status)
	}
	for _, n := range []*node{leader, followerA, followerB} {
		if got, status := get(t, n.srv.URL, "alpha"); status != http.StatusOK || got != "1" {
			t.Fatalf("GET alpha on %s = %q (%d), want 1 (200)", n.srv.URL, got, status)
		}
	}

	// One follower down must not stop writes: leader plus the
	// remaining follower is still a majority of three.
	followerB.srv.Close()
	if status := put(t, leader.srv.URL, "beta", "2"); status != http.StatusCreated {
		t.Fatalf("PUT with one follower down = %d, want 201", status)
	}
	if got, status := get(t, followerA.srv.URL, "beta"); status != http.StatusOK || got != "2" {
		t.Fatalf("GET beta on surviving follower = %q (%d), want 2 (200)", got, status)
	}

	if status := deleteKey(t, leader.srv.URL, "alpha"); status != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want 204", status)
	}
	for _, n := range []*node{leader, followerA} {
		if _, status := get(t, n.srv.URL, "alpha"); status != http.StatusNotFound {
			t.Fatalf("GET deleted alpha on %s = %d, want 404", n.srv.URL, status)
		}
	}

	// gRPC reads serve from the leader's store, so a write that
	// committed over HTTP is visible over gRPC too. Writes over
	// gRPC are not exercised here: a gRPC Set touches only one
	// node and would bypass replication.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	grpckv.Register(grpcSrv, leader.store)
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)
	client, err := grpckv.Dial(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if got, err := client.Get(ctx, "beta"); err != nil || got != "2" {
		t.Fatalf("gRPC Get beta = %q (%v), want 2", got, err)
	}
	keys, err := client.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "beta" {
		t.Fatalf("gRPC List = %v, want [beta]", keys)
	}
}
