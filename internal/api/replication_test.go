package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/replication"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func applyToStore(s *store.Store) func(replication.Entry) {
	return func(e replication.Entry) {
		if e.Op == replication.OpDelete {
			s.Delete(e.Key)
			return
		}
		s.Restore(store.Entry{Key: e.Key, Value: e.Value, ExpiresAt: e.ExpiresAt, HasTTL: e.HasTTL})
	}
}

func TestReplicatedPutReachesFollower(t *testing.T) {
	followerStore := store.New()
	t.Cleanup(followerStore.Close)
	followerRepl := replication.New("node2")
	followerRepl.SetApply(applyToStore(followerStore))
	followerSrv := httptest.NewServer(NewWithReplication(followerStore, nil, followerRepl, func() (bool, int) { return false, 1 }))
	defer followerSrv.Close()

	leaderStore := store.New()
	t.Cleanup(leaderStore.Close)
	leaderRepl := replication.New("node1")
	leaderRepl.SetApply(applyToStore(leaderStore))
	leaderRepl.SetPeers(func() []string { return []string{followerSrv.URL} })
	leaderRepl.SetSender(func(addr string, req replication.AppendRequest) (replication.AppendResponse, error) {
		body, _ := json.Marshal(req)
		resp, err := http.Post(followerSrv.URL+"/cluster/replicate", "application/json", bytes.NewReader(body))
		if err != nil {
			return replication.AppendResponse{}, err
		}
		defer resp.Body.Close()
		var out replication.AppendResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return replication.AppendResponse{}, err
		}
		return out, nil
	})
	leaderSrv := httptest.NewServer(NewWithReplication(leaderStore, nil, leaderRepl, func() (bool, int) { return true, 1 }))
	defer leaderSrv.Close()

	// A write to a follower is refused, so clients retry on the leader.
	req, _ := http.NewRequest(http.MethodPut, followerSrv.URL+"/kv/name", strings.NewReader(`{"value":"pranav"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("PUT to follower = %d, want 503", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPut, leaderSrv.URL+"/kv/name", strings.NewReader(`{"value":"pranav"}`))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT to leader = %d, want 201", resp.StatusCode)
	}

	// The committed write is readable on both nodes.
	for _, srv := range []*httptest.Server{leaderSrv, followerSrv} {
		resp, err := http.Get(srv.URL + "/kv/name")
		if err != nil {
			t.Fatal(err)
		}
		var got keyResponse
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got.Value != "pranav" {
			t.Fatalf("GET from %s = %q, want pranav", srv.URL, got.Value)
		}
	}
	if followerRepl.CommitIndex() != 1 || leaderRepl.CommitIndex() != 1 {
		t.Fatalf("commit indexes = leader %d follower %d, want 1 and 1", leaderRepl.CommitIndex(), followerRepl.CommitIndex())
	}

	// A replicated delete also reaches the follower.
	req, _ = http.NewRequest(http.MethodDelete, leaderSrv.URL+"/kv/name", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE on leader = %d, want 204", resp.StatusCode)
	}
	time.Sleep(10 * time.Millisecond)
	if _, ok := followerStore.Get("name"); ok {
		t.Fatal("follower still has deleted key")
	}
}

func TestCatchupBringsLateFollowerUpToDate(t *testing.T) {
	// A follower that joins after writes have already committed.
	followerStore := store.New()
	t.Cleanup(followerStore.Close)
	followerRepl := replication.New("node2")
	followerRepl.SetApply(applyToStore(followerStore))
	followerSrv := httptest.NewServer(NewWithReplication(followerStore, nil, followerRepl, func() (bool, int) { return false, 1 }))
	defer followerSrv.Close()

	leaderStore := store.New()
	t.Cleanup(leaderStore.Close)
	leaderRepl := replication.New("node1")
	leaderRepl.SetApply(applyToStore(leaderStore))
	peers := []string{followerSrv.URL}
	leaderRepl.SetPeers(func() []string { return peers })
	leaderRepl.SetSender(func(addr string, req replication.AppendRequest) (replication.AppendResponse, error) {
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
	})
	leaderSrv := httptest.NewServer(NewWithReplication(leaderStore, nil, leaderRepl, func() (bool, int) { return true, 1 }))
	defer leaderSrv.Close()

	put := func(srv, key, value string) {
		req, _ := http.NewRequest(http.MethodPut, srv+"/kv/"+key, strings.NewReader(`{"value":"`+value+`"}`))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("PUT %s = %d, want 201", key, resp.StatusCode)
		}
	}
	put(leaderSrv.URL, "a", "1")
	put(leaderSrv.URL, "b", "2")

	// A second follower starts now, with an empty log.
	lateStore := store.New()
	t.Cleanup(lateStore.Close)
	lateRepl := replication.New("node3")
	lateRepl.SetApply(applyToStore(lateStore))
	lateSrv := httptest.NewServer(NewWithReplication(lateStore, nil, lateRepl, func() (bool, int) { return false, 1 }))
	defer lateSrv.Close()
	peers = append(peers, lateSrv.URL)

	resp, err := http.Post(leaderSrv.URL+"/cluster/catchup", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /cluster/catchup = %d, want 200", resp.StatusCode)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}} {
		resp, err := http.Get(lateSrv.URL + "/kv/" + kv[0])
		if err != nil {
			t.Fatal(err)
		}
		var got keyResponse
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got.Value != kv[1] {
			t.Fatalf("late follower %s = %q, want %q", kv[0], got.Value, kv[1])
		}
	}

	// Catch-up on a follower is refused, so only the leader runs it.
	resp, err = http.Post(followerSrv.URL+"/cluster/catchup", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("POST /cluster/catchup on follower = %d, want 503", resp.StatusCode)
	}
}
