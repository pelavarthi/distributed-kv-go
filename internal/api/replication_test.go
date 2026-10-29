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
