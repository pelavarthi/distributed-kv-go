package api

import (
	"encoding/json"
	"net/http"

	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pelavarthi/distributed-kv-go/internal/sharding"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func TestShardRoutesReadsToOwner(t *testing.T) {
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(keyResponse{Key: "hello", Value: "from-owner"})
	}))
	defer owner.Close()

	s := store.New()
	t.Cleanup(s.Close)
	srv := New(s)
	srv.SetShardRouter(sharding.NewRouter("http://local", 10, func() []string { return []string{owner.URL} }))
	api := httptest.NewServer(srv)
	defer api.Close()

	resp, err := http.Get(api.URL + "/kv/hello")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got keyResponse
	if err := decode(resp, &got); err != nil {
		t.Fatal(err)
	}
	if got.Value != "from-owner" {
		t.Fatalf("GET via shard routing = %q, want from-owner", got.Value)
	}
}

func TestShardListShowsOnlyLocalKeys(t *testing.T) {
	s := store.New()
	t.Cleanup(s.Close)
	s.Set("a", "1", 0)
	s.Set("b", "2", 0)
	srv := New(s)
	// A one-node ring owns every key, so the list is unchanged.
	srv.SetShardRouter(sharding.NewRouter("node1", 10, func() []string { return []string{"node1"} }))
	api := httptest.NewServer(srv)
	defer api.Close()

	resp, err := http.Get(api.URL + "/kv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Keys) != 2 {
		t.Fatalf("sharded key list = %v, want both keys on a one-node ring", got.Keys)
	}
}

func TestShardDoesNotForwardWrites(t *testing.T) {
	s := store.New()
	t.Cleanup(s.Close)
	srv := New(s)
	srv.SetShardRouter(sharding.NewRouter("http://local", 10, func() []string { return []string{"http://somewhere-else"} }))
	api := httptest.NewServer(srv)
	defer api.Close()

	req, _ := http.NewRequest(http.MethodPut, api.URL+"/kv/name", strings.NewReader(`{"value":"local"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT with a remote shard owner = %d, want 201 (writes stay on the leader path)", resp.StatusCode)
	}
	if value, ok := s.Get("name"); !ok || value != "local" {
		t.Fatalf("PUT wrote %q, %v, want local", value, ok)
	}
}
