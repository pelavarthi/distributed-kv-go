package client

import (
	"net/http/httptest"
	"testing"

	"github.com/pelavarthi/distributed-kv-go/internal/api"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func TestClientRoundTrip(t *testing.T) {
	s := store.New()
	defer s.Close()
	srv := httptest.NewServer(api.New(s))
	defer srv.Close()

	c := New(srv.URL)
	if err := c.Health(); err != nil {
		t.Fatalf("Health: %v", err)
	}
	if err := c.Set("name", "pranav", 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := c.Get("name")
	if err != nil || got != "pranav" {
		t.Fatalf("Get = %q, %v, want pranav, nil", got, err)
	}
	if _, err := c.Get("missing"); err == nil {
		t.Fatal("expected Get of a missing key to fail")
	}
	keys, err := c.List()
	if err != nil || len(keys) != 1 || keys[0] != "name" {
		t.Fatalf("List = %v, %v, want [name], nil", keys, err)
	}
	if err := c.Delete("name"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := c.Delete("name"); err == nil {
		t.Fatal("expected deleting a missing key to fail")
	}
}
