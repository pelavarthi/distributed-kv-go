package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

// A PUT through the API must land in the log first, so a fresh
// store replaying that log ends up with the same key.
func TestPutIsRecoverableFromLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	s := store.New()
	defer s.Close()
	srv := httptest.NewServer(NewWithLog(s, l))
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/name", strings.NewReader(`{"value":"pranav"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	srv.Close()
	l.Close()

	recovered := store.New()
	defer recovered.Close()
	if _, err := wal.ReplayInto(path, recovered); err != nil {
		t.Fatal(err)
	}
	got, ok := recovered.Get("name")
	if !ok || got != "pranav" {
		t.Fatalf("recovered Get(name) = %q, %v, want pranav, true", got, ok)
	}
}
