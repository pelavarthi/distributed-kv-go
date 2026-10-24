package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := store.New()
	t.Cleanup(s.Close)
	return httptest.NewServer(New(s))
}

func TestPutGetDelete(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET missing = %d, want 404", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/name", strings.NewReader(`{"value":"pranav"}`))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT new key = %d, want 201", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/kv/name")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got keyResponse
	if err := decode(resp, &got); err != nil {
		t.Fatal(err)
	}
	if got.Value != "pranav" {
		t.Fatalf("GET = %q, want pranav", got.Value)
	}

	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/kv/name", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want 204", resp.StatusCode)
	}
}

func TestPutBadBodyAndNegativeTTL(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	for _, body := range []string{`not json`, `{"value":"x","ttl_seconds":-1}`} {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/k", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("PUT %q = %d, want 400", body, resp.StatusCode)
		}
	}
}

func TestHealthAndList(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health = %d, want 200", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/b", strings.NewReader(`{"value":"2"}`))
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
	req, _ = http.NewRequest(http.MethodPut, srv.URL+"/kv/a", strings.NewReader(`{"value":"1"}`))
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}

	resp, err = http.Get(srv.URL + "/kv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Keys []string `json:"keys"`
	}
	if err := decode(resp, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Keys) != 2 || out.Keys[0] != "a" || out.Keys[1] != "b" {
		t.Fatalf("list = %v, want [a b]", out.Keys)
	}
}

func decode(resp *http.Response, v any) error {
	return json.NewDecoder(resp.Body).Decode(v)
}
