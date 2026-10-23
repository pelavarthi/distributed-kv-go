package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetAndGet(t *testing.T) {
	s := New()
	defer s.Close()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected missing key to report false")
	}
	s.Set("name", "pranav", 0)
	got, ok := s.Get("name")
	if !ok || got != "pranav" {
		t.Fatalf("Get = %q, %v, want pranav, true", got, ok)
	}
}

func TestOverwriteAndDelete(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("k", "v1", 0)
	s.Set("k", "v2", 0)
	got, _ := s.Get("k")
	if got != "v2" {
		t.Fatalf("after overwrite Get = %q, want v2", got)
	}
	if !s.Delete("k") {
		t.Fatal("expected Delete to report true for a present key")
	}
	if s.Delete("k") {
		t.Fatal("expected Delete to report false for a missing key")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("expected deleted key to be gone")
	}
}

func TestTTLExpiry(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("short", "x", 50*time.Millisecond)
	s.Set("forever", "y", 0)
	if _, ok := s.Get("short"); !ok {
		t.Fatal("expected key to be readable before its TTL passes")
	}
	time.Sleep(100 * time.Millisecond)
	if _, ok := s.Get("short"); ok {
		t.Fatal("expected key to be gone after its TTL passes")
	}
	if _, ok := s.Get("forever"); !ok {
		t.Fatal("expected key without a TTL to survive")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			s.Set(key, "v", 0)
			s.Get(key)
			s.Delete(key)
		}(i)
	}
	wg.Wait()
}
