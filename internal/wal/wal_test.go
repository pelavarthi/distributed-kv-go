package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func openTestLog(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

func replayInto(t *testing.T, path string) *store.Store {
	t.Helper()
	s := store.New()
	t.Cleanup(s.Close)
	if _, err := ReplayInto(path, s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAppendAndReplay(t *testing.T) {
	l, path := openTestLog(t)
	if err := l.AppendSet("a", "1", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendSet("b", "2", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendSet("a", "3", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendDelete("a"); err != nil {
		t.Fatal(err)
	}
	l.Close()

	s := replayInto(t, path)
	if _, ok := s.Get("a"); ok {
		t.Fatal("expected deleted key a to be gone after replay")
	}
	got, ok := s.Get("b")
	if !ok || got != "2" {
		t.Fatalf("Get(b) = %q, %v, want 2, true", got, ok)
	}
}

func TestReplayCountAndMissingFile(t *testing.T) {
	l, path := openTestLog(t)
	if err := l.AppendSet("x", "y", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendDelete("x"); err != nil {
		t.Fatal(err)
	}
	l.Close()

	s := store.New()
	defer s.Close()
	n, err := ReplayInto(path, s)
	if err != nil || n != 2 {
		t.Fatalf("ReplayInto = %d, %v, want 2, nil", n, err)
	}

	n, err = ReplayInto(filepath.Join(t.TempDir(), "nope.log"), s)
	if err != nil || n != 0 {
		t.Fatalf("missing log ReplayInto = %d, %v, want 0, nil", n, err)
	}
}

func TestReplayIgnoresTornTail(t *testing.T) {
	l, path := openTestLog(t)
	if err := l.AppendSet("kept", "yes", 0); err != nil {
		t.Fatal(err)
	}
	l.Close()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// A torn write: a valid-looking record cut off mid-line, then
	// a complete record after it that must not be replayed.
	if _, err := f.WriteString(`{"op":"set","key":"torn","val`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"op":"set","key":"after","value":"no","checksum":0}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s := replayInto(t, path)
	if got, ok := s.Get("kept"); !ok || got != "yes" {
		t.Fatalf("Get(kept) = %q, %v, want yes, true", got, ok)
	}
	if _, ok := s.Get("after"); ok {
		t.Fatal("expected the record after the torn line to be ignored")
	}
}

func TestReplaySkipsExpiredSet(t *testing.T) {
	l, path := openTestLog(t)
	if err := l.AppendSetWithExpiry("old", "gone", time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendSet("fresh", "here", 0); err != nil {
		t.Fatal(err)
	}
	l.Close()

	s := replayInto(t, path)
	if _, ok := s.Get("old"); ok {
		t.Fatal("expected an already-expired set to be skipped on replay")
	}
	if got, ok := s.Get("fresh"); !ok || got != "here" {
		t.Fatalf("Get(fresh) = %q, %v, want here, true", got, ok)
	}
}

func TestConcurrentAppends(t *testing.T) {
	l, path := openTestLog(t)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := l.AppendSet(fmt.Sprintf("k-%d", i), "v", 0); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	l.Close()

	s := replayInto(t, path)
	if s.Len() != 20 {
		t.Fatalf("replayed %d keys, want 20", s.Len())
	}
}
