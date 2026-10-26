package snapshot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

func mustGet(t *testing.T, s *store.Store, key string) string {
	t.Helper()
	got, ok := s.Get(key)
	if !ok {
		t.Fatalf("Get(%q) missing, want a value", key)
	}
	return got
}

// A snapshot round-trip keeps values, and keeps a TTL key's
// original expiry instead of giving it a fresh lifetime.
func TestSaveLoadRoundTripKeepsExpiry(t *testing.T) {
	s := store.New()
	defer s.Close()
	s.Set("plain", "v1", 0)
	s.Set("timed", "v2", time.Hour)

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := Save(path, s.Entries()); err != nil {
		t.Fatal(err)
	}

	restored := store.New()
	defer restored.Close()
	entries, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("loaded %d entries, want 2", len(entries))
	}
	for _, e := range entries {
		restored.Restore(e)
	}
	if got := mustGet(t, restored, "plain"); got != "v1" {
		t.Fatalf("plain = %q, want v1", got)
	}
	if got := mustGet(t, restored, "timed"); got != "v2" {
		t.Fatalf("timed = %q, want v2", got)
	}
	for _, e := range restored.Entries() {
		if e.Key == "timed" && (!e.HasTTL || time.Until(e.ExpiresAt) > time.Hour || time.Until(e.ExpiresAt) < 59*time.Minute) {
			t.Fatalf("timed expiry = %v, want about one hour out", e.ExpiresAt)
		}
	}
}

// Expired keys never make it into a snapshot, and restoring an
// already-expired entry is a no-op.
func TestSnapshotSkipsExpiredKeys(t *testing.T) {
	s := store.New()
	defer s.Close()
	s.Set("live", "yes", 0)
	s.Restore(store.Entry{Key: "stale", Value: "no", HasTTL: true, ExpiresAt: time.Now().Add(-time.Minute)})

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := Save(path, s.Entries()); err != nil {
		t.Fatal(err)
	}
	entries, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "live" {
		t.Fatalf("loaded entries = %v, want only live", entries)
	}
	if _, ok := s.Get("stale"); ok {
		t.Fatal("expired restored entry should not be readable")
	}
}

// A missing snapshot is an empty start, not an error; a
// corrupted one is an error, never silently restored data.
func TestLoadMissingAndCorrupt(t *testing.T) {
	if entries, err := Load(filepath.Join(t.TempDir(), "nope.json")); err != nil || entries != nil {
		t.Fatalf("missing snapshot = %v, %v, want nil, nil", entries, err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	s := store.New()
	defer s.Close()
	s.Set("a", "1", 0)
	if err := Save(path, s.Entries()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := strings.Replace(string(data), `"1"`, `"2"`, 1)
	if err := os.WriteFile(path, []byte(corrupt), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("corrupt snapshot should fail to load")
	}
}

// Recovery is snapshot first, then the log: a key updated after
// the snapshot wins, a key deleted after it stays deleted, and a
// key written only after it appears.
func TestRecoverSnapshotThenLog(t *testing.T) {
	dir := t.TempDir()
	snapPath := filepath.Join(dir, "snapshot.json")
	logPath := filepath.Join(dir, "wal.log")

	s := store.New()
	defer s.Close()
	s.Set("keep", "old", 0)
	s.Set("gone", "old", 0)
	s.Set("changed", "old", 0)
	if err := Save(snapPath, s.Entries()); err != nil {
		t.Fatal(err)
	}

	l, err := wal.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.AppendSet("changed", "new", 0); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendDelete("gone"); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendSet("fresh", "hi", 0); err != nil {
		t.Fatal(err)
	}
	l.Close()

	recovered := store.New()
	defer recovered.Close()
	snapCount, walCount, err := Recover(snapPath, logPath, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if snapCount != 3 || walCount != 3 {
		t.Fatalf("recovered snap=%d wal=%d, want 3, 3", snapCount, walCount)
	}
	if got := mustGet(t, recovered, "keep"); got != "old" {
		t.Fatalf("keep = %q, want old", got)
	}
	if got := mustGet(t, recovered, "changed"); got != "new" {
		t.Fatalf("changed = %q, want new", got)
	}
	if got := mustGet(t, recovered, "fresh"); got != "hi" {
		t.Fatalf("fresh = %q, want hi", got)
	}
	if _, ok := recovered.Get("gone"); ok {
		t.Fatal("gone should stay deleted after recovery")
	}
}

// A checkpoint snapshots the store and empties the log, and the
// snapshot alone recovers the same state afterwards.
func TestCheckpointTruncatesLog(t *testing.T) {
	dir := t.TempDir()
	snapPath := filepath.Join(dir, "snapshot.json")
	logPath := filepath.Join(dir, "wal.log")

	l, err := wal.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.AppendSet("a", "1", 0); err != nil {
		t.Fatal(err)
	}
	l.Close()

	s := store.New()
	defer s.Close()
	s.Set("a", "1", 0)
	s.Set("b", "2", 0)
	if err := Checkpoint(s, snapPath, logPath); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(logPath); err != nil || info.Size() != 0 {
		t.Fatalf("log size after checkpoint = %v, %v, want empty file", info, err)
	}

	recovered := store.New()
	defer recovered.Close()
	if _, _, err := Recover(snapPath, logPath, recovered); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, recovered, "a"); got != "1" {
		t.Fatalf("a = %q, want 1", got)
	}
	if got := mustGet(t, recovered, "b"); got != "2" {
		t.Fatalf("b = %q, want 2", got)
	}
}
