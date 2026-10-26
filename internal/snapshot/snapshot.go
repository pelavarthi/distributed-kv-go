// Package snapshot saves the whole store to disk as one file and
// restores it on startup, so recovery does not have to replay a
// write-ahead log that has grown forever.
//
// The file is written to a temp file in the same directory, synced,
// then renamed over the old snapshot, so a crash mid-write leaves
// either the old snapshot or the new one, never a half-written
// file. A checksum over the entries catches a torn or corrupted
// snapshot instead of silently restoring bad data.
//
// Recovery loads the snapshot first, then replays the log. That
// order is safe even if a crash happened between writing a
// snapshot and truncating the log: replaying the old log over the
// snapshot just re-applies the same sets and deletes in order,
// which ends in the same state.
package snapshot

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

type fileEntry struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	ExpiresAt int64  `json:"expires_at_unix_nano,omitempty"`
}

type fileDoc struct {
	Version  int         `json:"version"`
	Entries  []fileEntry `json:"entries"`
	Checksum uint32      `json:"checksum"`
}

func checksumFor(entries []fileEntry) uint32 {
	data, _ := json.Marshal(entries)
	h := crc32.NewIEEE()
	h.Write(data)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(len(entries)))
	h.Write(buf[:])
	return h.Sum32()
}

func toFileEntries(entries []store.Entry) []fileEntry {
	out := make([]fileEntry, 0, len(entries))
	for _, e := range entries {
		fe := fileEntry{Key: e.Key, Value: e.Value}
		if e.HasTTL {
			fe.ExpiresAt = e.ExpiresAt.UnixNano()
		}
		out = append(out, fe)
	}
	return out
}

func toStoreEntries(entries []fileEntry) []store.Entry {
	out := make([]store.Entry, 0, len(entries))
	for _, fe := range entries {
		e := store.Entry{Key: fe.Key, Value: fe.Value}
		if fe.ExpiresAt != 0 {
			e.HasTTL = true
			e.ExpiresAt = time.Unix(0, fe.ExpiresAt)
		}
		out = append(out, e)
	}
	return out
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// Save writes entries to path atomically. The entries are expected
// to come from Store.Entries, so expired keys are already excluded.
func Save(path string, entries []store.Entry) error {
	doc := fileDoc{Version: 1, Entries: toFileEntries(entries)}
	doc.Checksum = checksumFor(doc.Entries)
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Load reads a snapshot written by Save. A missing file is not an
// error: it just means no snapshot exists yet.
func Load(path string) ([]store.Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var doc fileDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if doc.Version != 1 || doc.Checksum != checksumFor(doc.Entries) {
		return nil, errors.New("snapshot: checksum mismatch or unknown version")
	}
	return toStoreEntries(doc.Entries), nil
}

// Recover restores a store from a snapshot plus the log. It returns
// how many entries came from the snapshot and how many records
// were replayed from the log.
func Recover(snapshotPath, walPath string, s *store.Store) (snapshotCount, walCount int, err error) {
	entries, err := Load(snapshotPath)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		s.Restore(e)
	}
	walCount, err = wal.ReplayInto(walPath, s)
	if err != nil {
		return len(entries), walCount, err
	}
	return len(entries), walCount, nil
}

// Checkpoint snapshots the store, then truncates the log. The
// snapshot is synced to disk first, so a crash at any point still
// recovers: either the old log is still there and replays over the
// new snapshot, or it is gone and the snapshot alone is complete.
func Checkpoint(s *store.Store, snapshotPath, walPath string) error {
	if err := Save(snapshotPath, s.Entries()); err != nil {
		return err
	}
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
