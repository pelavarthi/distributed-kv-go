// Package wal is an append-only write-ahead log for the store.
// Every change is written to disk and synced before it is applied
// in memory, so a restart can replay the log and recover the data
// that was acknowledged before the crash.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	OpSet    = "set"
	OpDelete = "delete"
)

// Record is one logged change. ExpiresAt is a Unix time in
// nanoseconds, or zero for a key that never expires. It is stored
// as an absolute time, not a duration, so a key replayed after a
// restart still expires when it originally would have.
type Record struct {
	Op        string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	ExpiresAt int64  `json:"expires_at_unix_nano,omitempty"`
	Checksum  uint32 `json:"checksum"`
}

func checksumFor(op, key, value string, expiresAt int64) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(op))
	h.Write([]byte{0})
	h.Write([]byte(key))
	h.Write([]byte{0})
	h.Write([]byte(value))
	h.Write([]byte{0})
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(expiresAt))
	h.Write(buf[:])
	return h.Sum32()
}

func (r Record) valid() bool {
	if r.Op != OpSet && r.Op != OpDelete {
		return false
	}
	if r.Key == "" {
		return false
	}
	return r.Checksum == checksumFor(r.Op, r.Key, r.Value, r.ExpiresAt)
}

// Log is an open log file. Append is safe for concurrent use.
type Log struct {
	mu   sync.Mutex
	file *os.File
}

// Open creates the log file and its parent directory if needed,
// and opens it for appending. It does not replay anything; call
// ReplayInto before serving traffic from a recovered store.
func Open(path string) (*Log, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Log{file: f}, nil
}

// Close flushes and closes the log file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

func (l *Log) append(rec Record) error {
	rec.Checksum = checksumFor(rec.Op, rec.Key, rec.Value, rec.ExpiresAt)
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Write(append(data, '\n')); err != nil {
		return err
	}
	// Sync before the caller applies the change in memory, so a
	// crash right after this returns cannot lose the record.
	return l.file.Sync()
}

// AppendSet logs a set. A ttl of zero or less means no expiry.
func (l *Log) AppendSet(key, value string, ttl time.Duration) error {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}
	return l.append(Record{Op: OpSet, Key: key, Value: value, ExpiresAt: expiresAt})
}

// AppendSetWithExpiry logs a set that expires at the given time,
// or never if expiresAt is zero. Replay uses this for records
// whose expiry was already fixed when they were first written.
func (l *Log) AppendSetWithExpiry(key, value string, expiresAt time.Time) error {
	var unixNano int64
	if !expiresAt.IsZero() {
		unixNano = expiresAt.UnixNano()
	}
	return l.append(Record{Op: OpSet, Key: key, Value: value, ExpiresAt: unixNano})
}

// AppendDelete logs a delete.
func (l *Log) AppendDelete(key string) error {
	return l.append(Record{Op: OpDelete, Key: key})
}
