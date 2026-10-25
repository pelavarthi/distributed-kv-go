package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

// Replay reads records from path in order and calls apply for each
// valid one. It returns how many records were applied.
//
// A crash can leave a torn final line: a partial write, bad JSON,
// or a checksum mismatch. That line and everything after it are
// ignored, not treated as an error, because anything after a torn
// write was never acknowledged as durable.
func Replay(path string, apply func(Record) error) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	count := 0
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			var rec Record
			if json.Unmarshal([]byte(trimmed), &rec) != nil || !rec.valid() {
				return count, nil
			}
			if err := apply(rec); err != nil {
				return count, err
			}
			count++
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return count, nil
			}
			return count, err
		}
	}
}

// ReplayInto replays the log into a store, skipping sets whose
// expiry has already passed. It returns the number of records
// read, including expired sets that were skipped.
func ReplayInto(path string, s *store.Store) (int, error) {
	return Replay(path, func(rec Record) error {
		switch rec.Op {
		case OpSet:
			if rec.ExpiresAt != 0 {
				remaining := time.Until(time.Unix(0, rec.ExpiresAt))
				if remaining <= 0 {
					return nil
				}
				s.Set(rec.Key, rec.Value, remaining)
			} else {
				s.Set(rec.Key, rec.Value, 0)
			}
		case OpDelete:
			s.Delete(rec.Key)
		}
		return nil
	})
}
