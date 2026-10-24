// Package store is a thread-safe in-memory key-value store with
// optional per-key expiry. It is the single-node core that later
// pieces (HTTP API, write-ahead log, replication) will build on.
package store

import (
	"sort"
	"sync"
	"time"
)

type entry struct {
	value     string
	expiresAt time.Time
	hasTTL    bool
}

// Store holds keys in memory. The zero value is not usable;
// use New.
type Store struct {
	mu      sync.RWMutex
	entries map[string]entry
	stop    chan struct{}
	done    chan struct{}
}

// New returns an empty store and starts a background sweep that
// removes expired keys once a minute.
func New() *Store {
	s := &Store{
		entries: make(map[string]entry),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go s.sweepLoop(time.Minute)
	return s
}

// Close stops the background sweep.
func (s *Store) Close() {
	close(s.stop)
	<-s.done
}

func (s *Store) sweepLoop(interval time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			s.sweep(now)
		}
	}
}

func (s *Store) sweep(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, e := range s.entries {
		if e.hasTTL && now.After(e.expiresAt) {
			delete(s.entries, key)
		}
	}
}

// Set stores value under key. A ttl of zero or less means the key
// never expires.
func (s *Store) Set(key, value string, ttl time.Duration) {
	e := entry{value: value}
	if ttl > 0 {
		e.hasTTL = true
		e.expiresAt = time.Now().Add(ttl)
	}
	s.mu.Lock()
	s.entries[key] = e
	s.mu.Unlock()
}

// Get returns the value for key. It reports false for missing keys
// and for keys whose TTL has passed, and drops expired keys lazily
// instead of waiting for the next sweep.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	if e.hasTTL && time.Now().After(e.expiresAt) {
		s.mu.Lock()
		if current, stillThere := s.entries[key]; stillThere && current.expiresAt.Equal(e.expiresAt) {
			delete(s.entries, key)
		}
		s.mu.Unlock()
		return "", false
	}
	return e.value, true
}

// Delete removes key and reports whether it was present.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[key]; !ok {
		return false
	}
	delete(s.entries, key)
	return true
}

// Keys returns the keys currently stored, sorted, including keys
// that have expired but not yet been swept.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.entries))
	for key := range s.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Len returns the number of keys currently stored, including keys
// that have expired but not yet been swept.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}
