// Command server is the entry point for the store. For now it just
// brings the in-memory store up; the HTTP API lands on day 2.
package main

import (
	"log"

	"github.com/pelavarthi/distributed-kv-go/internal/store"
)

func main() {
	s := store.New()
	defer s.Close()
	s.Set("hello", "world", 0)
	if value, ok := s.Get("hello"); ok {
		log.Printf("store ready, hello=%s", value)
	}
}
