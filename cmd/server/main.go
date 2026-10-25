// Command server runs the store behind the HTTP API on :8080.
// On startup it replays the write-ahead log in the data directory,
// so keys written before a restart come back.
package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"github.com/pelavarthi/distributed-kv-go/internal/api"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

func main() {
	dataDir := flag.String("data-dir", "data", "directory holding the write-ahead log")
	flag.Parse()

	s := store.New()
	defer s.Close()

	logPath := filepath.Join(*dataDir, "wal.log")
	l, err := wal.Open(logPath)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	n, err := wal.ReplayInto(logPath, s)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("replayed %d records from %s", n, logPath)

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", api.NewWithLog(s, l)); err != nil {
		log.Fatal(err)
	}
}
