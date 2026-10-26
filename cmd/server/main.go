// Command server runs the store behind the HTTP API on :8080.
// On startup it restores the latest snapshot, then replays the
// write-ahead log, so keys written before a restart come back
// without replaying the whole history every time.
package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"github.com/pelavarthi/distributed-kv-go/internal/api"
	"github.com/pelavarthi/distributed-kv-go/internal/snapshot"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

func main() {
	dataDir := flag.String("data-dir", "data", "directory holding the store data")
	flag.Parse()

	s := store.New()
	defer s.Close()

	logPath := filepath.Join(*dataDir, "wal.log")
	snapPath := filepath.Join(*dataDir, "snapshot.json")
	snapCount, walCount, err := snapshot.Recover(snapPath, logPath, s)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("recovered %d keys from snapshot and %d records from %s", snapCount, walCount, logPath)
	l, err := wal.Open(logPath)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Println("listening on :8080")
	if err := http.ListenAndServe(":8080", api.NewWithLog(s, l)); err != nil {
		log.Fatal(err)
	}
}
