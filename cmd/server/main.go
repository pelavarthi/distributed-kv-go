// Command server runs the store behind the HTTP API.
// On startup it restores the latest snapshot, then replays the
// write-ahead log, so keys written before a restart come back
// without replaying the whole history every time. With -peers it
// also joins a cluster: it heartbeats its peers and serves its
// membership list, so nodes can tell when one has gone quiet.
package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelavarthi/distributed-kv-go/internal/api"
	"github.com/pelavarthi/distributed-kv-go/internal/cluster"
	"github.com/pelavarthi/distributed-kv-go/internal/snapshot"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

func main() {
	dataDir := flag.String("data-dir", "data", "directory holding the store data")
	listen := flag.String("listen", ":8080", "address to listen on")
	nodeID := flag.String("id", "node1", "this node's id")
	advertise := flag.String("advertise", "http://localhost:8080", "this node's address as peers see it")
	peers := flag.String("peers", "", "comma-separated peer addresses, e.g. http://localhost:8081,http://localhost:8082")
	interval := flag.Duration("heartbeat-interval", time.Second, "how often to heartbeat peers")
	timeout := flag.Duration("heartbeat-timeout", 3*time.Second, "how long without a heartbeat before a peer counts as dead")
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

	var peerAddrs []string
	if *peers != "" {
		peerAddrs = strings.Split(*peers, ",")
	}
	c := cluster.New(*nodeID, *advertise, peerAddrs, *interval, *timeout)
	c.Start()
	defer c.Stop()

	mux := http.NewServeMux()
	mux.Handle("/", api.NewWithLog(s, l))
	mux.HandleFunc("/cluster/heartbeat", c.HandleHeartbeat)
	mux.HandleFunc("/cluster/members", c.HandleMembers)

	log.Printf("node %s listening on %s, peers: %v", *nodeID, *listen, peerAddrs)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatal(err)
	}
}
