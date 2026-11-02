// Command server runs the store behind the HTTP API.
// On startup it restores the latest snapshot, then replays the
// write-ahead log, so keys written before a restart come back
// without replaying the whole history every time. With -peers it
// also joins a cluster: it heartbeats its peers and serves its
// membership list, so nodes can tell when one has gone quiet,
// and the alive member with the highest id is the leader. Keys
// are sharded across the alive members with a consistent hash
// ring, so reads for a key are forwarded to the node that owns
// it, and GET /kv lists only this node's keys.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/pelavarthi/distributed-kv-go/internal/api"
	"github.com/pelavarthi/distributed-kv-go/internal/cluster"
	"github.com/pelavarthi/distributed-kv-go/internal/election"
	"github.com/pelavarthi/distributed-kv-go/internal/grpckv"
	"github.com/pelavarthi/distributed-kv-go/internal/replication"
	"github.com/pelavarthi/distributed-kv-go/internal/sharding"
	"github.com/pelavarthi/distributed-kv-go/internal/snapshot"
	"github.com/pelavarthi/distributed-kv-go/internal/store"
	"github.com/pelavarthi/distributed-kv-go/internal/wal"
)

func main() {
	dataDir := flag.String("data-dir", "data", "directory holding the store data")
	listen := flag.String("listen", ":8080", "address to listen on")
	grpcListen := flag.String("grpc-listen", "", "address to serve gRPC on, e.g. :9090; empty means no gRPC server")
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

	e := election.New(*nodeID)

	// Replication: the leader appends each write to its log and
	// sends it to alive followers; a write is applied only after
	// a majority holds it. Applying a committed entry means
	// writing it to this node's WAL first, then to its store,
	// the same order the single-node path already used.
	repl := replication.New(*nodeID)
	repl.SetApply(func(en replication.Entry) {
		if en.Op == replication.OpDelete {
			_ = l.AppendDelete(en.Key)
			s.Delete(en.Key)
			return
		}
		_ = l.AppendSetWithExpiry(en.Key, en.Value, en.ExpiresAt)
		s.Restore(store.Entry{Key: en.Key, Value: en.Value, ExpiresAt: en.ExpiresAt, HasTTL: en.HasTTL})
	})
	repl.SetPeers(func() []string {
		var addrs []string
		for _, m := range c.Members() {
			if m.Alive && m.Addr != *advertise {
				addrs = append(addrs, m.Addr)
			}
		}
		return addrs
	})
	httpClient := &http.Client{Timeout: 5 * time.Second}
	repl.SetSender(func(addr string, req replication.AppendRequest) (replication.AppendResponse, error) {
		body, _ := json.Marshal(req)
		resp, err := httpClient.Post(addr+"/cluster/replicate", "application/json", bytes.NewReader(body))
		if err != nil {
			return replication.AppendResponse{}, err
		}
		defer resp.Body.Close()
		var out replication.AppendResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return replication.AppendResponse{}, err
		}
		return out, nil
	})
	leaderFn := func() (bool, int) {
		e.Refresh(c.Members())
		_, term := e.Leader()
		if e.IsLeader() {
			// A new leader adopts the election term, so its
			// appends are not rejected as stale by followers
			// that already saw the handover.
			repl.NoteTerm(term)
		}
		return e.IsLeader(), term
	}
	apiServer := api.NewWithReplication(s, l, repl, leaderFn)

	// Sharding: the ring is built from the alive members, so a
	// node joining or going quiet reshards the keys it owned
	// without moving anyone else's.
	shardRouter := sharding.NewRouter(*advertise, sharding.DefaultReplicas, func() []string {
		var addrs []string
		for _, m := range c.Members() {
			if m.Alive {
				addrs = append(addrs, m.Addr)
			}
		}
		return addrs
	})
	apiServer.SetShardRouter(shardRouter)

	mux := http.NewServeMux()
	mux.Handle("/", apiServer)
	mux.HandleFunc("/cluster/heartbeat", c.HandleHeartbeat)
	mux.HandleFunc("/cluster/members", c.HandleMembers)
	mux.HandleFunc("/cluster/shards", shardRouter.HandleShards)
	mux.HandleFunc("/cluster/route", shardRouter.HandleRoute)
	mux.HandleFunc("/cluster/leader", e.HandleLeader(c.Members))
	mux.HandleFunc("/cluster/replicate", apiServer.HandleReplicate)
	mux.HandleFunc("/cluster/catchup", apiServer.HandleCatchup)
	mux.HandleFunc("/cluster/replication", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		leader, _ := e.Refresh(c.Members())
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(repl.Status(leader))
	})

	// gRPC serves the same store on a second port when asked.
	// It reads from the store directly, so it is a second
	// transport, not a second write path: replicated writes
	// still go through the HTTP leader.
	if *grpcListen != "" {
		lis, err := net.Listen("tcp", *grpcListen)
		if err != nil {
			log.Fatal(err)
		}
		grpcSrv := grpc.NewServer()
		grpckv.Register(grpcSrv, s)
		go grpcSrv.Serve(lis)
		log.Printf("node %s serving gRPC on %s", *nodeID, *grpcListen)
	}

	log.Printf("node %s listening on %s, peers: %v", *nodeID, *listen, peerAddrs)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		log.Fatal(err)
	}
}
