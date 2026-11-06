# distributed-kv-go

A distributed key-value store in Go: an in-memory store with TTL, a
write-ahead log, snapshots, leader election, log replication, failover,
and sharding over a consistent hash ring. One binary per node, HTTP and
gRPC APIs, no external dependencies beyond gRPC.

```text
client ──► leader ──► WAL ──► store
              │  ▲
              │  │ majority acks
              ▼  │
           followers ──► WAL ──► store
```

## Quickstart

```sh
go run ./cmd/server
```

That starts one node on `:8080` with data in `./data`. Writes are
appended to `data/wal.log` and synced to disk before they are applied,
and the log is replayed on startup, so data survives a restart.

```sh
go run ./cmd/kvctl set hello world
go run ./cmd/kvctl get hello        # world
go run ./cmd/kvctl list
go run ./cmd/kvctl delete hello
```

## Three-node cluster

```sh
go run ./cmd/server -id node1 -listen :8080 -advertise http://localhost:8080 \
  -peers http://localhost:8081,http://localhost:8082 -data-dir data1
go run ./cmd/server -id node2 -listen :8081 -advertise http://localhost:8081 \
  -peers http://localhost:8080,http://localhost:8082 -data-dir data2
go run ./cmd/server -id node3 -listen :8082 -advertise http://localhost:8082 \
  -peers http://localhost:8080,http://localhost:8081 -data-dir data3
```

Or run the whole thing as a scripted demo, which starts three nodes,
writes through the leader, kills the leader, writes through the new
one, and brings the old leader back:

```sh
make demo
# or: ./scripts/demo.sh
```

## HTTP API

| Method | Path | What it does |
| --- | --- | --- |
| `PUT /kv/{key}` | `{"value": "...", "ttl_seconds": 60}` | Set a key (`ttl_seconds` optional, 0 means no expiry). Leader only; a follower answers 503. |
| `GET /kv/{key}` | | Get a key's value. Forwarded to the node that owns the key. |
| `DELETE /kv/{key}` | | Delete a key. Leader only. |
| `GET /kv` | | List the keys this node owns. |
| `GET /health` | | `{"status":"ok"}` when the server is up. |
| `GET /metrics` | | Request counts and per-route latencies as JSON. |
| `GET /cluster/members` | | Every known member and whether it is alive. |
| `GET /cluster/leader` | | Current leader id and election term. |
| `GET /cluster/replication` | | Replication log state: length, commit index, term. |
| `POST /cluster/catchup` | | On the leader: resend missing log entries to followers. |
| `GET /cluster/shards` | | Which node owns which part of the hash ring. |
| `GET /cluster/route?key=hello` | | Which node owns one key. |

The CLI covers the common calls:

```sh
go run ./cmd/kvctl -addr http://localhost:8081 members
go run ./cmd/kvctl -addr http://localhost:8081 leader
go run ./cmd/kvctl -addr http://localhost:8081 catchup
```

## gRPC

The same store is also served over gRPC on a second port, with Get,
Set, Delete, and List. Messages are encoded as JSON over gRPC, so
there is no code-generation step.

```sh
go run ./cmd/server -grpc-listen :9090
go run ./cmd/kvgrpc -addr localhost:9090 set hello world
go run ./cmd/kvgrpc -addr localhost:9090 get hello
```

## How a write works

1. The client sends the write to the leader. A follower answers 503,
   so the client retries on another node.
2. The leader appends the entry to its replication log and sends it to
   every alive follower (`POST /cluster/replicate`).
3. Once a majority of the whole cluster holds the entry, the leader
   commits it: it appends the write to its own WAL, syncs to disk,
   then applies it to the store, and tells the followers to commit too.
4. A follower applies a committed entry the same way: WAL first, then
   the store.

A write that cannot reach a majority is left uncommitted and never
applied, so a partitioned leader cannot invent data the rest of the
cluster never saw. Majority is counted against every known member,
dead or alive: counting only the nodes still answering would let a
lone leader commit on its own.

## How a read works

Keys are assigned to nodes on a consistent hash ring, with 100 virtual
points per node so the split stays roughly even. Adding a node only
moves the keys that land on its part of the ring; removing one hands
its keys to the next node clockwise. A node that gets a read for a key
it does not own forwards it to the owner, so any node can answer any
read. `GET /kv` lists only the keys the node itself owns.

## Failure handling

- **Leader dies.** Every node heartbeats its peers once a second and
  marks a peer dead after 3 seconds without one. The alive member
  with the highest id becomes leader, the term goes up by one, and
  writes continue. This is a simplified election: highest alive id
  wins, with no vote rounds.
- **Follower falls behind.** A follower that missed entries — it was
  down, or it joined late — is caught up by the leader resending the
  missing suffix, stepping back one entry at a time if the follower's
  tail conflicts, until the two logs agree. A write also triggers a
  catch-up on the spot when a follower rejects it for a gap, and
  `POST /cluster/catchup` runs it on demand. It also re-drives any
  uncommitted tail, so a write the old leader replicated but never
  committed gets its chance under the new leader.
- **Node restarts.** On startup a node restores the latest snapshot,
  then replays the WAL, so keys written before the restart come back
  without replaying the whole history every time.
- **Partition.** A leader cut off from its followers cannot commit:
  its writes stay uncommitted and answer 503, and the majority side
  keeps working. When the cut node returns, catch-up resends the
  missing entries and replaces any stale value the minority side wrote.

## Configuration

```sh
go run ./cmd/server -h
```

| Flag | Default | What it does |
| --- | --- | --- |
| `-id` | `node1` | This node's id. Highest alive id wins the election. |
| `-listen` | `:8080` | HTTP listen address. |
| `-advertise` | `http://localhost:8080` | This node's address as peers see it. |
| `-peers` | _(empty)_ | Comma-separated peer addresses. Empty means a single node. |
| `-data-dir` | `data` | Directory holding `wal.log` and `snapshot.json`. |
| `-grpc-listen` | _(empty)_ | gRPC listen address, e.g. `:9090`. Empty means no gRPC server. |
| `-heartbeat-interval` | `1s` | How often to heartbeat peers. |
| `-heartbeat-timeout` | `3s` | How long without a heartbeat before a peer counts as dead. |
| `-log-json` | `false` | Log requests as JSON instead of text, for log aggregation. |

## Metrics and logging

Every request is counted by method, route, and status, with its
latency folded into a per-route average, and one structured log line
is written per request. Routes are collapsed, so `/kv/hello` and
`/kv/world` both count as `/kv/:key` instead of one counter per key.

```sh
curl localhost:8080/metrics
# {"requests":{"PUT /kv/:key 201":1,"GET /kv/:key 404":1},
#  "latencies":{"PUT /kv/:key":{"count":1,"total_ms":0.4,"avg_ms":0.4}},
#  "counters":{}}
```

## Benchmarks

Go benchmarks cover the hot paths, so a change can be judged by a
number instead of a feeling:

```sh
go test ./internal/store/ ./internal/wal/ -bench . -benchtime 100000x
go test ./internal/api/ -bench . -benchtime 10000x
```

On this machine the in-memory store does a Set in about 600 ns and a
Get in about 170 ns. A durable WAL append is about 4-6 µs, and that is
the write ceiling for a single node: every write syncs to disk before
it is applied, and concurrent writers do not help because the syncs
serialize on the disk. The HTTP path, without the WAL, serves a GET in
about 60 µs. Numbers measured on this machine; re-run them on yours
before trusting them.

`cmd/kvbench` is a load generator for a running server. It seeds the
key space, then hammers it with N workers and prints throughput and
p50/p95/p99 latencies:

```sh
go run ./cmd/kvbench -n 20000 -c 20 -reads 80
```

Against a single node with the WAL on, that run did about 7,700
requests a second with p50 1.7 ms and p95 5.8 ms, and a writes-only run
did about 7,200 a second with p50 0.5 ms. Writes are slower than reads
because each one waits for the disk sync; reads only touch memory.

Two tunings came out of measuring:

- The WAL syncs with fdatasync on Linux instead of a full fsync. An
  append-only log never reads its own file metadata back, so skipping
  the metadata flush is free durability-wise. On this machine the disk
  sync is already nearly free, so the benchmarks show no reliable
  difference (3.8-5.8 µs per append either way); the change is kept
  because it is the right default on a real disk, and a restart after
  the load test still replayed all 10,933 records. Other platforms
  still use fsync.
- The HTTP server now has timeouts: 5 seconds to send request
  headers, 10 seconds to write a response, 120 seconds of idle
  keep-alive. Without them one slow client can hold a handler slot
  forever.

One thing deliberately not done: parallel reads are about 3x slower
per operation than single-threaded reads, because every goroutine
contends on the store's one RWMutex. A striped store with a lock per
shard would fix that, but in-memory ops are already sub-microsecond
and the HTTP round trip dominates, so it is not worth the complexity
until reads are actually the bottleneck.

## Testing

```sh
make test        # go vet + go test ./...
make bench       # benchmarks for the store, WAL, and HTTP paths
```

The test suite covers the store (TTL, concurrency), the WAL
(append, replay, crash recovery), snapshots, membership and
heartbeats, election, replication (commit, follower refusal,
catch-up), sharding (ring moves, routing), the gRPC client, and a
three-node end-to-end test in `internal/api/cluster_test.go`: a write
refused by a follower, committed through the leader, readable on both
followers, still committing with one follower down, deleted
everywhere, and readable over gRPC on the leader. The
`internal/chaos` package is a test-only fault injector that drops
traffic to cut-off nodes, so the partition scenarios in
`internal/replication/partition_test.go` and
`internal/chaos/chaos_test.go` run without real servers.

## Layout

```text
cmd/server    node binary: flags, recovery, cluster wiring, HTTP + gRPC
cmd/kvctl     CLI client for the HTTP API
cmd/kvgrpc    CLI client for the gRPC API
cmd/kvbench   load generator for a running server
internal/store       in-memory KV with TTL and safe concurrent access
internal/wal         append-only log, fsync/fdatasync, replay
internal/snapshot    point-in-time snapshot + recovery over the WAL
internal/cluster     membership, heartbeats, dead-peer detection
internal/election    highest-alive-id leader election with terms
internal/replication leader log, majority commit, follower catch-up
internal/sharding    consistent hash ring and read routing
internal/api         HTTP handlers, metrics middleware, server wiring
internal/metrics     request counters and per-route latencies
internal/grpckv      gRPC server over the same store (JSON messages)
internal/client      HTTP client used by kvctl
internal/chaos       test-only fault injector for partition tests
scripts/demo.sh      three-node demo: write, kill the leader, fail over
```

## Honest limits

- Writes go through one leader, and the replication log lives in
  memory: a restarted node comes back with an empty log and must be
  caught up by the current leader before it is trusted with the log
  again.
- Sharding routes reads, not write ownership: every node still holds
  every key, because replication is cluster-wide.
- The election has no vote rounds, so two nodes that disagree about
  membership can briefly disagree about the leader.
- A gRPC write touches only that one node's memory: it is not written
  to the WAL and not replicated, so replicated writes still go through
  the HTTP leader path. gRPC reads serve from the same store, so a
  committed write is visible over gRPC right away.
- There is no authentication, TLS, or access control. Do not expose a
  node to a network you do not trust.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the write path,
the replication protocol, and the failure modes in more detail.
