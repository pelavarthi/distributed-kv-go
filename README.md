# distributed-kv-go
Distributed key-value store in Go: in-memory KV, WAL, replication, sharding.

Run the server:

```
go run ./cmd/server
```

Every write is appended to `data/wal.log` and synced before it is
applied, and the log is replayed on startup, so data survives a
restart. Use `-data-dir` to put the log somewhere else.

HTTP API:

```
PUT /kv/{key}    {"value": "...", "ttl_seconds": 60}
GET /kv/{key}
DELETE /kv/{key}
GET /kv          list keys
GET /health
```

CLI client:

```
go run ./cmd/kvctl set hello world
go run ./cmd/kvctl get hello
go run ./cmd/kvctl list
go run ./cmd/kvctl delete hello
go run ./cmd/kvctl members
go run ./cmd/kvctl leader
```

Cluster membership:

Each node heartbeats its peers once a second and marks a peer
dead after 3 seconds without a heartbeat. A peer that starts
answering again is marked alive again.

Leader election:

The alive member with the highest id is the leader, so every
node that sees the same membership list picks the same leader.
When the leader goes quiet, the next-highest alive member takes
over, and the term number goes up by one on each change. Check
it with `kvctl leader` or `GET /cluster/leader`. This is a
simplified election: it decides who leads, but it has no vote
rounds, so it cannot yet be trusted to commit writes on its own.

Log replication:

Only the leader accepts writes. It appends each write to its
replication log, sends it to every alive follower
(`POST /cluster/replicate`), and applies it only once a
majority of nodes hold it, then tells the followers the write
committed so they apply it too. A write to a follower answers
503, and a write that cannot reach a majority is left
uncommitted and never applied. Check the state with
`GET /cluster/replication`.

Failover and follower catch-up:

When the leader goes quiet, the next-highest alive member takes
over and can keep accepting writes. A follower that missed
entries — it was down, or it joined late — is caught up by the
leader resending the missing suffix, stepping back one entry at
a time if the follower's tail conflicts, until the two logs
agree. A write also triggers a catch-up on the spot when a
follower rejects it for a gap. Run it on demand with
`kvctl catchup` or `POST /cluster/catchup` on the leader: it
also re-drives any uncommitted tail, so a write the old leader
replicated but never committed gets its chance to commit under
the new leader. One honest limit: the replication log lives in
memory, so a restarted node comes back with an empty log and
must be caught up by the current leader before it is trusted
with the log again.

Sharding with consistent hashing:

Keys are assigned to nodes on a consistent hash ring, with 100
virtual points per node so the split stays roughly even. Adding
a node only moves the keys that land on its part of the ring,
and removing one hands its keys to the next node clockwise.
Reads for a key are forwarded to its owner, and `GET /kv` lists
only the keys this node owns. Check the ring with
`GET /cluster/shards` and a key's owner with
`GET /cluster/route?key=hello`. One honest limit: writes still
go through the single leader, because replication is
cluster-wide for now, so sharding currently routes reads, not
write ownership.

```
go run ./cmd/server -id node1 -listen :8080 -advertise http://localhost:8080 -peers http://localhost:8081
go run ./cmd/server -id node2 -listen :8081 -advertise http://localhost:8081 -peers http://localhost:8080 -data-dir data2
```

gRPC client and multi-node end-to-end test:

The store is also served over gRPC on a second port, with the
same Get, Set, Delete, and List operations. Messages are
encoded as JSON over gRPC, so there is no code-generation step.
Start a node with gRPC on, then use the client:

```
go run ./cmd/server -grpc-listen :9090
go run ./cmd/kvgrpc -addr localhost:9090 set hello world
go run ./cmd/kvgrpc -addr localhost:9090 get hello
```

One honest limit: a gRPC write touches only that one node's
memory, it is not written to the WAL and not replicated, so
replicated writes still go through the HTTP leader path. gRPC
reads serve from the same store, so a committed write is
visible over gRPC right away. The three-node end-to-end test in
`internal/api/cluster_test.go` covers the whole path: a write
refused by a follower, committed through the leader, readable
on both followers, still committing with one follower down,
deleted everywhere, and readable over gRPC on the leader.

```
POST /cluster/heartbeat  {"id": "node1", "addr": "http://localhost:8080"}
GET /cluster/members
```

Metrics and logging:

Every request through the API is counted by method, route, and
status, with its latency folded into a per-route average, and
one structured log line is written per request (method, route,
status, duration). Routes are collapsed, so `/kv/hello` and
`/kv/world` both count as `/kv/:key` instead of one counter
per key. Check it with `GET /metrics`:

```
GET /metrics
{"requests":{"PUT /kv/:key 201":1,"GET /kv/:key 404":1},
 "latencies":{"PUT /kv/:key":{"count":1,"total_ms":0.4,"avg_ms":0.4}},
 "counters":{}}
```

Start the server with `-log-json` to write those request lines
as JSON, for log aggregation, instead of the default text
format. The counters map is there for non-request events, such
as replication commits, which later work can record.

Chaos and partition safety:

A majority must be counted against the whole cluster, not just
the followers that are still answering. Before this fix, a
leader cut off from both followers in a 3-node cluster saw no
alive peers and committed on its own, so the minority side and
the majority side could each commit a different value for the
same key. Now the server tells the replicator the full
membership count, including dead members, and a write with no
followers reachable only commits in a real one-node cluster.
A partitioned leader's write stays uncommitted and unapplied,
and a failed write answers 503 so the client retries on a node
that can still reach a majority. When the cut node returns,
`POST /cluster/catchup` on the leader resends the missing
entries and replaces any stale value the minority side wrote.
Benchmarks and tuning:

Go benchmarks cover the hot paths, so a change can be judged
by a number instead of a feeling:

```
go test ./internal/store/ ./internal/wal/ -bench . -benchtime 100000x
go test ./internal/api/ -bench . -benchtime 10000x
```

On this machine the in-memory store does a Set in about
600 ns and a Get in about 170 ns. A durable WAL append is
about 4-6 µs, and that is the write ceiling for a single
node: every write syncs to disk before it is applied, and
concurrent writers do not help because the syncs serialize
on the disk. The HTTP path, without the WAL, serves a GET in
about 60 µs. Numbers measured on this machine; re-run them
on yours before trusting them.

`cmd/kvbench` is a load generator for a running server. It
seeds the key space, then hammers it with N workers and
prints throughput and p50/p95/p99 latencies:

```
go run ./cmd/kvbench -n 20000 -c 20 -reads 80
```

Against a single node with the WAL on, that run did about
7,700 requests a second with p50 1.7 ms and p95 5.8 ms, and
a writes-only run did about 7,200 a second with p50 0.5 ms.
Writes are slower than reads because each one waits for the
disk sync; reads only touch memory.

Two tunings came out of measuring:

- The WAL syncs with fdatasync on Linux instead of a full
  fsync. An append-only log never reads its own file
  metadata back, so skipping the metadata flush is free
  durability-wise. On this machine the disk sync is already
  nearly free, so the benchmarks show no reliable difference
  (3.8-5.8 µs per append either way); the change is kept
  because it is the right default on a real disk, and a
  restart after the load test still replayed all 10,933
  records. Other platforms still use fsync.
- The HTTP server now has timeouts: 5 seconds to send
  request headers, 10 seconds to write a response, 120
  seconds of idle keep-alive. Without them one slow client
  can hold a handler slot forever.

One thing deliberately not done: parallel reads are about
3x slower per operation than single-threaded reads, because
every goroutine contends on the store's one RWMutex. A
striped store with a lock per shard would fix that, but
in-memory ops are already sub-microsecond and the HTTP
round trip dominates, so it is not worth the complexity
until reads are actually the bottleneck.

The `internal/chaos` package is a test-only fault injector that
drops traffic to cut-off nodes, so the partition scenarios in
`internal/replication/partition_test.go` and
`internal/chaos/chaos_test.go` run without real servers.
