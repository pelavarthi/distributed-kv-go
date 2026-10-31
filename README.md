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

```
go run ./cmd/server -id node1 -listen :8080 -advertise http://localhost:8080 -peers http://localhost:8081
go run ./cmd/server -id node2 -listen :8081 -advertise http://localhost:8081 -peers http://localhost:8080 -data-dir data2
```

```
POST /cluster/heartbeat  {"id": "node1", "addr": "http://localhost:8080"}
GET /cluster/members
```
