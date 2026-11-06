# Architecture

How the pieces of `distributed-kv-go` fit together, and what happens
when something fails. The README is the user guide; this is the map.

## Components

```text
                    ┌─────────────┐
                    │   kvctl /   │
                    │   kvgrpc /  │
                    │   curl      │
                    └──────┬──────┘
                           │ HTTP :8080, gRPC :9090
                    ┌──────▼──────┐
                    │ internal/api│  handlers, metrics middleware
                    └──────┬──────┘
              ┌────────────┼────────────┐
              ▼            ▼            ▼
     internal/store  internal/    internal/
     (in-memory KV,  replication  sharding
      TTL, RWMutex)  (leader log, (hash ring,
                      majority      read routing)
                      commit)
              ▲            ▲
              │            │
     internal/wal    internal/    internal/
     (append-only    cluster     election
      log, replay)   (heartbeat,  (highest
                      members)    alive id)
              ▲
              │
     internal/snapshot
     (point-in-time
      snapshot)
```

A node is one process. It owns a store, a WAL, a replication log, a
membership list, and a view of the hash ring. The store is the only
source of truth for reads; everything else exists to keep the stores
on different nodes in agreement.

## Write path

A write is a `PUT /kv/{key}` to the leader:

```text
client
  │ PUT /kv/hello {"value": "world"}
  ▼
leader
  │ 1. append entry to replication log
  │ 2. POST /cluster/replicate to each alive follower
  ├──────────────► follower: append to its log, ack
  ├──────────────► follower: append to its log, ack
  │ 3. majority of the whole cluster holds the entry → commit
  │ 4. append to own WAL, sync to disk, apply to store
  │ 5. tell followers to commit
  ▼
followers: append to own WAL, sync, apply to store
```

The order inside step 4 matters: the WAL entry is durable before the
key is visible in the store, so a crash between the two still replays
the write on restart. Followers do the same in step 5.

A write to a follower is refused with 503, so a client that finds the
leader by trying nodes will land on the right one. A write that
cannot reach a majority stays uncommitted and is never applied: the
client sees 503 and retries later, on a node that can still reach a
majority.

## Read path

```text
client
  │ GET /kv/hello
  ▼
any node
  │ hash("hello") on the ring → owner
  ├─ owner is me → read from my store
  └─ owner is a peer → forward the request, return its answer
```

Reads do not touch the WAL or the replication log. They serve from
memory, which is why a read is fast even while writes wait on disk
syncs. The tradeoff is that a read can be stale by one uncommitted
write at most: only committed entries are ever applied, and a
committed entry reaches every alive follower before the client is told
the write succeeded, except for a follower that was down at the time —
it catches up before it is trusted again.

`GET /kv` is the one read that does not forward: it lists only the
keys the node itself owns, so it shows the shard, not the cluster.

## Membership and election

Nodes learn about each other through heartbeats, not configuration:
each node POSTs its id and address to its peers once a second
(`/cluster/heartbeat`), and a peer that has not been heard from in 3
seconds counts as dead. A peer that answers again counts as alive
again, so a restart is just a node coming back, not a rejoin ceremony.

The leader is the alive member with the highest id. There are no vote
rounds: every node that sees the same membership list picks the same
leader, and the term number goes up by one on each change. When the
leader dies, the next-highest alive member takes over as soon as the
heartbeat timeout fires. The new leader adopts the election term, so
its appends are not rejected as stale by followers that already saw
the handover.

This is simple on purpose, and the simplicity has a price: if two
nodes disagree about who is alive — a partition that splits the
membership view — they can briefly disagree about the leader. The
replication rule is what keeps that disagreement safe: only a leader
that can reach a majority may commit, so at most one side of a
partition can keep writing.

## Replication protocol

The replication log is an ordered list of entries, each with an index,
a term, a key, and a value (or a delete). The leader is the only node
that appends to it. An entry is committed once a majority of the whole
cluster — dead members included in the count — holds it. That "whole
cluster" detail is the fix from Day 12: counting a majority against
only the followers still answering shrinks the denominator along with
the votes, and a leader cut off from both followers in a 3-node
cluster would commit on its own.

A follower that missed entries is caught up by resending the missing
suffix of the log. If the follower's tail conflicts with the leader's
— it holds entries the leader never had, from the minority side of a
partition — the leader steps back one entry at a time until the two
logs agree, then resends from there. Catch-up also re-drives any
uncommitted tail, so a write the old leader replicated but never
committed gets its chance to commit under the new leader. A write
triggers a catch-up on the spot when a follower rejects it for a gap,
and `POST /cluster/catchup` runs it on demand.

One honest limit: the replication log lives in memory. A restarted
node comes back with an empty log, so it must be caught up by the
current leader before it is trusted with the log again. Its keys are
fine — those come back from the snapshot and the WAL — but its log
position is not.

## Sharding

Keys are assigned by a consistent hash ring with 100 virtual points
per node. A key's owner is the first node clockwise from the key's
hash. Adding a node only moves the keys that land on its part of the
ring; removing one hands its keys to the next node clockwise. The ring
is built from the alive members, so a node going quiet reshards the
keys it owned without moving anyone else's.

Sharding currently routes reads, not write ownership: replication is
cluster-wide, so every node holds every key. The ring decides who
answers a read, not who stores the data. That is a deliberate scope
limit for a two-week build; real per-node ownership would mean
replicating each key only to its shard's nodes, and re-sharding the
data itself when the ring changes.

## Durability

Two mechanisms, in order:

1. **WAL.** Every applied write is appended to `wal.log` and synced
   before it is visible. On Linux the sync is fdatasync, which skips
   flushing file metadata an append-only log never reads back; other
   platforms use fsync. A crash loses nothing that was acknowledged.
2. **Snapshot.** A point-in-time dump of the store to
   `snapshot.json`. On startup the node restores the snapshot, then
   replays only the WAL records written after it, so restarts stay
   fast as the log grows.

## Failure modes

| What breaks | What happens |
| --- | --- |
| Leader process dies | Heartbeat timeout (3 s) fires, highest alive id takes over, term +1, writes resume. |
| Follower process dies | Leader keeps committing with the remaining majority; the follower catches up on return. |
| Follower misses writes while down | Leader resends the missing suffix; conflicting tails are rewound one entry at a time. |
| Leader partitioned from followers | Its writes stay uncommitted and answer 503; the majority side keeps working. |
| Partition heals | Catch-up resends missing entries and replaces any stale value the minority side wrote. |
| Node restarts | Snapshot + WAL replay restores its keys; its replication log is empty until the leader catches it up. |
| Slow client holds a connection | HTTP timeouts bound it: 5 s read-header, 10 s write, 120 s idle. |
| Key expires | TTL is checked on read and by a background sweep; an expired key reads as missing. |

## What is deliberately not here

- **No vote rounds in the election.** Highest alive id wins. Simple,
  and safe only because commit requires a whole-cluster majority.
- **No per-shard replication.** Every node holds every key; the ring
  routes reads.
- **No persistent replication log.** It lives in memory; restarts
  rebuild it through catch-up.
- **No auth, TLS, or access control.** The cluster trusts its network.
- **No multi-key transactions.** One key per write, in log order.

Each of those is a real next step, not an oversight: they were left
out to keep a two-week build to one tested idea per day. The tests in
`internal/replication/partition_test.go` and
`internal/chaos/chaos_test.go` pin down the safety property the
current design does guarantee — a partitioned minority cannot commit —
so any of those next steps starts from a tested base.
