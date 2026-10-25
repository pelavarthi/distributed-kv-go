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
```
