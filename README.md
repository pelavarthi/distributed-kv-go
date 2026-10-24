# distributed-kv-go
Distributed key-value store in Go: in-memory KV, WAL, replication, sharding.

Run the server:

```
go run ./cmd/server
```

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
