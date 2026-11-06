#!/usr/bin/env bash
# Demo: start a three-node cluster, write through the leader, kill
# the leader, write through the new leader, and bring the old leader
# back. Run from the repo root: ./scripts/demo.sh (or: make demo)
set -euo pipefail

cd "$(dirname "$0")/.."

PORTS=(8080 8081 8082)
IDS=(node1 node2 node3)
WORK=$(mktemp -d)
BIN="$WORK/bin"
mkdir -p "$BIN"
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

say() { printf '\n==> %s\n' "$*"; }

# peer_list <own port> -> the other two nodes as a comma list
peer_list() {
	local out="" p
	for p in 8080 8081 8082; do
		if [ "$p" != "$1" ]; then
			[ -n "$out" ] && out="$out,"
			out="${out}http://localhost:$p"
		fi
	done
	printf '%s' "$out"
}

wait_for() { # wait_for <port> <name>
	local i
	for i in $(seq 1 30); do
		if curl -sf "http://localhost:$1/health" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.5
	done
	echo "timed out waiting for $2 on :$1" >&2
	exit 1
}

leader_id() { # leader_id <port> -> node id, fails if unreachable
	local out
	out=$(kvctl -addr "http://localhost:$1" leader 2>/dev/null | awk '{print $1}')
	[ -n "$out" ] || return 1
	printf '%s' "$out"
}

port_of() { # port_of <node id>
	case "$1" in
		node1) echo 8080 ;;
		node2) echo 8081 ;;
		node3) echo 8082 ;;
		*) echo "unknown node: $1" >&2; exit 1 ;;
	esac
}

start_node() { # start_node <index>
	local i="$1"
	"$BIN/server" -id "${IDS[$i]}" -listen ":${PORTS[$i]}" \
		-advertise "http://localhost:${PORTS[$i]}" \
		-peers "$(peer_list "${PORTS[$i]}")" \
		-data-dir "$WORK/${IDS[$i]}" >>"$WORK/${IDS[$i]}.log" 2>&1 &
}

say "building server and kvctl"
go build -o "$BIN/server" ./cmd/server
go build -o "$BIN/kvctl" ./cmd/kvctl
export PATH="$BIN:$PATH"

say "starting three nodes (node1 :8080, node2 :8081, node3 :8082)"
for i in 0 1 2; do start_node "$i"; done
for i in 0 1 2; do wait_for "${PORTS[$i]}" "${IDS[$i]}"; done
sleep 2 # let the first heartbeat round settle the membership

say "cluster members (from node1)"
kvctl -addr http://localhost:8080 members

LEADER=$(leader_id 8080)
LPORT=$(port_of "$LEADER")
say "leader is $LEADER"

say "writing two keys through the leader ($LEADER)"
kvctl -addr "http://localhost:$LPORT" set hello world
kvctl -addr "http://localhost:$LPORT" set lang go
sleep 1 # let replication reach the followers

say "reading them back from node1 (forwarded to the key's owner)"
kvctl -addr http://localhost:8080 get hello
kvctl -addr http://localhost:8080 get lang

say "killing the leader ($LEADER on :$LPORT)"
pkill -f "server -id $LEADER" || true
sleep 5 # heartbeat timeout is 3s; a new leader is elected

say "members now"
kvctl -addr http://localhost:8080 members 2>/dev/null \
	|| kvctl -addr http://localhost:8081 members
NEW_LEADER=$(leader_id 8080 || leader_id 8081 || leader_id 8082)
NPORT=$(port_of "$NEW_LEADER")
say "new leader is $NEW_LEADER"

say "writing through the new leader ($NEW_LEADER)"
kvctl -addr "http://localhost:$NPORT" set after failover
sleep 1
say "reading it back from another node"
if [ "$NPORT" = 8080 ]; then RPORT=8081; else RPORT=8080; fi
kvctl -addr "http://localhost:$RPORT" get after

say "bringing $LEADER back"
case "$LEADER" in
	node1) start_node 0 ;;
	node2) start_node 1 ;;
	node3) start_node 2 ;;
esac
wait_for "$LPORT" "$LEADER"
sleep 2

say "catching the returned node up from the current leader"
CUR_LEADER=$(leader_id 8080 || leader_id 8081 || leader_id 8082)
CPORT=$(port_of "$CUR_LEADER")
say "current leader is $CUR_LEADER"
kvctl -addr "http://localhost:$CPORT" catchup
sleep 1
say "the key written while it was down, read from the returned node"
kvctl -addr "http://localhost:$LPORT" get after

say "demo done — all three nodes are being stopped"
