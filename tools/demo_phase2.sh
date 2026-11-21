#!/usr/bin/env bash
# Phase 2 demo on a running 5-node cluster (N=3, R=2, W=2):
#   write via node1, read via node4, show the replicas, kill one replica (still works),
#   kill a second replica (QUORUM FAILED), then start both again.
# Start the cluster first:  tools/cluster_local.sh start   or   make up  (then DOCKER=1)
set -uo pipefail
cd "$(dirname "$0")/.."

# The project's .venv if there is one (the system python usually lacks grpc), else python.
if [ -x .venv/bin/python ]; then PY=${PYTHON:-.venv/bin/python}; else PY=${PYTHON:-python}; fi
"$PY" -c "import grpc, yaml" 2>/dev/null ||
    { echo "$PY cannot import grpc/yaml: run 'pip install -r requirements.txt' or set PYTHON=..." >&2; exit 3; }
KEY=${KEY:-cart:42}
WORK=$(mktemp -d)
export KV_CONTEXT="$WORK/ctx.json"

kv()   { "$PY" -m client.cli --timeout 3 "$@"; }
port() { echo "5005${1#node}"; }                     # node3 -> 50053
kill_node() {
    if [ "${DOCKER:-0}" = 1 ]; then docker compose kill "$1" >/dev/null; else tools/cluster_local.sh kill "$1"; fi
}
start_node() {
    if [ "${DOCKER:-0}" = 1 ]; then docker compose start "$1" >/dev/null; else tools/cluster_local.sh start "$1"; fi
}
step() { echo; echo "== $*"; }
fail() { echo "PHASE 2 DEMO FAILED: $*"; exit 1; }

# Wait until all 5 nodes answer (containers may still be starting right after `make up`).
for i in 1 2 3 4 5; do
    for attempt in $(seq 50); do
        kv --node "localhost:5005$i" info >/dev/null 2>&1 && break
        [ "$attempt" -lt 50 ] || fail "node$i does not answer on localhost:5005$i (start the cluster first)"
        sleep 0.2
    done
done
sleep 1.5   # let node-to-node channels that were refused during startup reconnect (1 s backoff)

step "replicas of $KEY"
placement=$(kv --node localhost:50051 locate "$KEY") || fail "locate"
echo "$placement"
read -r first second third <<<"$(echo "$placement" | sed -n 's/^replicas //p' | tr -d ',')"

step "put $KEY = book via node1"
kv --node localhost:50051 put "$KEY" book -v || fail "put"

step "get $KEY via node4"
got=$(kv --node localhost:50054 get "$KEY" -v) || fail "get"
echo "$got"; [ "$got" = book ] || fail "expected book, got '$got'"

step "kill $third (one replica down)"
kill_node "$third"

step "put and get still work with 2 of 3 replicas"
kv --node "localhost:$(port "$first")" put "$KEY" pen -v || fail "put with one replica down"
got=$(kv --node "localhost:$(port "$first")" get "$KEY") || fail "get with one replica down"
echo "$got"; [ "$got" = pen ] || fail "expected pen, got '$got'"

step "kill $second too (two replicas down)"
kill_node "$second"

step "put now fails: only 1 of 3 replicas can answer, W=2"
kv --node "localhost:$(port "$first")" put "$KEY" cup
code=$?
[ "$code" -eq 4 ] || fail "expected exit code 4 (QUORUM FAILED), got $code"

step "start $second and $third again"
start_node "$second"; start_node "$third"
for _ in $(seq 50); do
    kv --node "localhost:$(port "$second")" info >/dev/null 2>&1 &&
        kv --node "localhost:$(port "$third")" info >/dev/null 2>&1 && break
    sleep 0.2
done
# node1's channels to the restarted nodes reconnect within about 1 s (1 s reconnect backoff);
# until then calls to them fail fast, so retry for a few seconds.
for attempt in $(seq 10); do
    if kv --node "localhost:$(port "$first")" put "$KEY" cup 2>/dev/null; then break; fi
    [ "$attempt" -lt 10 ] || fail "put after restart"
    sleep 0.5
done

rm -rf "$WORK"
echo
echo "PHASE 2 DEMO PASSED"
