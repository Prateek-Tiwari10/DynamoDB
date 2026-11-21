#!/usr/bin/env bash
# Sloppy quorum + hinted handoff demo on a running 5-node cluster (N=3, R=2, W=2, sloppy_quorum on):
#   kill 2 of the 3 owners of a key, a write still succeeds (an ALIVE stand-in holds a hint),
#   reads stay strict (R=2 fails, R=1 works), start the owners, the stand-in hands the write over.
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
fail() { echo "SLOPPY QUORUM DEMO FAILED: $*"; exit 1; }
state_of() { kv --node "localhost:$(port "$1")" info 2>/dev/null | awk -v n="$2" '$1 == n {print $3}'; }

# wait_until SECONDS DESCRIPTION COMMAND...: retry COMMAND every 0.5 s
wait_until() {
    local limit=$1 what=$2 start=$SECONDS
    shift 2
    until "$@"; do
        [ $((SECONDS - start)) -lt "$limit" ] || fail "$what (waited $limit s)"
        sleep 0.5
    done
    echo "$what after ~$((SECONDS - start)) s"
}
not_alive() { [ "$(state_of "$1" "$2")" != ALIVE ] && [ -n "$(state_of "$1" "$2")" ]; }
is_alive()  { [ "$(state_of "$1" "$2")" = ALIVE ]; }
holds()     { kv --node "localhost:$(port "$1")" local-get "$KEY" 2>/dev/null | grep -q "] $2 "; }

# Wait until all 5 nodes answer (containers may still be starting right after `make up`).
for i in 1 2 3 4 5; do
    for attempt in $(seq 50); do
        kv --node "localhost:5005$i" info >/dev/null 2>&1 && break
        [ "$attempt" -lt 50 ] || fail "node$i does not answer on localhost:5005$i (start the cluster first)"
        sleep 0.2
    done
done
sleep 1.5   # let node-to-node channels that were refused during startup reconnect (1 s backoff)

read -r first second third <<<"$(kv --node localhost:50051 locate "$KEY" | sed -n 's/^replicas //p' | tr -d ',')"
[ -n "$third" ] || fail "locate $KEY"
step "owners of $KEY: $first, $second, $third"

step "kill $second and $third (2 of 3 owners down)"
kill_node "$second"; kill_node "$third"
wait_until 20 "$first sees $second as not ALIVE" not_alive "$first" "$second"
wait_until 20 "$first sees $third as not ALIVE" not_alive "$first" "$third"

step "put $KEY = book via $first: W=2 is met with a stand-in"
out=$(kv --node "localhost:$(port "$first")" put "$KEY" book -v 2>&1) || { echo "$out"; fail "put with sloppy quorum"; }
echo "$out"
stand_in=$(echo "$out" | sed -n 's/^written_to: //p' | tr -d ',' | awk '{print $2}')
case "$stand_in" in ""|"$first"|"$second"|"$third") fail "expected a non-owner in written_to, got '$stand_in'";; esac
echo "stand-in: $stand_in"

step "$stand_in keeps it as a hint, not as its own data"
kv --node "localhost:$(port "$stand_in")" local-get "$KEY"
[ $? -eq 1 ] || fail "local-get on the stand-in should say NOT FOUND"

step "reads stay strict: R=2 needs 2 owners, only $first is up"
kv --node "localhost:$(port "$first")" get "$KEY"
[ $? -eq 4 ] || fail "expected QUORUM FAILED (exit 4) for R=2"
got=$(kv --node "localhost:$(port "$first")" get "$KEY" --r 1) || fail "get --r 1"
echo "with --r 1: $got"; [ "$got" = book ] || fail "expected book, got '$got'"

step "start $second and $third again"
start_node "$second"; start_node "$third"
wait_until 20 "$stand_in sees $second as ALIVE" is_alive "$stand_in" "$second"
wait_until 20 "$stand_in sees $third as ALIVE" is_alive "$stand_in" "$third"

step "the stand-ins hand the write over"
wait_until 15 "$second holds book" holds "$second" book
wait_until 15 "$third holds book" holds "$third" book
kv --node "localhost:$(port "$second")" local-get "$KEY"
kv --node "localhost:$(port "$third")" local-get "$KEY"
got=$(kv --node "localhost:$(port "$first")" get "$KEY") || fail "get after handoff"
echo "get with R=2: $got"; [ "$got" = book ] || fail "expected book, got '$got'"

rm -rf "$WORK"
echo
echo "SLOPPY QUORUM DEMO PASSED"
