#!/usr/bin/env bash
# Phase 3 demo on a running 5-node cluster (N=3, R=2, W=2, SUSPECT 5 s, DEAD 10 s):
#   1. failure detection: kill a node, watch it go SUSPECT then DEAD, start it, back to ALIVE
#   2. read repair: a node that missed a write during a short outage; one read fixes its copy
#   3. siblings: two clients write from the same context through two coordinators,
#      a read shows both values, a put with the read's context merges them
# Start the cluster first:  tools/cluster_local.sh start   or   make up  (then DOCKER=1)
set -uo pipefail
cd "$(dirname "$0")/.."

PY=${PYTHON:-python}
KEY=${KEY:-cart:42}
KEY2=${KEY2:-doc:7}
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
fail() { echo "PHASE 3 DEMO FAILED: $*"; exit 1; }

# state_of WATCHER NODE: NODE's state in WATCHER's member table (ALIVE / SUSPECT / DEAD)
state_of() { kv --node "localhost:$(port "$1")" info 2>/dev/null | awk -v n="$2" '$1 == n {print $3}'; }

# wait_state WATCHER NODE STATE SECONDS: wait until WATCHER sees NODE in STATE; prints the
# time since $SINCE (set just before the kill or start that the wait is about)
SINCE=$SECONDS
wait_state() {
    while [ "$(state_of "$1" "$2")" != "$3" ]; do
        [ $((SECONDS - SINCE)) -lt "$4" ] || fail "$1 still does not see $2 as $3 after $4 s"
        sleep 0.5
    done
    echo "$1 sees $2 as $3 ~$((SECONDS - SINCE)) s after the $EVENT"
}

# replicas_of KEY: prints "first second third"
replicas_of() { kv --node localhost:50051 locate "$1" | sed -n 's/^replicas //p' | tr -d ','; }

# Wait until all 5 nodes answer (containers may still be starting right after `make up`).
for i in 1 2 3 4 5; do
    for attempt in $(seq 50); do
        kv --node "localhost:5005$i" info >/dev/null 2>&1 && break
        [ "$attempt" -lt 50 ] || fail "node$i does not answer on localhost:5005$i (start the cluster first)"
        sleep 0.2
    done
done
sleep 1.5   # let node-to-node channels that were refused during startup reconnect (1 s backoff)

read -r first second third <<<"$(replicas_of "$KEY")"
[ -n "$third" ] || fail "locate $KEY"
step "replicas of $KEY: $first, $second, $third"
kv --node "localhost:$(port "$first")" put "$KEY" book -v || fail "put book"

# ---------- 1. failure detection ----------

step "1. kill $third and watch $first's view of it"
SINCE=$SECONDS; EVENT=kill; kill_node "$third"
wait_state "$first" "$third" SUSPECT 15
wait_state "$first" "$third" DEAD 25

step "start $third again"
SINCE=$SECONDS; EVENT=restart; start_node "$third"
wait_state "$first" "$third" ALIVE 20

# ---------- 2. read repair ----------
# The replica is made stale with a short outage: killed, one write, started again, all before
# gossip suspects it. (After a long outage a sloppy-quorum stand-in would hand the write over
# itself; that is the hinted handoff demo, tools/demo_sloppy.sh.)

step "2. kill $third, put $KEY = pen right away (W=2 met by $first and $second), start $third"
kill_node "$third"
kv --node "localhost:$(port "$first")" put "$KEY" pen -v || fail "put pen"
start_node "$third"
for _ in $(seq 50); do
    kv --node "localhost:$(port "$third")" info >/dev/null 2>&1 && break
    sleep 0.2
done
[ "$(state_of "$first" "$third")" = ALIVE ] || fail "$first should still see $third as ALIVE"

step "$third still holds the old value"
kv --node "localhost:$(port "$third")" local-get "$KEY"
kv --node "localhost:$(port "$third")" local-get "$KEY" | grep -q "] book " || fail "$third should still hold book"

step "get $KEY via $first (repairs $third in the background)"
got=$(kv --node "localhost:$(port "$first")" get "$KEY") || fail "get"
echo "$got"; [ "$got" = pen ] || fail "expected pen, got '$got'"

step "$third now holds the new value"
for attempt in $(seq 10); do
    kv --node "localhost:$(port "$third")" local-get "$KEY" | grep -q "] pen " && break
    [ "$attempt" -lt 10 ] || fail "$third was not repaired"
    sleep 0.5
    kv --node "localhost:$(port "$first")" get "$KEY" >/dev/null   # read again (channel may still reconnect)
done
kv --node "localhost:$(port "$third")" local-get "$KEY"

# ---------- 3. siblings ----------

read -r f2 s2 t2 <<<"$(replicas_of "$KEY2")"
[ -n "$t2" ] || fail "locate $KEY2"
step "3. siblings on $KEY2 (replicas $f2, $s2, $t2)"
A="$WORK/client_a.json"; B="$WORK/client_b.json"
kv --node "localhost:$(port "$f2")" --context-file "$A" put "$KEY2" draft || fail "put draft"
kv --node "localhost:$(port "$f2")" --context-file "$A" get "$KEY2" >/dev/null || fail "client A get"
kv --node "localhost:$(port "$s2")" --context-file "$B" get "$KEY2" >/dev/null || fail "client B get"
echo "clients A and B both read draft (same context)"

step "client A writes x (coordinated by $f2)"
kv --node "localhost:$(port "$f2")" --context-file "$A" put "$KEY2" x || fail "put x"

step "kill $f2, so client B's write is coordinated by $s2"
# Through the same coordinator the second write would look newer (its counter is higher);
# through a different coordinator the two clocks are really concurrent.
SINCE=$SECONDS; EVENT=kill; kill_node "$f2"
wait_state "$s2" "$f2" SUSPECT 15
kv --node "localhost:$(port "$s2")" --context-file "$B" put "$KEY2" y || fail "put y"

step "start $f2 again"
SINCE=$SECONDS; EVENT=restart; start_node "$f2"
wait_state "$s2" "$f2" ALIVE 20

step "get $KEY2: both writes are kept as siblings"
got=$(kv --node "localhost:$(port "$s2")" --context-file "$B" get "$KEY2") || fail "get siblings"
echo "$got"
echo "$got" | grep -qx "\[[12]\] x" && echo "$got" | grep -qx "\[[12]\] y" || fail "expected siblings x and y"

step "client B merges them: put xy with the context of that read"
kv --node "localhost:$(port "$s2")" --context-file "$B" put "$KEY2" xy || fail "put xy"
got=$(kv --node "localhost:$(port "$s2")" get "$KEY2") || fail "get after merge"
echo "$got"; [ "$got" = xy ] || fail "expected xy, got '$got'"

rm -rf "$WORK"
echo
echo "PHASE 3 DEMO PASSED"
