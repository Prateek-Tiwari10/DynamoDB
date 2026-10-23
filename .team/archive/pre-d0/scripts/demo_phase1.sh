#!/usr/bin/env bash
# Phase 1 demo: acknowledged writes survive a SIGKILL of the node's container.
set -uo pipefail
cd "$(dirname "$0")/.."

step=0
header() {
  step=$((step + 1))
  echo
  echo "=== Step $step: $1"
}

fail() {
  echo
  echo "PHASE 1 DEMO FAILED: $1"
  exit 1
}

wait_ready() {
  for _ in $(seq 1 30); do
    kv get __ping__ > /dev/null 2>&1
    code=$?
    if [ "$code" -eq 0 ] || [ "$code" -eq 1 ]; then
      echo "node1 is ready"
      return 0
    fi
    sleep 1
  done
  fail "node1 did not become ready within 30s"
}

header "Clean start"
docker compose down
rm -rf ./data

header "Start node1"
docker compose up -d --build || fail "docker compose up failed"
wait_ready

header "Write and delete a few keys"
kv put cart:42 book || fail "put cart:42"
kv put user:7 prateek || fail "put user:7"
kv delete user:7 || fail "delete user:7"

header "Bulk write 1500 keys (triggers a checkpoint)"
python scripts/bulk_write.py --count 1500 --prefix bulk || fail "bulk write"

header "Files on disk"
ls -l data/node1/
wc -l data/node1/wal.log

header "Hard crash: SIGKILL node1"
docker compose kill -s SIGKILL node1 || fail "kill node1"

header "Remove the container"
docker compose rm -f node1 || fail "rm node1"

header "Start a fresh container on the same data"
docker compose up -d || fail "docker compose up failed"
wait_ready
docker compose logs node1 | grep "listening on"

header "Check that every acknowledged write survived"
value=$(kv get cart:42)
[ "$value" = "book" ] || fail "kv get cart:42 printed '$value', expected 'book'"
echo "cart:42 = $value"

kv get user:7
[ $? -eq 1 ] || fail "user:7 should be deleted"
echo "user:7 is still deleted"

python scripts/bulk_write.py --count 1500 --prefix bulk --verify || fail "bulk verify"

echo
echo "PHASE 1 DEMO PASSED"
exit 0
