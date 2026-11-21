#!/usr/bin/env bash
# Check that the 5-node cluster agrees: every node answers, lists 5 members, has the same ring
# fingerprint and gives the same replicas for a key.
#   tools/check_cluster.sh [HOST] [KEY]      default: localhost cart:42, ports 50051-50055
# Prints CLUSTER OK and exits 0, or names what disagrees and exits 1.
set -uo pipefail
cd "$(dirname "$0")/.."

# The project's .venv if there is one (the system python usually lacks grpc), else python.
if [ -x .venv/bin/python ]; then PY=${PYTHON:-.venv/bin/python}; else PY=${PYTHON:-python}; fi
"$PY" -c "import grpc, yaml" 2>/dev/null ||
    { echo "$PY cannot import grpc/yaml: run 'pip install -r requirements.txt' or set PYTHON=..." >&2; exit 3; }
HOST=${1:-localhost}
KEY=${2:-cart:42}
ok=1
fingerprints=""
placements=""

for i in 1 2 3 4 5; do
    address="$HOST:5005$i"
    if ! info=$("$PY" -m client.cli --node "$address" --timeout 2 info 2>&1); then
        echo "$address: unreachable"
        ok=0
        continue
    fi
    members=$(echo "$info" | tail -n +3 | grep -c .)
    fingerprint=$(echo "$info" | head -1 | awk '{print $NF}')
    placement=$("$PY" -m client.cli --node "$address" --timeout 2 locate "$KEY" | tr '\n' ' ')
    echo "$address: $members members, ring $fingerprint, $KEY -> $placement"
    [ "$members" -eq 5 ] || { echo "  expected 5 members"; ok=0; }
    fingerprints+="$fingerprint"$'\n'
    placements+="$placement"$'\n'
done

if [ "$(printf '%s' "$fingerprints" | sort -u | grep -c .)" -gt 1 ]; then
    echo "ring fingerprints differ"
    ok=0
fi
if [ "$(printf '%s' "$placements" | sort -u | grep -c .)" -gt 1 ]; then
    echo "nodes disagree on where $KEY lives"
    ok=0
fi

if [ "$ok" -eq 1 ]; then
    echo "CLUSTER OK"
else
    echo "CLUSTER NOT OK"
    exit 1
fi
