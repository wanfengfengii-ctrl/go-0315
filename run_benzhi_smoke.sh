#!/usr/bin/env bash
# Smoke test for the archival replica integrity and recovery service.
#
# This script builds the service, starts it on a local port backed by a
# temporary SQLite database, and drives a real end-to-end API flow (create,
# catalog, freeze, open epoch, submit evidence, close) over localhost only.
# It never touches the network and cleans up every process and file it creates.
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PORT="${SMOKE_PORT:-18080}"
readonly BASE="http://127.0.0.1:${PORT}"

# A single trap guarantees cleanup of the server process and temp files even on
# early failure.
WORKDIR="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "==> building service"
( cd "$SCRIPT_DIR" && go build -o "$WORKDIR/server" ./cmd/server )

echo "==> starting service on ${PORT}"
DB_PATH="$WORKDIR/archival.db" LISTEN_ADDR="127.0.0.1:${PORT}" "$WORKDIR/server" &
SERVER_PID=$!

# Wait for the health endpoint to come up.
ready=""
for ((i=0; i<100; i++)); do
  if resp="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/healthz" 2>/dev/null || true)"; then
    if [[ "$resp" == "200" ]]; then
      ready="1"
      break
    fi
  fi
  sleep 0.1
done
if [[ "$ready" != "1" ]]; then
  echo "service did not become healthy" >&2
  exit 1
fi

HEX32="1111111111111111111111111111111111111111111111111111111111111111"

echo "==> create batch"
status="$(curl -s -o "$WORKDIR/create.json" -w '%{http_code}' -X POST "$BASE/v1/batches" \
  -H 'Content-Type: application/json' \
  -d "{\"batch_id\":\"smoke-1\"}")"
[[ "$status" == "201" ]] || { echo "create batch failed: $status"; cat "$WORKDIR/create.json"; exit 1; }

echo "==> catalog batch"
status="$(curl -s -o "$WORKDIR/catalog.json" -w '%{http_code}' -X PUT "$BASE/v1/batches/smoke-1/catalog" \
  -H 'Content-Type: application/json' \
  -d "{\"objects\":[{\"object_id\":\"obj-1\",\"expected_length\":8,\"expected_root\":\"$HEX32\"}],\"dependencies\":[],\"nodes\":[{\"node_id\":\"n1\",\"failure_domain\":\"rack-a\",\"enabled\":true},{\"node_id\":\"n2\",\"failure_domain\":\"rack-b\",\"enabled\":true}]}")"
[[ "$status" == "200" ]] || { echo "catalog failed: $status"; cat "$WORKDIR/catalog.json"; exit 1; }

echo "==> freeze batch"
status="$(curl -s -o "$WORKDIR/freeze.json" -w '%{http_code}' -X POST "$BASE/v1/batches/smoke-1/freeze" \
  -H 'Content-Type: application/json' \
  -d '{"chunk_size":8,"hash_algorithm":"sha256","replica_quorum":1,"coverage_bps":10000,"stable_ticks":0,"schedule":"daily","reviewers":["alice","bob"]}')"
[[ "$status" == "200" ]] || { echo "freeze failed: $status"; cat "$WORKDIR/freeze.json"; exit 1; }
policy_digest="$(sed -n 's/.*"policy_digest":"\([^"]*\)".*/\1/p' "$WORKDIR/freeze.json")"
[[ -n "$policy_digest" ]] || { echo "freeze returned no policy_digest"; exit 1; }

echo "==> open epoch"
status="$(curl -s -o "$WORKDIR/epoch.json" -w '%{http_code}' -X POST "$BASE/v1/batches/smoke-1/epochs")"
[[ "$status" == "201" ]] || { echo "open epoch failed: $status"; cat "$WORKDIR/epoch.json"; exit 1; }
epoch_no="$(sed -n 's/.*"epoch_no":\([0-9]*\).*/\1/p' "$WORKDIR/epoch.json")"
[[ -n "$epoch_no" ]] || { echo "open epoch returned no epoch_no"; exit 1; }

echo "==> submit evidence"
status="$(curl -s -o "$WORKDIR/evidence.json" -w '%{http_code}' -X POST "$BASE/v1/batches/smoke-1/epochs/${epoch_no}/evidence" \
  -H 'Content-Type: application/json' \
  -d "{\"object_id\":\"obj-1\",\"node_id\":\"n1\",\"chunk_no\":0,\"length\":8,\"digest\":\"$HEX32\",\"operation_id\":\"op-1\",\"observed_tick\":1000}")"
[[ "$status" == "201" ]] || { echo "evidence failed: $status"; cat "$WORKDIR/evidence.json"; exit 1; }

echo "==> close epoch"
status="$(curl -s -o "$WORKDIR/close.json" -w '%{http_code}' -X POST "$BASE/v1/batches/smoke-1/epochs/${epoch_no}/close")"
[[ "$status" == "200" ]] || { echo "close failed: $status"; cat "$WORKDIR/close.json"; exit 1; }

echo "==> read back batch"
status="$(curl -s -o "$WORKDIR/get.json" -w '%{http_code}' "$BASE/v1/batches/smoke-1")"
[[ "$status" == "200" ]] || { echo "get batch failed: $status"; exit 1; }
batch_status="$(sed -n 's/.*"status":"\([^"]*\)".*/\1/p' "$WORKDIR/get.json")"
[[ "$batch_status" == "frozen" ]] || { echo "unexpected batch status: $batch_status"; exit 1; }

echo "==> smoke test passed (policy_digest=$policy_digest)"
