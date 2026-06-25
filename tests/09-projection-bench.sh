#!/usr/bin/env bash
# Test 09 — Projection / Subscription-Lag Benchmark
#
# Validates the "500 most-recent orders immediately visible on the UI" pattern:
#   1. Cold-start rebuild: projector replays historical events, populates view
#   2. Subscription lag: write-ack → view-updated  (p50 / p99)
#   3. View read: materialised-view access latency (always sub-millisecond)
#
# Pass criteria:
#   - Cold-start rebuild < 5 000 ms   (10 000 events)
#   - Subscription lag p99 < 100 ms
#   - View read p99 < 1 ms  (1 000 000 ns)

set -euo pipefail

TESTBED="${TESTBED:-/tmp/cargo-target/debug/testbed}"
KURRENTDB_URL="${KURRENTDB_URL:-kurrentdb://kurrentdb-bench:2113?tls=false}"
MONGODB_URL="${MONGODB_URL:-mongodb://mongodb:27017/?directConnection=true}"
PG_URL="${PG_URL:-postgres://postgres:postgres@postgres:5432/eventbench}"

SEED_EVENTS=10000
LIVE_EVENTS=300
VIEW_SIZE=500
PASS=0
FAIL=0

check() {
  local label="$1" actual="$2" limit="$3"
  if [ "$actual" -le "$limit" ]; then
    echo "  PASS  $label: $actual <= $limit"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $label: $actual > $limit (limit $limit)"
    FAIL=$((FAIL + 1))
  fi
}

run_backend() {
  local name="$1" subcmd="$2"
  shift 2
  echo ""
  echo "=== $name ==="
  local out
  out=$("$TESTBED" \
    --kurrentdb-url "$KURRENTDB_URL" \
    --mongodb-url   "$MONGODB_URL" \
    --postgres-url  "$PG_URL" \
    "$subcmd" \
    --seed-events "$SEED_EVENTS" \
    --live-events "$LIVE_EVENTS" \
    --view-size   "$VIEW_SIZE" \
    --json "$@" 2>/dev/null)
  echo "$out"

  local cold_ms lag_p99 view_p99_us
  cold_ms=$(echo "$out"    | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['cold_start_ms']))")
  lag_p99=$(echo "$out"    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['lag_p99_us'])")
  view_p99_us=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['view_read_p99_us'])")

  check "$name cold-start ms" "$cold_ms"    5000
  check "$name lag p99 µs"    "$lag_p99"   100000
  check "$name view-read p99 µs" "$view_p99_us" 1000
}

run_backend "KurrentDB" "kurrentdb-projection-bench" \
  --stream-name "test09-kurrentdb"

run_backend "MongoDB" "mongo-projection-bench" \
  --stream-name "test09-mongo" \
  --database    "test09bench"

run_backend "PostgreSQL" "pg-projection-bench" \
  --stream-name "test09-postgres"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
