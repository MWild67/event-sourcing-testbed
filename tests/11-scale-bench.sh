#!/usr/bin/env bash
# Test 11 — Scale Benchmark (one year of history)
#
# Validates the requirement: "5 million events accessible (one year of history)".
#
# Default: 500 000 events (~35 s on PG, manageable in devcontainer).
# For the full 5M test: SCALE_EVENTS=5000000 ./tests/11-scale-bench.sh
#
# Metrics:
#   - Write throughput (overall + first-10% vs last-10%)
#   - Tail read: read last 500 events from a stream of N total
#   - Full-stream rehydration: replay all N events
#
# Pass criteria (at default 500 000 events):
#   - Write throughput > 1 000 ev/s
#   - Tail read < 500 ms  (500 000 µs)
#   - Rehydration throughput > 500 ev/s
#   - No throughput degradation > 50% from first-10% to last-10%

set -euo pipefail

TESTBED="${TESTBED:-/tmp/cargo-target/debug/testbed}"
KURRENTDB_URL="${KURRENTDB_URL:-kurrentdb://kurrentdb-bench:2113?tls=false}"
MONGODB_URL="${MONGODB_URL:-mongodb://mongodb:27017/?directConnection=true}"
PG_URL="${PG_URL:-postgres://postgres:postgres@postgres:5432/eventbench}"

SCALE_EVENTS="${SCALE_EVENTS:-500000}"
BATCH_SIZE=500
PASS=0
FAIL=0

check_ge() {
  local label="$1" actual="$2" min="$3"
  if [ "$actual" -ge "$min" ]; then
    echo "  PASS  $label: $actual >= $min"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $label: $actual < $min"
    FAIL=$((FAIL + 1))
  fi
}

check_le() {
  local label="$1" actual="$2" max="$3"
  if [ "$actual" -le "$max" ]; then
    echo "  PASS  $label: $actual <= $max"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $label: $actual > $max"
    FAIL=$((FAIL + 1))
  fi
}

run_backend() {
  local name="$1" subcmd="$2"
  shift 2
  echo ""
  echo "=== $name ($SCALE_EVENTS events) ==="
  local out
  out=$("$TESTBED" \
    --kurrentdb-url "$KURRENTDB_URL" \
    --mongodb-url   "$MONGODB_URL" \
    --postgres-url  "$PG_URL" \
    "$subcmd" \
    --scale-events "$SCALE_EVENTS" \
    --batch-size   "$BATCH_SIZE" \
    --json "$@" 2>/dev/null)
  echo "$out"

  local write_eps tail_us rehy_eps first10 last10
  write_eps=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['write_throughput_eps']))")
  tail_us=$(echo "$out"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['tail_read_us'])")
  rehy_eps=$(echo "$out"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['rehydrate_throughput_eps']))")
  first10=$(echo "$out"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['write_throughput_first10pct_eps']))")
  last10=$(echo "$out"    | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['write_throughput_last10pct_eps']))")

  check_ge "$name write throughput ev/s"   "$write_eps" 1000
  check_le "$name tail read µs"            "$tail_us"   500000
  check_ge "$name rehydration ev/s"        "$rehy_eps"  500
  # No more than 50% degradation: last10 >= first10 / 2
  local half_first10=$(( first10 / 2 ))
  check_ge "$name no write degradation"    "$last10"    "$half_first10"
}

run_backend "PostgreSQL" "pg-scale-bench" \
  --stream-name "test11-postgres"

run_backend "KurrentDB" "kurrentdb-scale-bench" \
  --stream-name "test11-kurrentdb"

run_backend "MongoDB" "mongo-scale-bench" \
  --stream-name "test11-mongo" \
  --database    "test11bench"

echo ""
echo "Results: $PASS passed, $FAIL failed"
echo ""
echo "Note: For the full 5M test run: SCALE_EVENTS=5000000 $0"
[ "$FAIL" -eq 0 ]
