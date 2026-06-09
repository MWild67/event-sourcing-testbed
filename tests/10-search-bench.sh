#!/usr/bin/env bash
# Test 10 — Search-Index Projection Benchmark
#
# Validates the requirement: "all events are searchable".
#
# A projector reads from each event-store backend and writes every event into
# a PostgreSQL full-text search table (search_index).  The BFF queries that
# table — never the event store — for search results.
#
# Metrics:
#   - Index build time (50 000 events projected)
#   - Indexing lag: write-ack → search-index updated (p99)
#   - Query latency: exact / prefix / full-text / date-range (200 reps each)
#
# Pass criteria:
#   - Index build < 60 000 ms
#   - Indexing lag p99 < 100 000 µs  (100 ms)
#   - Exact query p99 < 50 000 µs    (50 ms)
#   - Full-text query p99 < 200 000 µs (200 ms)

set -euo pipefail

TESTBED="${TESTBED:-/tmp/cargo-target/debug/testbed}"
KURRENTDB_URL="${KURRENTDB_URL:-kurrentdb://kurrentdb-bench:2113?tls=false}"
MONGODB_URL="${MONGODB_URL:-mongodb://mongodb:27017/?directConnection=true}"
PG_URL="${PG_URL:-postgres://postgres:postgres@postgres:5432/eventbench}"

SEED=50000
LIVE=300
PASS=0
FAIL=0

check() {
  local label="$1" actual="$2" limit="$3"
  if [ "$actual" -le "$limit" ]; then
    echo "  PASS  $label: $actual <= $limit"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $label: $actual > $limit"
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
    --seed-events "$SEED" \
    --live-events "$LIVE" \
    --json "$@" 2>/dev/null)
  echo "$out"

  local build_ms lag_p99 exact_p99 fts_p99
  build_ms=$(echo "$out"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['index_build_ms']))")
  lag_p99=$(echo "$out"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['lag_p99_us'])")
  exact_p99=$(echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['query_exact_p99_us'])")
  fts_p99=$(echo "$out"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['query_fts_p99_us'])")

  check "$name index build ms"   "$build_ms"  60000
  check "$name lag p99 µs"       "$lag_p99"   100000
  check "$name exact p99 µs"     "$exact_p99" 50000
  check "$name FTS p99 µs"       "$fts_p99"   200000
}

run_backend "KurrentDB" "kurrentdb-search-bench" \
  --stream-name "test10-kurrentdb"

run_backend "MongoDB" "mongo-search-bench" \
  --stream-name "test10-mongo" \
  --database    "test10bench"

run_backend "PostgreSQL" "pg-search-bench" \
  --stream-name "test10-postgres"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
