#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 08 — Hot-Tail-Cache Benchmark
#
# Scenario
# ────────
# Each backend (KurrentDB, MongoDB, PostgreSQL) is tested in four phases:
#
#   1. Seed      — write 50 000 events in batches of 100
#   2. Startup   — load the last 500 events into an in-memory ring buffer
#                  with a SINGLE database query; measure latency
#   3. Cache     — read the 500 in-memory events 1 000 times; no DB queries
#   4. Live write — append 500 more events one at a time while updating the
#                  cache; measure DB-write latency and cache-push latency
#
# Passes when:
#   • Startup load   < STARTUP_LIMIT_MS   (default 500 ms)
#   • Cache-read p99 < CACHE_P99_LIMIT_US (default 500 µs  = 500 000 ns)
#   • DB-write p99   < DB_WRITE_P99_LIMIT_MS (default 50 ms)
#
# Usage:
#   ./tests/08-hot-cache-bench.sh             # auto-detect direct / K8s
#   DIRECT=1 ./tests/08-hot-cache-bench.sh   # force direct binary execution
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SEED_EVENTS="${SEED_EVENTS:-50000}"
CACHE_SIZE="${CACHE_SIZE:-500}"
LIVE_WRITES="${LIVE_WRITES:-500}"

# Pass / fail thresholds
STARTUP_LIMIT_MS="${STARTUP_LIMIT_MS:-500}"         # startup load must be < this
CACHE_P99_LIMIT_US="${CACHE_P99_LIMIT_US:-500}"     # cache-read p99 in µs (ns/1000)
DB_WRITE_P99_LIMIT_MS="${DB_WRITE_P99_LIMIT_MS:-50}" # live DB-write p99 in ms

DIRECT="${DIRECT:-0}"

# Connection URLs (override via environment for CI / custom deployments)
KURRENT_URL="${KURRENT_URL:-kurrentdb://localhost:2113?tls=false}"
MONGO_URL="${MONGO_URL:-mongodb://localhost:27017}"
POSTGRES_URL="${POSTGRES_URL:-postgres://postgres:postgres@localhost:5432/eventbench}"

TESTBED="${TESTBED_BIN:-./target/release/testbed}"

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }
step() { echo; echo "▶ $*"; }

# ── Helpers ───────────────────────────────────────────────────────────────────
parse_json_field() {
    local json="$1" field="$2"
    echo "$json" | grep -oP "\"${field}\":\\s*\\K[^,}]+" || true
}

check_thresholds() {
    local backend="$1" json="$2"

    local startup_us cache_p99_us db_p99_us
    startup_us=$(parse_json_field "$json" "startup_load_us")
    cache_p99_us=$(parse_json_field "$json" "cache_read_p99_us")
    db_p99_us=$(parse_json_field "$json" "db_write_p99_us")

    # Convert to display units
    local startup_ms cache_p99_display db_p99_ms
    startup_ms=$(echo "$startup_us" | awk '{printf "%.1f", $1/1000}')
    cache_p99_display=$(echo "$cache_p99_us" | awk '{printf "%.1f", $1}')
    db_p99_ms=$(echo "$db_p99_us" | awk '{printf "%.1f", $1/1000}')

    echo
    echo "  ── $backend thresholds ──"
    echo "  Startup load   : ${startup_ms} ms  (limit ${STARTUP_LIMIT_MS} ms)"
    echo "  Cache-read p99 : ${cache_p99_display} µs  (limit ${CACHE_P99_LIMIT_US} µs)"
    echo "  DB-write p99   : ${db_p99_ms} ms  (limit ${DB_WRITE_P99_LIMIT_MS} ms)"

    # Integer comparisons (all in µs)
    local startup_limit_us=$(( STARTUP_LIMIT_MS * 1000 ))
    local cache_limit_us=$(( CACHE_P99_LIMIT_US ))
    local db_limit_us=$(( DB_WRITE_P99_LIMIT_MS * 1000 ))

    [[ "$startup_us" -le "$startup_limit_us" ]] \
        || fail "$backend startup load ${startup_ms} ms exceeds ${STARTUP_LIMIT_MS} ms limit"

    [[ "$cache_p99_us" -le "$cache_limit_us" ]] \
        || fail "$backend cache-read p99 ${cache_p99_display} µs exceeds ${CACHE_P99_LIMIT_US} µs limit"

    [[ "$db_p99_us" -le "$db_limit_us" ]] \
        || fail "$backend DB-write p99 ${db_p99_ms} ms exceeds ${DB_WRITE_P99_LIMIT_MS} ms limit"

    pass "$backend all thresholds met"
}

run_bench() {
    local cmd="$1" backend="$2"
    shift 2
    local extra_args=("$@")

    step "Running hot-cache benchmark: $backend"

    local json_output
    json_output=$(
        "$TESTBED" \
            --kurrentdb-url "$KURRENT_URL" \
            --mongodb-url   "$MONGO_URL" \
            --postgres-url  "$POSTGRES_URL" \
            "$cmd" \
            --seed-events  "$SEED_EVENTS" \
            --cache-size   "$CACHE_SIZE" \
            --live-writes  "$LIVE_WRITES" \
            "${extra_args[@]}" \
            --json 2>/dev/null
    )

    echo "  Raw JSON: $json_output"
    check_thresholds "$backend" "$json_output"
}

# ─────────────────────────────────────────────────────────────────────────────
# Build (if running directly and binary is stale)
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$DIRECT" == "1" ]]; then
    step "Building testbed binary (release)"
    (cd "$(dirname "$0")/.." && cargo build --release -q) \
        || fail "cargo build failed"
    TESTBED="$(dirname "$0")/../target/release/testbed"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Run all three backends
# ─────────────────────────────────────────────────────────────────────────────
run_bench "kurrentdb-hot-cache-bench" "KurrentDB"
run_bench "mongo-hot-cache-bench"     "MongoDB"   --database hotcache
run_bench "pg-hot-cache-bench"        "PostgreSQL"

echo
pass "Test 08 — hot-tail-cache benchmark: all backends passed"
