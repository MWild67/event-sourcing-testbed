#!/usr/bin/env bash
# Test 18 — Memcached write-through hot-tail-cache benchmark (all three backends)
#
# Phases per backend:
#   1. Seed 50 000 events to the DB
#   2. Cold-miss startup: load last 500 from DB → SET into Memcached
#   3. 1 000 × Memcached GET (measures network round-trip read latency)
#   4. 500 live writes: DB write + Memcached SET (write-through)
#
# Pass criteria:
#   startup_db_load_us < 500 000  (< 500 ms)
#   startup_mc_set_us  < 100 000  (< 100 ms)
#   cache_read_p99_us  < 50 000   (< 50 ms — generous for localhost loopback)
#   mc_set_p99_us      < 50 000   (< 50 ms)
#
# Usage:
#   ./tests/18-memcached-bench.sh
set -euo pipefail

KURRENTDB_URL="${KURRENTDB_URL:-kurrentdb://localhost:2113?tls=false}"
MONGODB_URL="${MONGODB_URL:-mongodb://localhost:27017}"
POSTGRES_URL="${POSTGRES_URL:-postgres://postgres:postgres@localhost:5432/eventbench}"
MEMCACHED_URL="${MEMCACHED_URL:-memcache://localhost:11211}"

SEED_EVENTS="${SEED_EVENTS:-50000}"
CACHE_SIZE="${CACHE_SIZE:-500}"
LIVE_WRITES="${LIVE_WRITES:-500}"

STARTUP_DB_LIMIT_US="${STARTUP_DB_LIMIT_US:-500000}"
STARTUP_MC_LIMIT_US="${STARTUP_MC_LIMIT_US:-100000}"
CACHE_READ_P99_LIMIT_US="${CACHE_READ_P99_LIMIT_US:-50000}"
MC_SET_P99_LIMIT_US="${MC_SET_P99_LIMIT_US:-50000}"

BINARY="${BINARY:-./rust-app/target/release/testbed}"
if [[ ! -f "$BINARY" ]]; then
    BINARY="./rust-app/target/debug/testbed"
fi
if [[ ! -f "$BINARY" ]]; then
    echo "testbed binary not found — run 'cargo build' first." >&2
    exit 1
fi

pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*" >&2; FAILURES=$((FAILURES + 1)); }

run_backend() {
    local cmd="$1" backend="$2" extra_args=("${@:3}")
    echo ""
    echo "▶ $backend"

    local result
    result=$("$BINARY" \
        --kurrentdb-url "$KURRENTDB_URL" \
        --mongodb-url   "$MONGODB_URL" \
        --postgres-url  "$POSTGRES_URL" \
        --memcached-url "$MEMCACHED_URL" \
        "$cmd" \
        --seed-events      "$SEED_EVENTS" \
        --cache-size       "$CACHE_SIZE" \
        --live-writes      "$LIVE_WRITES" \
        "${extra_args[@]}" \
        --json 2>/dev/null)

    echo "  Raw JSON: $result"

    local sdl sms cr99 ms99
    sdl=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['startup_db_load_us'])")
    sms=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['startup_mc_set_us'])")
    cr99=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['cache_read_p99_us'])")
    ms99=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['mc_set_p99_us'])")

    [[ "$sdl"  -lt "$STARTUP_DB_LIMIT_US"    ]] && pass "startup DB load ${sdl} µs < ${STARTUP_DB_LIMIT_US}" \
                                                 || fail "$backend startup DB load ${sdl} µs >= ${STARTUP_DB_LIMIT_US}"
    [[ "$sms"  -lt "$STARTUP_MC_LIMIT_US"    ]] && pass "startup MC set  ${sms} µs < ${STARTUP_MC_LIMIT_US}" \
                                                 || fail "$backend startup MC set ${sms} µs >= ${STARTUP_MC_LIMIT_US}"
    [[ "$cr99" -lt "$CACHE_READ_P99_LIMIT_US"]] && pass "cache read p99  ${cr99} µs < ${CACHE_READ_P99_LIMIT_US}" \
                                                 || fail "$backend cache read p99 ${cr99} µs >= ${CACHE_READ_P99_LIMIT_US}"
    [[ "$ms99" -lt "$MC_SET_P99_LIMIT_US"    ]] && pass "MC SET p99      ${ms99} µs < ${MC_SET_P99_LIMIT_US}" \
                                                 || fail "$backend MC SET p99 ${ms99} µs >= ${MC_SET_P99_LIMIT_US}"
}

echo "========================================================"
echo "  Test 18 — Memcached write-through hot-tail cache"
echo "========================================================"
echo "  Backends     : KurrentDB · MongoDB · PostgreSQL"
echo "  Memcached    : $MEMCACHED_URL"
echo "  Seed events  : $SEED_EVENTS"
echo "  Cache window : $CACHE_SIZE events"
echo "  Live writes  : $LIVE_WRITES"
echo "========================================================"

FAILURES=0

run_backend "kurrentdb-memcached-bench" "KurrentDB"
run_backend "mongo-memcached-bench"     "MongoDB"     --database mcbench
run_backend "pg-memcached-bench"        "PostgreSQL"

echo ""
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All assertions passed."
    exit 0
else
    echo "$FAILURES assertion(s) failed."
    exit 1
fi
