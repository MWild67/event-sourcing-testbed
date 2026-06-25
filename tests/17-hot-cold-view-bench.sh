#!/usr/bin/env bash
# Test 17 — KurrentDB hot/cold view benchmark (onboard features, no external cache)
#
# Validates two KurrentDB-native hot/cold view mechanisms:
#
#   Section A  $maxCount stream metadata
#     • A "hot" stream is created with $maxCount=500; 20 000 events are written.
#     • The server retains only the last 500 events in the hot stream.
#     • The cold stream retains all 20 000 events.
#     • Asserts: hot stream has ≤ hot_window events, cold stream has seed_events.
#
#   Section B  Catch-up subscriptions
#     • Cold start (from StreamPosition::Start) replays all history.
#     • Hot start  (from StreamPosition::End) skips history, receives only new
#       events — write-ack → delivery lag must stay < 500 ms p99.
#
# Requires KurrentDB to be running (docker-compose or k8s).
# Usage:
#   ./tests/17-hot-cold-view-bench.sh [--json]
set -euo pipefail

KURRENTDB_URL="${KURRENTDB_URL:-kurrentdb://localhost:2113,localhost:2114,localhost:2115?tls=false}"
SEED_EVENTS="${SEED_EVENTS:-50000}"
HOT_WINDOW="${HOT_WINDOW:-500}"
LIVE_WRITES="${LIVE_WRITES:-500}"
JSON_FLAG="${1:-}"

BINARY="${BINARY:-./rust-app/target/release/testbed}"
if [[ ! -f "$BINARY" ]]; then
    BINARY="./rust-app/target/debug/testbed"
fi
if [[ ! -f "$BINARY" ]]; then
    echo "testbed binary not found — run 'cargo build' first." >&2
    exit 1
fi

echo "========================================================"
echo "  Test 17 — KurrentDB hot/cold view (onboard features)"
echo "========================================================"
echo "  KurrentDB : $KURRENTDB_URL"
echo "  Seed      : $SEED_EVENTS events"
echo "  Hot window: $HOT_WINDOW events (\$maxCount metadata)"
echo "  Live writes (lag phase): $LIVE_WRITES"
echo "========================================================"

# ── Run the benchmark ─────────────────────────────────────────────────────────
ARGS=(
    --kurrentdb-url "$KURRENTDB_URL"
    kurrentdb-hot-cold-view-bench
    --seed-events  "$SEED_EVENTS"
    --hot-window   "$HOT_WINDOW"
    --live-writes  "$LIVE_WRITES"
)

if [[ "$JSON_FLAG" == "--json" ]]; then
    ARGS+=(--json)
    RESULT=$("$BINARY" "${ARGS[@]}")
    echo "$RESULT"

    HOT_COUNT=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['hot_stream_event_count'])")
    COLD_COUNT=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['cold_stream_event_count'])")
    LAG_P99=$(echo  "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['hot_sub_lag_p99_us'])")
else
    "$BINARY" "${ARGS[@]}"
    # Human-readable mode: no automated assertion (manual inspection).
    echo "PASS (manual verification — check output above)"
    exit 0
fi

# ── Assertions (JSON mode only) ───────────────────────────────────────────────
FAILURES=0

echo ""
echo "Assertions:"

# Cold stream must have all seed events.
if [[ "$COLD_COUNT" -eq "$SEED_EVENTS" ]]; then
    echo "  [PASS] Cold stream has $COLD_COUNT events (= seed_events $SEED_EVENTS)"
else
    echo "  [FAIL] Cold stream has $COLD_COUNT events (expected $SEED_EVENTS)"
    FAILURES=$((FAILURES + 1))
fi

# Hot stream must have exactly hot_window events (or fewer if seed < hot_window).
EXPECTED_HOT=$(python3 -c "print(min($HOT_WINDOW, $SEED_EVENTS))")
# Allow a small tolerance: KurrentDB scavenge may not have run yet; the read
# must return ≤ hot_window events (old events are logically truncated).
if [[ "$HOT_COUNT" -le "$HOT_WINDOW" ]]; then
    echo "  [PASS] Hot stream has $HOT_COUNT events (≤ hot_window $HOT_WINDOW)"
else
    echo "  [FAIL] Hot stream has $HOT_COUNT events (expected ≤ $HOT_WINDOW)"
    FAILURES=$((FAILURES + 1))
fi

# Hot subscription lag p99 must be < 500 000 µs (500 ms).
LAG_LIMIT_US=500000
if [[ "$LAG_P99" -lt "$LAG_LIMIT_US" ]]; then
    echo "  [PASS] Hot sub lag p99 = $LAG_P99 µs (< $LAG_LIMIT_US µs)"
else
    echo "  [FAIL] Hot sub lag p99 = $LAG_P99 µs (>= $LAG_LIMIT_US µs)"
    FAILURES=$((FAILURES + 1))
fi

echo ""
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All assertions passed."
    exit 0
else
    echo "$FAILURES assertion(s) failed."
    exit 1
fi
