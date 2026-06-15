#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 15 — Hot-Stream Contention
#
# Skews writes toward a tiny set of streams while keeping background distributed
# writes active. Reports conflict/retry behavior and tail latency impact.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DIRECT="${DIRECT:-1}"
TESTBED_BIN="${TESTBED_BIN:-rust-app/target/release/testbed}"

TARGET_RATE="${TARGET_RATE:-6000}"
BASELINE_DURATION_SECS="${BASELINE_DURATION_SECS:-12}"
CONTENTION_DURATION_SECS="${CONTENTION_DURATION_SECS:-18}"
CONCURRENCY="${CONCURRENCY:-96}"
HOT_STREAMS="${HOT_STREAMS:-2}"
COLD_STREAMS="${COLD_STREAMS:-128}"
HOT_RATIO="${HOT_RATIO:-0.95}"
MAX_RETRIES="${MAX_RETRIES:-10}"

KURRENT_URL_DIRECT_DEFAULT="${KURRENT_URL_DIRECT:-kurrentdb://localhost:2116?tls=false}"
KURRENT_URL_DIRECT="$KURRENT_URL_DIRECT_DEFAULT"

pass() { echo "  ✓ $*"; }
warn() { echo "  ⚠  $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }
step() { echo; echo "▶ $*"; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "'$1' not found in PATH"
}

wait_for_http_health() {
  local url="$1"
  local timeout_secs="$2"
  for _ in $(seq 1 "$timeout_secs"); do
    if curl -fsS --connect-timeout 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

prepare_kurrent_direct_url() {
  local default_health="http://localhost:2116/health/live"
  local alt_health="http://localhost:2113/health/live"

  if [[ "$KURRENT_URL_DIRECT" == "kurrentdb://localhost:2116?tls=false" ]]; then
    if ! wait_for_http_health "$default_health" 1 && wait_for_http_health "$alt_health" 1; then
      warn "KurrentDB healthy on 2113; switching direct URL from 2116 to 2113"
      KURRENT_URL_DIRECT="kurrentdb://localhost:2113?tls=false"
    fi
  fi

  if [[ "$KURRENT_URL_DIRECT" == *":2113"* || "$KURRENT_URL_DIRECT" == *":2113?"* ]]; then
    wait_for_http_health "$alt_health" 60 || fail "KurrentDB not ready on 2113 after 60s"
  else
    wait_for_http_health "$default_health" 60 || fail "KurrentDB not ready on 2116 after 60s"
  fi
}

extract_json_last() {
  local text="$1"
  echo "$text" | grep '^{' | tail -1
}

step "Hot-stream contention test (KurrentDB)"
echo "  Direct mode               : $DIRECT"
echo "  Target rate               : $TARGET_RATE ev/s"
echo "  Baseline duration         : ${BASELINE_DURATION_SECS}s"
echo "  Contention duration       : ${CONTENTION_DURATION_SECS}s"
echo "  Concurrency               : $CONCURRENCY"
echo "  Hot streams               : $HOT_STREAMS"
echo "  Cold streams              : $COLD_STREAMS"
echo "  Hot ratio                 : $HOT_RATIO"
echo "  Max retries               : $MAX_RETRIES"

[[ "$DIRECT" == "1" ]] || fail "this test currently supports DIRECT=1 only"
[[ -x "$TESTBED_BIN" ]] || fail "testbed binary not found or not executable: $TESTBED_BIN"
require_cmd curl
prepare_kurrent_direct_url

step "Executing benchmark"
attempts=3
RESULT_JSON=""
for attempt in $(seq 1 "$attempts"); do
  echo "  Attempt $attempt/$attempts..."
  output=$("$TESTBED_BIN" \
    --kurrentdb-url "$KURRENT_URL_DIRECT" \
    kurrentdb-hot-stream-contention-bench \
    --target-rate "$TARGET_RATE" \
    --baseline-duration-secs "$BASELINE_DURATION_SECS" \
    --contention-duration-secs "$CONTENTION_DURATION_SECS" \
    --concurrency "$CONCURRENCY" \
    --hot-streams "$HOT_STREAMS" \
    --cold-streams "$COLD_STREAMS" \
    --hot-ratio "$HOT_RATIO" \
    --max-retries "$MAX_RETRIES" \
    --json 2>&1) || true

  RESULT_JSON=$(extract_json_last "$output")
  if [[ -n "$RESULT_JSON" ]]; then
    break
  fi

  warn "benchmark attempt $attempt did not produce JSON"
  echo "$output" | tail -30 >&2 || true
  sleep 3
done

[[ -n "$RESULT_JSON" ]] || fail "hot-stream contention benchmark produced no JSON"

conflicts=$(echo "$RESULT_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("conflict_count", 0)))')
retries=$(echo "$RESULT_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("retry_count", 0)))')
retry_ok=$(echo "$RESULT_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("retry_success_count", 0)))')
baseline_p99=$(echo "$RESULT_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("baseline_p99_us", 0)))')
contention_p99=$(echo "$RESULT_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("contention_p99_us", 0)))')
tail_factor=$(echo "$RESULT_JSON" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(float(d.get("tail_latency_factor", 0.0)))')

[[ "$baseline_p99" -gt 0 ]] || fail "baseline p99 missing/invalid"
[[ "$contention_p99" -gt 0 ]] || fail "contention p99 missing/invalid"

if [[ "$conflicts" -le 0 ]]; then
  fail "expected contention conflicts but got 0 (tune HOT_RATIO/HOT_STREAMS/CONCURRENCY)"
fi

if [[ "$retries" -le 0 ]]; then
  fail "expected retries but got 0"
fi

if [[ "$retry_ok" -le 0 ]]; then
  fail "expected at least one successful retry but got 0"
fi

pass "conflicts: $conflicts"
pass "retries: $retries (successful retries: $retry_ok)"
pass "p99 baseline -> contention: ${baseline_p99}us -> ${contention_p99}us"
pass "tail latency factor: ${tail_factor}x"

echo "$RESULT_JSON"
