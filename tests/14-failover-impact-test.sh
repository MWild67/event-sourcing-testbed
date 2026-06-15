#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Test 14 - Short Failover-Impact Test
#
# Goal:
#   Trigger a short failover during active write load and quantify impact:
#   - pause_window_ms: longest continuous probe outage window
#   - error_spike_count: failed probes in first spike window after failover
#   - recovery_time_ms: failover trigger -> stable healthy probes
#   - tail_latency_factor: p99_during_failover / p99_baseline
#
# Notes:
#   - This test targets KurrentDB in Kubernetes (3 replicas).
#   - It uses the local testbed binary for load generation.
# -----------------------------------------------------------------------------
set -euo pipefail

NS="${NS:-event-store}"
STS="${STS:-kurrentdb}"
TESTBED_BIN="${TESTBED_BIN:-rust-app/target/release/testbed}"
KURRENT_URL="${KURRENT_URL:-kurrentdb://localhost:2116?tls=false}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:2116/health/live}"
PF_RESOURCE="${PF_RESOURCE:-svc/kurrentdb}"

BASELINE_DURATION_SECS="${BASELINE_DURATION_SECS:-12}"
IMPACT_DURATION_SECS="${IMPACT_DURATION_SECS:-35}"
TARGET_RATE="${TARGET_RATE:-4000}"
CONCURRENCY="${CONCURRENCY:-48}"
BATCH_SIZE="${BATCH_SIZE:-1}"

RECOVERY_SLA_SECS="${RECOVERY_SLA_SECS:-60}"
SPIKE_WINDOW_MS="${SPIKE_WINDOW_MS:-5000}"
PROBE_INTERVAL_SECS="${PROBE_INTERVAL_SECS:-0.2}"
RECOVERY_SUCCESS_STREAK="${RECOVERY_SUCCESS_STREAK:-5}"

export SPIKE_WINDOW_MS RECOVERY_SUCCESS_STREAK

TARGET_NODE=""
PF_PID=""
PROBE_PID=""
FAILOVER_TS_MS="0"
PROBE_CSV=""

pass() { echo "  OK: $*"; }
warn() { echo "  WARN: $*" >&2; }
fail() { echo "  ERR: $*" >&2; exit 1; }
step() { echo; echo "> $*"; }

now_ms() {
  date +%s%3N
}

cleanup() {
  set +e
  if [[ -n "$PROBE_PID" ]]; then
    kill "$PROBE_PID" 2>/dev/null || true
  fi
  if [[ -n "$PF_PID" ]]; then
    kill "$PF_PID" 2>/dev/null || true
  fi
  if [[ -n "$TARGET_NODE" ]]; then
    kubectl taint nodes "$TARGET_NODE" node.kubernetes.io/unreachable:NoExecute- 2>/dev/null || true
    kubectl taint nodes "$TARGET_NODE" node.kubernetes.io/not-ready:NoExecute- 2>/dev/null || true
    kubectl uncordon "$TARGET_NODE" 2>/dev/null || true
  fi
}
trap cleanup EXIT

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing command: $1"
}

pod_state_from_info() {
  local pod="$1"
  local raw=""
  local state="unknown"

  raw=$(kubectl exec "$pod" -n "$NS" -- sh -lc '
    if command -v wget >/dev/null 2>&1; then
      wget -qO- http://127.0.0.1:2113/info
    elif command -v curl >/dev/null 2>&1; then
      curl -fsS http://127.0.0.1:2113/info
    else
      exit 127
    fi
  ' 2>/dev/null || true)

  if [[ -n "$raw" ]]; then
    state=$(printf '%s' "$raw" | grep -Eo '"state"[[:space:]]*:[[:space:]]*"[^"]+"' | head -n1 | sed -E 's/^"state"[[:space:]]*:[[:space:]]*"([^"]+)"$/\1/' || true)
  fi

  [[ -n "${state:-}" ]] || state="unknown"
  printf '%s\n' "$state"
}

is_leader_state() {
  local s
  s=$(echo "$1" | tr '[:upper:]' '[:lower:]')
  [[ "$s" == "leader" || "$s" == "master" || "$s" == "primary" ]]
}

find_leader_node() {
  local leader_pod=""
  for pod in $(kubectl get pods -n "$NS" -l app="$STS" -o jsonpath='{.items[*].metadata.name}'); do
    state=$(pod_state_from_info "$pod")
    echo "  $pod state=$state" >&2
    if is_leader_state "$state"; then
      leader_pod="$pod"
    fi
  done

  if [[ -z "$leader_pod" ]]; then
    leader_pod=$(kubectl get pods -n "$NS" -l app="$STS" -o jsonpath='{.items[0].metadata.name}')
    warn "leader not detected from /info, using first pod: $leader_pod"
  fi

  kubectl get pod "$leader_pod" -n "$NS" -o jsonpath='{.spec.nodeName}'
}

wait_cluster_ready() {
  local deadline=$(( $(date +%s) + 120 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    ready=$(kubectl get statefulset "$STS" -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    if [[ "${ready:-0}" -ge 3 ]]; then
      return 0
    fi
    sleep 2
  done
  return 1
}

start_port_forward() {
  step "Start port-forward for benchmark client"
  kubectl port-forward -n "$NS" "$PF_RESOURCE" 2116:2113 >/tmp/failover-impact-pf.log 2>&1 &
  PF_PID=$!

  local ok=0
  for _ in $(seq 1 60); do
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 1
  done

  [[ "$ok" -eq 1 ]] || fail "port-forward health check did not come up"
  pass "port-forward ready (pid=$PF_PID)"
  sleep 2  # Extra stabilization time
}

wait_leader_elected() {
  step "Wait for KurrentDB leader election (best-effort)"
  local deadline=$(( $(date +%s) + 150 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local leader_found=0
    local snapshot=""
    for pod in $(kubectl get pods -n "$NS" -l app="$STS" -o jsonpath='{.items[*].metadata.name}'); do
      state=$(pod_state_from_info "$pod")
      snapshot+=" ${pod}=${state}"
      if is_leader_state "$state"; then
        leader_found=1
        break
      fi
    done

    warn "leader probe states:${snapshot}"

    if [[ "$leader_found" -eq 1 ]]; then
      pass "leader elected"
      sleep 2  # Allow leader to stabilize
      return 0
    fi
    sleep 2
  done

  # Match test 03 behavior: do not hard-fail leader detection; fall back and continue.
  warn "KurrentDB leader not observed within 150s; continuing with fallback pod selection"
  return 0
}

run_probe_loop() {
  PROBE_CSV="$(mktemp)"
  export PROBE_CSV
  : > "$PROBE_CSV"

  (
    while true; do
      ts=$(now_ms)
      if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
        echo "$ts,1" >> "$PROBE_CSV"
      else
        echo "$ts,0" >> "$PROBE_CSV"
      fi
      sleep "$PROBE_INTERVAL_SECS"
    done
  ) &
  PROBE_PID=$!
  pass "probe loop started (pid=$PROBE_PID)"
}

extract_json_last() {
  local file="$1"
  grep '^{' "$file" | tail -1
}

run_bench_json_with_retry() {
  local label="$1"
  local duration_secs="$2"
  local output_file="$3"
  local error_file="$4"
  local attempts="${5:-5}"

  for attempt in $(seq 1 "$attempts"); do
    echo "  Attempt $attempt/$attempts..." >&2
    
    if "$TESTBED_BIN" --kurrentdb-url "$KURRENT_URL" \
      kurrentdb-bench --target-rate "$TARGET_RATE" --concurrency "$CONCURRENCY" --batch-size "$BATCH_SIZE" --duration-secs "$duration_secs" --json \
      >"$output_file" 2>"$error_file"; then
      if grep -q '^{' "$output_file"; then
        return 0
      fi
    fi

    warn "${label} attempt ${attempt}/${attempts} did not produce valid JSON"
    if [[ -s "$output_file" ]]; then
      echo "  stdout (last 20 lines):" >&2
      tail -20 "$output_file" >&2
    fi
    if [[ -s "$error_file" ]]; then
      echo "  stderr (last 20 lines):" >&2
      tail -20 "$error_file" >&2
    fi
    
    [[ "$attempt" -lt "$attempts" ]] && sleep 5  # Increased from 3s to 5s
  done

  return 1
}

step "Pre-flight"
require_cmd kubectl
require_cmd curl
[[ -x "$TESTBED_BIN" ]] || fail "testbed binary not found or not executable: $TESTBED_BIN"

ready=$(kubectl get statefulset "$STS" -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
[[ "${ready:-0}" -ge 3 ]] || fail "need 3/3 ready replicas before test, got $ready"
pass "3/3 replicas ready"

node_count=$(kubectl get nodes --no-headers | grep -c ' Ready' || true)
[[ "${node_count:-0}" -ge 3 ]] || fail "need >=3 ready nodes for failover test, got $node_count"
pass "cluster has $node_count ready nodes"

start_port_forward

wait_leader_elected

step "Baseline latency run"
BASELINE_LOG="$(mktemp)"
BASELINE_ERR="$(mktemp)"

# Pre-flight check: verify KurrentDB is actually responding
pass "verifying KurrentDB connectivity..."
if ! curl -fsS "$HEALTH_URL" >/dev/null; then
  fail "KurrentDB health endpoint not responding before baseline benchmark"
fi

if ! run_bench_json_with_retry "baseline benchmark" "$BASELINE_DURATION_SECS" "$BASELINE_LOG" "$BASELINE_ERR" 5; then
  warn "=== BASELINE BENCHMARK FAILED ==="
  warn "Baseline benchmark stderr tail"
  tail -60 "$BASELINE_ERR" 2>/dev/null || true
  warn "Port-forward log tail"
  tail -60 /tmp/failover-impact-pf.log 2>/dev/null || true
  warn "KurrentDB pod states:"
  kubectl get pods -n "$NS" -l app="$STS" 2>/dev/null || true
  warn "KurrentDB pod events:"
  kubectl describe pods -n "$NS" -l app="$STS" 2>/dev/null | grep -A 5 "Events:" || true
  fail "baseline benchmark failed after retries"
fi

baseline_json=$(extract_json_last "$BASELINE_LOG")
[[ -n "$baseline_json" ]] || fail "baseline benchmark did not output JSON"
baseline_p99_us=$(echo "$baseline_json" | python3 -c 'import sys, json; d=json.load(sys.stdin); print(int(d.get("p99_us",0)))')
pass "baseline p99=${baseline_p99_us}us"

step "Impact run with induced failover"
run_probe_loop

IMPACT_LOG="$(mktemp)"
IMPACT_ERR="$(mktemp)"
"$TESTBED_BIN" --kurrentdb-url "$KURRENT_URL" \
  kurrentdb-bench --target-rate "$TARGET_RATE" --concurrency "$CONCURRENCY" --batch-size "$BATCH_SIZE" --duration-secs "$IMPACT_DURATION_SECS" --json \
  >"$IMPACT_LOG" 2>"$IMPACT_ERR" &
BENCH_PID=$!

sleep 5
TARGET_NODE=$(find_leader_node)
[[ -n "$TARGET_NODE" ]] || fail "failed to resolve leader node"
step "Evict leader node workload on $TARGET_NODE"
kubectl cordon "$TARGET_NODE" >/dev/null
kubectl taint nodes "$TARGET_NODE" node.kubernetes.io/unreachable:NoExecute --overwrite >/dev/null
kubectl taint nodes "$TARGET_NODE" node.kubernetes.io/not-ready:NoExecute --overwrite >/dev/null
FAILOVER_TS_MS=$(now_ms)
export FAILOVER_TS_MS
pass "failover triggered at ${FAILOVER_TS_MS}ms"

wait "$BENCH_PID" || true

# Give system time for failover and new leader election
sleep 10

# Restart port-forward if it died during failover
if ! curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
  pass "Reconnecting port-forward after failover..."
  kill "$PF_PID" 2>/dev/null || true
  sleep 2
  kubectl port-forward -n "$NS" "$PF_RESOURCE" 2116:2113 >/tmp/failover-impact-pf-recovery.log 2>&1 &
  PF_PID=$!
  for _ in $(seq 1 30); do
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
      pass "port-forward recovered"
      break
    fi
    sleep 1
  done
fi

# Allow additional time for recovery observation
sleep 5

# Stop probe loop now that impact load ended.
if [[ -n "$PROBE_PID" ]]; then
  kill "$PROBE_PID" 2>/dev/null || true
fi

impact_json=$(extract_json_last "$IMPACT_LOG")
if [[ -z "$impact_json" ]]; then
  warn "Impact benchmark stdout tail"
  tail -60 "$IMPACT_LOG" 2>/dev/null || true
  warn "Impact benchmark stderr tail"
  tail -60 "$IMPACT_ERR" 2>/dev/null || true
  fail "impact benchmark did not output JSON"
fi
impact_p99_us=$(echo "$impact_json" | python3 -c 'import sys, json; d=json.load(sys.stdin); print(int(d.get("p99_us",0)))')

write_error_count=$(grep -c "append_batch failed" "$IMPACT_ERR" 2>/dev/null || true)

# Parse probe CSV and compute outage metrics.
metrics_json=$(python3 << 'PY'
import json
import os

path = os.environ['PROBE_CSV']
failover_ts = int(os.environ['FAILOVER_TS_MS'])
spike_window_ms = int(os.environ['SPIKE_WINDOW_MS'])
recovery_streak = int(os.environ['RECOVERY_SUCCESS_STREAK'])

samples = []
with open(path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        ts_s, ok_s = line.split(',')
        samples.append((int(ts_s), int(ok_s)))

post = [(ts, ok) for ts, ok in samples if ts >= failover_ts]

max_pause_ms = 0
cur_fail_start = None
error_spike = 0
first_failure_seen = False
recovery_ms = None
success_streak = 0

for ts, ok in post:
    if ts <= failover_ts + spike_window_ms and ok == 0:
        error_spike += 1

    if ok == 0:
        first_failure_seen = True
        success_streak = 0
        if cur_fail_start is None:
            cur_fail_start = ts
    else:
        if cur_fail_start is not None:
            max_pause_ms = max(max_pause_ms, ts - cur_fail_start)
            cur_fail_start = None
        if first_failure_seen and recovery_ms is None:
            success_streak += 1
            if success_streak >= recovery_streak:
                recovery_ms = ts - failover_ts

if cur_fail_start is not None and post:
    max_pause_ms = max(max_pause_ms, post[-1][0] - cur_fail_start)

if recovery_ms is None:
    recovery_ms = -1

print(json.dumps({
    'pause_window_ms': int(max_pause_ms),
    'error_spike_count': int(error_spike),
    'recovery_time_ms': int(recovery_ms),
}))
PY
)

pause_window_ms=$(echo "$metrics_json" | python3 -c 'import sys, json; d=json.load(sys.stdin); print(int(d["pause_window_ms"]))')
error_spike_count=$(echo "$metrics_json" | python3 -c 'import sys, json; d=json.load(sys.stdin); print(int(d["error_spike_count"]))')
recovery_time_ms=$(echo "$metrics_json" | python3 -c 'import sys, json; d=json.load(sys.stdin); print(int(d["recovery_time_ms"]))')

if [[ "$baseline_p99_us" -gt 0 ]]; then
  tail_latency_factor=$(awk -v a="$impact_p99_us" -v b="$baseline_p99_us" 'BEGIN { printf "%.2f", a / b }')
else
  tail_latency_factor="0.00"
fi

if [[ "$recovery_time_ms" -lt 0 ]]; then
  warn "Recovery not observed; dumping probe CSV diagnostics:"
  if [[ -s "$PROBE_CSV" ]]; then
    local total_lines=$(wc -l < "$PROBE_CSV")
    warn "Probe CSV: $total_lines samples total"
    warn "First 10 samples (baseline period):"
    head -10 "$PROBE_CSV" | sed 's/^/    /' >&2
    warn "Last 20 samples (recovery period):"
    tail -20 "$PROBE_CSV" | sed 's/^/    /' >&2
  else
    warn "Probe CSV is empty!"
  fi
  warn "Checking port-forward status..."
  if ! curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    fail "FAILURE: port-forward not responding; recovery impossible"
  else
    warn "Port-forward IS responding; recovery should have been captured"
    fail "did not observe recovery in probe window"
  fi
fi

if (( recovery_time_ms > RECOVERY_SLA_SECS * 1000 )); then
  fail "recovery time ${recovery_time_ms}ms exceeds ${RECOVERY_SLA_SECS}s SLA"
fi

# Cluster-level readiness check after recovery.
wait_cluster_ready || fail "cluster did not return to 3/3 ready"

step "Failover-impact metrics"
echo "  baseline_p99_us    : $baseline_p99_us"
echo "  impact_p99_us      : $impact_p99_us"
echo "  tail_latency_factor: $tail_latency_factor"
echo "  pause_window_ms    : $pause_window_ms"
echo "  error_spike_count  : $error_spike_count"
echo "  write_error_count  : $write_error_count"
echo "  recovery_time_ms   : $recovery_time_ms"

# Machine-readable payload for CI artifact/report.
echo "{\"backend\":\"kurrentdb\",\"pause_window_ms\":${pause_window_ms},\"error_spike_count\":${error_spike_count},\"write_error_count\":${write_error_count},\"recovery_time_ms\":${recovery_time_ms},\"baseline_p99_us\":${baseline_p99_us},\"impact_p99_us\":${impact_p99_us},\"tail_latency_factor\":${tail_latency_factor}}"

pass "short failover-impact test completed"
