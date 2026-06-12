#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 12 — Rate Ramp Test (Knee-Point Discovery)
#
# Runs fixed target-rate steps and reports where p99 latency starts exploding.
#
# Default steps: 1000, 3000, 5000, 8000, 10000 ev/s
# Knee criterion (default):
#   - p99(step_n) / p99(step_n-1) >= KNEE_FACTOR (1.8)
#   - p99(step_n) >= MIN_KNEE_P99_US (2000 µs)
#
# Usage examples:
#   BACKEND=kurrentdb DIRECT=1 bash tests/12-rate-ramp-test.sh
#   BACKEND=mongodb  DIRECT=1 EVENT_STORE_MODE=1 bash tests/12-rate-ramp-test.sh
#   BACKEND=postgres DIRECT=1 EVENT_STORE_MODE=1 bash tests/12-rate-ramp-test.sh
#
# K8s mode (DIRECT=0) is also supported.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
IMAGE="${TESTBED_IMAGE:-event-sourcing-testbed:latest}"
DIRECT="${DIRECT:-0}"

BACKEND="${BACKEND:-kurrentdb}"
RATE_STEPS="${RATE_STEPS:-1000 3000 5000 8000 10000}"
CONCURRENCY="${CONCURRENCY:-64}"
BATCH_SIZE="${BATCH_SIZE:-1}"
DURATION_SECS="${DURATION_SECS:-20}"

# MongoDB / PostgreSQL only. Ignored for KurrentDB.
EVENT_STORE_MODE="${EVENT_STORE_MODE:-0}"

# Knee-point detector knobs.
KNEE_FACTOR="${KNEE_FACTOR:-1.8}"
MIN_KNEE_P99_US="${MIN_KNEE_P99_US:-2000}"

KURRENT_URL="${KURRENT_URL:-kurrentdb://kurrentdb.event-store.svc.cluster.local:2113?tls=false}"
KURRENT_URL_DIRECT="${KURRENT_URL_DIRECT:-kurrentdb://localhost:2116?tls=false}"
MONGO_URL="${MONGO_URL:-mongodb://mongodb.event-store.svc.cluster.local:27017}"
MONGO_URL_DIRECT="${MONGO_URL_DIRECT:-mongodb://localhost:27017}"
POSTGRES_URL="${POSTGRES_URL:-postgres://postgres:postgres@postgres.event-store.svc.cluster.local:5432/eventbench}"
POSTGRES_URL_DIRECT="${POSTGRES_URL_DIRECT:-postgres://postgres:postgres@localhost:5432/eventbench}"

pass() { echo "  + $*"; }
fail() { echo "  x $*" >&2; exit 1; }
step() { echo; echo "> $*"; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "'$1' not found in PATH"
}

parse_json_field() {
  local json="$1" field="$2"
  echo "$json" | grep -oP "\"${field}\":\\s*\\K[^,}]+" || true
}

is_true() {
  [[ "$1" == "1" || "$1" == "true" || "$1" == "TRUE" ]]
}

bench_cmd_for_backend() {
  case "$BACKEND" in
    kurrentdb) echo "kurrentdb-bench" ;;
    mongodb) echo "mongo-bench" ;;
    postgres) echo "pg-bench" ;;
    *) fail "Unsupported BACKEND '$BACKEND' (expected: kurrentdb|mongodb|postgres)" ;;
  esac
}

backend_url_flag_and_value() {
  case "$BACKEND" in
    kurrentdb)
      if [[ "$DIRECT" == "1" ]]; then
        echo "--kurrentdb-url|$KURRENT_URL_DIRECT"
      else
        echo "--kurrentdb-url|$KURRENT_URL"
      fi
      ;;
    mongodb)
      if [[ "$DIRECT" == "1" ]]; then
        echo "--mongodb-url|$MONGO_URL_DIRECT"
      else
        echo "--mongodb-url|$MONGO_URL"
      fi
      ;;
    postgres)
      if [[ "$DIRECT" == "1" ]]; then
        echo "--postgres-url|$POSTGRES_URL_DIRECT"
      else
        echo "--postgres-url|$POSTGRES_URL"
      fi
      ;;
  esac
}

run_direct_step() {
  local rate="$1"
  local bench_cmd="$2"
  local url_flag="$3"
  local url_value="$4"

  local mode_flag=""
  if [[ "$BACKEND" != "kurrentdb" ]] && is_true "$EVENT_STORE_MODE"; then
    mode_flag="--event-store-mode"
  fi

  testbed \
    "$url_flag" "$url_value" \
    "$bench_cmd" \
    --target-rate "$rate" \
    --concurrency "$CONCURRENCY" \
    --batch-size "$BATCH_SIZE" \
    --duration-secs "$DURATION_SECS" \
    $mode_flag \
    --json 2>&1 | tail -1
}

run_k8s_step() {
  local rate="$1"
  local bench_cmd="$2"
  local url_flag="$3"
  local url_value="$4"

  local job="rate-ramp-${BACKEND}-${rate}-$(date +%s)"
  local mode_arg=""
  if [[ "$BACKEND" != "kurrentdb" ]] && is_true "$EVENT_STORE_MODE"; then
    mode_arg='            - --event-store-mode'
  fi

  kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $NS
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 120
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: testbed
          image: $IMAGE
          args:
            - $url_flag
            - "$url_value"
            - $bench_cmd
            - --target-rate
            - "$rate"
            - --concurrency
            - "$CONCURRENCY"
            - --batch-size
            - "$BATCH_SIZE"
            - --duration-secs
            - "$DURATION_SECS"
$mode_arg
            - --json
EOF

  kubectl wait job/"$job" -n "$NS" --for=condition=complete --timeout="$((DURATION_SECS + 120))s" >/dev/null || {
    kubectl logs -n "$NS" "job/$job" --tail=50 >&2 || true
    kubectl delete job "$job" -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
    fail "Job '$job' failed or timed out"
  }

  local output
  output=$(kubectl logs -n "$NS" "job/$job" 2>/dev/null | grep '^{' | tail -1)
  kubectl delete job "$job" -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
  echo "$output"
}

step "Rate ramp test (${BACKEND})"
echo "  Mode              : $([[ "$DIRECT" == "1" ]] && echo direct || echo kubernetes-job)"
echo "  Steps (ev/s)      : $RATE_STEPS"
echo "  Concurrency       : $CONCURRENCY"
echo "  Batch size        : $BATCH_SIZE"
echo "  Duration per step : ${DURATION_SECS}s"
if [[ "$BACKEND" != "kurrentdb" ]]; then
  echo "  Event-store mode  : $EVENT_STORE_MODE"
fi
echo "  Knee factor       : ${KNEE_FACTOR}x"
echo "  Knee min p99      : ${MIN_KNEE_P99_US} us"

if [[ "$DIRECT" == "1" ]]; then
  require_cmd testbed
else
  require_cmd kubectl
fi

bench_cmd=$(bench_cmd_for_backend)
url_pair=$(backend_url_flag_and_value)
url_flag=${url_pair%%|*}
url_value=${url_pair#*|}

rates=()
p99s=()
actual_rates=()

step "Executing ramp"
printf "| %-8s | %-12s | %-10s |\n" "target" "actual_rate" "p99_us"
printf "|-%-8s-|-%-12s-|-%-10s-|\n" "--------" "------------" "----------"

for rate in $RATE_STEPS; do
  if [[ "$DIRECT" == "1" ]]; then
    output=$(run_direct_step "$rate" "$bench_cmd" "$url_flag" "$url_value")
  else
    output=$(run_k8s_step "$rate" "$bench_cmd" "$url_flag" "$url_value")
  fi

  actual_rate=$(parse_json_field "$output" "actual_rate_eps")
  p99=$(parse_json_field "$output" "p99_us")

  [[ -n "$actual_rate" && -n "$p99" ]] || fail "Could not parse benchmark output for rate=$rate: $output"

  rates+=("$rate")
  p99s+=("$p99")
  actual_rates+=("$actual_rate")

  printf "| %-8s | %-12s | %-10s |\n" "$rate" "$actual_rate" "$p99"
done

knee_rate=""
knee_p99=""
knee_ratio=""

for ((i=1; i<${#rates[@]}; i++)); do
  prev_p99="${p99s[$((i-1))]}"
  cur_p99="${p99s[$i]}"

  ratio=$(awk -v a="$cur_p99" -v b="$prev_p99" 'BEGIN { if (b <= 0) print 0; else printf "%.3f", a / b }')

  if awk -v r="$ratio" -v f="$KNEE_FACTOR" -v p="$cur_p99" -v m="$MIN_KNEE_P99_US" 'BEGIN { exit ! (r >= f && p >= m) }'; then
    knee_rate="${rates[$i]}"
    knee_p99="$cur_p99"
    knee_ratio="$ratio"
    break
  fi
done

echo
if [[ -n "$knee_rate" ]]; then
  pass "Knee point detected at ~${knee_rate} ev/s (p99=${knee_p99} us, jump=${knee_ratio}x)."
else
  pass "No knee point detected in configured steps (${RATE_STEPS})."
fi

# Machine-readable summary for CI/log parsing.
if [[ -n "$knee_rate" ]]; then
  echo "{\"backend\":\"${BACKEND}\",\"knee_detected\":true,\"knee_rate_eps\":${knee_rate},\"knee_p99_us\":${knee_p99},\"knee_jump\":${knee_ratio},\"rates\":\"${RATE_STEPS}\"}"
else
  echo "{\"backend\":\"${BACKEND}\",\"knee_detected\":false,\"rates\":\"${RATE_STEPS}\"}"
fi
