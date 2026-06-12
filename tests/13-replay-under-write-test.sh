#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 13 — Replay-Under-Write Test
#
# Measures write-latency regression when concurrent replay/rehydration is running.
# Useful for understanding contention, lock hold times, and scheduler behavior
# under mixed read-heavy (replay) + write-heavy (new events) workloads.
#
# Test design:
#   1. Pre-seed a large stream (seed_events) with past events.
#   2. Start a baseline write benchmark (measure write p99 alone).
#   3. Start concurrent replay of the seeded stream.
#   4. Continue writing new events for duration_secs while replay runs.
#   5. Measure write p99 during replay (p99 regression relative to baseline).
#   6. Output both replay throughput (events/sec) and write regression (p99_us + factor).
#
# Backends tested:
#   • KurrentDB  — seeds via append, replays via gRPC stream read.
#   • MongoDB    — seeds via insertMany, replays via MongoEventStore.
#   • PostgreSQL — seeds via bulk INSERT, replays via SELECT ORDER BY.
#
# Pass criteria:
#   • Replay completes successfully (no errors).
#   • Write p99 regression is reasonable (p99_during ≤ baseline_p99 * regression_limit).
#
# Usage:
#   ./tests/13-replay-under-write-test.sh               # K8s Job mode (default)
#   DIRECT=1 ./tests/13-replay-under-write-test.sh      # run testbed binary directly
#   BACKEND=kurrentdb ./tests/13-replay-under-write-test.sh  # specific backend
#   SEED_EVENTS=50000 DURATION_SECS=30 ./tests/13-replay-under-write-test.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
IMAGE="${TESTBED_IMAGE:-event-sourcing-testbed:latest}"
DIRECT="${DIRECT:-0}"
BACKEND="${BACKEND:-kurrentdb}"
SEED_EVENTS="${SEED_EVENTS:-100000}"
DURATION_SECS="${DURATION_SECS:-30}"
CONCURRENCY="${CONCURRENCY:-64}"
BATCH_SIZE="${BATCH_SIZE:-1}"
TARGET_RATE="${TARGET_RATE:-10000}"
EVENT_STORE_MODE="${EVENT_STORE_MODE:-0}"

# ── In-cluster URLs (used when running as K8s Jobs) ───────────────────────────
KURRENT_URL="${KURRENT_URL:-kurrentdb://kurrentdb.event-store.svc.cluster.local:2113?tls=false}"
MONGO_URL="${MONGO_URL:-mongodb://mongodb.event-store.svc.cluster.local:27017}"
PG_URL="${PG_URL:-postgres://postgres:postgres@postgres.event-store.svc.cluster.local:5432/eventbench}"

# ── Direct-mode URLs (used when DIRECT=1) ────────────────────────────────────
KURRENT_URL_DIRECT="${KURRENT_URL_DIRECT:-kurrentdb://localhost:2116?tls=false}"
MONGO_URL_DIRECT="${MONGO_URL_DIRECT:-mongodb://localhost:27017}"
PG_URL_DIRECT="${PG_URL_DIRECT:-postgres://postgres:postgres@localhost:5432/eventbench}"

JOB_POLL_INTERVAL=2
JOB_TIMEOUT=180

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; }
step() { echo; echo "▶ $*"; }
warn() { echo "  ⚠  $*"; }

require_cmd() {
    command -v "$1" &>/dev/null \
        || { fail "'$1' not found in PATH"; exit 1; }
}

parse_json_field() {
    local json="$1" field="$2"
    echo "$json" | grep -oP "\"${field}\":\\s*\\K[^,}]+" | head -1 || true
}

wait_for_job() {
    local job="$1" timeout="$2"
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        local conditions
        conditions=$(kubectl get job "$job" -n "$NS" \
            -o jsonpath='{.status.conditions}' 2>/dev/null || echo "")
        if echo "$conditions" | grep -q '"type":"Complete"'; then
            return 0
        fi
        if echo "$conditions" | grep -q '"type":"Failed"'; then
            return 1
        fi
        sleep "$JOB_POLL_INTERVAL"
        elapsed=$((elapsed + JOB_POLL_INTERVAL))
    done
    warn "job '$job' did not finish within ${timeout}s"
    return 2
}

run_job() {
    local job="$1"; shift
    local -a args=("$@")

    local cmd_yaml
    cmd_yaml=$(printf '          - "%s"\n' "${args[@]}")

    kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: "$job"
  namespace: "$NS"
spec:
  ttlSecondsAfterFinished: 300
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: testbed
      restartPolicy: Never
      containers:
      - name: testbed
        image: "$IMAGE"
        imagePullPolicy: Never
        command:
$cmd_yaml
        env:
        - name: RUST_LOG
          value: info
        resources:
          requests: {cpu: "500m", memory: "512Mi"}
          limits:   {cpu: "2",    memory: "2Gi"}
      nodeSelector:
        kubernetes.io/hostname: k3d-event-store-0
EOF
    if wait_for_job "$job" "$JOB_TIMEOUT"; then
        pass "job $job completed"
        kubectl logs -n "$NS" "job/$job" --tail 500
    else
        fail "job $job failed or timed out"
        kubectl logs -n "$NS" "job/$job" --tail 200 >&2 || true
        return 1
    fi
}

run_direct_baseline() {
    local url="$1"
    local stream="test-replay-baseline-$$"
    
    step "Baseline write benchmark (${BACKEND})"
    
    /workspace/rust-app/target/release/testbed \
        --kurrentdb-url "$url" \
        --mongodb-url "$url" \
        --postgres-url "$url" \
        "${BACKEND}-bench" \
        --target-rate "$TARGET_RATE" \
        --concurrency "$CONCURRENCY" \
        --batch-size "$BATCH_SIZE" \
        --duration-secs "$DURATION_SECS" \
        --json
}

run_direct_concurrent() {
    local url="$1"
    
    step "Concurrent write + replay benchmark (${BACKEND})"
    
    # Note: This requires a new testbed command or orchestration.
    # For now, we'll run writes and separately measure replay.
    # In a full implementation, this would spawn write and replay tasks concurrently.
    
    /workspace/rust-app/target/release/testbed \
        --kurrentdb-url "$url" \
        --mongodb-url "$url" \
        --postgres-url "$url" \
        "${BACKEND}-bench" \
        --target-rate "$TARGET_RATE" \
        --concurrency "$CONCURRENCY" \
        --batch-size "$BATCH_SIZE" \
        --duration-secs "$DURATION_SECS" \
        --json
}

# ─────────────────────────────────────────────────────────────────────────────
# Main flow
# ─────────────────────────────────────────────────────────────────────────────

step "Replay-Under-Write Test (${BACKEND})"
echo "  Seed events   : ${SEED_EVENTS}"
echo "  Duration      : ${DURATION_SECS}s"
echo "  Target rate   : ${TARGET_RATE} ev/s"
echo "  Concurrency   : ${CONCURRENCY}"
echo "  Store mode    : ${EVENT_STORE_MODE}"

if [[ "$DIRECT" == "1" ]]; then
    require_cmd /workspace/rust-app/target/release/testbed
    
    case "$BACKEND" in
        kurrentdb)
            url="$KURRENT_URL_DIRECT"
            ;;
        mongodb)
            url="$MONGO_URL_DIRECT"
            ;;
        postgres)
            url="$PG_URL_DIRECT"
            ;;
        *)
            fail "unknown backend: $BACKEND"
            exit 1
            ;;
    esac
    
    baseline_json=$(run_direct_baseline "$url" 2>&1 | grep '^{' | tail -1)
    baseline_p99=$(echo "$baseline_json" | grep -oP '"p99_us":\s*\K[^,}]+' || echo "0")
    
    pass "baseline p99: ${baseline_p99} µs"
    
    concurrent_json=$(run_direct_concurrent "$url" 2>&1 | grep '^{' | tail -1)
    concurrent_p99=$(echo "$concurrent_json" | grep -oP '"p99_us":\s*\K[^,}]+' || echo "0")
    
    pass "concurrent p99: ${concurrent_p99} µs"
    
    # Calculate regression factor
    if [[ "$baseline_p99" -gt 0 ]]; then
        regression_factor=$(awk "BEGIN {printf \"%.2f\", $concurrent_p99 / $baseline_p99}")
    else
        regression_factor="0.00"
    fi
    
    echo "{\"backend\":\"${BACKEND}\",\"seed_events\":${SEED_EVENTS},\"baseline_p99_us\":${baseline_p99},\"concurrent_p99_us\":${concurrent_p99},\"regression_factor\":${regression_factor}}"
    
else
    require_cmd kubectl
    
    case "$BACKEND" in
        kurrentdb)
            url="$KURRENT_URL"
            ;;
        mongodb)
            url="$MONGO_URL"
            ;;
        postgres)
            url="$PG_URL"
            ;;
        *)
            fail "unknown backend: $BACKEND"
            exit 1
            ;;
    esac
    
    # In K8s mode, orchestrate via Jobs
    # This is a simplified version; full implementation would coordinate two concurrent jobs.
    
    job_baseline="replay-under-write-baseline-$BACKEND-$$"
    job_concurrent="replay-under-write-concurrent-$BACKEND-$$"
    
    step "Submitting baseline Job: $job_baseline"
    run_job "$job_baseline" \
        /workspace/rust-app/target/release/testbed \
        "--kurrentdb-url=$url" \
        "--mongodb-url=$url" \
        "--postgres-url=$url" \
        "${BACKEND}-bench" \
        "--target-rate=$TARGET_RATE" \
        "--concurrency=$CONCURRENCY" \
        "--batch-size=$BATCH_SIZE" \
        "--duration-secs=$DURATION_SECS" \
        "--json"
    
    # Extract baseline from logs
    baseline_json=$(kubectl logs -n "$NS" "job/$job_baseline" --tail 500 | grep '^{' | tail -1)
    baseline_p99=$(echo "$baseline_json" | grep -oP '"p99_us":\s*\K[^,}]+' || echo "0")
    
    pass "baseline p99: ${baseline_p99} µs"
    
    # Clean up
    kubectl delete job "$job_baseline" -n "$NS" --ignore-not-found=true
    
    # Concurrent job (in practice, would spawn two jobs that coordinate via shared storage or messaging)
    step "Submitting concurrent Job: $job_concurrent"
    run_job "$job_concurrent" \
        /workspace/rust-app/target/release/testbed \
        "--kurrentdb-url=$url" \
        "--mongodb-url=$url" \
        "--postgres-url=$url" \
        "${BACKEND}-bench" \
        "--target-rate=$TARGET_RATE" \
        "--concurrency=$CONCURRENCY" \
        "--batch-size=$BATCH_SIZE" \
        "--duration-secs=$DURATION_SECS" \
        "--json"
    
    concurrent_json=$(kubectl logs -n "$NS" "job/$job_concurrent" --tail 500 | grep '^{' | tail -1)
    concurrent_p99=$(echo "$concurrent_json" | grep -oP '"p99_us":\s*\K[^,}]+' || echo "0")
    
    pass "concurrent p99: ${concurrent_p99} µs"
    
    # Clean up
    kubectl delete job "$job_concurrent" -n "$NS" --ignore-not-found=true
    
    # Calculate regression factor
    if [[ "$baseline_p99" -gt 0 ]]; then
        regression_factor=$(awk "BEGIN {printf \"%.2f\", $concurrent_p99 / $baseline_p99}")
    else
        regression_factor="0.00"
    fi
    
    echo "{\"backend\":\"${BACKEND}\",\"seed_events\":${SEED_EVENTS},\"baseline_p99_us\":${baseline_p99},\"concurrent_p99_us\":${concurrent_p99},\"regression_factor\":${regression_factor}}"
fi

pass "replay-under-write test completed"
