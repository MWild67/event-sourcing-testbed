#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 07 — PostgreSQL Write-Latency Stress Test
#
# Passes when:
#   • Actual throughput reaches ≥ 9 000 events/second
#   • p99 insert latency < p99_limit_ms (default 2 ms / 2 000 µs)
#
# Runs the Rust 'testbed pg-bench' subcommand either inside a Kubernetes
# Job or, when DIRECT=1, directly against a local PostgreSQL instance.
#
# Usage:
#   ./tests/07-postgres-stress-test.sh                         # K8s Job mode (default)
#   DIRECT=1 ./tests/07-postgres-stress-test.sh                # run testbed binary directly
#   POSTGRES_URL=postgres://... DIRECT=1 ./tests/07-postgres-stress-test.sh
#   P99_LIMIT_MS=5 DIRECT=1 ./tests/07-postgres-stress-test.sh  # relax p99 threshold
#
# Isolation notes:
#   • The testbed binary recreates the 'events' table before every run so
#     leftover data from a prior run cannot inflate index lookup times.
#   • This test targets PostgreSQL only.  It does NOT touch KurrentDB or
#     MongoDB — those backends are completely separate.
#   • Do NOT run this test concurrently with 02-stress-test.sh or
#     05-mongodb-stress-test.sh on the same host.  All three benchmarks
#     saturate the host's CPU/memory/disk I/O and will inflate each other's
#     latency numbers.  Run them sequentially.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
JOB="pg-stress-test-$(date +%s)"
IMAGE="${TESTBED_IMAGE:-event-sourcing-testbed:latest}"
PG_URL="${PG_URL:-postgres://postgres:postgres@postgres.event-store.svc.cluster.local:5432/eventbench}"
POSTGRES_URL="${POSTGRES_URL:-${PG_URL_DIRECT:-postgres://postgres:postgres@localhost:5432/eventbench}}"
DIRECT="${DIRECT:-0}"

TARGET_RATE="${TARGET_RATE:-10000}"
CONCURRENCY=64
BATCH_SIZE=1
DURATION_SECS=30
P99_LIMIT_MS="${P99_LIMIT_MS:-2}"

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }
step() { echo; echo "▶ $*"; }

# ── Helpers ───────────────────────────────────────────────────────────────────
require_cmd() {
    command -v "$1" &>/dev/null || fail "'$1' not found in PATH"
}

parse_json_field() {
    local json="$1" field="$2"
    echo "$json" | grep -oP "\"${field}\":\\s*\\K[^,}]+" || true
}

# ─────────────────────────────────────────────────────────────────────────────
if [[ "$DIRECT" == "1" ]]; then
    # ── Direct mode: run testbed binary on this machine ────────────────────
    require_cmd testbed

    step "Running PostgreSQL stress test directly (target ${TARGET_RATE} ev/s for ${DURATION_SECS}s)"
    echo "  PostgreSQL: $POSTGRES_URL"

    OUTPUT=$(testbed \
        --postgres-url  "$POSTGRES_URL" \
        pg-bench \
        --target-rate   "$TARGET_RATE" \
        --concurrency   "$CONCURRENCY" \
        --batch-size    "$BATCH_SIZE" \
        --duration-secs "$DURATION_SECS" \
        --json 2>&1 | tail -1)

    echo "  Raw output: $OUTPUT"

    RATE=$(parse_json_field "$OUTPUT" "actual_rate_eps")
    P99=$(parse_json_field  "$OUTPUT" "p99_us")
    P99_LIMIT_US=$((P99_LIMIT_MS * 1000))

    [[ "${RATE%.*}" -ge "$((TARGET_RATE * 90 / 100))" ]] \
      || fail "PostgreSQL benchmark FAILED: throughput ${RATE} ev/s below 90% of target (${TARGET_RATE})"
    [[ "${P99}" -le "${P99_LIMIT_US}" ]] \
      || fail "PostgreSQL benchmark FAILED: p99=${P99} µs > limit=${P99_LIMIT_US} µs (${P99_LIMIT_MS} ms)"
    pass "PostgreSQL benchmark passed  (rate=${RATE} ev/s, p99=${P99} µs < ${P99_LIMIT_US} µs)"

else
    # ── Kubernetes Job mode ────────────────────────────────────────────────
    require_cmd kubectl

    step "Submitting PostgreSQL stress-test Job '$JOB' to namespace '$NS'"

    kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  namespace: $NS
  labels:
    test: pg-stress
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: testbed
          image: $IMAGE
          args:
            - --postgres-url
            - "$PG_URL"
            - pg-bench
            - --target-rate
            - "$TARGET_RATE"
            - --concurrency
            - "$CONCURRENCY"
            - --batch-size
            - "$BATCH_SIZE"
            - --duration-secs
            - "$DURATION_SECS"
            - --json
          resources:
            requests:
              cpu: "500m"
              memory: "256Mi"
EOF

    step "Waiting for Job to complete (timeout: $((DURATION_SECS + 120))s)"
    kubectl wait job/"$JOB" \
        --namespace="$NS" \
        --for=condition=complete \
        --timeout="${DURATION_SECS + 120}s" \
    || {
        kubectl logs -n "$NS" "job/$JOB" --tail=50 >&2 || true
        fail "Job '$JOB' did not complete in time"
    }

    OUTPUT=$(kubectl logs -n "$NS" "job/$JOB" 2>/dev/null | grep '^{' | tail -1)
    echo "  Raw output: $OUTPUT"

    RATE=$(parse_json_field "$OUTPUT" "actual_rate_eps")
    P99=$(parse_json_field  "$OUTPUT" "p99_us")
    P99_LIMIT_US=$((P99_LIMIT_MS * 1000))

    [[ "${RATE%.*}" -ge "$((TARGET_RATE * 90 / 100))" ]] \
      || fail "PostgreSQL benchmark FAILED: throughput ${RATE} ev/s below 90% of target (${TARGET_RATE})"
    [[ "${P99}" -le "${P99_LIMIT_US}" ]] \
      || fail "PostgreSQL benchmark FAILED: p99=${P99} µs > limit=${P99_LIMIT_US} µs (${P99_LIMIT_MS} ms)"
    pass "PostgreSQL benchmark passed  (rate=${RATE} ev/s, p99=${P99} µs < ${P99_LIMIT_US} µs)"

    # Clean up the completed job.
    kubectl delete job "$JOB" -n "$NS" --ignore-not-found=true &>/dev/null || true
fi

echo
pass "Test 07 — PostgreSQL stress test COMPLETE"
