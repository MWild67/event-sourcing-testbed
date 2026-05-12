#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 05 — MongoDB Write-Latency Stress Test
#
# Passes when:
#   • Actual throughput reaches ≥ 9 000 events/second
#   • p99 insert latency < p99_limit_ms (default 2 ms / 2 000 µs)
#
# Runs the Rust 'testbed mongo-bench' subcommand either inside a Kubernetes
# Job or, when DIRECT=1, directly against a local MongoDB instance.
#
# Usage:
#   ./tests/05-mongodb-stress-test.sh                     # K8s Job mode (default)
#   DIRECT=1 ./tests/05-mongodb-stress-test.sh            # run testbed binary directly
#   MONGO_URL=mongodb://myhost:27017 \
#     DIRECT=1 ./tests/05-mongodb-stress-test.sh
#   P99_LIMIT_MS=5 DIRECT=1 ./tests/05-mongodb-stress-test.sh  # relax p99 threshold
#
# Isolation notes:
#   • The testbed binary drops the 'eventbench' database before every run so
#     leftover data from a prior run cannot inflate latency.  Pass --no-drop to
#     keep existing data (e.g. when intentionally testing a warm database).
#   • This test targets MongoDB only.  It does NOT touch EventStoreDB or
#     RabbitMQ streams — those backends are completely separate.
#   • Do NOT run this test concurrently with 02-stress-test.sh on the same
#     host.  Both benchmarks saturate the host's CPU/memory/disk I/O, which
#     inflates each other's latency numbers.  Run them sequentially.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
JOB="mongo-stress-test-$(date +%s)"
IMAGE="${TESTBED_IMAGE:-event-sourcing-testbed:latest}"
MONGO_URL="${MONGO_URL:-mongodb://mongodb.event-store.svc.cluster.local:27017}"
MONGO_URL_DIRECT="${MONGO_URL_DIRECT:-mongodb://localhost:27017}"
DIRECT="${DIRECT:-0}"

TARGET_RATE=10000
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
    echo "$json" | grep -oP "\"${field}\":\s*\K[^,}]+"
}

# ─────────────────────────────────────────────────────────────────────────────
if [[ "$DIRECT" == "1" ]]; then
    # ── Direct mode: run testbed binary on this machine ────────────────────
    require_cmd testbed

    step "Running MongoDB stress test directly (target ${TARGET_RATE} ev/s for ${DURATION_SECS}s)"
    echo "  MongoDB: $MONGO_URL_DIRECT"

    OUTPUT=$(testbed \
        --mongodb-url "$MONGO_URL_DIRECT" \
        mongo-bench \
        --target-rate   "$TARGET_RATE" \
        --concurrency   "$CONCURRENCY" \
        --batch-size    "$BATCH_SIZE" \
        --duration-secs "$DURATION_SECS" \
        --p99-limit-ms  "$P99_LIMIT_MS" \
        --json 2>&1 | tail -1)

    echo "  Raw output: $OUTPUT"

    PASSED=$(parse_json_field "$OUTPUT" "passed")
    RATE=$(parse_json_field "$OUTPUT" "actual_rate_eps")
    P99=$(parse_json_field "$OUTPUT" "p99_us")

    [[ "$PASSED" == "true" ]] || fail "MongoDB benchmark FAILED (passed=$PASSED, rate=${RATE} ev/s, p99=${P99} µs)"
    pass "MongoDB benchmark passed  (rate=${RATE} ev/s, p99=${P99} µs < $((P99_LIMIT_MS * 1000)) µs)"

else
    # ── Kubernetes Job mode ────────────────────────────────────────────────
    require_cmd kubectl

    step "Submitting MongoDB stress-test Job '$JOB' to namespace '$NS'"

    kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  namespace: $NS
  labels:
    test: mongo-stress
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
            - --mongodb-url
            - "$MONGO_URL"
            - mongo-bench
            - --target-rate
            - "$TARGET_RATE"
            - --concurrency
            - "$CONCURRENCY"
            - --batch-size
            - "$BATCH_SIZE"
            - --duration-secs
            - "$DURATION_SECS"
            - --p99-limit-ms
            - "$P99_LIMIT_MS"
            - --json
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

    PASSED=$(parse_json_field "$OUTPUT" "passed")
    RATE=$(parse_json_field "$OUTPUT" "actual_rate_eps")
    P99=$(parse_json_field "$OUTPUT" "p99_us")

    [[ "$PASSED" == "true" ]] || fail "MongoDB benchmark FAILED (passed=$PASSED, rate=${RATE} ev/s, p99=${P99} µs)"
    pass "MongoDB benchmark passed  (rate=${RATE} ev/s, p99=${P99} µs < $((P99_LIMIT_MS * 1000)) µs)"

    # Clean up the completed job.
    kubectl delete job "$JOB" -n "$NS" --ignore-not-found=true &>/dev/null || true
fi

echo
pass "Test 05 — MongoDB stress test COMPLETE"
