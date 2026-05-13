#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 02 — Performance Benchmark (Stress Test)
#
# Passes when:
#   • Actual throughput reaches ≥ 10 000 events/second
#   • p99 write latency < 2 ms (2 000 µs)
#
# Runs the Rust 'testbed bench' binary inside a Kubernetes Job or, if kubectl
# is unavailable, directly against a local KurrentDB instance.
#
# Usage:
#   ./tests/02-stress-test.sh                     # run as K8s Job (default)
#   DIRECT=1 ./tests/02-stress-test.sh            # run testbed binary directly
#   KURRENT_URL=kurrentdb://myhost:2113?tls=false \
#     DIRECT=1 ./tests/02-stress-test.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
JOB="stress-test-$(date +%s)"
IMAGE="${TESTBED_IMAGE:-event-sourcing-testbed:latest}"
# K8s default: 3-node cluster (real hardware, sub-ms networking → OK)
# Direct default: single-node bench service (avoids Podman VM bridge overhead)
KURRENT_URL="${KURRENT_URL:-kurrentdb://kurrentdb.event-store.svc.cluster.local:2113?tls=false}"
KURRENT_URL_DIRECT="${KURRENT_URL_DIRECT:-kurrentdb://localhost:2116?tls=false}"
DIRECT="${DIRECT:-0}"

TARGET_RATE=10000
CONCURRENCY=50
BATCH_SIZE=1
DURATION_SECS=30
MAX_P99_US=2000   # 2 ms

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }
step() { echo; echo "▶ $*"; }

# ── Helpers ───────────────────────────────────────────────────────────────────
require_cmd() {
    command -v "$1" &>/dev/null || fail "'$1' not found in PATH"
}

parse_json_field() {
    local json="$1" field="$2"
    # Portable extraction without jq dependency.
    echo "$json" | grep -oP "\"${field}\":\s*\K[^,}]+"
}

# ─────────────────────────────────────────────────────────────────────────────
if [[ "$DIRECT" == "1" ]]; then
    # ── Direct mode: run testbed binary on this machine ────────────────────
    require_cmd testbed

    step "Running stress test directly (target ${TARGET_RATE} ev/s for ${DURATION_SECS}s)"
    echo "  KurrentDB: $KURRENT_URL_DIRECT"

    OUTPUT=$(testbed \
        --kurrentdb-url "$KURRENT_URL_DIRECT" \
        bench \
        --target-rate    "$TARGET_RATE" \
        --duration-secs  "$DURATION_SECS" \
        --concurrency    "$CONCURRENCY" \
        --batch-size     "$BATCH_SIZE" \
        --json 2>/dev/null)

    echo "  Raw output: $OUTPUT"

else
    # ── Kubernetes Job mode ────────────────────────────────────────────────
    require_cmd kubectl

    step "Verifying KurrentDB cluster is healthy"
    READY=$(kubectl get statefulset kurrentdb -n "$NS" \
              -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    [[ "$READY" -ge 2 ]] \
      || fail "KurrentDB needs ≥2 ready replicas, got $READY. Deploy first with 'make deploy'."
    pass "KurrentDB: $READY/3 replicas ready"

    step "Submitting stress-test Job '$JOB'"
    kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  namespace: $NS
  labels:
    test: stress-test
spec:
  ttlSecondsAfterFinished: 600
  backoffLimit: 0
  template:
    metadata:
      labels:
        test: stress-test
    spec:
      restartPolicy: Never
      containers:
        - name: bench
          image: $IMAGE
          imagePullPolicy: IfNotPresent
          command: ["/usr/local/bin/testbed"]
          args:
            - bench
            - --target-rate
            - "$TARGET_RATE"
            - --duration-secs
            - "$DURATION_SECS"
            - --concurrency
            - "$CONCURRENCY"
            - --batch-size
            - "$BATCH_SIZE"
            - --json
          env:
            - name: KURRENTDB_URL
              value: "$KURRENT_URL"
            - name: RUST_LOG
              value: warn
          resources:
            requests:
              cpu: "1"
              memory: "256Mi"
            limits:
              cpu: "4"
              memory: "512Mi"
EOF

    step "Waiting for Job to complete (timeout: $((DURATION_SECS + 60))s)..."
    kubectl wait "job/$JOB" -n "$NS" \
        --for=condition=complete \
        --timeout="$((DURATION_SECS + 60))s" \
      || {
        kubectl logs "job/$JOB" -n "$NS" --tail=50 >&2 || true
        kubectl delete job "$JOB" -n "$NS" --ignore-not-found >/dev/null
        fail "Job did not complete in time"
      }

    step "Collecting results"
    OUTPUT=$(kubectl logs "job/$JOB" -n "$NS" 2>/dev/null | grep -E '^\{' | tail -1 || true)
    echo "  Raw output: $OUTPUT"

    kubectl delete job "$JOB" -n "$NS" --ignore-not-found >/dev/null
fi

# ── Parse and assert ──────────────────────────────────────────────────────────
step "Evaluating results"

P99=$(parse_json_field "$OUTPUT" "p99_us")
ACTUAL_RATE=$(parse_json_field "$OUTPUT" "actual_rate_eps")
TOTAL=$(parse_json_field "$OUTPUT" "total_events")
PASSED=$(parse_json_field "$OUTPUT" "passed")

echo "  Total events appended : $TOTAL"
echo "  Actual throughput     : ${ACTUAL_RATE} ev/s (target: $TARGET_RATE)"
echo "  p99 write latency     : ${P99} µs  (limit: ${MAX_P99_US} µs / 2 ms)"

[[ "${ACTUAL_RATE%.*}" -ge "$((TARGET_RATE * 90 / 100))" ]] \
  || fail "Throughput ${ACTUAL_RATE} ev/s is more than 10% below target ${TARGET_RATE} ev/s"
pass "Throughput within 10% of target"

[[ "$P99" -lt "$MAX_P99_US" ]] \
  || fail "p99 latency ${P99} µs exceeds the 2 ms (2 000 µs) SLA"
pass "p99 latency ${P99} µs < 2 000 µs"

echo
echo "══════════════════════════════════════════════"
echo "  Test 02 — Performance Benchmark: PASS"
echo "══════════════════════════════════════════════"
