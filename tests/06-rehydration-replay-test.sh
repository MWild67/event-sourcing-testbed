#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 06 — Rehydration / Replay Test
#
# Verifies that all three event store backends can:
#   1. Write a batch of domain events to a dedicated stream.
#   2. Replay (rehydrate) the full event stream from revision/version 0.
#   3. Reconstruct aggregate state correctly — every written OrderPlaced event
#      is replayed, in order, with no gaps and the correct event count.
#   4. Resume from a saved checkpoint (catch-up subscription pattern).
#
# Backends tested:
#   • KurrentDB  — native gRPC stream read; validates revision sequence.
#   • MongoDB    — rehydrate() via MongoEventStore (Feature 2/8 demo).
#   • PostgreSQL — rehydrate() via PgEventStore    (Feature 2/8 demo).
#
# Pass criteria (per backend):
#   • KurrentDB : JSON field "passed" == true AND events_written == events_replayed
#   • MongoDB   : JSON field "passed" == true AND events_written == events_replayed
#   • PostgreSQL: JSON field "passed" == true AND events_written == events_replayed
#
# Usage:
#   ./tests/06-rehydration-replay-test.sh               # K8s Job mode (default)
#   DIRECT=1 ./tests/06-rehydration-replay-test.sh      # run testbed binary directly
#   EVENTS=50000 ./tests/06-rehydration-replay-test.sh  # events to write per backend (default 50k)
#   SKIP_MONGO=1 SKIP_PG=1 ./tests/06-rehydration-replay-test.sh  # KurrentDB only
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
IMAGE="${TESTBED_IMAGE:-event-sourcing-testbed:latest}"
DIRECT="${DIRECT:-0}"
EVENTS="${EVENTS:-50000}"
SKIP_MONGO="${SKIP_MONGO:-0}"
SKIP_PG="${SKIP_PG:-0}"
MONGO_DB="${MONGO_DB:-rehydrate-demo}"

# ── In-cluster URLs (used when running as K8s Jobs) ───────────────────────────
KURRENT_URL="${KURRENT_URL:-kurrentdb://kurrentdb.event-store.svc.cluster.local:2113?tls=false}"
MONGO_URL="${MONGO_URL:-mongodb://mongodb.event-store.svc.cluster.local:27017}"
PG_URL="${PG_URL:-postgres://postgres:postgres@postgres.event-store.svc.cluster.local:5432/eventbench}"

# ── Direct-mode URLs (used when DIRECT=1) ────────────────────────────────────
KURRENT_URL_DIRECT="${KURRENT_URL_DIRECT:-kurrentdb://localhost:2116?tls=false}"
MONGO_URL_DIRECT="${MONGO_URL_DIRECT:-mongodb://localhost:27017}"
PG_URL_DIRECT="${PG_URL_DIRECT:-postgres://postgres:postgres@localhost:5432/eventbench}"

JOB_POLL_INTERVAL=3   # seconds between status checks
JOB_TIMEOUT=120       # seconds to wait per Job before declaring timeout

PASSED_BACKENDS=0
FAILED_BACKENDS=0
FAILED_NAMES=()

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
    echo "$json" | grep -oP "\"${field}\":\s*\K[^,}]+" | head -1
}

# ── Wait for a K8s Job to reach Complete or Failed ────────────────────────────
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

# ── Submit a Job, wait, stream its logs, return exit status ───────────────────
run_job() {
    local job="$1"; shift
    local -a args=("$@")

    # Build the command array as a YAML sequence so quoting is unambiguous.
    local cmd_yaml
    cmd_yaml=$(printf '          - "%s"\n' "${args[@]}")

    kubectl apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $NS
  labels:
    test: rehydration-replay
spec:
  ttlSecondsAfterFinished: 600
  backoffLimit: 0
  template:
    metadata:
      labels:
        test: rehydration-replay
    spec:
      restartPolicy: Never
      containers:
        - name: testbed
          image: $IMAGE
          imagePullPolicy: IfNotPresent
          command:
$cmd_yaml
EOF

    local rc
    if wait_for_job "$job" "$JOB_TIMEOUT"; then
        rc=0
    else
        rc=1
    fi

    # Always capture logs for inspection / assertions.
    JOB_LOGS=$(kubectl logs -l "job-name=${job}" -n "$NS" --tail=200 2>/dev/null || echo "")
    return "$rc"
}

# ── Record backend result ──────────────────────────────────────────────────────
record_result() {
    local backend="$1" ok="$2"
    if [[ "$ok" == "0" ]]; then
        pass "$backend rehydration PASSED"
        PASSED_BACKENDS=$((PASSED_BACKENDS + 1))
    else
        fail "$backend rehydration FAILED"
        FAILED_BACKENDS=$((FAILED_BACKENDS + 1))
        FAILED_NAMES+=("$backend")
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
#  DIRECT MODE — run testbed binary on this machine
# ═════════════════════════════════════════════════════════════════════════════
if [[ "$DIRECT" == "1" ]]; then
    require_cmd testbed

    # ── KurrentDB ─────────────────────────────────────────────────────────────
    step "KurrentDB — rehydration/replay (direct, ${EVENTS} events)"
    echo "  URL: $KURRENT_URL_DIRECT"

    OUTPUT=$(testbed \
        --kurrentdb-url "$KURRENT_URL_DIRECT" \
        kurrentdb-rehydrate-demo \
        --events "$EVENTS" \
        --json 2>/dev/null | tail -1)
    echo "  Output: $OUTPUT"

    PASSED=$(parse_json_field "$OUTPUT" "passed")
    WRITTEN=$(parse_json_field "$OUTPUT" "events_written")
    REPLAYED=$(parse_json_field "$OUTPUT" "events_replayed")
    RATE=$(parse_json_field "$OUTPUT" "replay_rate_eps")
    REV_OK=$(parse_json_field "$OUTPUT" "revisions_ok")

    KURRENT_OK=1
    [[ "$PASSED" == "true" ]] \
        || { fail "passed=$PASSED (expected true)"; }
    [[ "$WRITTEN" == "$REPLAYED" ]] \
        || { fail "events_written=$WRITTEN != events_replayed=$REPLAYED"; }
    [[ "$REV_OK" == "true" ]] \
        || { fail "revisions_ok=$REV_OK (expected true)"; }

    if [[ "$PASSED" == "true" && "$WRITTEN" == "$REPLAYED" && "$REV_OK" == "true" ]]; then
        pass "events_written=$WRITTEN, events_replayed=$REPLAYED, replay_rate=${RATE} ev/s"
        KURRENT_OK=0
    fi
    record_result "KurrentDB" "$KURRENT_OK"

    # ── MongoDB ───────────────────────────────────────────────────────────────
    if [[ "$SKIP_MONGO" != "1" ]]; then
        step "MongoDB — rehydration/replay (direct, ${EVENTS} events)"
        echo "  URL: $MONGO_URL_DIRECT"

        MONGO_OUTPUT=$(testbed \
            --mongodb-url "$MONGO_URL_DIRECT" \
            mongo-rehydrate-demo \
            --database "$MONGO_DB" \
            --events "$EVENTS" \
            --json 2>/dev/null | tail -1)
        echo "  Output: $MONGO_OUTPUT"

        MONGO_OK=1
        MONGO_PASSED=$(parse_json_field "$MONGO_OUTPUT" "passed")
        MONGO_WRITTEN=$(parse_json_field "$MONGO_OUTPUT" "events_written")
        MONGO_REPLAYED=$(parse_json_field "$MONGO_OUTPUT" "events_replayed")
        MONGO_RATE=$(parse_json_field "$MONGO_OUTPUT" "replay_rate_eps")

        if [[ "$MONGO_PASSED" == "true" && "$MONGO_WRITTEN" == "$MONGO_REPLAYED" ]]; then
            pass "events_written=$MONGO_WRITTEN, events_replayed=$MONGO_REPLAYED, replay_rate=${MONGO_RATE} ev/s"
            MONGO_OK=0
        else
            fail "assertion failed: passed=$MONGO_PASSED, written=$MONGO_WRITTEN, replayed=$MONGO_REPLAYED"
        fi
        record_result "MongoDB" "$MONGO_OK"
    else
        warn "MongoDB skipped (SKIP_MONGO=1)"
    fi

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    if [[ "$SKIP_PG" != "1" ]]; then
        step "PostgreSQL — rehydration/replay (direct, ${EVENTS} events)"
        echo "  URL: $PG_URL_DIRECT"

        PG_OUTPUT=$(testbed \
            --postgres-url "$PG_URL_DIRECT" \
            pg-rehydrate-demo \
            --events "$EVENTS" \
            --json 2>/dev/null | tail -1)
        echo "  Output: $PG_OUTPUT"

        PG_OK=1
        PG_PASSED=$(parse_json_field "$PG_OUTPUT" "passed")
        PG_WRITTEN=$(parse_json_field "$PG_OUTPUT" "events_written")
        PG_REPLAYED=$(parse_json_field "$PG_OUTPUT" "events_replayed")
        PG_RATE=$(parse_json_field "$PG_OUTPUT" "replay_rate_eps")

        if [[ "$PG_PASSED" == "true" && "$PG_WRITTEN" == "$PG_REPLAYED" ]]; then
            pass "events_written=$PG_WRITTEN, events_replayed=$PG_REPLAYED, replay_rate=${PG_RATE} ev/s"
            PG_OK=0
        else
            fail "assertion failed: passed=$PG_PASSED, written=$PG_WRITTEN, replayed=$PG_REPLAYED"
        fi
        record_result "PostgreSQL" "$PG_OK"
    else
        warn "PostgreSQL skipped (SKIP_PG=1)"
    fi

# ═════════════════════════════════════════════════════════════════════════════
#  KUBERNETES JOB MODE (default)
# ═════════════════════════════════════════════════════════════════════════════
else
    require_cmd kubectl

    # ── Pre-flight: cluster health ─────────────────────────────────────────────
    step "Pre-flight checks"

    KURRENT_READY=$(kubectl get statefulset kurrentdb -n "$NS" \
        -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    [[ "${KURRENT_READY:-0}" -ge 1 ]] \
        || { fail "KurrentDB not ready (readyReplicas=${KURRENT_READY:-0})"; exit 1; }
    pass "KurrentDB ${KURRENT_READY}/3 replicas ready"

    # ── KurrentDB Rehydration Job ─────────────────────────────────────────────
    step "KurrentDB — rehydration/replay Job (${EVENTS} events)"

    KURRENT_JOB="rehydrate-kurrentdb-$(date +%s)"

    JOB_LOGS=""
    run_job "$KURRENT_JOB" \
        testbed \
        --kurrentdb-url "$KURRENT_URL" \
        kurrentdb-rehydrate-demo \
        --events "$EVENTS" \
        --json
    KURRENT_EXIT=$?

    # The testbed binary writes JSON to stdout and logs to stderr.
    # kubectl logs merges both, so extract the JSON line.
    OUTPUT=$(echo "$JOB_LOGS" | grep -oP '\{.*"passed".*\}' | tail -1 || echo "")
    echo "  Raw output: ${OUTPUT:-(none — check job logs)}"

    KURRENT_OK=1
    if [[ $KURRENT_EXIT -eq 0 && -n "$OUTPUT" ]]; then
        PASSED=$(parse_json_field "$OUTPUT" "passed")
        WRITTEN=$(parse_json_field "$OUTPUT" "events_written")
        REPLAYED=$(parse_json_field "$OUTPUT" "events_replayed")
        RATE=$(parse_json_field "$OUTPUT" "replay_rate_eps")
        REV_OK=$(parse_json_field "$OUTPUT" "revisions_ok")

        if [[ "$PASSED" == "true" && "$WRITTEN" == "$REPLAYED" && "$REV_OK" == "true" ]]; then
            pass "events_written=$WRITTEN, events_replayed=$REPLAYED, replay_rate=${RATE} ev/s, revisions_ok=$REV_OK"
            KURRENT_OK=0
        else
            fail "assertion failed: passed=$PASSED, written=$WRITTEN, replayed=$REPLAYED, revisions_ok=$REV_OK"
        fi
    else
        fail "job exit=$KURRENT_EXIT or no JSON output found"
        echo "$JOB_LOGS" | tail -30 >&2
    fi
    record_result "KurrentDB" "$KURRENT_OK"

    # ── MongoDB Rehydration Job ────────────────────────────────────────────────
    if [[ "$SKIP_MONGO" != "1" ]]; then
        MONGO_READY=$(kubectl get statefulset mongodb -n "$NS" \
            -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        if [[ "${MONGO_READY:-0}" -lt 1 ]]; then
            warn "MongoDB not ready — skipping (readyReplicas=${MONGO_READY:-0})"
            SKIP_MONGO=1
        fi
    fi

    if [[ "$SKIP_MONGO" != "1" ]]; then
        step "MongoDB — rehydration/replay Job (${EVENTS} events)"

        MONGO_JOB="rehydrate-mongo-$(date +%s)"

        JOB_LOGS=""
        run_job "$MONGO_JOB" \
            testbed \
            --mongodb-url "$MONGO_URL" \
            mongo-rehydrate-demo \
            --database "$MONGO_DB" \
            --events "$EVENTS" \
            --json
        MONGO_EXIT=$?

        OUTPUT=$(echo "$JOB_LOGS" | grep -oP '\{.*"passed".*\}' | tail -1 || echo "")
        echo "  Raw output: ${OUTPUT:-(none — check job logs)}"

        MONGO_OK=1
        if [[ $MONGO_EXIT -eq 0 && -n "$OUTPUT" ]]; then
            MONGO_PASSED=$(parse_json_field "$OUTPUT" "passed")
            MONGO_WRITTEN=$(parse_json_field "$OUTPUT" "events_written")
            MONGO_REPLAYED=$(parse_json_field "$OUTPUT" "events_replayed")
            MONGO_RATE=$(parse_json_field "$OUTPUT" "replay_rate_eps")

            if [[ "$MONGO_PASSED" == "true" && "$MONGO_WRITTEN" == "$MONGO_REPLAYED" ]]; then
                pass "events_written=$MONGO_WRITTEN, events_replayed=$MONGO_REPLAYED, replay_rate=${MONGO_RATE} ev/s"
                MONGO_OK=0
            else
                fail "assertion failed: passed=$MONGO_PASSED, written=$MONGO_WRITTEN, replayed=$MONGO_REPLAYED"
            fi
        else
            fail "job exit=$MONGO_EXIT or no JSON output found"
            echo "$JOB_LOGS" | tail -30 >&2
        fi
        record_result "MongoDB" "$MONGO_OK"
    else
        warn "MongoDB skipped (SKIP_MONGO=1)"
    fi

    # ── PostgreSQL Rehydration Job ─────────────────────────────────────────────
    if [[ "$SKIP_PG" != "1" ]]; then
        PG_READY=$(kubectl get statefulset postgres -n "$NS" \
            -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        if [[ "${PG_READY:-0}" -lt 1 ]]; then
            warn "PostgreSQL not ready — skipping (readyReplicas=${PG_READY:-0})"
            SKIP_PG=1
        fi
    fi

    if [[ "$SKIP_PG" != "1" ]]; then
        step "PostgreSQL — rehydration/replay Job (${EVENTS} events)"

        PG_JOB="rehydrate-pg-$(date +%s)"

        JOB_LOGS=""
        run_job "$PG_JOB" \
            testbed \
            --postgres-url "$PG_URL" \
            pg-rehydrate-demo \
            --events "$EVENTS" \
            --json
        PG_EXIT=$?

        OUTPUT=$(echo "$JOB_LOGS" | grep -oP '\{.*"passed".*\}' | tail -1 || echo "")
        echo "  Raw output: ${OUTPUT:-(none — check job logs)}"

        PG_OK=1
        if [[ $PG_EXIT -eq 0 && -n "$OUTPUT" ]]; then
            PG_PASSED=$(parse_json_field "$OUTPUT" "passed")
            PG_WRITTEN=$(parse_json_field "$OUTPUT" "events_written")
            PG_REPLAYED=$(parse_json_field "$OUTPUT" "events_replayed")
            PG_RATE=$(parse_json_field "$OUTPUT" "replay_rate_eps")

            if [[ "$PG_PASSED" == "true" && "$PG_WRITTEN" == "$PG_REPLAYED" ]]; then
                pass "events_written=$PG_WRITTEN, events_replayed=$PG_REPLAYED, replay_rate=${PG_RATE} ev/s"
                PG_OK=0
            else
                fail "assertion failed: passed=$PG_PASSED, written=$PG_WRITTEN, replayed=$PG_REPLAYED"
            fi
        else
            fail "job exit=$PG_EXIT or no JSON output found"
            echo "$JOB_LOGS" | tail -30 >&2
        fi
        record_result "PostgreSQL" "$PG_OK"
    else
        warn "PostgreSQL skipped (SKIP_PG=1)"
    fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════════"
echo "  Rehydration / Replay — Test Summary"
echo "════════════════════════════════════════════════"
echo "  Passed : $PASSED_BACKENDS backend(s)"
echo "  Failed : $FAILED_BACKENDS backend(s)"
if [[ ${#FAILED_NAMES[@]} -gt 0 ]]; then
    echo "  Failed : ${FAILED_NAMES[*]}"
fi
echo "════════════════════════════════════════════════"

[[ $FAILED_BACKENDS -eq 0 ]] \
    || { echo "  OVERALL: FAIL" >&2; exit 1; }
echo "  OVERALL: PASS"
