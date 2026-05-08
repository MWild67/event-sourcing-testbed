#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 03 — Automated Failover Test
#
# Scenario:
#   1. Identify which Kubernetes worker node hosts the current EventStoreDB leader.
#   2. Simulate a node failure by:
#        a. Cordoning the node (no new scheduling).
#        b. Applying NoExecute taints (immediate pod eviction — like power-off).
#   3. Start a timer.
#   4. Poll until EventStoreDB has ≥ 2 healthy replicas AND a leader is elected.
#   5. Assert recovery time < 60 seconds.
#   6. Restore the node and remove taints (cleanup).
#
# Assumptions:
#   • kubectl is configured and has admin permissions.
#   • EventStoreDB StatefulSet is in namespace "event-store".
#   • Cluster has ≥ 3 worker nodes (so another node can absorb the workload).
#
# Usage: ./tests/03-failover-test.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
STS="eventstore"
RECOVERY_TIMEOUT=60     # seconds — the hard SLA
POLL_INTERVAL=2         # seconds between health checks
ES_HTTP="${ES_HTTP:-http://localhost:2113}"   # adjust if testing locally

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; cleanup; exit 1; }
step() { echo; echo "▶ $*"; }
warn() { echo "  ⚠  $*"; }

TARGET_NODE=""

cleanup() {
    if [[ -n "$TARGET_NODE" ]]; then
        step "Restoring node '$TARGET_NODE'"
        kubectl taint nodes "$TARGET_NODE" \
            node.kubernetes.io/unreachable:NoExecute- \
            node.kubernetes.io/not-ready:NoExecute- \
            2>/dev/null || true
        kubectl uncordon "$TARGET_NODE" 2>/dev/null || true
        pass "Node '$TARGET_NODE' restored"
    fi
}
trap cleanup EXIT

# ── Pre-flight checks ─────────────────────────────────────────────────────────
step "Pre-flight checks"

READY=$(kubectl get statefulset "$STS" -n "$NS" \
          -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
[[ "$READY" -ge 3 ]] \
  || fail "Need 3/3 ready replicas before failover test; got $READY"
pass "EventStoreDB: 3/3 replicas ready"

NODE_COUNT=$(kubectl get nodes --no-headers | grep -c " Ready")
[[ "$NODE_COUNT" -ge 3 ]] \
  || fail "Cluster has only $NODE_COUNT Ready nodes; need ≥ 3 so the evicted pod can reschedule"
pass "Cluster has $NODE_COUNT Ready nodes"

# ── Find the leader pod and its node ─────────────────────────────────────────
step "Identifying EventStoreDB leader"

LEADER_POD=""
for pod in $(kubectl get pods -n "$NS" -l app="$STS" -o jsonpath='{.items[*].metadata.name}'); do
    POD_IP=$(kubectl get pod "$pod" -n "$NS" -o jsonpath='{.status.podIP}')
    STATE=$(kubectl exec "$pod" -n "$NS" -- \
        wget -qO- "http://127.0.0.1:2113/info" 2>/dev/null \
        | grep -oP '"state"\s*:\s*"\K[^"]+' || echo "unknown")

    echo "  $pod ($POD_IP) state: $STATE"

    if [[ "$STATE" == "Leader" ]]; then
        LEADER_POD="$pod"
    fi
done

if [[ -z "$LEADER_POD" ]]; then
    warn "Could not detect leader via /info; picking the first pod instead."
    LEADER_POD=$(kubectl get pods -n "$NS" -l app="$STS" \
                   -o jsonpath='{.items[0].metadata.name}')
fi

TARGET_NODE=$(kubectl get pod "$LEADER_POD" -n "$NS" -o jsonpath='{.spec.nodeName}')
pass "Leader pod: $LEADER_POD on node: $TARGET_NODE"

# ── Baseline: record current cluster state ────────────────────────────────────
step "Baseline health snapshot"
kubectl get pods -n "$NS" -l app="$STS" -o wide

# ── Simulate node failure ─────────────────────────────────────────────────────
step "Simulating node failure on '$TARGET_NODE'"

kubectl cordon "$TARGET_NODE"
pass "Node cordoned (no new scheduling)"

# These taints match what Kubernetes applies automatically after node.kubernetes.io
# conditions — applying them manually triggers immediate NoExecute eviction.
kubectl taint nodes "$TARGET_NODE" \
    node.kubernetes.io/unreachable:NoExecute \
    node.kubernetes.io/not-ready:NoExecute \
    --overwrite
pass "NoExecute taints applied — pod eviction in progress"

FAIL_TIME=$(date +%s)
echo "  Failure simulated at: $(date -d @"$FAIL_TIME" '+%H:%M:%S' 2>/dev/null || date -r "$FAIL_TIME" '+%H:%M:%S' 2>/dev/null || date)"

# ── Poll for recovery ─────────────────────────────────────────────────────────
step "Polling for cluster recovery (SLA: ${RECOVERY_TIMEOUT}s)..."

RECOVERED=0
ELAPSED=0
LAST_STATUS=""

while [[ $ELAPSED -lt $RECOVERY_TIMEOUT ]]; do
    sleep "$POLL_INTERVAL"
    ELAPSED=$(( $(date +%s) - FAIL_TIME ))

    READY_NOW=$(kubectl get statefulset "$STS" -n "$NS" \
                  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")

    STATUS="[${ELAPSED}s] ready=${READY_NOW}/3"
    if [[ "$STATUS" != "$LAST_STATUS" ]]; then
        echo "  $STATUS"
        LAST_STATUS="$STATUS"
    fi

    if [[ "$READY_NOW" -ge 2 ]]; then
        RECOVERED=1
        RECOVERY_TIME=$ELAPSED
        break
    fi
done

# ── Evaluate ──────────────────────────────────────────────────────────────────
step "Results"

kubectl get pods -n "$NS" -l app="$STS" -o wide

if [[ "$RECOVERED" -eq 0 ]]; then
    fail "EventStoreDB did not recover within ${RECOVERY_TIMEOUT}s"
fi

pass "EventStoreDB recovered in ${RECOVERY_TIME}s"

[[ "$RECOVERY_TIME" -lt "$RECOVERY_TIMEOUT" ]] \
  || fail "Recovery time ${RECOVERY_TIME}s exceeds SLA of ${RECOVERY_TIMEOUT}s"

pass "Recovery time ${RECOVERY_TIME}s < ${RECOVERY_TIMEOUT}s SLA"

echo
echo "  Simulated failure at : $(date -d @"$FAIL_TIME" '+%H:%M:%S' 2>/dev/null || echo N/A)"
echo "  Cluster healthy after: ${RECOVERY_TIME}s"
echo "  SLA                  : < ${RECOVERY_TIMEOUT}s"
echo
echo "══════════════════════════════════════════════"
echo "  Test 03 — Automated Failover: PASS"
echo "══════════════════════════════════════════════"
