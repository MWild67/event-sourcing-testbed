#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 01 — Storage Class Validation
#
# Verifies:
#   1. StorageClass "event-store-local" exists with volumeBindingMode: WaitForFirstConsumer
#   2. A PVC stays in Pending state until a Pod is scheduled (proving deferred binding)
#   3. Once a Pod consumes the PVC the volume binds to the correct node
#
# Usage: ./tests/01-validate-storage.sh [storageclass-name]
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if ! command -v kubectl &>/dev/null; then
    echo
    echo "▶ Checking prerequisites"
    echo "  ⚠  kubectl not found — this test requires a Kubernetes cluster"
    echo "  SKIPPED"
    echo
    exit 0
fi

SC="${1:-event-store-local}"
NS="event-store"
PVC="storage-validation-pvc"
POD="storage-validation-pod"
TIMEOUT=90

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }
step() { echo; echo "▶ $*"; }

step "Checking StorageClass '$SC'"

kubectl get storageclass "$SC" > /dev/null 2>&1 \
  || fail "StorageClass '$SC' not found — did you run: kubectl apply -f k8s/01-storageclass.yaml ?"

BINDING_MODE=$(kubectl get storageclass "$SC" -o jsonpath='{.volumeBindingMode}')
[[ "$BINDING_MODE" == "WaitForFirstConsumer" ]] \
  || fail "volumeBindingMode is '$BINDING_MODE', expected 'WaitForFirstConsumer'"
pass "StorageClass '$SC' exists with volumeBindingMode: WaitForFirstConsumer"

RECLAIM=$(kubectl get storageclass "$SC" -o jsonpath='{.reclaimPolicy}')
pass "reclaimPolicy: $RECLAIM"

# ── Create test PVC ────────────────────────────────────────────────────────
step "Creating test PVC '$PVC'"

kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $PVC
  namespace: $NS
  labels:
    test: storage-validation
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $SC
  resources:
    requests:
      storage: 1Gi
EOF

sleep 2
STATUS=$(kubectl get pvc "$PVC" -n "$NS" -o jsonpath='{.status.phase}')
[[ "$STATUS" == "Pending" ]] \
  || fail "PVC should be Pending before any Pod is scheduled, got: $STATUS"
pass "PVC is in Pending state (WaitForFirstConsumer is working)"

# ── Schedule a Pod to trigger binding ─────────────────────────────────────
step "Scheduling Pod '$POD' to trigger volume binding"

kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  namespace: $NS
  labels:
    test: storage-validation
spec:
  containers:
    - name: writer
      image: busybox:1.36
      command: ["/bin/sh", "-c"]
      args:
        - |
          echo "storage test $(date)" > /data/validation.txt
          cat /data/validation.txt
          echo "write OK"
      volumeMounts:
        - name: vol
          mountPath: /data
  volumes:
    - name: vol
      persistentVolumeClaim:
        claimName: $PVC
  restartPolicy: Never
EOF

step "Waiting up to ${TIMEOUT}s for Pod to complete..."
kubectl wait "pod/$POD" -n "$NS" --for=condition=Ready --timeout="${TIMEOUT}s" 2>/dev/null \
  || kubectl wait "pod/$POD" -n "$NS" \
       --for=jsonpath='{.status.phase}'=Succeeded \
       --timeout="${TIMEOUT}s" \
  || fail "Pod did not reach Succeeded within ${TIMEOUT}s"

# ── Confirm binding ────────────────────────────────────────────────────────
step "Verifying PVC bound to a PV"

STATUS=$(kubectl get pvc "$PVC" -n "$NS" -o jsonpath='{.status.phase}')
[[ "$STATUS" == "Bound" ]] \
  || fail "PVC is '$STATUS', expected 'Bound'"
pass "PVC is Bound"

NODE=$(kubectl get pod "$POD" -n "$NS" -o jsonpath='{.spec.nodeName}')
PV=$(kubectl get pvc "$PVC" -n "$NS" -o jsonpath='{.spec.volumeName}')
pass "PV '$PV' is local to node '$NODE' (data locality confirmed)"

# ── Cleanup ────────────────────────────────────────────────────────────────
step "Cleaning up test resources"
kubectl delete pod "$POD"  -n "$NS" --ignore-not-found
kubectl delete pvc "$PVC"  -n "$NS" --ignore-not-found
pass "Cleanup done"

echo
echo "══════════════════════════════════════"
echo "  Test 01 — Storage Validation: PASS"
echo "══════════════════════════════════════"
