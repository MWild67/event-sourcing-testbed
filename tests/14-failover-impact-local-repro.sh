#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-failover-impact-local}"
NS="${NS:-event-store}"
K3D_TAG="${K3D_TAG:-v5.8.3}"
AUTO_INSTALL_TOOLS="${AUTO_INSTALL_TOOLS:-1}"

export PATH="$HOME/.local/bin:$PATH"

step() { echo; echo "> $*"; }
pass() { echo "  OK: $*"; }
fail() { echo "  ERR: $*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

install_kubectl_local() {
  command -v curl >/dev/null 2>&1 || fail "curl is required to auto-install kubectl"
  step "Installing kubectl into ~/.local/bin"
  local version
  local arch
  version="$(curl -L -s https://dl.k8s.io/release/stable.txt)"
  arch="$(uname -m)"
  case "$arch" in
    x86_64) arch="amd64" ;;
    aarch64) arch="arm64" ;;
    *) fail "unsupported architecture for kubectl auto-install: $arch" ;;
  esac

  mkdir -p "$HOME/.local/bin"
  curl -fsSL "https://dl.k8s.io/release/${version}/bin/linux/${arch}/kubectl" -o "$HOME/.local/bin/kubectl"
  chmod +x "$HOME/.local/bin/kubectl"
  pass "kubectl installed: $($HOME/.local/bin/kubectl version --client --output=yaml | grep gitVersion | head -n1 | awk '{print $2}')"
}

install_k3d_local() {
  command -v curl >/dev/null 2>&1 || fail "curl is required to auto-install k3d"
  step "Installing k3d (${K3D_TAG}) into ~/.local/bin"
  mkdir -p "$HOME/.local/bin"
  curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | K3D_INSTALL_DIR="$HOME/.local/bin" TAG="$K3D_TAG" bash -s -- --no-sudo
  pass "k3d installed: $(k3d version | head -n1)"
}

ensure_tooling() {
  need cargo

  if ! command -v kubectl >/dev/null 2>&1; then
    if [[ "$AUTO_INSTALL_TOOLS" == "1" ]]; then
      install_kubectl_local
    else
      fail "kubectl missing (set AUTO_INSTALL_TOOLS=1 to install automatically)"
    fi
  fi

  if ! command -v k3d >/dev/null 2>&1; then
    if [[ "$AUTO_INSTALL_TOOLS" == "1" ]]; then
      install_k3d_local
    else
      fail "k3d missing (set AUTO_INSTALL_TOOLS=1 to install automatically)"
    fi
  fi

  if command -v docker >/dev/null 2>&1; then
    CONTAINER_RUNTIME="docker"
  elif command -v podman >/dev/null 2>&1; then
    CONTAINER_RUNTIME="podman"
  else
    fail "No supported container runtime found. Install docker or podman, then re-run make test-failover-impact-local"
  fi
}

dump_diagnostics() {
  step "Diagnostics"
  kubectl get nodes -o wide || true
  kubectl get pods -n "$NS" -o wide || true
  kubectl get events -n "$NS" --sort-by='.lastTimestamp' 2>/dev/null | tail -80 || true
  kubectl describe pods -n "$NS" || true
  for pod in $(kubectl get pods -n "$NS" -o name 2>/dev/null); do
    kubectl logs "$pod" -n "$NS" --tail=120 2>&1 || true
  done
}

cleanup() {
  set +e
  if k3d cluster list 2>/dev/null | grep -q "^$CLUSTER_NAME\b"; then
    step "Deleting k3d cluster '$CLUSTER_NAME'"
    k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

step "Pre-flight"
ensure_tooling
pass "using container runtime: $CONTAINER_RUNTIME"

step "Create k3d cluster"
k3d cluster create "$CLUSTER_NAME" --servers 1 --agents 3 --wait --timeout 180s
kubectl get nodes -o wide

step "Pre-pull and import KurrentDB image"
"$CONTAINER_RUNTIME" pull kurrentplatform/kurrentdb:latest
k3d image import kurrentplatform/kurrentdb:latest -c "$CLUSTER_NAME"

step "Deploy namespace and services"
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/02-kurrentdb/01-services.yaml

step "Deploy KurrentDB 3-node StatefulSet (CI-like)"
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kurrentdb
  namespace: event-store
spec:
  serviceName: kurrentdb-headless
  replicas: 3
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      app: kurrentdb
  template:
    metadata:
      labels:
        app: kurrentdb
    spec:
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              app: kurrentdb
      enableServiceLinks: false
      terminationGracePeriodSeconds: 10
      containers:
        - name: kurrentdb
          image: kurrentplatform/kurrentdb:latest
          imagePullPolicy: IfNotPresent
          ports:
            - {name: http, containerPort: 2113}
            - {name: grpc, containerPort: 2114}
          env:
            - {name: KURRENTDB_CLUSTER_SIZE, value: "3"}
            - {name: KURRENTDB_DISCOVER_VIA_DNS, value: "true"}
            - {name: KURRENTDB_CLUSTER_DNS, value: "kurrentdb-headless.event-store.svc.cluster.local"}
            - {name: KURRENTDB_CLUSTER_GOSSIP_PORT, value: "2113"}
            - {name: KURRENTDB_NODE_PORT, value: "2113"}
            - {name: KURRENTDB_INSECURE, value: "true"}
            - {name: KURRENTDB_RUN_PROJECTIONS, value: "None"}
            - {name: KURRENTDB_START_STANDARD_PROJECTIONS, value: "false"}
            - {name: KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP, value: "true"}
            - {name: KURRENTDB_UNSAFE_DISABLE_FLUSH_TO_DISK, value: "true"}
            - {name: KURRENTDB_DB, value: /data/db}
            - {name: KURRENTDB_LOG, value: /data/log}
          readinessProbe:
            httpGet: {path: /health/live, port: 2113}
            initialDelaySeconds: 20
            periodSeconds: 5
            failureThreshold: 6
          resources:
            requests: {cpu: "200m", memory: "256Mi"}
            limits: {cpu: "1", memory: "1Gi"}
          volumeMounts:
            - {name: data, mountPath: /data}
      volumes:
        - name: data
          emptyDir: {medium: Memory, sizeLimit: 512Mi}
EOF

step "Wait for cluster ready"
kubectl rollout status statefulset/kurrentdb -n "$NS" --timeout=300s
kubectl get pods -n "$NS" -o wide

step "Build benchmark binary"
cargo build --release --manifest-path rust-app/Cargo.toml

step "Run test 14"
if ! bash tests/14-failover-impact-test.sh | tee /tmp/failover-impact-local.log; then
  dump_diagnostics
  fail "test 14 failed"
fi

if ! grep '^{' /tmp/failover-impact-local.log >/tmp/kdb-failover-impact-local.json; then
  dump_diagnostics
  fail "test 14 produced no JSON output"
fi

pass "local failover-impact run succeeded"
pass "result: /tmp/kdb-failover-impact-local.json"
