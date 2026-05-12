# Event Sourcing Testbed

A fully automated testbed for event sourcing using **Rust**, **RabbitMQ**, and **KurrentDB** on Kubernetes.

## Architecture

``` diagram
┌─────────────────────────────────────────────────────────────┐
│                    event-store namespace                     │
│                                                             │
│  ┌──────────────┐   appends    ┌──────────────────────────┐ │
│  │  Rust App    │─────────────▶│  KurrentDB (3-node)   │ │
│  │  (testbed)   │              │  StatefulSet + local PVs │ │
│  └──────┬───────┘              └──────────────────────────┘ │
│         │ publishes                                          │
│         ▼                                                   │
│  ┌──────────────┐              ┌──────────────────────────┐ │
│  │  RabbitMQ    │              │  Prometheus + Grafana    │ │
│  │  (3-node)    │              │  node-exporter DaemonSet │ │
│  └──────────────┘              └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

| Component       | Version              | Role                                  |
|-----------------|----------------------|---------------------------------------|
| KurrentDB    | 23.10.0              | Append-only event log (gRPC/HTTP)     |
| RabbitMQ        | 3.13 + management    | Event fan-out (topic exchange)        |
| Rust app        | Tokio + lapin        | Benchmark harness & event producer    |
| MongoDB         | 7.x                  | Event-log alternative — write latency comparison |
| Prometheus      | v2.51                | Metrics collection                    |
| Grafana         | 10.4                 | Dashboards                            |
| node-exporter   | v1.7                 | Disk I/O, IOPS, CPU iowait            |

---

## Prerequisites

| Tool              | Version          | Platform        |
|-------------------|------------------|-----------------|
| Podman            | ≥ 4.2            | Linux           |
| Podman            | ≥ 4.0            | Windows (with docker-compose.exe provider) |
| Docker + Compose  | Docker ≥ 24, Compose plugin ≥ 2 | Linux alternative |
| kubectl           | ≥ 1.26           | K8s deploy only |
| make              | any              | all             |

> **Linux Podman < 4.2**: the built-in `podman compose` passes `--policy=missing` to
> `podman pull`, which is not supported in older versions.  Either upgrade Podman
> (`sudo apt install podman` on Ubuntu 24.04+ gives 4.x) or use Docker Engine with
> the Compose plugin: `make up COMPOSE="docker compose" RUNTIME=docker`

The cluster needs **≥ 3 worker nodes** (for KurrentDB HA and the failover test).

### StorageClass provisioner

The default StorageClass (`event-store-local`) uses `rancher.io/local-path` which ships with k3s/k3d.
Swap the `provisioner:` line in [k8s/01-storageclass.yaml](k8s/01-storageclass.yaml) for your environment:

| Environment | Provisioner                    |
|-------------|-------------------------------|
| k3s / k3d   | `rancher.io/local-path`        |
| AWS EKS     | `ebs.csi.aws.com`              |
| Azure AKS   | `disk.csi.azure.com`           |
| GKE         | `pd.csi.storage.gke.io`        |
| Bare-metal  | `kubernetes.io/no-provisioner` (manual PVs required) |

---

## Quick Start

### Option A — Local (docker compose)

**Windows (Podman):**

```powershell
# Build the benchmark image
make build

# Start all services (includes MongoDB on port 27017)
make up

# Run the KurrentDB benchmark
make bench-local

# Run the MongoDB benchmark
make mongo-bench-local

# Explore the UIs
start http://localhost:2113/web   # KurrentDB
start http://localhost:15672       # RabbitMQ  (guest / guest)
start http://localhost:3000        # Grafana   (admin / admin)

# Stop and remove all containers
make down
```

**Linux (Podman ≥ 4.2 or Docker + Compose plugin):**

```bash
# Build the benchmark image
make build

# Start all services (includes MongoDB on port 27017)
make up

# Run the KurrentDB benchmark
make bench-local

# Run the MongoDB benchmark
make mongo-bench-local

# Explore the UIs
xdg-open http://localhost:2113/web   # KurrentDB
xdg-open http://localhost:15672       # RabbitMQ  (guest / guest)
xdg-open http://localhost:3000        # Grafana   (admin / admin)

# Stop and remove all containers
make down
```

> **Why the benchmark only passes on Linux:** On Windows, containers run inside a
> HyperV guest VM.  The .NET KurrentDB process inherits Windows host CPU scheduling,
> which delays async thread wake-ups by ~15 ms per scheduler tick.  A single event write
> requires several async continuations inside KurrentDB, making the hard floor ~45 ms
> — well above the 2 ms p99 SLA.  On Linux, Podman runs containers directly on the host
> kernel (no VM), so the same async continuations complete in ~200 µs.

### Option B — Kubernetes

```bash
# 1. Build and push the benchmark image
REGISTRY=myregistry.io/ make push

# 2. Deploy all manifests
make deploy

# 3. Run all four test suites
make test-all
```

---

## File Layout

``` fs
event-sourcing-testbed/
├── Makefile                         # Orchestration helpers
├── docker-compose.yml               # Local dev stack
├── docker/
│   ├── prometheus.yml               # Prometheus scrape config (compose)
│   └── grafana/provisioning/        # Grafana datasource + dashboard provider
│
├── k8s/
│   ├── 00-namespace.yaml            # Namespace: event-store
│   ├── 01-storageclass.yaml         # StorageClass (WaitForFirstConsumer)
│   ├── 02-eventstore/
│   │   ├── 01-services.yaml         # Headless + ClusterIP services
│   │   └── 02-statefulset.yaml      # 3-replica StatefulSet + PVC templates
│   ├── 03-rabbitmq/
│   │   ├── 01-services.yaml
│   │   └── 02-statefulset.yaml      # RBAC + ConfigMap + StatefulSet
│   └── 04-monitoring/
│       ├── 01-node-exporter.yaml    # DaemonSet for Disk I/O metrics
│       ├── 02-prometheus-rbac.yaml  # ServiceAccount + ClusterRole
│       ├── 03-prometheus-config.yaml# Scrape targets (ES, RMQ, node-exporter)
│       ├── 04-prometheus.yaml       # Deployment + Service
│       ├── 05-grafana-datasource.yaml
│       ├── 06-grafana-dashboard.yaml# Full dashboard JSON (ConfigMap)
│       └── 07-grafana.yaml          # Deployment + Service
│
├── rust-app/
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                  # CLI entry point (bench / produce / ping)
│       ├── events.rs                # Domain events + benchmark payload
│       ├── kurrentdb/client.rs   # KurrentDB gRPC wrapper
│       ├── rabbitmq_client.rs       # AMQP producer (lapin)
│       └── benchmark.rs             # HDR-histogram stress test
│
└── tests/
    ├── 01-validate-storage.sh       # StorageClass + WaitForFirstConsumer
    ├── 02-stress-test.sh            # 10k ev/s, p99 < 2 ms
    ├── 03-failover-test.sh          # Node failure, recovery < 60 s
    └── 04-monitoring-check.sh       # Prometheus targets + Grafana dashboard
```

---

## Tests

### Test 01 — Storage Class Validation

Proves that `volumeBindingMode: WaitForFirstConsumer` is active and that volumes
are created on the node where the Pod lands (data locality).

```bash
make test-storage
# or directly:
bash tests/01-validate-storage.sh
```

**Pass criteria:**

- StorageClass exists with `volumeBindingMode: WaitForFirstConsumer`
- PVC stays in `Pending` state until a Pod is scheduled
- PVC binds to a PV on the same node as the consumer Pod

---

### Test 05 — MongoDB Write-Latency Stress Test

Inserts events into a single MongoDB 7 node at **10 000 events/second** for 30 seconds
using 64 concurrent Tokio tasks across separate collections.
The database is **dropped before each run** so leftover data from a prior run
cannot inflate B-tree index lookup times.

```bash
# Start MongoDB locally first:
docker compose up -d mongodb
# or with Podman:
podman compose up -d mongodb

# Run the benchmark directly:
MONGO_URL=mongodb://localhost:27017 \
  DIRECT=1 bash tests/05-mongodb-stress-test.sh

# Or via the testbed binary:
rust-app/target/release/testbed \
  --mongodb-url mongodb://localhost:27017 \
  mongo-bench \
  --target-rate 10000 \
  --concurrency 64 \
  --duration-secs 30 \
  --p99-limit-ms 5

# Relax the p99 threshold on slower machines:
P99_LIMIT_MS=20 DIRECT=1 MONGO_URL=mongodb://localhost:27017 \
  bash tests/05-mongodb-stress-test.sh

# Keep data from a prior run (warm-database test):
rust-app/target/release/testbed \
  --mongodb-url mongodb://localhost:27017 \
  mongo-bench --no-drop --p99-limit-ms 5
```

**Pass criteria:**

- Actual rate ≥ 9 000 ev/s
- **p99 insert latency < p99-limit-ms** (default 2 ms; 5 ms recommended on GitHub-hosted runners)

**CLI flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--target-rate` | 10000 | Target events/second |
| `--concurrency` | 64 | Parallel insert tasks |
| `--batch-size` | 1 | Documents per `insert_many` call |
| `--duration-secs` | 30 | Run duration |
| `--database` | `eventbench` | MongoDB database name |
| `--p99-limit-ms` | 2 | Failure threshold |
| `--no-drop` | off | Skip pre-run database drop |
| `--json` | off | Emit results as a single JSON line |

> **Isolation:** The MongoDB benchmark is completely independent of the KurrentDB
> and RabbitMQ tests — separate server, separate port, separate collections.
> Do **not** run `mongo-bench` concurrently with `bench` on the same machine;
> both saturate host I/O and will inflate each other's latency numbers.

---

### Test 02 — Performance Benchmark (I/O Stress Test)

Appends events to KurrentDB at **10 000 events/second** for 30 seconds
using 50 concurrent Tokio tasks across separate streams.
Latency is measured with a 3-significant-digit HDR histogram.

```bash
make test-bench

# Kubernetes Job output example:
#   Total events appended : 298 741
#   Actual throughput     : 9 958.0 ev/s
#   p99 write latency     : 1 412 µs  (1.41 ms)  ✓
```

**Pass criteria:**

- Actual rate ≥ 9 000 ev/s (within 10 % of 10 000)
- **p99 write latency < 2 000 µs (2 ms)**

Tuning tips if you fail this test:

- Increase node IOPS (NVMe preferred over spinning disk).
- Raise `--concurrency` (more parallel in-flight requests raise throughput without changing per-request latency).
- Use the `event-store-local` StorageClass on a dedicated SSD mount.

---

### Test 03 — Automated Failover

Simulates a worker node power-off by applying `NoExecute` taints and measures
how long it takes for KurrentDB to re-elect a leader and reach ≥ 2 ready
replicas on healthy nodes.

```bash
make test-failover
```

**Pass criteria:**

- KurrentDB re-mounts data on a healthy node and has ≥ 2 ready replicas
- **Recovery time < 60 seconds** from taint application

The node is automatically uncordoned and taints removed after the test.

> **Note:** This test requires ≥ 3 Ready worker nodes in the cluster.

---

### Test 04 — Monitoring Integration

Verifies that the Grafana dashboard and Prometheus scrape targets are wired up
correctly.

```bash
make test-monitoring
```

**Checked metrics:**

| Metric                              | Meaning                     |
|-------------------------------------|-----------------------------|
| `node_cpu_seconds_total{mode="iowait"}` | Disk I/O Wait %          |
| `node_disk_reads_completed_total`   | Read IOPS                   |
| `node_disk_writes_completed_total`  | Write IOPS                  |
| `up{job="eventstore"}`              | KurrentDB cluster health |
| `up{job="rabbitmq"}`                | RabbitMQ health             |

**Pass criteria:**

- Prometheus has active scrape targets for `node-exporter`, `eventstore`, `rabbitmq`
- All five metric families return data
- Grafana API returns the "Event Store Namespace" dashboard

---

## Grafana Dashboard

The provisioned dashboard ([k8s/04-monitoring/06-grafana-dashboard.yaml](k8s/04-monitoring/06-grafana-dashboard.yaml))
contains three row groups:

**Storage Performance:**

- Disk I/O Wait % per node
- Read IOPS / Write IOPS per node
- Disk throughput MB/s
- Average I/O completion time (ms)

**KurrentDB Cluster Health:**

- Active client connections
- Write queue depth
- Alive nodes (stat panel — green = 3, yellow = 2, red < 2)
- Leader elected indicator
- Write bytes/s
- Chunk flush rate (append throughput proxy)

**RabbitMQ Health:**

- Messages ready in queue
- Consumer count
- Publish / deliver rate
- Alive broker nodes

Access the dashboard:

```bash
make pf-grafana   # port-forwards localhost:3000
open http://localhost:3000   # admin / admin
```

---

## Rust App Reference

``` shell
testbed [OPTIONS] <COMMAND>

Options:
  --kurrentdb-url  ESDB connection URL  [env: KURRENTDB_URL]
  --rabbitmq-url    AMQP URL             [env: RABBITMQ_URL]
  --mongodb-url     MongoDB URL          [env: MONGODB_URL]

Commands:
  bench        Run the KurrentDB write-latency stress test
  mongo-bench  Run the MongoDB write-latency stress test
  produce      Continuously produce events to KurrentDB + RabbitMQ
  ping         Probe KurrentDB + RabbitMQ connectivity and exit
  mongo-ping   Probe MongoDB connectivity and exit

bench options:
  --target-rate    <N>     Target events/second  [default: 10000]
  --duration-secs  <N>     Run duration          [default: 30]
  --concurrency    <N>     Parallel tasks        [default: 64]
  --batch-size     <N>     Events per gRPC call  [default: 1]
  --p99-limit-ms   <N>     Failure threshold ms  [default: 2]
  --json                   Emit results as JSON  (for CI parsing)

mongo-bench options:
  --target-rate    <N>     Target events/second  [default: 10000]
  --duration-secs  <N>     Run duration          [default: 30]
  --concurrency    <N>     Parallel tasks        [default: 64]
  --batch-size     <N>     Docs per insert_many  [default: 1]
  --database       <NAME>  MongoDB database      [default: eventbench]
  --p99-limit-ms   <N>     Failure threshold ms  [default: 2]
  --no-drop                Skip pre-run DB drop
  --json                   Emit results as JSON  (for CI parsing)
```

---

## Troubleshooting

**KurrentDB pods stuck in Pending**
→ Check that `event-store-local` StorageClass provisioner is installed.
→ k3d: StorageClass is available by default.
→ Cloud: replace the provisioner with the cloud-native CSI driver.

**p99 latency exceeds 2 ms**
→ Check Disk I/O Wait panel in Grafana — high iowait means the disk is saturated.
→ Try NVMe-backed nodes or reduce competing workloads.
→ Increase `--concurrency` to amortise individual request latency.
→ On Windows/Podman the benchmark will never pass — see the platform note in Quick Start.

**`eventstore-bench` container exits immediately / benchmark reports 0 events**
→ The container crashed (OOM or internal fault). Because it uses tmpfs, all state is lost on exit.
→ A simple `podman start eventstore-bench` is not enough — tmpfs mounts are not recreated.
→ Recreate it from scratch: `podman rm eventstore-bench && podman compose up -d eventstore-bench`
→ Wait ~15 s for the `IS LEADER... SPARTA!` log line before running the benchmark:
  `podman logs -f eventstore-bench`

**Failover test fails — recovery > 60 s**
→ Check `kubelet` pod eviction timer: `kubectl describe node <node>` — default `node.kubernetes.io/not-ready:NoExecute` tolerance is **5 minutes** for system components.
→ Tune KurrentDB `gossipIntervalMs` and `deadMemberRemovalPeriodSec` via env vars.
→ Ensure `podManagementPolicy: Parallel` is set (already configured).

**RabbitMQ peers not joining the cluster**
→ Verify the headless service DNS resolves inside pods:
  `kubectl exec -it rabbitmq-0 -n event-store -- nslookup rabbitmq-headless`
→ Check that the `rabbitmq-peer-discovery` RBAC role has `endpoints/get` permission.
