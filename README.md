# Event Sourcing Testbed

A performance testbed that compares three event-store backends — **KurrentDB**, **MongoDB**, and **PostgreSQL** — under identical conditions on GitHub Actions.
The Rust benchmark harness drives each backend at 10 000 events/second and measures write latency with an HDR histogram.

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
| KurrentDB       | 23.10.0              | Append-only event log (gRPC/HTTP)     |
| MongoDB         | 7.x                  | Document store — event-store mode     |
| PostgreSQL      | 16                   | Relational DB — versioned CTE inserts |
| RabbitMQ        | 3.13 + management    | Event fan-out (topic exchange)        |
| Rust app        | Tokio async          | Benchmark harness & event producer    |
| Prometheus      | v2.51                | Metrics collection                    |
| Grafana         | 10.4                 | Dashboards                            |
| node-exporter   | v1.7                 | Disk I/O, IOPS, CPU iowait            |

---

## Understanding the benchmark numbers

### What is being measured

The CI workflow runs **7 benchmark scenarios** on every push to `main`.
Each scenario runs one backend in isolation on a fresh ubuntu-22.04 runner (2 vCPU, 7 GB RAM).

| # | Scenario | Backend | Deployment | Storage |
|---|----------|---------|------------|---------|
| 1 | `bench-kurrentdb-memdb` | KurrentDB | Docker (native) | MemDb flag — pure in-memory, zero I/O |
| 2 | `bench-kurrentdb` | KurrentDB | Docker | `--tmpfs` (RAM) + `UNSAFE_DISABLE_FLUSH_TO_DISK=true` |
| 3 | `bench-kurrentdb-k8s` | KurrentDB | k3d (single-node) | `emptyDir Memory` (RAM) + `UNSAFE_DISABLE_FLUSH_TO_DISK=true` |
| 4 | `bench-mongodb` | MongoDB | Docker | `--tmpfs` (RAM) + `j:true` write concern |
| 5 | `bench-mongodb-k8s` | MongoDB | k3d (single-node) | `emptyDir Memory` (RAM) + `j:true` write concern |
| 6 | `bench-postgres` | PostgreSQL | Docker | `--tmpfs` (RAM) + `fsync=off` |
| 7 | `bench-postgres-k8s` | PostgreSQL | k3d (single-node) | `emptyDir Memory` (RAM) + `fsync=off` |

Scenario 1 is the theoretical maximum — KurrentDB with no persistence at all.  
Scenarios 2, 4, 6 (Docker) and 3, 5, 7 (k8s) each run the real database server with
persistence enabled.  The difference between a Docker and its k8s counterpart is purely
deployment overhead: k8s pod networking and the port-forward loopback tunnel.

All 6 "real DB" scenarios share the same storage type (RAM-backed tmpfs) and the same
durability level (OS-buffer, no fsync) so that storage I/O is never a variable — only
protocol and semantic overhead are being measured.  See the durability alignment section
below for why tmpfs is required to keep all three backends at the same level.

### Benchmark internals

**Rate pacing**  
A token-bucket loop fires one insert per slot at the target interval (`1 / rate`).
If all 64 concurrency slots are in-flight when a tick fires, the tick is skipped
(non-blocking `try_acquire_owned`) rather than queued — this prevents artificial
backpressure buildup and keeps the offered rate honest.

**Warm-up phase**  
Before the timed window opens, the harness fires `concurrency + 4` (68) concurrent
pings to pre-heat connection pools and JIT-compiled server code.  Warm-up latency is
discarded; only the 30-second steady-state window is reported.

**HDR histogram**  
Each completed write records its latency (in microseconds) into a
3-significant-digit HDR histogram.  HDR histograms are lossless for the reported
percentiles up to 2^63 µs and do not require buffering individual samples, so
memory usage is constant regardless of event count.

**Concurrency model**  
```
Arc<Semaphore>(max_in_flight = min(concurrency, 96))
│
├─ token-bucket tick fires → try_acquire_owned()
│   ├─ permit acquired → spawn(async { write event, record latency, drop permit })
│   └─ permit unavailable → skip tick (back-pressure signal)
│
└─ after timed window → acquire_many(max_in_flight) to drain all in-flight tasks
```
The semaphore bound prevents unbounded goroutine fan-out when the backend is slower
than the target rate (e.g. first seconds of warm-up).  Draining after the window
ensures every in-flight write is counted before the histogram is printed.

### Durability alignment — why this is a fair comparison

All six "real DB" scenarios are configured at exactly the same **"OS-buffer" durability level**:
data is written to the OS page cache (tmpfs, so effectively RAM), but `fsync()` is
never called.  A crash would lose the last few events.

| Backend | Setting | What it means |
|---------|---------|---------------|
| **KurrentDB** | `UNSAFE_DISABLE_FLUSH_TO_DISK=true` | Events written to tmpfs; `fsync()` skipped |
| **MongoDB** | `j:true` write concern + tmpfs mount | Journal record written to tmpfs before ACK; `fdatasync()` is a no-op on tmpfs |
| **PostgreSQL** | `fsync=off` + `full_page_writes=off` + tmpfs | WAL record written to tmpfs before ACK (`synchronous_commit` default = on); no `fsync()` |

**Why tmpfs is required (not just fsync=off):**  
MongoDB's `j:true` calls `fdatasync()` on the journal file. On a real disk this is an
actual blocking flush — making MongoDB inherently slower than KurrentDB and PostgreSQL
(which both skip fsync entirely). On tmpfs, `fdatasync()` is a kernel no-op since there
is no backing disk to flush to. This is the only way to put all three backends at the
same OS-buffer durability level.

> **Why not `synchronous_commit=off` for PostgreSQL?**  
> That setting acknowledges the commit before the WAL record is written to the OS at all —
> data lives only in PostgreSQL's internal shared-memory buffers.  KurrentDB and MongoDB
> both write to the OS buffer before acknowledging, so `synchronous_commit=off` would give
> PostgreSQL an unfair extra advantage.

> **Why not `j:false` for MongoDB?**  
> `j:false` acknowledges before the journal record reaches the OS (sub-OS-buffer) — the
> same shortcut that `synchronous_commit=off` provides for PostgreSQL.  Using `j:true` on
> tmpfs keeps MongoDB at the correct OS-buffer level.

### What the numbers do *not* tell you

- **Production throughput.** On a real disk with full durability (`fsync=on`,
  `synchronous_commit=on`, `j:true`), PostgreSQL would be 10–100× slower (disk-bound).
  KurrentDB is optimised for sequential append with batched WAL flushing and would retain
  much more of its in-memory performance at production durability levels.
- **Distributed / replicated performance.** All benchmark scenarios run single-node.  
  KurrentDB is designed for quorum-replicated clusters; PostgreSQL streaming replication
  adds significant write-path overhead.  The KurrentDB HA deployment (3-node StatefulSet
  in `k8s/02-kurrentdb/`) is not benchmarked here to keep the comparison fair.
- **Read workloads, subscriptions, or projections.** KurrentDB provides native catch-up
  subscriptions, persistent consumer groups, and server-side projections.  These have no
  equivalent in a raw PostgreSQL table and are not benchmarked here.
- **Concurrent writers to the same stream.** Each task writes to its own dedicated stream,
  so optimistic-concurrency conflicts never occur.  Workloads with high per-stream
  contention would penalise PostgreSQL's row-level locking more than KurrentDB's
  append-only log.

### Reading CI results

After each push to `main` the **"Benchmark Comparison Summary"** job writes a side-by-side
Markdown table to the Actions run summary page (the `:bar_chart:` tab on the run detail
page).  Each Docker benchmark job also echoes its raw JSON line to the step log so
individual numbers can be inspected without digging through histogram output.

### JSON output format

Pass `--json` to any bench command to get a single-line JSON result on stdout
(all log output goes to stderr so it never pollutes the captured line):

```json
{
  "actual_rate_eps": 9959.1,
  "target_rate_eps": 10000,
  "duration_secs":   30,
  "total_events":    298773,
  "p50_us":          689,
  "p75_us":          821,
  "p95_us":          1203,
  "p99_us":          1581,
  "p999_us":         2047,
  "p9999_us":        3071,
  "concurrency":     64,
  "batch_size":      1
}
```

All latency values are in **microseconds**.  The CI `report` job reads `actual_rate_eps`
and the four percentile fields (`p50_us`, `p95_us`, `p99_us`, `p999_us`) to build the
summary table.

---

## CI Pipeline

The GitHub Actions workflow (`.github/workflows/bench.yml`) runs **8 jobs** on every push
to `main`: 7 benchmark jobs + 1 report job.  All use `ubuntu-22.04` (2 vCPU, 7 GB RAM) runners.

### Job summary

| Job | Backend | Deploy | Storage | Durability |
|-----|---------|--------|---------|------------|
| `bench-kurrentdb-memdb` | KurrentDB | Docker (native) | RAM (MemDb flag) | none — no I/O at all |
| `bench-kurrentdb` | KurrentDB | Docker | tmpfs | OS-buffer, no fsync |
| `bench-kurrentdb-k8s` | KurrentDB | k3d single-node | emptyDir Memory | OS-buffer, no fsync |
| `bench-mongodb` | MongoDB | Docker | tmpfs | OS-buffer, no fsync |
| `bench-mongodb-k8s` | MongoDB | k3d single-node | emptyDir Memory | OS-buffer, no fsync |
| `bench-postgres` | PostgreSQL | Docker | tmpfs | OS-buffer, no fsync |
| `bench-postgres-k8s` | PostgreSQL | k3d single-node | emptyDir Memory | OS-buffer, no fsync |
| `report` | — | — | — | Reads all 6 bench outputs; writes two comparison tables to run summary |

### How the Docker benchmark jobs work

Each Docker bench job follows the same pattern:

1. **Build** the Rust binary in release mode (`cargo build --release`).
2. **Start** the backend container with a `--tmpfs` mount and OS-buffer durability flags.
3. **Wait** for the backend to become ready (health-check loop with `--ping`).
4. **Run** the benchmark binary with `--json` and capture stdout into `$RESULT`.
5. **Parse** `$RESULT` with a one-liner Python `json.load` to extract the five fields.
6. **Write** each field to `$GITHUB_OUTPUT` so the `report` job can read them via
   `needs.<job>.outputs.<field>`.

The k8s jobs follow the same parse-and-output pattern after the benchmark step.

### The `report` job

`report` runs with `if: always()` so it executes even if a benchmark fails.
It uses `needs` on all six bench jobs and consumes their `outputs` to build
**two side-by-side comparison tables** written to `$GITHUB_STEP_SUMMARY`:

- **Docker Benchmark Comparison** — scenarios 2, 4, 6 (pure semantic overhead, no deployment overhead)
- **Kubernetes Benchmark Comparison** — scenarios 3, 5, 7 (same durability, adds k8s pod networking + port-forward overhead)

If a job was skipped or failed, its cells show `n/a`.

Example output:

```
## Docker Benchmark Comparison
> Conditions: event-store mode · 10 k ev/s target · 30 s · 64 concurrent tasks
> Durability level: OS-buffer write, no fsync (all three backends equivalent)

| Backend    | Storage / flags                            | Rate (ev/s) | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |
|------------|--------------------------------------------|------------:|:--------:|:--------:|:--------:|:----------:|
| KurrentDB  | tmpfs + UNSAFE_DISABLE_FLUSH_TO_DISK=true  | 9959.1      | 0.69     | 1.20     | 1.58     | 2.05       |
| MongoDB    | tmpfs + j:true write concern               | 9887.3      | 0.71     | 1.32     | 1.69     | 2.30       |
| PostgreSQL | tmpfs + fsync=off + synchronous_commit=on  | 9941.0      | 0.70     | 1.24     | 1.61     | 2.11       |

## Kubernetes Benchmark Comparison
> Conditions: event-store mode · k3d cluster · port-forward tunnel
> All backends: single-node, emptyDir Memory (tmpfs), OS-buffer durability, concurrency 64

| Backend    | Deployment                                        | Rate (ev/s) | p50 (ms) | ...
|------------|---------------------------------------------------|------------:|:--------:|
| KurrentDB  | k3d single-node (emptyDir Memory, UNSAFE_DISABLE) | 8200.0      | 0.85     |
| MongoDB    | k3d single-node (emptyDir Memory, j:true)         | 7900.0      | 0.92     |
| PostgreSQL | k3d single-node (emptyDir Memory, fsync=off)      | 8100.0      | 0.88     |
```

### k3d jobs

All three k8s jobs pin k3d to `TAG=v5.8.3`.  Without a pinned version the
`k3d-install.sh` script calls the GitHub API to resolve "latest", which 502-fails
intermittently on GitHub-hosted runners.

Each k8s benchmark job deploys a **single-node** instance using `emptyDir: {medium: Memory}`
(tmpfs) so the storage type is identical to the Docker jobs.  The KurrentDB k8s job uses
an inline StatefulSet (not the production 3-node PVC manifest in `k8s/02-kurrentdb/`) for
this reason.  The production HA manifests remain unchanged for real deployments.

---

| Tool    | Version | Platform        |
|---------|---------|-----------------|
| kubectl | ≥ 1.26  | K8s deploy only |
| make    | any     | all             |

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

### Option A — Devcontainer (local dev)

All backend services (KurrentDB, MongoDB, PostgreSQL, RabbitMQ) start automatically
when you open the repo in the devcontainer.  No manual container management needed.

1. Open the repo in VS Code and choose **Reopen in Container**.
2. Wait for the `postStartCommand` to finish building the `testbed` binary (~1–2 min on first open).
3. Run all tests with a single command:

```bash
make test-all
```

The env vars `KURRENTDB_URL`, `MONGODB_URL`, `PG_URL`, `RABBITMQ_URL`, and `DIRECT=1`
are pre-set in the devcontainer so test scripts pick up the running services automatically.
Thresholds are automatically scaled to devcontainer-appropriate levels.

You can also invoke the `testbed` binary directly:

```bash
# KurrentDB benchmark
testbed kurrentdb-bench --target-rate 10000 --concurrency 20 --duration-secs 30

# MongoDB benchmark
testbed mongo-bench --target-rate 10000 --concurrency 64 --duration-secs 30

# PostgreSQL benchmark
testbed pg-bench --target-rate 10000 --concurrency 64 --duration-secs 30

# Snapshot demo
testbed kurrentdb-snapshot-demo

# Rehydration test (all backends)
bash tests/06-rehydration-replay-test.sh
```

> **Note:** Tests 01, 03, and 04 require a Kubernetes cluster and are automatically
> skipped in the devcontainer with a clear `SKIPPED` message.

### Option B — Kubernetes

```bash
# 1. Build and push the benchmark image
docker build -t myregistry.io/event-sourcing-testbed:latest rust-app/
docker push myregistry.io/event-sourcing-testbed:latest

# 2. Deploy all manifests in order
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-storageclass.yaml
kubectl apply -f k8s/02-kurrentdb/
kubectl apply -f k8s/03-rabbitmq/
kubectl apply -f k8s/04-monitoring/
kubectl rollout status statefulset/kurrentdb -n event-store --timeout=180s
kubectl rollout status statefulset/rabbitmq   -n event-store --timeout=180s

# 3. Run the K8s test suites
bash tests/01-validate-storage.sh
TESTBED_IMAGE=myregistry.io/event-sourcing-testbed:latest bash tests/02-stress-test.sh
bash tests/03-failover-test.sh
bash tests/04-monitoring-check.sh

# Tear down
kubectl delete namespace event-store --ignore-not-found
```

---

## File Layout

``` fs
event-sourcing-testbed/
├── docker-compose.yml               # Full local stack (3-node cluster + monitoring)
├── .devcontainer/
│   ├── devcontainer.json            # VS Code devcontainer config
│   └── docker-compose.yml          # All backend services for local dev
├── docker/
│   ├── prometheus.yml               # Prometheus scrape config (compose)
│   └── grafana/provisioning/        # Grafana datasource + dashboard provider
│
├── k8s/
│   ├── 00-namespace.yaml            # Namespace: event-store
│   ├── 01-storageclass.yaml         # StorageClass (WaitForFirstConsumer)
│   ├── 02-kurrentdb/
│   │   ├── 01-services.yaml         # Headless + ClusterIP services
│   │   └── 02-statefulset.yaml      # 3-replica StatefulSet + PVC templates
│   ├── 03-rabbitmq/
│   │   ├── 01-services.yaml
│   │   └── 02-statefulset.yaml      # RBAC + ConfigMap + StatefulSet
│   ├── 04-monitoring/
│   │   ├── 01-node-exporter.yaml    # DaemonSet for Disk I/O metrics
│   │   ├── 02-prometheus-rbac.yaml  # ServiceAccount + ClusterRole
│   │   ├── 03-prometheus-config.yaml# Scrape targets (ES, RMQ, node-exporter)
│   │   ├── 04-prometheus.yaml       # Deployment + Service
│   │   ├── 05-grafana-datasource.yaml
│   │   ├── 06-grafana-dashboard.yaml# Full dashboard JSON (ConfigMap)
│   │   └── 07-grafana.yaml          # Deployment + Service
│   ├── 05-mongodb/
│   │   ├── 01-services.yaml
│   │   └── 02-statefulset.yaml      # emptyDir (Memory) + replica-set init
│   └── 06-postgres/
│       ├── 01-services.yaml
│       └── 02-statefulset.yaml      # emptyDir (Memory) + fsync=off
│
├── rust-app/
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/
│       ├── main.rs                  # CLI entry point
│       ├── events.rs                # Domain events + benchmark payload
│       ├── kurrentdb/
│       │   ├── client.rs            # KurrentDB gRPC wrapper
│       │   ├── benchmark.rs         # HDR-histogram stress test
│       │   └── snapshot_demo.rs     # Snapshot + rehydration demo
│       ├── mongodb/
│       │   ├── client.rs            # MongoDB driver wrapper + write concern
│       │   ├── benchmark.rs         # HDR-histogram stress test
│       │   └── event_store.rs       # Event-store demo (8 properties)
│       ├── postgres/
│       │   ├── client.rs            # sqlx PgPool wrapper (test_before_acquire=false)
│       │   ├── benchmark.rs         # HDR-histogram stress test
│       │   └── event_store.rs       # Event-store demo (8 properties)
│       └── rabbitmq_client.rs       # AMQP producer (lapin)
│
└── tests/
    ├── 01-validate-storage.sh       # StorageClass + WaitForFirstConsumer (K8s only)
    ├── 02-stress-test.sh            # KurrentDB throughput benchmark
    ├── 03-failover-test.sh          # Node failure, recovery < 60 s (K8s only)
    ├── 04-monitoring-check.sh       # Prometheus targets + Grafana dashboard (K8s only)
    ├── 05-mongodb-stress-test.sh    # MongoDB throughput benchmark
    ├── 06-rehydration-replay-test.sh# Event rehydration/replay (KurrentDB, MongoDB, PG)
    └── 07-postgres-stress-test.sh   # PostgreSQL throughput benchmark
```

---

## Tests

Run all tests with:

```bash
make test-all
```

In the devcontainer (`DIRECT=1`), thresholds are automatically scaled to what a shared
dev VM can sustain. Tests requiring Kubernetes (01, 03, 04) are skipped with a clear
`SKIPPED` message. In K8s CI mode (`DIRECT=0`), full production SLAs apply.

| # | Test | Devcontainer | K8s CI |
|---|------|-------------|--------|
| 01 | Storage Class validation | SKIPPED (no K8s) | runs |
| 02 | KurrentDB throughput benchmark | PASS | PASS |
| 03 | Automated failover | SKIPPED (no K8s) | runs |
| 04 | Monitoring (Prometheus + Grafana) | SKIPPED (no K8s) | runs |
| 05 | MongoDB throughput benchmark | PASS | PASS |
| 06 | Event rehydration/replay | PASS (MongoDB needs rebuild) | PASS |
| 07 | PostgreSQL throughput benchmark | PASS | PASS |

---

### Test 01 — Storage Class Validation

Proves that `volumeBindingMode: WaitForFirstConsumer` is active and that volumes
are created on the node where the Pod lands (data locality).

```bash
bash tests/01-validate-storage.sh
```

> **Devcontainer:** skipped automatically when `kubectl` is not found.

**Pass criteria:**

- StorageClass exists with `volumeBindingMode: WaitForFirstConsumer`
- PVC stays in `Pending` state until a Pod is scheduled
- PVC binds to a PV on the same node as the consumer Pod

---

### Test 05 — MongoDB Write-Latency Stress Test

Inserts events into MongoDB at a target rate for 30 seconds using 64 concurrent Tokio tasks.
The database is **dropped before each run** so leftover data cannot inflate index lookup times.

```bash
# Devcontainer (DIRECT=1 already set, thresholds auto-scaled):
make test-mongodb

# Binary directly:
testbed \
  --mongodb-url "$MONGODB_URL" \
  mongo-bench \
  --target-rate 10000 \
  --concurrency 64 \
  --duration-secs 30

# Relax the p99 threshold on slower machines:
P99_LIMIT_MS=20 DIRECT=1 bash tests/05-mongodb-stress-test.sh
```

**Pass criteria (K8s):**

- Actual rate ≥ 9 000 ev/s
- **p99 insert latency < p99-limit-ms** (default 2 ms)

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

> **Isolation:** Do **not** run `mongo-bench` concurrently with `kurrentdb-bench` on the
> same machine; both saturate host I/O and will inflate each other's latency numbers.

---

### Test 06 — Event Rehydration / Replay

Writes 50 000 events to each backend (KurrentDB, MongoDB, PostgreSQL) and then
replays them in full, verifying event count, ordering, and stream revision consistency.

```bash
# Devcontainer — runs all three backends:
bash tests/06-rehydration-replay-test.sh

# Skip individual backends:
SKIP_MONGO=1 bash tests/06-rehydration-replay-test.sh
SKIP_PG=1    bash tests/06-rehydration-replay-test.sh
```

**Pass criteria (per backend):**

- `events_written` == `events_replayed` == 50 000
- `revisions_ok` == `true` (stream version sequence is gapless)
- `passed` == `true`

> **MongoDB note:** Requires a MongoDB replica set (for multi-document transactions).
> In the devcontainer this is configured in `.devcontainer/docker-compose.yml` but
> takes effect only after a **Rebuild Container** (`Ctrl+Shift+P → Rebuild Container`).
> Until then, the MongoDB section is skipped with a warning.

---

### Test 02 — KurrentDB Performance Benchmark

Appends events to KurrentDB at a target rate for 30 seconds using concurrent Tokio tasks.
Latency is measured with a 3-significant-digit HDR histogram.

```bash
# Devcontainer (DIRECT=1 already set, thresholds auto-scaled):
make test-bench

# K8s Job mode:
TESTBED_IMAGE=myregistry.io/event-sourcing-testbed:latest bash tests/02-stress-test.sh

# Binary directly:
testbed kurrentdb-bench --target-rate 10000 --concurrency 50 --duration-secs 30 --json
```

**Pass criteria (K8s):**

- Actual rate ≥ 9 000 ev/s (within 10% of 10 000)
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
bash tests/03-failover-test.sh
```

> **Devcontainer:** skipped automatically when `kubectl` is not found.

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
bash tests/04-monitoring-check.sh
```

> **Devcontainer:** skipped automatically when `kubectl` is not found.

**Checked metrics:**

| Metric                              | Meaning                     |
|-------------------------------------|-----------------------------|
| `node_cpu_seconds_total{mode="iowait"}` | Disk I/O Wait %          |
| `node_disk_reads_completed_total`   | Read IOPS                   |
| `node_disk_writes_completed_total`  | Write IOPS                  |
| `up{job="kurrentdb"}`              | KurrentDB cluster health |
| `up{job="rabbitmq"}`                | RabbitMQ health             |

**Pass criteria:**

- Prometheus has active scrape targets for `node-exporter`, `kurrentdb`, `rabbitmq`
- All five metric families return data
- Grafana API returns the "Event Store Namespace" dashboard

---

### Test 07 — PostgreSQL Write-Latency Stress Test

Inserts events into PostgreSQL at a target rate for 30 seconds using 64 concurrent Tokio
tasks (`sqlx` + `PgPool`, `max_connections=128`, `test_before_acquire=false`).

```bash
# Devcontainer (DIRECT=1 already set, thresholds auto-scaled):
make test-postgres

# Binary directly:
testbed \
  --postgres-url "$POSTGRES_URL" \
  pg-bench \
  --target-rate 10000 \
  --concurrency 64 \
  --duration-secs 30 \
  --json

# Relax the p99 threshold on slower machines:
P99_LIMIT_MS=20 DIRECT=1 bash tests/07-postgres-stress-test.sh
```

**Pass criteria (K8s):**

- Actual rate ≥ 9 000 ev/s
- **p99 insert latency < p99-limit-ms** (default 2 ms)

> **`test_before_acquire=false`:** The sqlx 0.8 default is `true`, which pings every idle
> connection before checkout. At 64 concurrent tasks this doubles the effective query rate
> (10k pings + 10k inserts = 20k QPS), saturating PostgreSQL's `max_connections` and causing
> `PoolTimedOut` errors. With the flag disabled, connections are reused without re-validation.

---

### Test 12 — Rate Ramp Test (Knee Point)

Runs fixed target-rate steps and prints where p99 starts exploding.

Default ramp steps:

- **1 000 ev/s**
- **3 000 ev/s**
- **5 000 ev/s**
- **8 000 ev/s**
- **10 000 ev/s**

```bash
# KurrentDB (default backend):
make test-rate-ramp BACKEND=kurrentdb

# MongoDB with event-store-mode enabled:
make test-rate-ramp BACKEND=mongodb EVENT_STORE_MODE=1

# PostgreSQL with event-store-mode enabled:
make test-rate-ramp BACKEND=postgres EVENT_STORE_MODE=1

# Custom ramp:
make test-rate-ramp BACKEND=postgres RATE_STEPS="1000 2000 4000 6000 8000 10000"
```

Output includes a table per step (`target`, `actual_rate`, `p99_us`) and a summary JSON line with:

- `knee_detected`
- `knee_rate_eps`
- `knee_p99_us`
- `knee_jump`

The knee detector marks the first step where p99 jumps by at least `KNEE_FACTOR` (default `1.8x`)
and p99 is at least `MIN_KNEE_P99_US` (default `2000`).

---

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
kubectl port-forward svc/grafana -n event-store 3000:3000
open http://localhost:3000   # admin / admin
```

---

## Rust App Reference

``` shell
testbed [OPTIONS] <COMMAND>

Options:
  --kurrentdb-url  KurrentDB gRPC URL   [env: KURRENTDB_URL]
  --rabbitmq-url   AMQP URL             [env: RABBITMQ_URL]
  --mongodb-url    MongoDB URL          [env: MONGODB_URL]
  --postgres-url   PostgreSQL URL       [env: POSTGRES_URL]

Commands:
  kurrentdb-bench         Run the KurrentDB write-latency stress test
  mongo-bench             Run the MongoDB write-latency stress test
  pg-bench                Run the PostgreSQL write-latency stress test
  produce                 Continuously produce events to KurrentDB + RabbitMQ
  ping                    Probe KurrentDB + RabbitMQ connectivity and exit
  mongo-ping              Probe MongoDB connectivity and exit
  pg-ping                 Probe PostgreSQL connectivity and exit
  mongo-event-store-demo  Demonstrate 8 event-sourcing properties (MongoDB)
  pg-event-store-demo          Demonstrate 8 event-sourcing properties (PostgreSQL)
  kurrentdb-snapshot-demo      Write 1000 events with snapshots, then rehydrate
```

### `kurrentdb-snapshot-demo` flags

| Flag | Default | Description |
|------|---------|-------------|
| `--events` | 1000 | Total domain events to append (3 types round-robin: `ItemAdded`, `ItemPriceUpdated`, `ItemRemoved`) |
| `--snapshot-every` | 55 | Append an `InventorySnapshot` to the snapshot stream after every N events |

The demo:
1. **Writes** `--events` domain events to stream `snapshot-demo-{uuid}`, taking a snapshot to `snapshot-demo-{uuid}-snapshots` after every `--snapshot-every` events.
2. **Drops** the in-memory state to simulate a process restart.
3. **Rehydrates** by loading the latest snapshot then replaying only the trailing events that followed it.
4. **Verifies** the restored state matches a full cold replay from the beginning of the stream.

With the defaults (1000 events, snapshot every 55) this produces **18 snapshots**, leaving **10 trailing events** to replay after the last snapshot.

```bash
# Inside the devcontainer — KurrentDB is already running:
testbed kurrentdb-snapshot-demo

# Custom options:
testbed kurrentdb-snapshot-demo --events 2000 --snapshot-every 100
```

---

Common bench flags (all three bench commands):

| Flag | Default | Description |
|------|---------|-------------|
| `--target-rate` | 10000 | Target events/second |
| `--concurrency` | 64 | Parallel insert tasks |
| `--batch-size` | 1 | Events per call/insert |
| `--duration-secs` | 30 | Run duration |
| `--event-store-mode` | off | Enable versioned inserts + unique constraint |
| `--json` | off | Emit results as a single JSON line (for CI parsing) |

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

**Failover test fails — recovery > 60 s**
→ Check `kubelet` pod eviction timer: `kubectl describe node <node>` — default `node.kubernetes.io/not-ready:NoExecute` tolerance is **5 minutes** for system components.
→ Tune KurrentDB `gossipIntervalMs` and `deadMemberRemovalPeriodSec` via env vars.
→ Ensure `podManagementPolicy: Parallel` is set (already configured).

**RabbitMQ peers not joining the cluster**
→ Verify the headless service DNS resolves inside pods:
  `kubectl exec -it rabbitmq-0 -n event-store -- nslookup rabbitmq-headless`
→ Check that the `rabbitmq-peer-discovery` RBAC role has `endpoints/get` permission.

**PostgreSQL pool timeouts (`PoolTimedOut` / `sorry, too many clients`)**
→ Root cause: sqlx 0.8 defaults `test_before_acquire` to `true`, pinging every idle
  connection on checkout.  At 64 concurrent tasks this doubles effective QPS and
  exhausts `max_connections`.
→ Fix already applied: `PgPoolOptions::new().test_before_acquire(false)`.
→ If you see it again after a sqlx upgrade, check the changelog for default changes.
→ PostgreSQL is started with `-c max_connections=200` — if you change `--concurrency`
  above 96, also raise `max_connections` in `.devcontainer/docker-compose.yml`.

**k3d install fails with HTTP 502**
→ The `k3d-install.sh` script calls the GitHub API to resolve the "latest" tag.
  GitHub-hosted runners intermittently 502 on this API under load.
→ Fix already applied: all k8s jobs pin `TAG=v5.8.3` to bypass the API call entirely:
  `wget -q -O - https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.8.3 bash`
→ To upgrade k3d, change the tag value in all three k8s jobs simultaneously.

**Benchmark reports 0 events / exits before 30 s**
→ The backend was not ready when the benchmark started.  The readiness wait loop
  timed out.  Increase the sleep/retry count in the `--ping` loop or add a longer
  `sleep` before the bench command.
→ For KurrentDB: wait for `IS LEADER... SPARTA!` in container logs before benchmarking.

**`cargo build` fails with `aws-lc-sys` / `cmake` errors**
→ Build inside the devcontainer — the toolchain is pre-installed.
→ Open the repo with **Reopen in Container** in VS Code.
