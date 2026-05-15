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

Every job runs one backend in isolation on a **fresh ubuntu-22.04 GitHub Actions runner
(2 vCPU, 7 GB RAM)**.  The Rust harness fires 10 000 events/second for 30 seconds using
64 concurrent Tokio tasks, each writing to its own dedicated stream.  Latency is the
wall-clock time from *before* `pool.acquire()` / gRPC call to *after* the server
acknowledgement — it includes all protocol and semantic overhead.

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

All three backends are configured at exactly the same **"OS-buffer" durability level**:
data is written to the OS page cache (tmpfs in CI, so effectively RAM), but `fsync()` is
never called.  A crash would lose the last few events.  This level is chosen because it is
the lowest common denominator that all three backends support:

| Backend | Setting | What it means |
|---------|---------|---------------|
| **KurrentDB** | `UNSAFE_DISABLE_FLUSH_TO_DISK=true` | Events written to tmpfs; `fsync()` skipped |
| **MongoDB** | `j:true` write concern + `--tmpfs /data/db` | Journal record written to tmpfs before ACK; no `fsync()` |
| **PostgreSQL** | `fsync=off` + `full_page_writes=off` + tmpfs | WAL record written to tmpfs before ACK (`synchronous_commit` default = on); no `fsync()` |

> **Why not `synchronous_commit=off` for PostgreSQL?**  
> That setting acknowledges the commit before the WAL record is written to the OS at all —
> data lives only in PostgreSQL's internal shared-memory buffers.  KurrentDB and MongoDB
> both write to the OS buffer before acknowledging, so `synchronous_commit=off` would give
> PostgreSQL an unfair extra advantage.

> **Why not `j:false` for MongoDB?**  
> `j:false` acknowledges before the journal record reaches the OS — the same
> sub-OS-buffer shortcut that `synchronous_commit=off` provides.  Using `j:true` on tmpfs
> keeps MongoDB at the same level as the other two backends.

### What the numbers do *not* tell you

- **Production throughput.** On a real disk with full durability (`fsync=on`,
  `synchronous_commit=on`, `j:true`), PostgreSQL would be 10–100× slower (disk-bound).
  KurrentDB is optimised for sequential append with batched WAL flushing and would retain
  much more of its in-memory performance at production durability levels.
- **Distributed / replicated performance.** All jobs run single-node.  KurrentDB is
  designed for quorum-replicated clusters; PostgreSQL streaming replication adds
  significant write-path overhead.
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
to `main`.  All jobs use `ubuntu-22.04` (2 vCPU, 7 GB RAM) runners.

### Job summary

| Job | Backend | Mode | Storage | Notes |
|-----|---------|------|---------|-------|
| `bench-kurrentdb-memdb` | KurrentDB | in-memory | tmpfs | `MemDb` flag; no persistence layer |
| `bench-kurrentdb` | KurrentDB | Docker | tmpfs + `UNSAFE_DISABLE_FLUSH_TO_DISK=true` | 64-way concurrency; captures `--json` output |
| `bench-kurrentdb-k8s` | KurrentDB | k3d | emptyDir (Memory) | k3d pinned to `v5.8.3` |
| `bench-mongodb` | MongoDB | Docker | `--tmpfs /data/db` + `j:true` | captures `--json` output |
| `bench-mongodb-k8s` | MongoDB | k3d | emptyDir (Memory) | k3d pinned to `v5.8.3` |
| `bench-postgres` | PostgreSQL | Docker | `--tmpfs /var/lib/postgresql/data` + `fsync=off` | `max_connections=200`; captures `--json` output |
| `bench-postgres-k8s` | PostgreSQL | k3d | emptyDir (Memory) + `fsync=off` | k3d pinned to `v5.8.3` |
| `report` | — | — | — | Reads outputs from the three Docker jobs; writes comparison table to run summary |

### How the Docker benchmark jobs work

Each Docker bench job follows the same pattern:

1. **Build** the Rust binary in release mode (`cargo build --release`).
2. **Start** the backend container with tmpfs storage and durability flags.
3. **Wait** for the backend to become ready (health-check loop with `--ping`).
4. **Run** the benchmark binary with `--json` and capture stdout into `$RESULT`.
5. **Parse** `$RESULT` with a one-liner Python `json.load` to extract the five fields.
6. **Write** each field to `$GITHUB_OUTPUT` so the `report` job can read them via
   `needs.<job>.outputs.<field>`.

### The `report` job

`report` runs with `if: always()` so it executes even if one benchmark fails.
It uses `needs: [bench-kurrentdb, bench-mongodb, bench-postgres]` to consume outputs.
A short Python script assembles a Markdown table and writes it to `$GITHUB_STEP_SUMMARY`:

```
## Benchmark Comparison

| Backend     | Durability              | Rate (ev/s) | p50 (µs) | p95 (µs) | p99 (µs) | p99.9 (µs) |
|-------------|-------------------------|-------------|----------|----------|----------|------------|
| KurrentDB   | OS-buffer, no fsync     | 9 959       | 689      | 1 203    | 1 581    | 2 047      |
| MongoDB     | OS-buffer, no fsync     | 9 887       | 712      | 1 318    | 1 694    | 2 303      |
| PostgreSQL  | OS-buffer, no fsync     | 9 941       | 701      | 1 241    | 1 612    | 2 111      |
```

### k3d jobs

k3d is pinned to `TAG=v5.8.3` in all three k8s jobs.  Without a pinned version the
`k3d-install.sh` script calls the GitHub API to resolve "latest", which 502-fails
intermittently on GitHub-hosted runners.

---

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

# Run the PostgreSQL benchmark
make pg-bench-local

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

# Run the PostgreSQL benchmark
make pg-bench-local

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
│       │   └── benchmark.rs         # HDR-histogram stress test
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

### Test 06 — PostgreSQL Write-Latency Stress Test

Inserts events into a PostgreSQL 16 instance at **10 000 events/second** for 30 seconds
using 64 concurrent Tokio tasks (`sqlx` + `PgPool`, `max_connections=128`,
`test_before_acquire=false`).

> **`test_before_acquire=false`** is critical.  The sqlx 0.8 default is `true`, which
> pings every idle connection before checkout.  At 64 concurrent tasks this doubles
> the effective query rate (10k pings + 10k inserts = 20k QPS), saturating PostgreSQL's
> `max_connections` limit and causing `PoolTimedOut` errors.  With the flag disabled the
> warm-up pre-heats connections once and they are reused without re-validation.

```bash
# Start PostgreSQL locally first:
docker compose up -d postgres
# or with Podman:
podman compose up -d postgres

# Run via the testbed binary:
rust-app/target/release/testbed \
  --postgres-url "postgres://postgres:postgres@localhost:5432/eventbench" \
  pg-bench \
  --target-rate 10000 \
  --concurrency 64 \
  --duration-secs 30 \
  --p99-limit-ms 5

# Emit JSON (for scripting):
rust-app/target/release/testbed \
  --postgres-url "postgres://postgres:postgres@localhost:5432/eventbench" \
  pg-bench --json
```

**Pass criteria:**

- Actual rate ≥ 9 000 ev/s
- **p99 insert latency < p99-limit-ms** (default 2 ms in CI; 5 ms recommended locally)

**Schema used by the benchmark:**

```sql
CREATE TABLE IF NOT EXISTS events (
    stream_id   TEXT        NOT NULL,
    version     BIGINT      NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_id, version)
);
```

Each task owns one `stream_id` and increments `version` monotonically.  The
`PRIMARY KEY (stream_id, version)` constraint enforces optimistic-concurrency
guarantees identical to KurrentDB's stream version check.  In `--event-store-mode`
the insert is wrapped in a versioned CTE that rejects out-of-order writes atomically.

---

### Test 02 — Performance Benchmark (I/O Stress Test)

Appends events to KurrentDB at **10 000 events/second** for 30 seconds
using 64 concurrent Tokio tasks across separate streams.
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
| `up{job="kurrentdb"}`              | KurrentDB cluster health |
| `up{job="rabbitmq"}`                | RabbitMQ health             |

**Pass criteria:**

- Prometheus has active scrape targets for `node-exporter`, `kurrentdb`, `rabbitmq`
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
  pg-event-store-demo     Demonstrate 8 event-sourcing properties (PostgreSQL)
```

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
→ On Windows/Podman the benchmark will never pass — see the platform note in Quick Start.

**`kurrentdb-bench` container exits immediately / benchmark reports 0 events**
→ The container crashed (OOM or internal fault). Because it uses tmpfs, all state is lost on exit.
→ A simple `podman start kurrentdb-bench` is not enough — tmpfs mounts are not recreated.
→ Recreate it from scratch: `podman rm kurrentdb-bench && podman compose up -d kurrentdb-bench`
→ Wait ~15 s for the `IS LEADER... SPARTA!` log line before running the benchmark:
  `podman logs -f kurrentdb-bench`

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
→ PostgreSQL Docker container is started with `-c max_connections=200` — if you
  change `--concurrency` above 96, also raise `max_connections` accordingly.

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

**`cargo build` fails with `aws-lc-sys` / `cmake` errors on Windows**
→ The Rust app links against `aws-lc-rs` (via `rustls`).  On Windows this requires
  cmake and NASM.  Install via `winget install Kitware.CMake NASM.NASM`.
→ Alternatively, build inside the Dockerfile (Linux) where the build environment is
  already set up: `make build` uses `docker buildx` / `podman build`.
