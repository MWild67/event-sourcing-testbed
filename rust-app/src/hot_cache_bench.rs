//! Hot-tail-cache benchmark.
//!
//! Tests the pattern where the **latest `cache_size` events** are always held
//! in an in-memory ring buffer so callers never have to query the database for
//! recent events.  The benchmark runs four phases for each backend:
//!
//! 1. **Seed** — write `seed_events` (default 50 000) events in batches of 100.
//! 2. **Startup** — load the last `cache_size` (default 500) events from the DB
//!    into the cache in a **single query**; measure that startup latency.
//! 3. **Cache reads** — snapshot the in-memory cache 1 000 times and record
//!    per-snapshot latency in nanoseconds.
//! 4. **Live writes** — append `live_writes` (default 500) further events one
//!    at a time, pushing each into the cache immediately after the DB confirms
//!    the write.  Measures DB-write latency, cache-push latency, and cache
//!    snapshot latency during concurrent activity.

use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::Instant,
};

use anyhow::Result;
use hdrhistogram::Histogram;
use tracing::info;

use crate::events::BenchmarkEvent;

// ─── In-memory ring buffer ───────────────────────────────────────────────────

/// Thread-safe ring buffer that retains the most-recent `capacity` events.
///
/// After the startup phase pre-populates this from the DB, all subsequent
/// reads are served from memory with no database involvement.
pub struct RecentEventsCache {
    inner: RwLock<VecDeque<BenchmarkEvent>>,
    capacity: usize,
}

impl RecentEventsCache {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(VecDeque::with_capacity(capacity + 1)),
            capacity,
        })
    }

    /// Append a new event, evicting the oldest when the buffer is full.
    pub fn push(&self, event: BenchmarkEvent) {
        let mut guard = self.inner.write().unwrap();
        if guard.len() == self.capacity {
            guard.pop_front();
        }
        guard.push_back(event);
    }

    /// Clone all cached events as a `Vec` (oldest → newest order).
    pub fn snapshot(&self) -> Vec<BenchmarkEvent> {
        self.inner.read().unwrap().iter().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }
}

// ─── Configuration ────────────────────────────────────────────────────────────

pub struct HotCacheConfig {
    /// Total events to write during the seed phase (default 50 000).
    pub seed_events: u64,
    /// Number of events held in the in-memory cache (default 500).
    pub cache_size: usize,
    /// Additional events written during the live-write phase (default 500).
    pub live_writes: u64,
    /// Batch size for the seed phase (default 100).
    pub seed_batch_size: u64,
    /// MongoDB database name (ignored for KurrentDB / PostgreSQL).
    pub database: String,
    /// Stream / collection / table-suffix name.
    pub stream_name: String,
    /// Emit results as a single JSON line instead of a formatted report.
    pub json: bool,
}

impl Default for HotCacheConfig {
    fn default() -> Self {
        Self {
            seed_events: 50_000,
            cache_size: 500,
            live_writes: 500,
            seed_batch_size: 100,
            database: "hotcache".to_string(),
            stream_name: "hot-cache".to_string(),
            json: false,
        }
    }
}

// ─── Result ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct HotCacheResult {
    pub backend: &'static str,
    /// Events written during the seed phase.
    pub seed_events: u64,
    /// Wall-clock time for the seed phase (ms).
    pub seed_elapsed_ms: f64,
    /// Throughput during the seed phase (events/s).
    pub seed_rate_eps: f64,
    /// Time to load the last `cache_size` events from the DB at startup (µs).
    pub startup_load_us: u64,
    /// Events present in the cache immediately after startup (no extra query).
    pub cache_size_after_startup: usize,
    /// In-memory cache snapshot p50 latency (ns).
    pub cache_read_p50_ns: u64,
    /// In-memory cache snapshot p99 latency (ns).
    pub cache_read_p99_ns: u64,
    /// DB write p50 latency during the live-write phase (µs).
    pub db_write_p50_us: u64,
    /// DB write p95 latency during the live-write phase (µs).
    pub db_write_p95_us: u64,
    /// DB write p99 latency during the live-write phase (µs).
    pub db_write_p99_us: u64,
    /// Cache push p50 latency during the live-write phase (ns).
    pub cache_push_p50_ns: u64,
    /// Cache push p99 latency during the live-write phase (ns).
    pub cache_push_p99_ns: u64,
    /// Number of events written during the live-write phase.
    pub live_writes: u64,
}

impl HotCacheResult {
    #[allow(clippy::cast_precision_loss)]
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════════════════════");
        println!("  Hot-Tail-Cache Benchmark — {}", self.backend);
        println!(
            "  ({} events in stream, cache size {})",
            self.seed_events, self.cache_size_after_startup
        );
        println!("══════════════════════════════════════════════════════════════");
        println!("  STARTUP  — one query → cache ready, no further DB queries");
        println!("  ──────────────────────────────────────────────────────────");
        println!(
            "  Load time      : {} µs  ({:.2} ms)",
            self.startup_load_us,
            self.startup_load_us as f64 / 1_000.0
        );
        println!("  Events cached  : {}", self.cache_size_after_startup);
        println!();
        println!(
            "  CACHE READS  — 1 000 × snapshot {} events, zero DB queries",
            self.cache_size_after_startup
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!("  p50            : {} ns", self.cache_read_p50_ns);
        println!("  p99            : {} ns", self.cache_read_p99_ns);
        println!();
        println!(
            "  LIVE WRITE PHASE  ({} events, 1 at a time)",
            self.live_writes
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!(
            "  DB write p50   : {} µs  ({:.2} ms)",
            self.db_write_p50_us,
            self.db_write_p50_us as f64 / 1_000.0
        );
        println!(
            "  DB write p95   : {} µs  ({:.2} ms)",
            self.db_write_p95_us,
            self.db_write_p95_us as f64 / 1_000.0
        );
        println!(
            "  DB write p99   : {} µs  ({:.2} ms)",
            self.db_write_p99_us,
            self.db_write_p99_us as f64 / 1_000.0
        );
        println!("  Cache push p50 : {} ns", self.cache_push_p50_ns);
        println!("  Cache push p99 : {} ns", self.cache_push_p99_ns);
        println!("══════════════════════════════════════════════════════════════");
        println!(
            "  (seed: {} events written in {:.0} ms to establish the stream)",
            self.seed_events, self.seed_elapsed_ms
        );
        println!();
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn print_json(&self) {
        println!(
            r#"{{"backend":"{backend}","seed_events":{seed},"seed_elapsed_ms":{seed_ms:.1},"seed_rate_eps":{seed_rate:.0},"startup_load_us":{startup},"cache_size_after_startup":{cache_sz},"cache_read_p50_ns":{cr_p50},"cache_read_p99_ns":{cr_p99},"db_write_p50_us":{dw_p50},"db_write_p95_us":{dw_p95},"db_write_p99_us":{dw_p99},"cache_push_p50_ns":{cp_p50},"cache_push_p99_ns":{cp_p99},"live_writes":{lw}}}"#,
            backend = self.backend,
            seed = self.seed_events,
            seed_ms = self.seed_elapsed_ms,
            seed_rate = self.seed_rate_eps,
            startup = self.startup_load_us,
            cache_sz = self.cache_size_after_startup,
            cr_p50 = self.cache_read_p50_ns,
            cr_p99 = self.cache_read_p99_ns,
            dw_p50 = self.db_write_p50_us,
            dw_p95 = self.db_write_p95_us,
            dw_p99 = self.db_write_p99_us,
            cp_p50 = self.cache_push_p50_ns,
            cp_p99 = self.cache_push_p99_ns,
            lw = self.live_writes,
        );
    }
}

// ─── Shared histogram helpers ─────────────────────────────────────────────────

fn ns_histogram() -> Histogram<u64> {
    // Range: 1 ns … 30 s expressed in nanoseconds; 3 significant figures.
    Histogram::<u64>::new_with_bounds(1, 30_000_000_000, 3).unwrap()
}

fn us_histogram() -> Histogram<u64> {
    // Range: 1 µs … 30 s expressed in microseconds; 3 significant figures.
    Histogram::<u64>::new_with_bounds(1, 30_000_000, 3).unwrap()
}

// ─── KurrentDB runner ────────────────────────────────────────────────────────

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub async fn run_kurrentdb(kurrentdb_url: &str, config: HotCacheConfig) -> Result<HotCacheResult> {
    use crate::kurrentdb::client::KurrentClient;

    info!("hot-cache/kurrentdb: connecting …");
    let client = KurrentClient::connect(kurrentdb_url)?;

    // Wait for KurrentDB to be ready.
    for attempt in 1..=30 {
        match client.ping().await {
            Ok(()) => break,
            Err(e) => {
                if attempt == 30 {
                    anyhow::bail!("KurrentDB did not become ready within 30 s: {e}");
                }
                tracing::warn!(attempt, error = %e, "waiting for KurrentDB …");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    let stream_name = &config.stream_name;

    // ── Phase 1: Seed ─────────────────────────────────────────────────────────
    info!(
        seed_events = config.seed_events,
        batch_size = config.seed_batch_size,
        "hot-cache/kurrentdb: seeding …"
    );
    let seed_start = Instant::now();
    let batches = config.seed_events / config.seed_batch_size;
    for b in 0..batches {
        let batch: Vec<BenchmarkEvent> = (0..config.seed_batch_size)
            .map(|i| BenchmarkEvent::new(b * config.seed_batch_size + i, 0))
            .collect();
        client
            .append_batch(stream_name, "BenchmarkEvent", &batch)
            .await?;
    }
    // Any remainder events.
    let remainder = config.seed_events % config.seed_batch_size;
    if remainder > 0 {
        let base = batches * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> = (0..remainder)
            .map(|i| BenchmarkEvent::new(base + i, 0))
            .collect();
        client
            .append_batch(stream_name, "BenchmarkEvent", &batch)
            .await?;
    }
    let seed_elapsed = seed_start.elapsed();
    let seed_elapsed_ms = seed_elapsed.as_secs_f64() * 1_000.0;
    let seed_rate_eps = config.seed_events as f64 / seed_elapsed.as_secs_f64();
    info!(
        elapsed_ms = seed_elapsed_ms as u64,
        rate_eps = seed_rate_eps as u64,
        "hot-cache/kurrentdb: seeding done"
    );

    // ── Phase 2: Startup load ─────────────────────────────────────────────────
    info!(
        cache_size = config.cache_size,
        "hot-cache/kurrentdb: loading last {} events at startup …", config.cache_size
    );
    let startup_start = Instant::now();
    let initial_events = client
        .read_last_n_bench_events(stream_name, config.cache_size)
        .await?;
    let startup_load_us = startup_start.elapsed().as_micros() as u64;

    let cache = RecentEventsCache::new(config.cache_size);
    for ev in initial_events {
        cache.push(ev);
    }
    let cache_size_after_startup = cache.len();
    info!(
        startup_load_us,
        cache_size = cache_size_after_startup,
        "hot-cache/kurrentdb: startup load complete — cache ready, no further DB queries needed"
    );

    // ── Phase 3: Cache reads ──────────────────────────────────────────────────
    let mut cache_read_hist = ns_histogram();
    for _ in 0..1_000u64 {
        let t0 = Instant::now();
        let snap = cache.snapshot();
        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        let _ = snap; // prevent optimisation
        cache_read_hist.record(elapsed_ns.max(1)).unwrap_or(());
    }
    info!("hot-cache/kurrentdb: cache read phase done");

    // ── Phase 4: Live writes ──────────────────────────────────────────────────
    info!(
        live_writes = config.live_writes,
        "hot-cache/kurrentdb: live-write phase …"
    );
    let mut db_write_hist = us_histogram();
    let mut cache_push_hist = ns_histogram();
    let base_seq = config.seed_events;

    for i in 0..config.live_writes {
        let event = BenchmarkEvent::new(base_seq + i, 1);

        // DB write
        let t_write = Instant::now();
        client.append(stream_name, "BenchmarkEvent", &event).await?;
        let write_us = t_write.elapsed().as_micros() as u64;
        db_write_hist.record(write_us.max(1)).unwrap_or(());

        // Cache push
        let t_push = Instant::now();
        cache.push(event);
        let push_ns = t_push.elapsed().as_nanos() as u64;
        cache_push_hist.record(push_ns.max(1)).unwrap_or(());
    }
    info!("hot-cache/kurrentdb: live-write phase done");

    Ok(HotCacheResult {
        backend: "KurrentDB",
        seed_events: config.seed_events,
        seed_elapsed_ms,
        seed_rate_eps,
        startup_load_us,
        cache_size_after_startup,
        cache_read_p50_ns: cache_read_hist.value_at_quantile(0.50),
        cache_read_p99_ns: cache_read_hist.value_at_quantile(0.99),
        db_write_p50_us: db_write_hist.value_at_quantile(0.50),
        db_write_p95_us: db_write_hist.value_at_quantile(0.95),
        db_write_p99_us: db_write_hist.value_at_quantile(0.99),
        cache_push_p50_ns: cache_push_hist.value_at_quantile(0.50),
        cache_push_p99_ns: cache_push_hist.value_at_quantile(0.99),
        live_writes: config.live_writes,
    })
}

// ─── MongoDB runner ───────────────────────────────────────────────────────────
//
// Uses full event-store mode: each event gets a monotonic `stream_version` and
// a global `global_position`, enforced by a unique compound index — identical
// semantics to KurrentDB's native per-stream versioning.

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub async fn run_mongo(mongo_url: &str, config: HotCacheConfig) -> Result<HotCacheResult> {
    use crate::mongodb::client::MongoClient;

    info!("hot-cache/mongodb: connecting …");
    let client = MongoClient::connect(mongo_url, &config.database).await?;

    // Wait for MongoDB to be ready.
    for attempt in 1..=30 {
        match client.ping().await {
            Ok(()) => break,
            Err(e) => {
                if attempt == 30 {
                    anyhow::bail!("MongoDB did not become ready within 30 s: {e}");
                }
                tracing::warn!(attempt, error = %e, "waiting for MongoDB …");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    let coll = &config.stream_name;

    // Event-store collection: JSON Schema validator + unique (stream_id, stream_version).
    client.ensure_collection_event_store(coll).await?;
    client.truncate_collection(coll).await?;
    // Reset the per-stream and global counters so version numbering starts at 0.
    client.init_event_store_counters(&[coll.clone()]).await?;

    // ── Phase 1: Seed ─────────────────────────────────────────────────────────
    info!(
        seed_events = config.seed_events,
        batch_size = config.seed_batch_size,
        "hot-cache/mongodb: seeding (event-store mode) …"
    );
    let seed_start = Instant::now();
    let batches = config.seed_events / config.seed_batch_size;
    for b in 0..batches {
        let batch: Vec<BenchmarkEvent> = (0..config.seed_batch_size)
            .map(|i| BenchmarkEvent::new(b * config.seed_batch_size + i, 0))
            .collect();
        client
            .append_batch_versioned(coll, "BenchmarkEvent", &batch)
            .await?;
    }
    let remainder = config.seed_events % config.seed_batch_size;
    if remainder > 0 {
        let base = batches * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> = (0..remainder)
            .map(|i| BenchmarkEvent::new(base + i, 0))
            .collect();
        client
            .append_batch_versioned(coll, "BenchmarkEvent", &batch)
            .await?;
    }
    let seed_elapsed = seed_start.elapsed();
    let seed_elapsed_ms = seed_elapsed.as_secs_f64() * 1_000.0;
    let seed_rate_eps = config.seed_events as f64 / seed_elapsed.as_secs_f64();
    info!(
        elapsed_ms = seed_elapsed_ms as u64,
        rate_eps = seed_rate_eps as u64,
        "hot-cache/mongodb: seeding done"
    );

    // ── Phase 2: Startup load ─────────────────────────────────────────────────
    // Read by stream_version DESC — same as reading a KurrentDB stream backwards.
    info!(
        cache_size = config.cache_size,
        "hot-cache/mongodb: loading last {} events at startup …", config.cache_size
    );
    let startup_start = Instant::now();
    let initial_events = client
        .read_last_n_bench_events(coll, config.cache_size)
        .await?;
    let startup_load_us = startup_start.elapsed().as_micros() as u64;

    let cache = RecentEventsCache::new(config.cache_size);
    for ev in initial_events {
        cache.push(ev);
    }
    let cache_size_after_startup = cache.len();
    info!(
        startup_load_us,
        cache_size = cache_size_after_startup,
        "hot-cache/mongodb: startup load complete — cache ready, no further DB queries needed"
    );

    // ── Phase 3: Cache reads ──────────────────────────────────────────────────
    let mut cache_read_hist = ns_histogram();
    for _ in 0..1_000u64 {
        let t0 = Instant::now();
        let snap = cache.snapshot();
        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        let _ = snap;
        cache_read_hist.record(elapsed_ns.max(1)).unwrap_or(());
    }
    info!("hot-cache/mongodb: cache read phase done");

    // ── Phase 4: Live writes (event-store mode, one event at a time) ──────────
    info!(
        live_writes = config.live_writes,
        "hot-cache/mongodb: live-write phase …"
    );
    let mut db_write_hist = us_histogram();
    let mut cache_push_hist = ns_histogram();
    let base_seq = config.seed_events;

    for i in 0..config.live_writes {
        let event = BenchmarkEvent::new(base_seq + i, 1);
        let batch = std::slice::from_ref(&event);

        let t_write = Instant::now();
        client
            .append_batch_versioned(coll, "BenchmarkEvent", batch)
            .await?;
        let write_us = t_write.elapsed().as_micros() as u64;
        db_write_hist.record(write_us.max(1)).unwrap_or(());

        let t_push = Instant::now();
        cache.push(event);
        let push_ns = t_push.elapsed().as_nanos() as u64;
        cache_push_hist.record(push_ns.max(1)).unwrap_or(());
    }
    info!("hot-cache/mongodb: live-write phase done");

    Ok(HotCacheResult {
        backend: "MongoDB (event-store mode)",
        seed_events: config.seed_events,
        seed_elapsed_ms,
        seed_rate_eps,
        startup_load_us,
        cache_size_after_startup,
        cache_read_p50_ns: cache_read_hist.value_at_quantile(0.50),
        cache_read_p99_ns: cache_read_hist.value_at_quantile(0.99),
        db_write_p50_us: db_write_hist.value_at_quantile(0.50),
        db_write_p95_us: db_write_hist.value_at_quantile(0.95),
        db_write_p99_us: db_write_hist.value_at_quantile(0.99),
        cache_push_p50_ns: cache_push_hist.value_at_quantile(0.50),
        cache_push_p99_ns: cache_push_hist.value_at_quantile(0.99),
        live_writes: config.live_writes,
    })
}

// ─── PostgreSQL runner ────────────────────────────────────────────────────────
//
// Uses full event-store mode: events land in `bench_events` with a unique
// `(stream_id, stream_version)` constraint and an auto-increment
// `global_position` column — structurally equivalent to KurrentDB streams.

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub async fn run_postgres(pg_url: &str, config: HotCacheConfig) -> Result<HotCacheResult> {
    use crate::postgres::client::PostgresClient;

    info!("hot-cache/postgres: connecting …");
    let client = PostgresClient::connect(pg_url).await?;

    // Wait for PostgreSQL to be ready.
    for attempt in 1..=30 {
        match client.ping().await {
            Ok(()) => break,
            Err(e) => {
                if attempt == 30 {
                    anyhow::bail!("PostgreSQL did not become ready within 30 s: {e}");
                }
                tracing::warn!(attempt, error = %e, "waiting for PostgreSQL …");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    let stream_id = &config.stream_name;

    // Event-store schema: bench_events (stream_id, stream_version UNIQUE) +
    // stream_versions counter table.  Drop leftovers from a prior run.
    client.ensure_bench_table_event_store().await?;
    client.ensure_stream_versions_table().await?;
    client.truncate_bench_table().await?;
    client.init_stream_versions(&[stream_id.clone()]).await?;

    // ── Phase 1: Seed ─────────────────────────────────────────────────────────
    info!(
        seed_events = config.seed_events,
        batch_size = config.seed_batch_size,
        "hot-cache/postgres: seeding (event-store mode) …"
    );
    let seed_start = Instant::now();
    let batches = config.seed_events / config.seed_batch_size;
    for b in 0..batches {
        let base = b * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> = (0..config.seed_batch_size)
            .map(|i| BenchmarkEvent::new(base + i, 0))
            .collect();
        client
            .append_batch_versioned(stream_id, "BenchmarkEvent", &batch, base, 0)
            .await?;
    }
    let remainder = config.seed_events % config.seed_batch_size;
    if remainder > 0 {
        let base = batches * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> = (0..remainder)
            .map(|i| BenchmarkEvent::new(base + i, 0))
            .collect();
        client
            .append_batch_versioned(stream_id, "BenchmarkEvent", &batch, base, 0)
            .await?;
    }
    let seed_elapsed = seed_start.elapsed();
    let seed_elapsed_ms = seed_elapsed.as_secs_f64() * 1_000.0;
    let seed_rate_eps = config.seed_events as f64 / seed_elapsed.as_secs_f64();
    info!(
        elapsed_ms = seed_elapsed_ms as u64,
        rate_eps = seed_rate_eps as u64,
        "hot-cache/postgres: seeding done"
    );

    // ── Phase 2: Startup load ─────────────────────────────────────────────────
    // Read by stream_version DESC — mirrors KurrentDB backwards stream read.
    info!(
        cache_size = config.cache_size,
        "hot-cache/postgres: loading last {} events at startup …", config.cache_size
    );
    let startup_start = Instant::now();
    let initial_events = client
        .read_last_n_stream_bench_events(stream_id, config.cache_size as i64)
        .await?;
    let startup_load_us = startup_start.elapsed().as_micros() as u64;

    let cache = RecentEventsCache::new(config.cache_size);
    for ev in initial_events {
        cache.push(ev);
    }
    let cache_size_after_startup = cache.len();
    info!(
        startup_load_us,
        cache_size = cache_size_after_startup,
        "hot-cache/postgres: startup load complete — cache ready, no further DB queries needed"
    );

    // ── Phase 3: Cache reads ──────────────────────────────────────────────────
    let mut cache_read_hist = ns_histogram();
    for _ in 0..1_000u64 {
        let t0 = Instant::now();
        let snap = cache.snapshot();
        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        let _ = snap;
        cache_read_hist.record(elapsed_ns.max(1)).unwrap_or(());
    }
    info!("hot-cache/postgres: cache read phase done");

    // ── Phase 4: Live writes (event-store mode, one event at a time) ──────────
    info!(
        live_writes = config.live_writes,
        "hot-cache/postgres: live-write phase …"
    );
    let mut db_write_hist = us_histogram();
    let mut cache_push_hist = ns_histogram();
    let base_seq = config.seed_events;

    for i in 0..config.live_writes {
        let event = BenchmarkEvent::new(base_seq + i, 1);
        let seq = base_seq + i;

        let t_write = Instant::now();
        client
            .append_batch_versioned(
                stream_id,
                "BenchmarkEvent",
                std::slice::from_ref(&event),
                seq,
                1,
            )
            .await?;
        let write_us = t_write.elapsed().as_micros() as u64;
        db_write_hist.record(write_us.max(1)).unwrap_or(());

        let t_push = Instant::now();
        cache.push(event);
        let push_ns = t_push.elapsed().as_nanos() as u64;
        cache_push_hist.record(push_ns.max(1)).unwrap_or(());
    }
    info!("hot-cache/postgres: live-write phase done");

    Ok(HotCacheResult {
        backend: "PostgreSQL (event-store mode)",
        seed_events: config.seed_events,
        seed_elapsed_ms,
        seed_rate_eps,
        startup_load_us,
        cache_size_after_startup,
        cache_read_p50_ns: cache_read_hist.value_at_quantile(0.50),
        cache_read_p99_ns: cache_read_hist.value_at_quantile(0.99),
        db_write_p50_us: db_write_hist.value_at_quantile(0.50),
        db_write_p95_us: db_write_hist.value_at_quantile(0.95),
        db_write_p99_us: db_write_hist.value_at_quantile(0.99),
        cache_push_p50_ns: cache_push_hist.value_at_quantile(0.50),
        cache_push_p99_ns: cache_push_hist.value_at_quantile(0.99),
        live_writes: config.live_writes,
    })
}
