//! Memcached hot-tail-cache benchmark.
//!
//! Identical four-phase workload to `hot_cache_bench` but uses an **external
//! Memcached server** as the hot-window cache instead of an in-process ring
//! buffer.  This lets you measure the overhead of a network round-trip to
//! Memcached (typically ~100 µs on localhost) vs. the sub-microsecond in-process
//! reads of Strategy A and the KurrentDB-native `$maxCount` reads of Strategy B.
//!
//! ## Phases
//!
//! 1. **Seed** — write `seed_events` to the DB in batches.
//! 2. **Startup** — cold miss on Memcached; load last `cache_size` events from
//!    the DB, serialize to JSON, SET into Memcached. Measures db_load + mc_set.
//! 3. **Cache reads** — 1 000 × Memcached GET + JSON deserialize. Latency in µs.
//! 4. **Live writes** — write event to DB, append to local tail, SET serialized
//!    tail back to Memcached (write-through). Measures db_write + mc_set per op.

use std::{collections::VecDeque, time::Instant};

use anyhow::Result;
use hdrhistogram::Histogram;
use tracing::info;

use crate::events::BenchmarkEvent;

// ─── Memcached helpers ────────────────────────────────────────────────────────

pub fn connect_mc(url: &str) -> Result<memcache::Client> {
    memcache::Client::connect(url)
        .map_err(|e| anyhow::anyhow!("Memcached connect failed ({url}): {e}"))
}

async fn mc_get_tail(mc: &memcache::Client, key: &str) -> Result<Option<Vec<BenchmarkEvent>>> {
    let mc = mc.clone();
    let key = key.to_owned();
    let bytes: Option<Vec<u8>> = tokio::task::spawn_blocking(move || {
        mc.get::<Vec<u8>>(&key)
            .map_err(|e| anyhow::anyhow!("mc get: {e}"))
    })
    .await??;
    match bytes {
        Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        None => Ok(None),
    }
}

async fn mc_set_tail(mc: &memcache::Client, key: &str, tail: &[BenchmarkEvent]) -> Result<()> {
    let mc = mc.clone();
    let key = key.to_owned();
    let bytes = serde_json::to_vec(tail)?;
    tokio::task::spawn_blocking(move || {
        mc.set(&key, bytes.as_slice(), 3600)
            .map_err(|e| anyhow::anyhow!("mc set: {e}"))
    })
    .await??;
    Ok(())
}

async fn mc_delete(mc: &memcache::Client, key: &str) -> Result<()> {
    let mc = mc.clone();
    let key = key.to_owned();
    tokio::task::spawn_blocking(move || {
        mc.delete(&key)
            .map_err(|e| anyhow::anyhow!("mc delete: {e}"))
    })
    .await??;
    Ok(())
}

// ─── Configuration ────────────────────────────────────────────────────────────

pub struct MemcachedCacheConfig {
    pub seed_events: u64,
    pub cache_size: usize,
    pub live_writes: u64,
    pub seed_batch_size: u64,
    pub stream_name: String,
    /// MongoDB database name (ignored for KurrentDB / PostgreSQL).
    pub database: String,
}

impl Default for MemcachedCacheConfig {
    fn default() -> Self {
        Self {
            seed_events: 50_000,
            cache_size: 500,
            live_writes: 500,
            seed_batch_size: 100,
            stream_name: "mc-bench".to_string(),
            database: "mcbench".to_string(),
        }
    }
}

// ─── Result ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct MemcachedCacheResult {
    pub backend: &'static str,
    pub seed_events: u64,
    pub cache_size: usize,
    /// Time to load the tail from the DB on a cold Memcached miss (µs).
    pub startup_db_load_us: u64,
    /// Time to SET the loaded tail into Memcached at startup (µs).
    pub startup_mc_set_us: u64,
    /// Memcached GET + JSON deserialize p50 (µs).
    pub cache_read_p50_us: u64,
    /// Memcached GET + JSON deserialize p99 (µs).
    pub cache_read_p99_us: u64,
    /// DB write p50 (µs) during the live-write phase.
    pub db_write_p50_us: u64,
    /// DB write p95 (µs) during the live-write phase.
    pub db_write_p95_us: u64,
    /// DB write p99 (µs) during the live-write phase.
    pub db_write_p99_us: u64,
    /// Memcached SET (write-through) p50 (µs) during the live-write phase.
    pub mc_set_p50_us: u64,
    /// Memcached SET (write-through) p99 (µs) during the live-write phase.
    pub mc_set_p99_us: u64,
    pub live_writes: u64,
}

impl MemcachedCacheResult {
    #[allow(clippy::cast_precision_loss)]
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════════════════════");
        println!("  Memcached Hot-Tail-Cache Benchmark — {}", self.backend);
        println!(
            "  ({} events in DB · hot window {} · Memcached write-through)",
            self.seed_events, self.cache_size
        );
        println!("══════════════════════════════════════════════════════════════");
        println!("  STARTUP (cold miss → DB load → Memcached populate)");
        println!("  ──────────────────────────────────────────────────────────");
        println!(
            "  DB tail load    : {} µs  ({:.2} ms)",
            self.startup_db_load_us,
            self.startup_db_load_us as f64 / 1_000.0
        );
        println!(
            "  Memcached SET   : {} µs  ({:.2} ms)",
            self.startup_mc_set_us,
            self.startup_mc_set_us as f64 / 1_000.0
        );
        println!();
        println!(
            "  CACHE READS  — 1 000 × Memcached GET + deserialize {} events",
            self.cache_size
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!("  p50             : {} µs", self.cache_read_p50_us);
        println!("  p99             : {} µs", self.cache_read_p99_us);
        println!();
        println!(
            "  LIVE WRITE PHASE  ({} events · DB write + Memcached SET)",
            self.live_writes
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!(
            "  DB write p50    : {} µs  ({:.2} ms)",
            self.db_write_p50_us,
            self.db_write_p50_us as f64 / 1_000.0
        );
        println!(
            "  DB write p95    : {} µs  ({:.2} ms)",
            self.db_write_p95_us,
            self.db_write_p95_us as f64 / 1_000.0
        );
        println!(
            "  DB write p99    : {} µs  ({:.2} ms)",
            self.db_write_p99_us,
            self.db_write_p99_us as f64 / 1_000.0
        );
        println!("  MC SET   p50    : {} µs", self.mc_set_p50_us);
        println!("  MC SET   p99    : {} µs", self.mc_set_p99_us);
        println!("══════════════════════════════════════════════════════════════");
        println!();
    }

    pub fn print_json(&self) {
        println!(
            r#"{{"backend":"{backend}","seed_events":{seed},"cache_size":{cs},"startup_db_load_us":{sdl},"startup_mc_set_us":{sms},"cache_read_p50_us":{cr50},"cache_read_p99_us":{cr99},"db_write_p50_us":{dw50},"db_write_p95_us":{dw95},"db_write_p99_us":{dw99},"mc_set_p50_us":{ms50},"mc_set_p99_us":{ms99},"live_writes":{lw}}}"#,
            backend = self.backend,
            seed = self.seed_events,
            cs = self.cache_size,
            sdl = self.startup_db_load_us,
            sms = self.startup_mc_set_us,
            cr50 = self.cache_read_p50_us,
            cr99 = self.cache_read_p99_us,
            dw50 = self.db_write_p50_us,
            dw95 = self.db_write_p95_us,
            dw99 = self.db_write_p99_us,
            ms50 = self.mc_set_p50_us,
            ms99 = self.mc_set_p99_us,
            lw = self.live_writes,
        );
    }
}

// ─── Shared histogram helpers ─────────────────────────────────────────────────

fn us_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 30_000_000, 3).unwrap()
}

// ─── KurrentDB runner ────────────────────────────────────────────────────────

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub async fn run_kurrentdb(
    kurrentdb_url: &str,
    memcached_url: &str,
    config: MemcachedCacheConfig,
) -> Result<MemcachedCacheResult> {
    use crate::kurrentdb::client::KurrentClient;

    info!("mc-bench/kurrentdb: connecting …");
    let db = KurrentClient::connect(kurrentdb_url)?;
    let mc = connect_mc(memcached_url)?;
    let mc_key = format!("mc-bench:kdb:{}", config.stream_name);

    // Wait for KurrentDB readiness.
    for attempt in 1..=30 {
        match db.ping().await {
            Ok(()) => break,
            Err(e) => {
                if attempt == 30 {
                    anyhow::bail!("KurrentDB not ready: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    // Clear any leftover Memcached key from a prior run.
    let _ = mc_delete(&mc, &mc_key).await;

    // ── Phase 1: Seed ─────────────────────────────────────────────────────────
    info!(
        "mc-bench/kurrentdb: seeding {} events …",
        config.seed_events
    );
    let batches = config.seed_events / config.seed_batch_size;
    for b in 0..batches {
        let batch: Vec<BenchmarkEvent> = (0..config.seed_batch_size)
            .map(|i| BenchmarkEvent::new(b * config.seed_batch_size + i, 0))
            .collect();
        db.append_batch(&config.stream_name, "BenchmarkEvent", &batch)
            .await?;
    }
    let rem = config.seed_events % config.seed_batch_size;
    if rem > 0 {
        let base = batches * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> =
            (0..rem).map(|i| BenchmarkEvent::new(base + i, 0)).collect();
        db.append_batch(&config.stream_name, "BenchmarkEvent", &batch)
            .await?;
    }
    info!("mc-bench/kurrentdb: seed done");

    // ── Phase 2: Startup — cold miss → DB load → Memcached populate ──────────
    let t_db = Instant::now();
    let tail_events = db
        .read_last_n_bench_events(&config.stream_name, config.cache_size)
        .await?;
    let startup_db_load_us = t_db.elapsed().as_micros() as u64;

    let t_mc = Instant::now();
    mc_set_tail(&mc, &mc_key, &tail_events).await?;
    let startup_mc_set_us = t_mc.elapsed().as_micros() as u64;

    info!(
        startup_db_load_us,
        startup_mc_set_us,
        "mc-bench/kurrentdb: startup complete ({} events cached in Memcached)",
        tail_events.len()
    );

    // ── Phase 3: Cache reads (1 000 × Memcached GET) ──────────────────────────
    let mut read_hist = us_histogram();
    for _ in 0..1_000u64 {
        let t0 = Instant::now();
        let _ = mc_get_tail(&mc, &mc_key).await?;
        read_hist
            .record(t0.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());
    }
    info!("mc-bench/kurrentdb: cache-read phase done");

    // ── Phase 4: Live writes (DB write + Memcached write-through) ─────────────
    let mut db_hist = us_histogram();
    let mut mc_hist = us_histogram();
    let mut local_tail: VecDeque<BenchmarkEvent> = tail_events.into_iter().collect();

    for i in 0..config.live_writes {
        let event = BenchmarkEvent::new(config.seed_events + i, 1);

        let t_db = Instant::now();
        db.append(&config.stream_name, "BenchmarkEvent", &event)
            .await?;
        db_hist
            .record(t_db.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());

        // Maintain the local tail for serialisation (avoids a GET per write).
        if local_tail.len() == config.cache_size {
            local_tail.pop_front();
        }
        local_tail.push_back(event);
        let tail_slice: Vec<_> = local_tail.iter().cloned().collect();

        let t_mc = Instant::now();
        mc_set_tail(&mc, &mc_key, &tail_slice).await?;
        mc_hist
            .record(t_mc.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());
    }
    info!("mc-bench/kurrentdb: live-write phase done");

    Ok(MemcachedCacheResult {
        backend: "KurrentDB + Memcached",
        seed_events: config.seed_events,
        cache_size: config.cache_size,
        startup_db_load_us,
        startup_mc_set_us,
        cache_read_p50_us: read_hist.value_at_quantile(0.50),
        cache_read_p99_us: read_hist.value_at_quantile(0.99),
        db_write_p50_us: db_hist.value_at_quantile(0.50),
        db_write_p95_us: db_hist.value_at_quantile(0.95),
        db_write_p99_us: db_hist.value_at_quantile(0.99),
        mc_set_p50_us: mc_hist.value_at_quantile(0.50),
        mc_set_p99_us: mc_hist.value_at_quantile(0.99),
        live_writes: config.live_writes,
    })
}

// ─── MongoDB runner ───────────────────────────────────────────────────────────

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub async fn run_mongo(
    mongo_url: &str,
    memcached_url: &str,
    config: MemcachedCacheConfig,
) -> Result<MemcachedCacheResult> {
    use crate::mongodb::client::MongoClient;

    info!("mc-bench/mongodb: connecting …");
    let db = MongoClient::connect(mongo_url, &config.database).await?;
    let mc = connect_mc(memcached_url)?;
    let mc_key = format!("mc-bench:mdb:{}", config.stream_name);

    for attempt in 1..=30 {
        match db.ping().await {
            Ok(()) => break,
            Err(e) => {
                if attempt == 30 {
                    anyhow::bail!("MongoDB not ready: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    let _ = mc_delete(&mc, &mc_key).await;

    let coll = &config.stream_name;
    db.ensure_collection_event_store(coll).await?;
    db.truncate_collection(coll).await?;
    db.init_event_store_counters(std::slice::from_ref(coll))
        .await?;

    // ── Phase 1: Seed ─────────────────────────────────────────────────────────
    info!("mc-bench/mongodb: seeding {} events …", config.seed_events);
    let batches = config.seed_events / config.seed_batch_size;
    for b in 0..batches {
        let batch: Vec<BenchmarkEvent> = (0..config.seed_batch_size)
            .map(|i| BenchmarkEvent::new(b * config.seed_batch_size + i, 0))
            .collect();
        db.append_batch_versioned(coll, "BenchmarkEvent", &batch)
            .await?;
    }
    let rem = config.seed_events % config.seed_batch_size;
    if rem > 0 {
        let base = batches * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> =
            (0..rem).map(|i| BenchmarkEvent::new(base + i, 0)).collect();
        db.append_batch_versioned(coll, "BenchmarkEvent", &batch)
            .await?;
    }
    info!("mc-bench/mongodb: seed done");

    // ── Phase 2: Startup ──────────────────────────────────────────────────────
    let t_db = Instant::now();
    let tail_events = db.read_last_n_bench_events(coll, config.cache_size).await?;
    let startup_db_load_us = t_db.elapsed().as_micros() as u64;

    let t_mc = Instant::now();
    mc_set_tail(&mc, &mc_key, &tail_events).await?;
    let startup_mc_set_us = t_mc.elapsed().as_micros() as u64;
    info!(
        startup_db_load_us,
        startup_mc_set_us, "mc-bench/mongodb: startup complete"
    );

    // ── Phase 3: Cache reads ──────────────────────────────────────────────────
    let mut read_hist = us_histogram();
    for _ in 0..1_000u64 {
        let t0 = Instant::now();
        let _ = mc_get_tail(&mc, &mc_key).await?;
        read_hist
            .record(t0.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());
    }

    // ── Phase 4: Live writes ──────────────────────────────────────────────────
    let mut db_hist = us_histogram();
    let mut mc_hist = us_histogram();
    let mut local_tail: VecDeque<BenchmarkEvent> = tail_events.into_iter().collect();

    for i in 0..config.live_writes {
        let event = BenchmarkEvent::new(config.seed_events + i, 1);
        let t_db = Instant::now();
        db.append_batch_versioned(coll, "BenchmarkEvent", std::slice::from_ref(&event))
            .await?;
        db_hist
            .record(t_db.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());

        if local_tail.len() == config.cache_size {
            local_tail.pop_front();
        }
        local_tail.push_back(event);
        let tail_slice: Vec<_> = local_tail.iter().cloned().collect();

        let t_mc = Instant::now();
        mc_set_tail(&mc, &mc_key, &tail_slice).await?;
        mc_hist
            .record(t_mc.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());
    }
    info!("mc-bench/mongodb: live-write phase done");

    Ok(MemcachedCacheResult {
        backend: "MongoDB + Memcached",
        seed_events: config.seed_events,
        cache_size: config.cache_size,
        startup_db_load_us,
        startup_mc_set_us,
        cache_read_p50_us: read_hist.value_at_quantile(0.50),
        cache_read_p99_us: read_hist.value_at_quantile(0.99),
        db_write_p50_us: db_hist.value_at_quantile(0.50),
        db_write_p95_us: db_hist.value_at_quantile(0.95),
        db_write_p99_us: db_hist.value_at_quantile(0.99),
        mc_set_p50_us: mc_hist.value_at_quantile(0.50),
        mc_set_p99_us: mc_hist.value_at_quantile(0.99),
        live_writes: config.live_writes,
    })
}

// ─── PostgreSQL runner ────────────────────────────────────────────────────────

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub async fn run_postgres(
    pg_url: &str,
    memcached_url: &str,
    config: MemcachedCacheConfig,
) -> Result<MemcachedCacheResult> {
    use crate::postgres::client::PostgresClient;

    info!("mc-bench/postgres: connecting …");
    let db = PostgresClient::connect(pg_url).await?;
    let mc = connect_mc(memcached_url)?;
    let mc_key = format!("mc-bench:pg:{}", config.stream_name);

    for attempt in 1..=30 {
        match db.ping().await {
            Ok(()) => break,
            Err(e) => {
                if attempt == 30 {
                    anyhow::bail!("PostgreSQL not ready: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    let _ = mc_delete(&mc, &mc_key).await;

    let stream_id = &config.stream_name;
    db.ensure_bench_table_event_store().await?;
    db.ensure_stream_versions_table().await?;
    db.truncate_bench_table().await?;
    db.init_stream_versions(std::slice::from_ref(stream_id))
        .await?;

    // ── Phase 1: Seed ─────────────────────────────────────────────────────────
    info!("mc-bench/postgres: seeding {} events …", config.seed_events);
    let batches = config.seed_events / config.seed_batch_size;
    for b in 0..batches {
        let base = b * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> = (0..config.seed_batch_size)
            .map(|i| BenchmarkEvent::new(base + i, 0))
            .collect();
        db.append_batch_versioned(stream_id, "BenchmarkEvent", &batch, base, 0)
            .await?;
    }
    let rem = config.seed_events % config.seed_batch_size;
    if rem > 0 {
        let base = batches * config.seed_batch_size;
        let batch: Vec<BenchmarkEvent> =
            (0..rem).map(|i| BenchmarkEvent::new(base + i, 0)).collect();
        db.append_batch_versioned(stream_id, "BenchmarkEvent", &batch, base, 0)
            .await?;
    }
    info!("mc-bench/postgres: seed done");

    // ── Phase 2: Startup ──────────────────────────────────────────────────────
    let t_db = Instant::now();
    let tail_events = db
        .read_last_n_stream_bench_events(stream_id, config.cache_size as i64)
        .await?;
    let startup_db_load_us = t_db.elapsed().as_micros() as u64;

    let t_mc = Instant::now();
    mc_set_tail(&mc, &mc_key, &tail_events).await?;
    let startup_mc_set_us = t_mc.elapsed().as_micros() as u64;
    info!(
        startup_db_load_us,
        startup_mc_set_us, "mc-bench/postgres: startup complete"
    );

    // ── Phase 3: Cache reads ──────────────────────────────────────────────────
    let mut read_hist = us_histogram();
    for _ in 0..1_000u64 {
        let t0 = Instant::now();
        let _ = mc_get_tail(&mc, &mc_key).await?;
        read_hist
            .record(t0.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());
    }

    // ── Phase 4: Live writes ──────────────────────────────────────────────────
    let mut db_hist = us_histogram();
    let mut mc_hist = us_histogram();
    let mut local_tail: VecDeque<BenchmarkEvent> = tail_events.into_iter().collect();

    for i in 0..config.live_writes {
        let event = BenchmarkEvent::new(config.seed_events + i, 1);
        let seq = config.seed_events + i;

        let t_db = Instant::now();
        db.append_batch_versioned(
            stream_id,
            "BenchmarkEvent",
            std::slice::from_ref(&event),
            seq,
            1,
        )
        .await?;
        db_hist
            .record(t_db.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());

        if local_tail.len() == config.cache_size {
            local_tail.pop_front();
        }
        local_tail.push_back(event);
        let tail_slice: Vec<_> = local_tail.iter().cloned().collect();

        let t_mc = Instant::now();
        mc_set_tail(&mc, &mc_key, &tail_slice).await?;
        mc_hist
            .record(t_mc.elapsed().as_micros().max(1) as u64)
            .unwrap_or(());
    }
    info!("mc-bench/postgres: live-write phase done");

    Ok(MemcachedCacheResult {
        backend: "PostgreSQL + Memcached",
        seed_events: config.seed_events,
        cache_size: config.cache_size,
        startup_db_load_us,
        startup_mc_set_us,
        cache_read_p50_us: read_hist.value_at_quantile(0.50),
        cache_read_p99_us: read_hist.value_at_quantile(0.99),
        db_write_p50_us: db_hist.value_at_quantile(0.50),
        db_write_p95_us: db_hist.value_at_quantile(0.95),
        db_write_p99_us: db_hist.value_at_quantile(0.99),
        mc_set_p50_us: mc_hist.value_at_quantile(0.50),
        mc_set_p99_us: mc_hist.value_at_quantile(0.99),
        live_writes: config.live_writes,
    })
}
