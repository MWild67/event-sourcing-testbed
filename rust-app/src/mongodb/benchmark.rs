//! Stress-test benchmark: inserts events into MongoDB at a target rate and
//! measures write latency using an HDR histogram.
//!
//! Mirrors the structure of the KurrentDB benchmark (`kurrentdb/benchmark.rs`)
//! so results are directly comparable.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use futures::future::try_join_all;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{events::BenchmarkEvent, mongodb::client::MongoClient};

// ─── Configuration ────────────────────────────────────────────────────────────

pub struct BenchmarkConfig {
    /// Total desired events per second across all tasks.
    pub target_rate: u64,
    /// How long to run the benchmark (seconds).
    pub duration_secs: u64,
    /// Number of concurrent Tokio tasks / collections.
    pub concurrency: u64,
    /// Prefix for collection names ("bench-events-0", "bench-events-1", …).
    pub collection_prefix: String,
    /// Number of events per `insert_many` call (batching).
    pub batch_size: u64,
    /// MongoDB database name.
    pub database: String,
    /// Drop the database before the run starts to guarantee a clean slate.
    /// Prevents leftover documents/indexes from prior runs inflating latency.
    pub drop_before_run: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            target_rate: 10_000,
            duration_secs: 30,
            concurrency: 64,
            collection_prefix: "bench-events".to_string(),
            batch_size: 1,
            database: "eventbench".to_string(),
            drop_before_run: true,
        }
    }
}

// ─── Result ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct BenchmarkResult {
    pub total_events: u64,
    pub actual_rate: f64,
    pub elapsed_secs: f64,
    /// Latency percentiles in **microseconds**.
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
}

impl BenchmarkResult {
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════");
        println!("  MongoDB Stress-Test Result");
        println!("══════════════════════════════════════════════");
        println!("  Duration     : {:.2}s", self.elapsed_secs);
        println!("  Total events : {}", self.total_events);
        println!("  Actual rate  : {:.1} ev/s", self.actual_rate);
        println!(
            "  p50  latency : {} µs ({:.2} ms)",
            self.p50_us,
            self.p50_us as f64 / 1000.0
        );
        println!(
            "  p95  latency : {} µs ({:.2} ms)",
            self.p95_us,
            self.p95_us as f64 / 1000.0
        );
        println!(
            "  p99  latency : {} µs ({:.2} ms)",
            self.p99_us,
            self.p99_us as f64 / 1000.0
        );
        println!(
            "  p99.9 latency: {} µs ({:.2} ms)",
            self.p999_us,
            self.p999_us as f64 / 1000.0
        );
        println!("══════════════════════════════════════════════");
        println!();
    }

    /// Emit as a single line of JSON so CI scripts can `jq` it easily.
    pub fn print_json(&self) {
        println!(
            r#"{{"total_events":{total},"actual_rate_eps":{rate:.1},"p50_us":{p50},"p95_us":{p95},"p99_us":{p99},"p999_us":{p999}}}"#,
            total = self.total_events,
            rate = self.actual_rate,
            p50 = self.p50_us,
            p95 = self.p95_us,
            p99 = self.p99_us,
            p999 = self.p999_us,
        );
    }
}

// ─── Benchmark runner ─────────────────────────────────────────────────────────

/// Spawns one shared MongoDB client and fans writes out across `concurrency`
/// logical collections.  A semaphore bounds in-flight inserts to avoid
/// overwhelming the server's connection pool.
pub async fn run(mongo_url: &str, config: BenchmarkConfig) -> Result<BenchmarkResult> {
    info!(
        target_rate = config.target_rate,
        concurrency = config.concurrency,
        batch_size = config.batch_size,
        duration = config.duration_secs,
        "starting MongoDB stress-test benchmark"
    );

    // Validate connectivity — retry for up to 30 s so a freshly-started
    // MongoDB instance has time to become ready before writes begin.
    let probe = MongoClient::connect(mongo_url, &config.database).await?;
    let mut ready = false;
    for attempt in 1..=30 {
        match probe.ping().await {
            Ok(()) => {
                ready = true;
                break;
            }
            Err(e) => {
                warn!(attempt, error = %e, "waiting for MongoDB to become ready...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    if !ready {
        anyhow::bail!("MongoDB did not become ready within 30 s");
    }
    info!("MongoDB connectivity OK");

    // Drop the database so leftover documents and B-tree indexes from prior
    // runs don't inflate insert latency.  Skip only when the caller explicitly
    // opts out (e.g. when appending to an existing dataset on purpose).
    if config.drop_before_run {
        probe.drop_database().await?;
        info!(database = %config.database, "dropped database for clean-slate run");
    }

    // Wrap probe in Arc before pre-creation so the same client is reused
    // throughout warm-up and the timed loop.
    let client = Arc::new(probe);

    // Pre-create every collection and its _id index before the timer starts.
    //
    // Without this, the first insert_many into each collection triggers
    // WiredTiger schema creation (data file allocation, index-tree build, oplog
    // entry) — a multi-millisecond operation that lands inside the timed window
    // and directly inflates the p99.  Pre-creating moves that one-time cost
    // entirely outside the measurement window.
    info!(
        collections = config.concurrency,
        "pre-creating collections (warm-up, outside timed window)"
    );
    let collection_names: Vec<String> = (0..config.concurrency)
        .map(|i| format!("{}-{}", config.collection_prefix, i))
        .collect();
    try_join_all(collection_names.iter().map(|n| client.ensure_collection(n))).await?;
    info!("collection warm-up complete, starting timed run");

    let total_events = Arc::new(AtomicU64::new(0));
    let shared_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let batch_size = config.batch_size.max(1);
    let ticks_per_sec = ((config.target_rate as f64) / (batch_size as f64)).ceil() as u64;
    let tick_us = 1_000_000u64 / ticks_per_sec.max(1);

    // Cap in-flight inserts to stay within the driver's default pool size (100).
    let max_in_flight = (config.concurrency as usize).min(96);
    let in_flight = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

    let duration = Duration::from_secs(config.duration_secs);
    let wall_start = Instant::now();

    let mut interval = tokio::time::interval(Duration::from_micros(tick_us));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut seq: u64 = 0;

    loop {
        interval.tick().await;
        if wall_start.elapsed() >= duration {
            break;
        }

        // Non-blocking: skip this tick if the write pipeline is already full.
        let permit = match Arc::clone(&in_flight).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => continue,
        };

        let client = Arc::clone(&client);
        let total_events = Arc::clone(&total_events);
        let hist = Arc::clone(&shared_hist);
        let collection_name = format!("{}-{}", config.collection_prefix, seq % config.concurrency);
        let events: Vec<BenchmarkEvent> = (0..batch_size)
            .map(|i| BenchmarkEvent::new(seq * batch_size + i, seq % config.concurrency))
            .collect();
        seq += 1;

        tokio::spawn(async move {
            let _permit = permit; // returned to semaphore when this task exits
            let t0 = Instant::now();
            match client
                .append_batch(&collection_name, "BenchmarkEvent", &events)
                .await
            {
                Ok(_) => {
                    let lat_us = t0.elapsed().as_micros() as u64;
                    let _ = hist.lock().await.record(lat_us);
                    total_events.fetch_add(batch_size, Ordering::Relaxed);
                }
                Err(e) => warn!(error = %e, "insert_many failed, skipping"),
            }
        });
    }

    // Wait for all in-flight inserts to drain before computing results.
    let _ = in_flight.acquire_many(max_in_flight as u32).await;

    let elapsed = wall_start.elapsed();
    let total = total_events.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64();
    let hist = shared_hist.lock().await;
    let p99_us = hist.value_at_quantile(0.99);

    Ok(BenchmarkResult {
        total_events: total,
        actual_rate: rate,
        elapsed_secs: elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p95_us: hist.value_at_quantile(0.95),
        p99_us,
        p999_us: hist.value_at_quantile(0.999),
    })
}
