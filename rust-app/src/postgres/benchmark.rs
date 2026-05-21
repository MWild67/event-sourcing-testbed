//! Stress-test benchmark: inserts events into `PostgreSQL` at a target rate and
//! measures write latency using an HDR histogram.
//!
//! Mirrors the structure of [`crate::mongodb::benchmark`] so results are
//! directly comparable between backends.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{events::BenchmarkEvent, postgres::client::PostgresClient};

// ─── Configuration ────────────────────────────────────────────────────────────

pub struct BenchmarkConfig {
    /// Total desired events per second across all tasks.
    pub target_rate: u64,
    /// How long to run the benchmark (seconds).
    pub duration_secs: u64,
    /// Number of concurrent Tokio tasks / streams.
    pub concurrency: u64,
    /// Prefix for stream names (`"bench-stream-0"`, `"bench-stream-1"`, …).
    pub stream_prefix: String,
    /// Number of events per `INSERT … VALUES` call.
    pub batch_size: u64,
    /// `PostgreSQL` database URL (overrides any URL in the connection string).
    #[allow(dead_code)]
    pub database_url: String,
    /// Truncate the bench table before the run starts.
    pub truncate_before_run: bool,
    /// Enable event-store-mode features:
    ///   - Unique `(stream_id, stream_version)` constraint.
    ///   - `GENERATED ALWAYS AS IDENTITY` global position.
    ///   - Per-stream version counter with optimistic-concurrency enforcement.
    pub event_store_mode: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            target_rate: 10_000,
            duration_secs: 30,
            concurrency: 64,
            stream_prefix: "bench-stream".to_string(),
            batch_size: 1,
            database_url: String::new(),
            truncate_before_run: true,
            event_store_mode: false,
        }
    }
}

// ─── Result ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct BenchmarkResult {
    pub total_events: u64,
    pub actual_rate: f64,
    pub elapsed_secs: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
}

impl BenchmarkResult {
    #[allow(clippy::cast_precision_loss)]
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════");
        println!("  PostgreSQL Stress-Test Result");
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

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]
pub async fn run(pg_url: &str, config: BenchmarkConfig) -> Result<BenchmarkResult> {
    info!(
        target_rate = config.target_rate,
        concurrency = config.concurrency,
        batch_size = config.batch_size,
        duration = config.duration_secs,
        event_store_mode = config.event_store_mode,
        "starting PostgreSQL stress-test benchmark"
    );

    let client = Arc::new(PostgresClient::connect(pg_url).await?);

    // Readiness retry — wait up to 30 s for a freshly-started PostgreSQL.
    let mut ready = false;
    for attempt in 1..=30 {
        match client.ping().await {
            Ok(()) => {
                ready = true;
                break;
            }
            Err(e) => {
                warn!(attempt, error = %e, "waiting for PostgreSQL to become ready...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    if !ready {
        anyhow::bail!("PostgreSQL did not become ready within 30 s");
    }
    info!("PostgreSQL connectivity OK");

    // ── Schema bootstrap (outside timed window) ───────────────────────────────
    if config.event_store_mode {
        client.ensure_bench_table_event_store().await?;
        client.ensure_stream_versions_table().await?;
        let stream_names: Vec<String> = (0..config.concurrency)
            .map(|i| format!("{}-{}", config.stream_prefix, i))
            .collect();
        client.init_stream_versions(&stream_names).await?;
        info!("event-store schema ready (unique constraint + global position)");
    } else {
        client.ensure_bench_table().await?;
    }

    if config.truncate_before_run {
        client.truncate_bench_table().await?;
        info!("bench table truncated for clean-slate run");
    }

    // ── Connection pool warm-up (outside timed window) ────────────────────────
    //
    // sqlx creates connections lazily.  Without this warm-up, the first burst
    // of `concurrency` concurrent tasks all race to CREATE new connections
    // through the network path (including any k8s port-forward tunnel).
    // Connection establishment is slow relative to the 5 s acquire_timeout,
    // so those pending requests time out, producing the periodic WARN bursts
    // seen in CI logs.
    //
    // We warm up max_in_flight + 4 connections: the extra 4 act as a small
    // buffer so the pool never has to create a connection on the hot path
    // even if a task briefly holds its connection longer than expected.
    let max_in_flight = usize::try_from(config.concurrency).unwrap_or(usize::MAX).min(96);
    {
        let warm_count = max_in_flight + 4;
        let warm_handles: Vec<_> = (0..warm_count)
            .map(|_| {
                let c = Arc::clone(&client);
                tokio::spawn(async move { c.ping().await.ok() })
            })
            .collect();
        futures::future::join_all(warm_handles).await;
        info!(connections = warm_count, "connection pool warmed up");
    }

    // ── Timed loop ────────────────────────────────────────────────────────────
    let total_events = Arc::new(AtomicU64::new(0));
    let shared_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let batch_size = config.batch_size.max(1);
    let ticks_per_sec = ((config.target_rate as f64) / (batch_size as f64)).ceil() as u64;
    let tick_us = 1_000_000u64 / ticks_per_sec.max(1);

    let in_flight = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

    let duration = Duration::from_secs(config.duration_secs);
    let wall_start = Instant::now();

    let mut interval = tokio::time::interval(Duration::from_micros(tick_us));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut seq: u64 = 0;
    let event_store_mode = config.event_store_mode;
    let stream_prefix = config.stream_prefix.clone();

    loop {
        interval.tick().await;
        if wall_start.elapsed() >= duration {
            break;
        }

        let Ok(permit) = Arc::clone(&in_flight).try_acquire_owned() else { continue };

        let client = Arc::clone(&client);
        let total_ev = Arc::clone(&total_events);
        let hist = Arc::clone(&shared_hist);
        let stream_id = format!("{}-{}", stream_prefix, seq % config.concurrency);
        let events: Vec<BenchmarkEvent> = (0..batch_size)
            .map(|i| BenchmarkEvent::new(seq * batch_size + i, seq % config.concurrency))
            .collect();
        let base_seq = seq * batch_size;
        let task_id = seq % config.concurrency;
        seq += 1;

        tokio::spawn(async move {
            let _permit = permit;
            let t0 = Instant::now();
            let result = if event_store_mode {
                client
                    .append_batch_versioned(
                        &stream_id,
                        "BenchmarkEvent",
                        &events,
                        base_seq,
                        task_id,
                    )
                    .await
                    .map(|_| ())
            } else {
                client
                    .append_batch(&stream_id, "BenchmarkEvent", &events, base_seq, task_id)
                    .await
            };
            match result {
                Ok(()) => {
                    let lat_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
                    let _ = hist.lock().await.record(lat_us);
                    total_ev.fetch_add(batch_size, Ordering::Relaxed);
                }
                Err(e) => warn!("insert failed, skipping: {:#}", e),
            }
        });
    }

    let _ = in_flight.acquire_many(u32::try_from(max_in_flight).unwrap_or(96)).await;

    let elapsed = wall_start.elapsed();
    let total = total_events.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64();
    let hist = shared_hist.lock().await;

    Ok(BenchmarkResult {
        total_events: total,
        actual_rate: rate,
        elapsed_secs: elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p95_us: hist.value_at_quantile(0.95),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}
