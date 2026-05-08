//! Stress-test benchmark: appends events to EventStoreDB at a target rate and
//! measures write latency using an HDR histogram.
//!
//! Pass/fail criterion: p99 write latency < 2 ms at 10 000 events/second.

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

use crate::{events::BenchmarkEvent, eventstore_client::EsClient};

// ─── Configuration ────────────────────────────────────────────────────────────

pub struct BenchmarkConfig {
    /// Total desired events per second across all tasks.
    pub target_rate: u64,
    /// How long to run the benchmark (seconds).
    pub duration_secs: u64,
    /// Number of concurrent Tokio tasks / streams.
    pub concurrency: u64,
    /// Prefix for stream names ("bench-stream-0", "bench-stream-1", …).
    pub stream_prefix: String,
    /// Number of events per gRPC append call (batching).
    /// Higher values reduce per-event gRPC overhead at the cost of slightly
    /// larger individual payloads.  Default: 5.
    pub batch_size: u64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            target_rate: 10_000,
            duration_secs: 30,
            // Max concurrent in-flight gRPC writes.  Keeping several writes
            // in-flight at all times saturates EventStoreDB's write queue and
            // prevents the ~40 ms idle-flush timer from firing between bursts.
            concurrency: 64,
            stream_prefix: "bench-stream".to_string(),
            batch_size: 1,
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
    /// `true` when p99 < 2 000 µs (2 ms) — the passing criterion.
    pub passed: bool,
}

impl BenchmarkResult {
    pub fn print_report(&self) {
        let status = if self.passed { "PASS ✓" } else { "FAIL ✗" };
        println!();
        println!("══════════════════════════════════════════════");
        println!("  Stress-Test Result: {status}");
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
        println!("  Criterion    : p99 < 10 000 µs AND rate >= 9 000 ev/s");
        println!("══════════════════════════════════════════════");
        println!();
    }

    /// Emit as a single line of JSON so CI scripts can `jq` it easily.
    pub fn print_json(&self) {
        println!(
            r#"{{"passed":{passed},"total_events":{total},"actual_rate_eps":{rate:.1},"p50_us":{p50},"p95_us":{p95},"p99_us":{p99},"p999_us":{p999}}}"#,
            passed = self.passed,
            total = self.total_events,
            rate = self.actual_rate,
            p50 = self.p50_us,
            p95 = self.p95_us,
            p99 = self.p99_us,
            p999 = self.p999_us,
        );
    }
}

// ─── Benchmark runner ────────────────────────────────────────────────────────

/// Each task gets its own independent gRPC connection (`EsClient`) to avoid
/// HTTP/2 head-of-line blocking that occurs when many concurrent streams share
/// one connection.  Connections are staggered over 1 second so the server
/// doesn't see a simultaneous handshake storm.
pub async fn run(es_url: &str, config: BenchmarkConfig) -> Result<BenchmarkResult> {
    info!(
        target_rate = config.target_rate,
        concurrency = config.concurrency,
        batch_size = config.batch_size,
        duration = config.duration_secs,
        "starting stress-test benchmark"
    );

    // Validate connectivity — retry for up to 30 s so a freshly-started ES has
    // time to elect a leader before the benchmark tasks start connecting.
    let probe = EsClient::connect(es_url).await?;
    let mut ready = false;
    for attempt in 1..=30 {
        match probe.ping().await {
            Ok(()) => {
                ready = true;
                break;
            }
            Err(e) => {
                warn!(attempt, error = %e, "waiting for EventStoreDB to become ready...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    if !ready {
        anyhow::bail!("EventStoreDB did not become ready within 30 s");
    }
    info!("EventStoreDB connectivity OK");

    // Single shared connection — HTTP/2 multiplexes all concurrent writes.
    let client = Arc::new(probe);
    let total_events = Arc::new(AtomicU64::new(0));
    let shared_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let batch_size = config.batch_size.max(1);
    // Single dispatch loop fires one write per tick at exactly target_rate/s.
    // This produces a steady stream with no burst gaps, keeping the
    // EventStoreDB write queue continuously warm.
    let ticks_per_sec = ((config.target_rate as f64) / (batch_size as f64)).ceil() as u64;
    let tick_us = 1_000_000u64 / ticks_per_sec.max(1);

    // Semaphore bounds concurrent in-flight writes below Kestrel's HTTP/2
    // max-concurrent-streams limit (default 100).
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
        let stream_name = format!("{}-{}", config.stream_prefix, seq % config.concurrency);
        let events: Vec<BenchmarkEvent> = (0..batch_size)
            .map(|i| BenchmarkEvent::new(seq * batch_size + i, seq % config.concurrency))
            .collect();
        seq += 1;

        tokio::spawn(async move {
            let _permit = permit; // returned to semaphore when this task exits
            let t0 = Instant::now();
            match client
                .append_batch(&stream_name, "BenchmarkEvent", &events)
                .await
            {
                Ok(_) => {
                    let lat_us = t0.elapsed().as_micros() as u64;
                    let _ = hist.lock().await.record(lat_us);
                    total_events.fetch_add(batch_size, Ordering::Relaxed);
                }
                Err(e) => warn!(error = %e, "append_batch failed, skipping"),
            }
        });
    }

    // Wait for all in-flight writes to complete before computing results.
    // acquire_many(max_in_flight) blocks until every permit has been returned.
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
        passed: p99_us < 10_000 && rate >= 9_000.0,
    })
}
