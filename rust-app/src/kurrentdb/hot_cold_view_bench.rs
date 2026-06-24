//! Hot/Cold view benchmark — KurrentDB onboard features only, no external cache.
//!
//! Tests two complementary hot/cold view mechanisms that are native to KurrentDB:
//!
//! ## Section A — `$maxCount` stream metadata (server-enforced sliding window)
//!
//! KurrentDB lets you attach metadata to any stream.  The `$maxCount` field
//! instructs the server to keep only the last N events in that stream; older
//! events are scavenged automatically.  This is the simplest possible hot-view:
//! the database itself enforces the window, with no application-side cache,
//! no external Redis/Memcached, and no background worker.
//!
//! Steps:
//! 1. Write `seed_events` to a **cold** stream (no limits).
//! 2. Set `$maxCount = hot_window` on a **hot** stream and write the same events.
//! 3. Read cold stream → full `seed_events` events.
//! 4. Read hot  stream → only last `hot_window` events (server truncated the rest).
//! 5. Compare per-event read latency and total event counts.
//!
//! ## Section B — Catch-up subscriptions (cold-start vs live-only)
//!
//! KurrentDB's `subscribe_to_stream` can begin from any position:
//!
//! * `StreamPosition::Start` — **cold view**: replay the entire history, then
//!   transition to live delivery.  Time-to-ready scales with event count.
//! * `StreamPosition::End`  — **hot view**: skip all history and receive only
//!   events written *after* the subscription is established.  Time-to-ready
//!   is effectively zero regardless of stream depth.
//!
//! Steps:
//! 1. Cold subscription: start from `Start` on the cold stream; measure replay
//!    throughput (events/s) and total replay duration.
//! 2. Hot  subscription: start from `End`, then write `live_writes` events one
//!    at a time; measure write-ack → subscription-delivery lag (p50/p95/p99).

use std::time::Instant;

use anyhow::Result;
use hdrhistogram::Histogram;
use kurrentdb::{
    AppendToStreamOptions, StreamMetadata, StreamPosition, StreamState, SubscribeToStreamOptions,
};
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;

use crate::{events::BenchmarkEvent, kurrentdb::client::KurrentClient};

// ─── Configuration ────────────────────────────────────────────────────────────

pub struct HotColdViewConfig {
    /// Events written during the seed phase.
    pub seed_events: usize,
    /// Number of most-recent events the hot stream is capped at via `$maxCount`.
    pub hot_window: usize,
    /// Events written one-at-a-time in the live-subscription lag phase.
    pub live_writes: usize,
    /// Batch size used when seeding (larger = faster seeding).
    pub seed_batch: usize,
    /// Base name for streams created by this run.
    pub stream_prefix: String,
}

impl Default for HotColdViewConfig {
    fn default() -> Self {
        Self {
            seed_events: 20_000,
            hot_window: 500,
            live_writes: 200,
            seed_batch: 200,
            stream_prefix: "hot-cold-view".to_string(),
        }
    }
}

// ─── Result ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct HotColdViewResult {
    pub seed_events: usize,
    pub hot_window: usize,

    // Section A — $maxCount
    pub cold_stream_event_count: usize,
    pub hot_stream_event_count: usize,
    pub cold_read_elapsed_us: u64,
    pub hot_read_elapsed_us: u64,
    pub cold_read_per_event_us: f64,
    pub hot_read_per_event_us: f64,

    // Section B — catch-up subscription
    pub cold_sub_replay_ms: f64,
    pub cold_sub_replay_rate_eps: f64,
    pub hot_sub_lag_p50_us: u64,
    pub hot_sub_lag_p95_us: u64,
    pub hot_sub_lag_p99_us: u64,
    pub hot_sub_lag_max_us: u64,
    pub live_writes: usize,
}

impl HotColdViewResult {
    #[allow(clippy::cast_precision_loss)]
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════════════════════");
        println!("  KurrentDB Hot/Cold View Benchmark (onboard features only)");
        println!(
            "  seed={} events  |  hot_window={} events",
            self.seed_events, self.hot_window
        );
        println!("══════════════════════════════════════════════════════════════");
        println!();
        println!("  SECTION A — $maxCount stream metadata (server-side window)");
        println!("  ────────────────────────────────────────────────────────────");
        println!(
            "  Cold stream events read : {}  ({} µs total, {:.2} µs/event)",
            self.cold_stream_event_count, self.cold_read_elapsed_us, self.cold_read_per_event_us
        );
        println!(
            "  Hot  stream events read : {}  ({} µs total, {:.2} µs/event)",
            self.hot_stream_event_count, self.hot_read_elapsed_us, self.hot_read_per_event_us
        );
        println!(
            "  Events scavenged by DB  : {}  (= seed - hot_window, server-enforced)",
            self.cold_stream_event_count
                .saturating_sub(self.hot_stream_event_count)
        );
        println!(
            "  Cold/Hot read speedup   : {:.1}×  (fewer events = faster reads)",
            self.cold_read_elapsed_us as f64 / self.hot_read_elapsed_us.max(1) as f64
        );
        println!();
        println!("  SECTION B — Catch-up subscriptions");
        println!("  ────────────────────────────────────────────────────────────");
        println!("  Cold view (start from beginning):");
        println!(
            "    Replay {seed} events     : {ms:.1} ms  ({rate:.0} ev/s)",
            seed = self.seed_events,
            ms = self.cold_sub_replay_ms,
            rate = self.cold_sub_replay_rate_eps
        );
        println!();
        println!(
            "  Hot view (start from end, {} live writes):",
            self.live_writes
        );
        println!("    Lag p50 : {} µs", self.hot_sub_lag_p50_us);
        println!("    Lag p95 : {} µs", self.hot_sub_lag_p95_us);
        println!("    Lag p99 : {} µs", self.hot_sub_lag_p99_us);
        println!("    Lag max : {} µs", self.hot_sub_lag_max_us);
        println!();
        println!(
            "  Hot sub catches up in 0 ms (no history) vs {:.1} ms for cold start.",
            self.cold_sub_replay_ms
        );
        println!("══════════════════════════════════════════════════════════════");
        println!();
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn print_json(&self) {
        println!(
            r#"{{"seed_events":{seed},"hot_window":{hw},"cold_stream_event_count":{cec},"hot_stream_event_count":{hec},"cold_read_elapsed_us":{cru},"hot_read_elapsed_us":{hru},"cold_read_per_event_us":{crpe:.3},"hot_read_per_event_us":{hrpe:.3},"cold_sub_replay_ms":{csrm:.1},"cold_sub_replay_rate_eps":{csrr:.0},"hot_sub_lag_p50_us":{p50},"hot_sub_lag_p95_us":{p95},"hot_sub_lag_p99_us":{p99},"hot_sub_lag_max_us":{pmax},"live_writes":{lw}}}"#,
            seed = self.seed_events,
            hw = self.hot_window,
            cec = self.cold_stream_event_count,
            hec = self.hot_stream_event_count,
            cru = self.cold_read_elapsed_us,
            hru = self.hot_read_elapsed_us,
            crpe = self.cold_read_per_event_us,
            hrpe = self.hot_read_per_event_us,
            csrm = self.cold_sub_replay_ms,
            csrr = self.cold_sub_replay_rate_eps,
            p50 = self.hot_sub_lag_p50_us,
            p95 = self.hot_sub_lag_p95_us,
            p99 = self.hot_sub_lag_p99_us,
            pmax = self.hot_sub_lag_max_us,
            lw = self.live_writes,
        );
    }
}

// ─── Run ──────────────────────────────────────────────────────────────────────

#[allow(clippy::cast_precision_loss)]
pub async fn run(kurrentdb_url: &str, cfg: HotColdViewConfig) -> Result<HotColdViewResult> {
    let client = KurrentClient::connect(kurrentdb_url)?;
    let raw = client.inner().clone();

    let cold_stream = format!("{}-cold", cfg.stream_prefix);
    let hot_stream = format!("{}-hot", cfg.stream_prefix);

    // ── Set $maxCount metadata on the hot stream BEFORE any writes ────────────
    info!(
        "hot-cold-view: setting $maxCount={} on '{}'",
        cfg.hot_window, hot_stream
    );
    let metadata = StreamMetadata::builder()
        .max_count(cfg.hot_window as u64)
        .build();
    raw.set_stream_metadata(
        hot_stream.as_str(),
        &AppendToStreamOptions::default().stream_state(StreamState::Any),
        &metadata,
    )
    .await
    .map_err(|e| anyhow::anyhow!("set_stream_metadata failed: {e}"))?;

    // ── Seed both streams ─────────────────────────────────────────────────────
    info!(
        "hot-cold-view: seeding {} events ({} per batch) …",
        cfg.seed_events, cfg.seed_batch
    );
    let append_opts = AppendToStreamOptions::default().stream_state(StreamState::Any);
    let mut seq = 0u64;
    let mut offset = 0usize;

    while offset < cfg.seed_events {
        let batch_len = cfg.seed_batch.min(cfg.seed_events - offset);
        let events: Result<Vec<kurrentdb::EventData>> = (0..batch_len)
            .map(|_| {
                seq += 1;
                let ev = BenchmarkEvent::new(seq, 0);
                Ok(kurrentdb::EventData::json("BenchmarkEvent", &ev)
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .id(Uuid::new_v4()))
            })
            .collect();
        let batch = events?;

        // Rebuild the same payloads for the hot stream (EventData is not Clone).
        let mut seq_hot = seq - batch_len as u64;
        let hot_events: Result<Vec<kurrentdb::EventData>> = (0..batch_len)
            .map(|_| {
                seq_hot += 1;
                let ev = BenchmarkEvent::new(seq_hot, 0);
                Ok(kurrentdb::EventData::json("BenchmarkEvent", &ev)
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .id(Uuid::new_v4()))
            })
            .collect();

        raw.append_to_stream(cold_stream.as_str(), &append_opts, batch)
            .await
            .map_err(|e| anyhow::anyhow!("cold seed append failed: {e}"))?;
        raw.append_to_stream(hot_stream.as_str(), &append_opts, hot_events?)
            .await
            .map_err(|e| anyhow::anyhow!("hot seed append failed: {e}"))?;

        offset += batch_len;
    }
    info!("hot-cold-view: seed complete ({} events written)", seq);

    // ═══════════════════════════════════════════════════════════════════════════
    // SECTION A — $maxCount: compare direct stream reads
    // ═══════════════════════════════════════════════════════════════════════════

    // Cold read — full stream, no truncation.
    let t_cold_read = Instant::now();
    let cold_events = client.read_stream_events(cold_stream.as_str()).await?;
    let cold_read_elapsed_us = t_cold_read.elapsed().as_micros() as u64;
    let cold_count = cold_events.len();

    // Hot read — KurrentDB enforces $maxCount, only the tail survives.
    let t_hot_read = Instant::now();
    let hot_events = client.read_stream_events(hot_stream.as_str()).await?;
    let hot_read_elapsed_us = t_hot_read.elapsed().as_micros() as u64;
    let hot_count = hot_events.len();

    info!(
        "hot-cold-view §A: cold={} events in {} µs, hot={} events in {} µs",
        cold_count, cold_read_elapsed_us, hot_count, hot_read_elapsed_us
    );

    let cold_read_per_event_us = if cold_count > 0 {
        cold_read_elapsed_us as f64 / cold_count as f64
    } else {
        0.0
    };
    let hot_read_per_event_us = if hot_count > 0 {
        hot_read_elapsed_us as f64 / hot_count as f64
    } else {
        0.0
    };

    // ═══════════════════════════════════════════════════════════════════════════
    // SECTION B — Subscriptions: cold-start replay vs live-only hot start
    // ═══════════════════════════════════════════════════════════════════════════

    // ── B1: Cold catch-up subscription ────────────────────────────────────────
    // Start a subscription from the very beginning of the cold stream and
    // wait until all seed_events have been delivered.  This simulates a
    // projector or read-model that rebuilds from scratch (e.g. on first deploy
    // or after a re-index).
    info!(
        "hot-cold-view §B1: cold catch-up subscription — replaying {} events …",
        cfg.seed_events
    );

    let seed_count = cfg.seed_events as u64;
    let (progress_tx, progress_rx) = watch::channel(0u64);
    let raw_sub = raw.clone();
    let cold_stream_sub = cold_stream.clone();

    let t_cold_sub = Instant::now();

    let cold_projector = tokio::spawn(async move {
        let opts = SubscribeToStreamOptions::default().start_from(StreamPosition::Start);
        let mut sub = raw_sub
            .subscribe_to_stream(cold_stream_sub.as_str(), &opts)
            .await;
        let mut count = 0u64;
        loop {
            match sub.next().await {
                Ok(ev) => {
                    let recorded = ev.get_original_event();
                    if serde_json::from_slice::<BenchmarkEvent>(&recorded.data).is_ok() {
                        count += 1;
                        let _ = progress_tx.send(count);
                        if count >= seed_count {
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("cold subscription error: {e}");
                    break;
                }
            }
        }
        count
    });

    // Wait for the projector to catch up.
    {
        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= seed_count {
                break;
            }
            rx.changed().await?;
        }
    }
    let cold_sub_elapsed = t_cold_sub.elapsed();
    let cold_sub_replay_ms = cold_sub_elapsed.as_secs_f64() * 1000.0;
    let cold_sub_replay_rate_eps =
        cfg.seed_events as f64 / cold_sub_elapsed.as_secs_f64().max(f64::EPSILON);

    cold_projector.abort();

    info!(
        "hot-cold-view §B1: cold replay done — {:.1} ms  ({:.0} ev/s)",
        cold_sub_replay_ms, cold_sub_replay_rate_eps
    );

    // ── B2: Hot live-only subscription ────────────────────────────────────────
    // Start the subscription from `End`.  The server will not deliver any
    // historical events — only events written *after* this subscription was
    // created.  This is how KurrentDB powers live dashboards and low-latency
    // read models that only care about the "now".
    info!(
        "hot-cold-view §B2: hot live-only subscription — writing {} events and measuring lag …",
        cfg.live_writes
    );

    let (lag_tx, mut lag_rx) = tokio::sync::mpsc::channel::<u64>(cfg.live_writes + 16);
    let raw_live = raw.clone();
    let cold_stream_live = cold_stream.clone();

    let hot_projector = tokio::spawn(async move {
        let opts = SubscribeToStreamOptions::default().start_from(StreamPosition::End);
        let mut sub = raw_live
            .subscribe_to_stream(cold_stream_live.as_str(), &opts)
            .await;
        loop {
            match sub.next().await {
                Ok(ev) => {
                    let recorded = ev.get_original_event();
                    // The BenchmarkEvent carries a `written_at_ns` timestamp we
                    // use to compute end-to-end lag.
                    if let Ok(bench_ev) = serde_json::from_slice::<BenchmarkEvent>(&recorded.data) {
                        // Lag = now - created_at (set immediately before the
                        // append call, so this captures write-ack + delivery).
                        let lag_us = chrono::Utc::now()
                            .signed_duration_since(bench_ev.created_at)
                            .num_microseconds()
                            .unwrap_or(0)
                            .unsigned_abs()
                            .max(1);
                        let _ = lag_tx.send(lag_us).await;
                    }
                }
                Err(e) => {
                    tracing::warn!("hot subscription error: {e}");
                    break;
                }
            }
        }
    });

    // Give the subscription a moment to establish before writing.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut lag_hist: Histogram<u64> = Histogram::new(4)?;
    seq += 1; // continue from where the seed left off

    for _ in 0..cfg.live_writes {
        let ev = BenchmarkEvent::new_with_timestamp(seq);
        seq += 1;
        let event_data = kurrentdb::EventData::json("BenchmarkEvent", &ev)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .id(Uuid::new_v4());

        raw.append_to_stream(cold_stream.as_str(), &append_opts, vec![event_data])
            .await
            .map_err(|e| anyhow::anyhow!("live write failed: {e}"))?;

        // Collect the lag value (already in µs) from the subscription receiver.
        match tokio::time::timeout(std::time::Duration::from_secs(5), lag_rx.recv()).await {
            Ok(Some(lag_us)) => {
                lag_hist.record(lag_us)?;
            }
            Ok(None) => break,
            Err(_) => {
                tracing::warn!("hot subscription lag timeout on event {}", seq);
                lag_hist.record(5_000_000)?; // 5 s sentinel
            }
        }
    }

    hot_projector.abort();

    info!(
        "hot-cold-view §B2: lag p50={} µs  p99={} µs  max={} µs",
        lag_hist.value_at_quantile(0.5),
        lag_hist.value_at_quantile(0.99),
        lag_hist.max()
    );

    Ok(HotColdViewResult {
        seed_events: cfg.seed_events,
        hot_window: cfg.hot_window,
        cold_stream_event_count: cold_count,
        hot_stream_event_count: hot_count,
        cold_read_elapsed_us,
        hot_read_elapsed_us,
        cold_read_per_event_us,
        hot_read_per_event_us,
        cold_sub_replay_ms,
        cold_sub_replay_rate_eps,
        hot_sub_lag_p50_us: lag_hist.value_at_quantile(0.5),
        hot_sub_lag_p95_us: lag_hist.value_at_quantile(0.95),
        hot_sub_lag_p99_us: lag_hist.value_at_quantile(0.99),
        hot_sub_lag_max_us: lag_hist.max(),
        live_writes: cfg.live_writes,
    })
}
