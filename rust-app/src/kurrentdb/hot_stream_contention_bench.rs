use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use hdrhistogram::Histogram;
use kurrentdb::StreamState;
use rand::Rng;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{events::BenchmarkEvent, kurrentdb::client::KurrentClient};

pub struct HotStreamContentionConfig {
    pub target_rate: u64,
    pub baseline_duration_secs: u64,
    pub contention_duration_secs: u64,
    pub concurrency: u64,
    pub hot_streams: u64,
    pub cold_streams: u64,
    pub hot_ratio: f64,
    pub max_retries: u64,
    pub stream_prefix: String,
}

impl Default for HotStreamContentionConfig {
    fn default() -> Self {
        Self {
            target_rate: 8_000,
            baseline_duration_secs: 15,
            contention_duration_secs: 20,
            concurrency: 64,
            hot_streams: 4,
            cold_streams: 128,
            hot_ratio: 0.9,
            max_retries: 8,
            stream_prefix: "hot-stream".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct HotStreamContentionResult {
    pub baseline_total_events: u64,
    pub baseline_rate_eps: f64,
    pub baseline_p99_us: u64,
    pub contention_total_events: u64,
    pub contention_rate_eps: f64,
    pub contention_p99_us: u64,
    pub tail_latency_factor: f64,
    pub hot_events: u64,
    pub cold_events: u64,
    pub conflict_count: u64,
    pub retry_count: u64,
    pub retry_success_count: u64,
    pub retry_exhausted_count: u64,
    pub failed_writes: u64,
}

impl HotStreamContentionResult {
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════");
        println!("  KurrentDB Hot-Stream Contention Result");
        println!("══════════════════════════════════════════════");
        println!(
            "  Baseline rate / p99      : {:.1} ev/s / {} us",
            self.baseline_rate_eps, self.baseline_p99_us
        );
        println!(
            "  Contention rate / p99    : {:.1} ev/s / {} us",
            self.contention_rate_eps, self.contention_p99_us
        );
        println!(
            "  Tail latency factor      : {:.2}x",
            self.tail_latency_factor
        );
        println!(
            "  Hot / cold writes        : {} / {}",
            self.hot_events, self.cold_events
        );
        println!("  Conflicts                : {}", self.conflict_count);
        println!("  Retries                  : {}", self.retry_count);
        println!("  Retry successes          : {}", self.retry_success_count);
        println!(
            "  Retry exhausted          : {}",
            self.retry_exhausted_count
        );
        println!("  Failed writes            : {}", self.failed_writes);
        println!("══════════════════════════════════════════════");
        println!();
    }

    pub fn print_json(&self) {
        println!(
            r#"{{"backend":"kurrentdb","baseline_total_events":{baseline_total},"baseline_rate_eps":{baseline_rate:.1},"baseline_p99_us":{baseline_p99},"contention_total_events":{cont_total},"contention_rate_eps":{cont_rate:.1},"contention_p99_us":{cont_p99},"tail_latency_factor":{factor:.2},"hot_events":{hot_events},"cold_events":{cold_events},"conflict_count":{conflicts},"retry_count":{retries},"retry_success_count":{retry_ok},"retry_exhausted_count":{retry_exhausted},"failed_writes":{failed}}}"#,
            baseline_total = self.baseline_total_events,
            baseline_rate = self.baseline_rate_eps,
            baseline_p99 = self.baseline_p99_us,
            cont_total = self.contention_total_events,
            cont_rate = self.contention_rate_eps,
            cont_p99 = self.contention_p99_us,
            factor = self.tail_latency_factor,
            hot_events = self.hot_events,
            cold_events = self.cold_events,
            conflicts = self.conflict_count,
            retries = self.retry_count,
            retry_ok = self.retry_success_count,
            retry_exhausted = self.retry_exhausted_count,
            failed = self.failed_writes,
        );
    }
}

struct PhaseResult {
    total_events: u64,
    actual_rate_eps: f64,
    p99_us: u64,
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]
pub async fn run(
    kurrent_url: &str,
    config: HotStreamContentionConfig,
) -> Result<HotStreamContentionResult> {
    let hot_streams = config.hot_streams.max(1);
    let cold_streams = config.cold_streams.max(1);
    let hot_ratio = config.hot_ratio.clamp(0.0, 1.0);

    info!(
        target_rate = config.target_rate,
        baseline_duration = config.baseline_duration_secs,
        contention_duration = config.contention_duration_secs,
        concurrency = config.concurrency,
        hot_streams,
        cold_streams,
        hot_ratio,
        max_retries = config.max_retries,
        "starting hot-stream contention benchmark"
    );

    let probe = KurrentClient::connect(kurrent_url)?;
    let mut ready = false;
    for attempt in 1..=45 {
        match probe.ping().await {
            Ok(()) => {
                ready = true;
                break;
            }
            Err(e) => {
                warn!(attempt, error = %e, "waiting for KurrentDB to become ready...");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    if !ready {
        anyhow::bail!("KurrentDB did not become ready within 45 s");
    }

    let client = Arc::new(probe);
    let hot_revisions: Arc<Vec<AtomicU64>> = Arc::new(
        (0..hot_streams)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<AtomicU64>>(),
    );

    // Seed hot streams once and initialize expected revisions from server ACKs.
    for stream_id in 0..hot_streams {
        let stream = format!("{}-hot-{}", config.stream_prefix, stream_id);
        let seed_event = BenchmarkEvent::new(stream_id, stream_id);
        let next = client
            .append_with_stream_state(&stream, "BenchmarkEvent", &seed_event, StreamState::Any)
            .await?;
        hot_revisions[stream_id as usize].store(next, Ordering::Release);
    }

    let baseline = run_baseline_phase(Arc::clone(&client), &config).await?;

    let total_events = Arc::new(AtomicU64::new(0));
    let hot_events = Arc::new(AtomicU64::new(0));
    let cold_events = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let retries = Arc::new(AtomicU64::new(0));
    let retry_successes = Arc::new(AtomicU64::new(0));
    let retry_exhausted = Arc::new(AtomicU64::new(0));
    let failed_writes = Arc::new(AtomicU64::new(0));
    let contention_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let max_in_flight = usize::try_from(config.concurrency)
        .unwrap_or(usize::MAX)
        .min(96);
    let in_flight = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

    let tick_us = 1_000_000u64 / config.target_rate.max(1);
    let mut interval = tokio::time::interval(Duration::from_micros(tick_us));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let wall_start = Instant::now();
    let phase_duration = Duration::from_secs(config.contention_duration_secs.max(1));
    let mut seq: u64 = 0;

    while wall_start.elapsed() < phase_duration {
        interval.tick().await;
        let Ok(permit) = Arc::clone(&in_flight).try_acquire_owned() else {
            continue;
        };

        let client = Arc::clone(&client);
        let hot_revisions = Arc::clone(&hot_revisions);
        let total_events = Arc::clone(&total_events);
        let hot_events_ctr = Arc::clone(&hot_events);
        let cold_events_ctr = Arc::clone(&cold_events);
        let conflicts_ctr = Arc::clone(&conflicts);
        let retries_ctr = Arc::clone(&retries);
        let retry_successes_ctr = Arc::clone(&retry_successes);
        let retry_exhausted_ctr = Arc::clone(&retry_exhausted);
        let failed_writes_ctr = Arc::clone(&failed_writes);
        let hist = Arc::clone(&contention_hist);
        let prefix = config.stream_prefix.clone();
        let max_retries = config.max_retries;
        seq = seq.wrapping_add(1);
        let seq_now = seq;

        tokio::spawn(async move {
            let _permit = permit;
            let (is_hot, hot_idx, cold_idx) = {
                let mut rng = rand::thread_rng();
                let is_hot = rng.gen_bool(hot_ratio);
                let hot_idx = rng.gen_range(0..hot_streams);
                let cold_idx = rng.gen_range(0..cold_streams);
                (is_hot, hot_idx, cold_idx)
            };

            if is_hot {
                hot_events_ctr.fetch_add(1, Ordering::Relaxed);
                let stream_name = format!("{}-hot-{}", prefix, hot_idx);
                let hot_rev = &hot_revisions[hot_idx as usize];
                let ev = BenchmarkEvent::new(seq_now, hot_idx);

                let mut wrote = false;
                let t0 = Instant::now();
                for attempt in 0..=max_retries {
                    let expected = hot_rev.load(Ordering::Acquire);
                    let result = client
                        .append_with_stream_state(
                            &stream_name,
                            "BenchmarkEvent",
                            &ev,
                            StreamState::StreamRevision(expected),
                        )
                        .await;

                    match result {
                        Ok(next_rev) => {
                            let _ = hot_rev.compare_exchange(
                                expected,
                                next_rev,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            );

                            if attempt > 0 {
                                retries_ctr.fetch_add(attempt, Ordering::Relaxed);
                                retry_successes_ctr.fetch_add(1, Ordering::Relaxed);
                            }

                            let lat_us =
                                u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
                            let _ = hist.lock().await.record(lat_us);
                            total_events.fetch_add(1, Ordering::Relaxed);
                            wrote = true;
                            break;
                        }
                        Err(err) => {
                            // Use {:#} to get the full anyhow error chain, not just the
                            // outermost context message. Without this, the inner
                            // WrongExpectedVersion is hidden behind "append to stream '...' failed".
                            let msg = format!("{:#}", err).to_lowercase();
                            let is_conflict = msg.contains("wrongexpectedversion")
                                || msg.contains("wrong expected version")
                                || msg.contains("wrong expected")
                                || msg.contains("expected revision")
                                || msg.contains("expected version");

                            if is_conflict {
                                conflicts_ctr.fetch_add(1, Ordering::Relaxed);
                                retries_ctr.fetch_add(1, Ordering::Relaxed);
                                // Re-read current stream state to get latest revision
                                if let Ok(Some(current)) =
                                    client.read_last_event(&stream_name).await
                                {
                                    hot_rev.store(current.1, Ordering::Release);
                                } else {
                                    // If read fails, reset to zero and let server assign
                                    hot_rev.store(0, Ordering::Release);
                                }
                                continue;
                            }

                            warn!(error = %err, stream = %stream_name, attempt = attempt, "hot write failed");
                            failed_writes_ctr.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }

                if !wrote {
                    retry_exhausted_ctr.fetch_add(1, Ordering::Relaxed);
                    failed_writes_ctr.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                cold_events_ctr.fetch_add(1, Ordering::Relaxed);
                let stream_name = format!("{}-cold-{}", prefix, cold_idx);
                let ev = BenchmarkEvent::new(seq_now, cold_idx);
                let t0 = Instant::now();
                match client.append(&stream_name, "BenchmarkEvent", &ev).await {
                    Ok(_) => {
                        let lat_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
                        let _ = hist.lock().await.record(lat_us);
                        total_events.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!(error = %e, stream = %stream_name, "cold write failed");
                        failed_writes_ctr.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    let _ = in_flight
        .acquire_many(u32::try_from(max_in_flight).unwrap_or(96))
        .await;

    let elapsed = wall_start.elapsed().as_secs_f64();
    let contention_total = total_events.load(Ordering::Relaxed);
    let contention_rate = if elapsed > 0.0 {
        contention_total as f64 / elapsed
    } else {
        0.0
    };
    let contention_hist = contention_hist.lock().await;
    let contention_p99 = contention_hist.value_at_quantile(0.99);

    let tail_latency_factor = if baseline.p99_us > 0 {
        contention_p99 as f64 / baseline.p99_us as f64
    } else {
        0.0
    };

    Ok(HotStreamContentionResult {
        baseline_total_events: baseline.total_events,
        baseline_rate_eps: baseline.actual_rate_eps,
        baseline_p99_us: baseline.p99_us,
        contention_total_events: contention_total,
        contention_rate_eps: contention_rate,
        contention_p99_us: contention_p99,
        tail_latency_factor,
        hot_events: hot_events.load(Ordering::Relaxed),
        cold_events: cold_events.load(Ordering::Relaxed),
        conflict_count: conflicts.load(Ordering::Relaxed),
        retry_count: retries.load(Ordering::Relaxed),
        retry_success_count: retry_successes.load(Ordering::Relaxed),
        retry_exhausted_count: retry_exhausted.load(Ordering::Relaxed),
        failed_writes: failed_writes.load(Ordering::Relaxed),
    })
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
async fn run_baseline_phase(
    client: Arc<KurrentClient>,
    config: &HotStreamContentionConfig,
) -> Result<PhaseResult> {
    let total_events = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));

    let max_in_flight = usize::try_from(config.concurrency)
        .unwrap_or(usize::MAX)
        .min(96);
    let in_flight = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

    let tick_us = 1_000_000u64 / config.target_rate.max(1);
    let mut interval = tokio::time::interval(Duration::from_micros(tick_us));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let phase_duration = Duration::from_secs(config.baseline_duration_secs.max(1));
    let wall_start = Instant::now();
    let mut seq = 0u64;

    while wall_start.elapsed() < phase_duration {
        interval.tick().await;
        let Ok(permit) = Arc::clone(&in_flight).try_acquire_owned() else {
            continue;
        };

        let client = Arc::clone(&client);
        let total = Arc::clone(&total_events);
        let hist = Arc::clone(&hist);
        let stream_name = format!(
            "{}-baseline-{}",
            config.stream_prefix,
            seq % config.cold_streams.max(1)
        );
        let ev = BenchmarkEvent::new(seq, seq % config.concurrency.max(1));
        seq = seq.wrapping_add(1);

        tokio::spawn(async move {
            let _permit = permit;
            let t0 = Instant::now();
            match client.append(&stream_name, "BenchmarkEvent", &ev).await {
                Ok(_) => {
                    let lat_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
                    let _ = hist.lock().await.record(lat_us);
                    total.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => warn!(error = %e, "baseline append failed"),
            }
        });
    }

    let _ = in_flight
        .acquire_many(u32::try_from(max_in_flight).unwrap_or(96))
        .await;

    let elapsed = wall_start.elapsed().as_secs_f64();
    let total = total_events.load(Ordering::Relaxed);
    let rate = if elapsed > 0.0 {
        total as f64 / elapsed
    } else {
        0.0
    };
    let hist = hist.lock().await;

    Ok(PhaseResult {
        total_events: total,
        actual_rate_eps: rate,
        p99_us: hist.value_at_quantile(0.99),
    })
}
