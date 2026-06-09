//! Projection / subscription-lag benchmark.
//!
//! Measures the three properties that directly map to the UI requirement
//! "500 most-recent orders always visible immediately":
//!
//! 1. **Cold-start rebuild** — projector starts from event position 0,
//!    replays all `seed_events` historical events, and populates the
//!    materialised view.  Time to ready.
//!
//! 2. **Subscription lag** — after cold-start, the writer appends
//!    `live_events` one at a time.  For each event: time from write-ack
//!    → view-updated (p50 / p95 / p99).
//!
//! 3. **View read latency** — time to read the materialised view 1 000
//!    times (p50 / p99).  Represents what the BFF does on every UI request.
//!
//! The materialised view is an in-process `Mutex<VecDeque<u64>>` capped at
//! `view_size` entries — structurally identical to a Memcached sorted-set key.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::Result;
use hdrhistogram::Histogram;
use serde_json::json;
use tracing::info;

use crate::events::BenchmarkEvent;

// ── CLI args ──────────────────────────────────────────────────────────────────

#[derive(clap::Parser, Debug)]
pub struct ProjectionBenchArgs {
    /// Events to seed before the test (used for cold-start replay).
    #[arg(long, default_value_t = 10_000)]
    pub seed_events: usize,

    /// Events to write during the live subscription-lag phase.
    #[arg(long, default_value_t = 500)]
    pub live_events: usize,

    /// Maximum entries maintained in the materialised view.
    #[arg(long, default_value_t = 500)]
    pub view_size: usize,

    /// Stream / collection name.
    #[arg(long, default_value = "proj-bench-stream")]
    pub stream_name: String,

    /// Polling interval in milliseconds (PostgreSQL only).
    #[arg(long, default_value_t = 1)]
    pub poll_interval_ms: u64,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug)]
pub struct ProjectionBenchArgs2 {
    pub seed_events: usize,
    pub live_events: usize,
    pub view_size: usize,
    pub stream_name: String,
    pub poll_interval_ms: u64,
    pub json: bool,
    pub database: String,
}

// ── MongoDB-specific args ─────────────────────────────────────────────────────

#[derive(clap::Parser, Debug)]
pub struct MongoProjectionBenchArgs {
    /// Events to seed before the test.
    #[arg(long, default_value_t = 10_000)]
    pub seed_events: usize,

    /// Events to write during the live subscription-lag phase.
    #[arg(long, default_value_t = 500)]
    pub live_events: usize,

    /// Maximum entries maintained in the materialised view.
    #[arg(long, default_value_t = 500)]
    pub view_size: usize,

    /// Collection name.
    #[arg(long, default_value = "proj-bench-stream")]
    pub stream_name: String,

    /// MongoDB database name.
    #[arg(long, default_value = "projbench")]
    pub database: String,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

// ── Result ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ProjectionBenchResult {
    pub backend: String,
    pub seed_events: usize,
    pub view_size: usize,
    pub cold_start_ms: f64,
    pub lag_p50_us: u64,
    pub lag_p95_us: u64,
    pub lag_p99_us: u64,
    pub lag_max_us: u64,
    pub view_read_p50_ns: u64,
    pub view_read_p99_ns: u64,
}

// ── Shared materialised view ──────────────────────────────────────────────────

type View = Arc<Mutex<VecDeque<u64>>>;

fn new_view(capacity: usize) -> View {
    Arc::new(Mutex::new(VecDeque::with_capacity(capacity + 1)))
}

fn push_to_view(view: &View, seq: u64, capacity: usize) {
    let mut v = view.lock().unwrap();
    if v.len() == capacity {
        v.pop_front();
    }
    v.push_back(seq);
}

fn read_view(view: &View) -> Vec<u64> {
    view.lock().unwrap().iter().copied().collect()
}

// ── KurrentDB ────────────────────────────────────────────────────────────────

pub async fn run_kurrentdb(
    kurrentdb_url: &str,
    args: &ProjectionBenchArgs,
) -> Result<ProjectionBenchResult> {
    use kurrentdb::{AppendToStreamOptions, StreamPosition, StreamState, SubscribeToStreamOptions};

    let writer = crate::kurrentdb::client::KurrentClient::connect(kurrentdb_url)?;
    let stream = args.stream_name.as_str();

    info!(
        "projection/kurrentdb: seeding {} events …",
        args.seed_events
    );

    // ── Seed phase ────────────────────────────────────────────────────────────
    let batch_size = 100usize;
    let mut global_seq = 0u64;
    let opts = AppendToStreamOptions::default().stream_state(StreamState::Any);
    for _ in 0..(args.seed_events / batch_size) {
        let events: Result<Vec<kurrentdb::EventData>> = (0..batch_size)
            .map(|_| {
                global_seq += 1;
                let ev = BenchmarkEvent::new(global_seq, 0);
                Ok(kurrentdb::EventData::json("BenchmarkEvent", &ev)
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .id(uuid::Uuid::new_v4()))
            })
            .collect();
        writer
            .inner()
            .append_to_stream(stream, &opts, events?)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    // handle remainder
    let remainder = args.seed_events % batch_size;
    if remainder > 0 {
        let events: Result<Vec<kurrentdb::EventData>> = (0..remainder)
            .map(|_| {
                global_seq += 1;
                let ev = BenchmarkEvent::new(global_seq, 0);
                Ok(kurrentdb::EventData::json("BenchmarkEvent", &ev)
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .id(uuid::Uuid::new_v4()))
            })
            .collect();
        writer
            .inner()
            .append_to_stream(stream, &opts, events?)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    info!("projection/kurrentdb: starting catch-up subscription …");

    // ── Cold-start: subscribe from the beginning, replay all seed events ──────
    let view = new_view(args.view_size);
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0u64);

    let view_for_proj = Arc::clone(&view);
    let view_size = args.view_size;
    let seed_count = args.seed_events as u64;
    let sub_client = writer.inner().clone();
    let sub_stream = args.stream_name.clone();
    let tx = progress_tx.clone();

    let t_cold = Instant::now();

    let projector = tokio::spawn(async move {
        let sub_opts = SubscribeToStreamOptions::default().start_from(StreamPosition::Start);
        let mut sub = sub_client
            .subscribe_to_stream(sub_stream.as_str(), &sub_opts)
            .await;
        let mut processed = 0u64;
        loop {
            match sub.next().await {
                Ok(ev) => {
                    let recorded = ev.get_original_event();
                    if let Ok(bench_ev) = serde_json::from_slice::<BenchmarkEvent>(&recorded.data) {
                        push_to_view(&view_for_proj, bench_ev.seq, view_size);
                        processed += 1;
                        let _ = tx.send(processed);
                    }
                }
                Err(e) => {
                    tracing::warn!("kurrentdb subscription error: {e}");
                    break;
                }
            }
        }
    });

    // Wait for all seed events to be processed
    {
        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= seed_count {
                break;
            }
            rx.changed().await?;
        }
    }
    let cold_start_ms = t_cold.elapsed().as_secs_f64() * 1000.0;
    info!(
        "projection/kurrentdb: cold-start done — {:.1} ms for {} events",
        cold_start_ms, args.seed_events
    );

    // ── Live subscription-lag phase ───────────────────────────────────────────
    let mut lag_hist: Histogram<u64> = Histogram::new(4)?;
    let mut expected = seed_count;

    for i in 0..args.live_events {
        global_seq += 1;
        let ev = BenchmarkEvent::new(global_seq, 0);
        let event_data = kurrentdb::EventData::json("BenchmarkEvent", &ev)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .id(uuid::Uuid::new_v4());

        writer
            .inner()
            .append_to_stream(stream, &opts, vec![event_data])
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let t_ack = Instant::now();
        expected += 1;

        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= expected {
                break;
            }
            rx.changed().await?;
        }
        let lag_us = t_ack.elapsed().as_micros() as u64;
        lag_hist.record(lag_us.max(1))?;

        if i % 100 == 0 && i > 0 {
            info!(
                "projection/kurrentdb: live-write {}/{} done, lag p50 {} µs",
                i,
                args.live_events,
                lag_hist.value_at_quantile(0.5)
            );
        }
    }

    projector.abort();

    // ── View read phase ───────────────────────────────────────────────────────
    let mut view_hist: Histogram<u64> = Histogram::new(4)?;
    for _ in 0..1_000 {
        let t = Instant::now();
        let _ = read_view(&view);
        let ns = t.elapsed().as_nanos() as u64;
        view_hist.record(ns.max(1))?;
    }

    Ok(ProjectionBenchResult {
        backend: "KurrentDB".to_string(),
        seed_events: args.seed_events,
        view_size: args.view_size,
        cold_start_ms,
        lag_p50_us: lag_hist.value_at_quantile(0.5),
        lag_p95_us: lag_hist.value_at_quantile(0.95),
        lag_p99_us: lag_hist.value_at_quantile(0.99),
        lag_max_us: lag_hist.max(),
        view_read_p50_ns: view_hist.value_at_quantile(0.5),
        view_read_p99_ns: view_hist.value_at_quantile(0.99),
    })
}

// ── MongoDB ───────────────────────────────────────────────────────────────────

pub async fn run_mongo(
    mongodb_url: &str,
    args: &MongoProjectionBenchArgs,
) -> Result<ProjectionBenchResult> {
    use futures::StreamExt as _;
    use mongodb::bson::{doc, Document};

    let writer = crate::mongodb::client::MongoClient::connect(mongodb_url, &args.database).await?;
    let stream = &args.stream_name;

    // Ensure collection exists (simple, no event-store validation needed here)
    writer.ensure_collection(stream).await?;
    writer.truncate_collection(stream).await?;

    info!("projection/mongodb: seeding {} events …", args.seed_events);

    // ── Seed phase ────────────────────────────────────────────────────────────
    let batch_size = 100usize;
    let mut global_seq = 0u64;
    for _ in 0..(args.seed_events / batch_size) {
        let payloads: Vec<BenchmarkEvent> = (0..batch_size)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        writer
            .append_batch(stream, "BenchmarkEvent", &payloads)
            .await?;
    }
    let remainder = args.seed_events % batch_size;
    if remainder > 0 {
        let payloads: Vec<BenchmarkEvent> = (0..remainder)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        writer
            .append_batch(stream, "BenchmarkEvent", &payloads)
            .await?;
    }

    info!("projection/mongodb: starting change stream + cold-start replay …");

    // ── Cold-start: read historical events via find(), then switch to watch ───
    //
    // Phase A: replay all existing documents ordered by insertion order (_id).
    // Phase B: open change stream for live delivery.
    //
    // This matches the production pattern: catch-up via cursor, then live via
    // Change Stream.

    let view = new_view(args.view_size);
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0u64);

    let view_for_proj = Arc::clone(&view);
    let view_size = args.view_size;
    let seed_count = args.seed_events as u64;
    let db_url = mongodb_url.to_string();
    let db_name = args.database.clone();
    let coll_name = stream.clone();
    let tx = progress_tx.clone();

    let t_cold = Instant::now();

    let projector = tokio::spawn(async move {
        // Phase A: replay historical documents
        let client = crate::mongodb::client::MongoClient::connect(&db_url, &db_name)
            .await
            .expect("projector mongo connect");
        let coll: mongodb::Collection<Document> = client.database().collection(&coll_name);

        let mut cursor = coll
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .await
            .expect("find cursor");
        let mut processed = 0u64;
        while let Some(result) = cursor.next().await {
            if let Ok(doc) = result {
                if let Ok(seq) = doc.get_i64("seq") {
                    push_to_view(&view_for_proj, seq as u64, view_size);
                    processed += 1;
                    let _ = tx.send(processed);
                }
            }
        }

        // Phase B: open change stream from now, receive live events
        let mut cs = coll.watch().await.expect("watch");
        loop {
            match cs.next().await {
                Some(Ok(event)) => {
                    if let Some(doc) = event.full_document {
                        if let Ok(seq) = doc.get_i64("seq") {
                            push_to_view(&view_for_proj, seq as u64, view_size);
                            processed += 1;
                            let _ = tx.send(processed);
                        }
                    }
                }
                Some(Err(e)) => {
                    tracing::warn!("mongodb change stream error: {e}");
                    break;
                }
                None => break,
            }
        }
    });

    // Wait for all seed events to be replayed
    {
        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= seed_count {
                break;
            }
            rx.changed().await?;
        }
    }
    let cold_start_ms = t_cold.elapsed().as_secs_f64() * 1000.0;
    info!(
        "projection/mongodb: cold-start done — {:.1} ms for {} events",
        cold_start_ms, args.seed_events
    );

    // ── Live subscription-lag phase ───────────────────────────────────────────
    let mut lag_hist: Histogram<u64> = Histogram::new(4)?;
    let mut expected = seed_count;

    for i in 0..args.live_events {
        global_seq += 1;
        let payload = BenchmarkEvent::new(global_seq, 0);
        writer.append(stream, "BenchmarkEvent", &payload).await?;

        let t_ack = Instant::now();
        expected += 1;

        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= expected {
                break;
            }
            rx.changed().await?;
        }
        let lag_us = t_ack.elapsed().as_micros() as u64;
        lag_hist.record(lag_us.max(1))?;

        if i % 100 == 0 && i > 0 {
            info!(
                "projection/mongodb: live-write {}/{} done, lag p50 {} µs",
                i,
                args.live_events,
                lag_hist.value_at_quantile(0.5)
            );
        }
    }

    projector.abort();

    // ── View read phase ───────────────────────────────────────────────────────
    let mut view_hist: Histogram<u64> = Histogram::new(4)?;
    for _ in 0..1_000 {
        let t = Instant::now();
        let _ = read_view(&view);
        let ns = t.elapsed().as_nanos() as u64;
        view_hist.record(ns.max(1))?;
    }

    Ok(ProjectionBenchResult {
        backend: "MongoDB (change stream)".to_string(),
        seed_events: args.seed_events,
        view_size: args.view_size,
        cold_start_ms,
        lag_p50_us: lag_hist.value_at_quantile(0.5),
        lag_p95_us: lag_hist.value_at_quantile(0.95),
        lag_p99_us: lag_hist.value_at_quantile(0.99),
        lag_max_us: lag_hist.max(),
        view_read_p50_ns: view_hist.value_at_quantile(0.5),
        view_read_p99_ns: view_hist.value_at_quantile(0.99),
    })
}

// ── PostgreSQL ────────────────────────────────────────────────────────────────

pub async fn run_postgres(
    pg_url: &str,
    args: &ProjectionBenchArgs,
) -> Result<ProjectionBenchResult> {
    let writer = crate::postgres::client::PostgresClient::connect(pg_url).await?;
    let stream = &args.stream_name;

    // Use the event-store table (provides global_position for polling).
    writer.ensure_bench_table_event_store().await?;
    writer.ensure_stream_versions_table().await?;
    // Truncate to start clean (TRUNCATE RESTART IDENTITY resets global_position).
    writer.truncate_bench_table().await?;

    info!("projection/postgres: seeding {} events …", args.seed_events);

    // ── Seed phase ────────────────────────────────────────────────────────────
    let batch_size = 100usize;
    let mut global_seq = 0u64;
    for _ in 0..(args.seed_events / batch_size) {
        let payloads: Vec<BenchmarkEvent> = (0..batch_size)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        writer
            .append_batch_versioned(
                stream,
                "BenchmarkEvent",
                &payloads,
                global_seq - batch_size as u64,
                0,
            )
            .await?;
    }
    let remainder = args.seed_events % batch_size;
    if remainder > 0 {
        let payloads: Vec<BenchmarkEvent> = (0..remainder)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        writer
            .append_batch_versioned(
                stream,
                "BenchmarkEvent",
                &payloads,
                global_seq - remainder as u64,
                0,
            )
            .await?;
    }

    info!("projection/postgres: starting polling projector …");

    // ── Cold-start: projector polls from global_position = 0 ─────────────────
    let view = new_view(args.view_size);
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0u64);

    let view_for_proj = Arc::clone(&view);
    let view_size = args.view_size;
    let seed_count = args.seed_events as u64;
    let pg_url_s = pg_url.to_string();
    let stream_name = stream.clone();
    let poll_ms = args.poll_interval_ms;
    let tx = progress_tx.clone();

    let t_cold = Instant::now();

    let projector = tokio::spawn(async move {
        let client = crate::postgres::client::PostgresClient::connect(&pg_url_s)
            .await
            .expect("projector pg connect");
        let mut checkpoint: i64 = 0;
        let mut processed = 0u64;
        loop {
            match client.poll_new_events(&stream_name, checkpoint, 200).await {
                Ok(rows) => {
                    if rows.is_empty() {
                        tokio::time::sleep(tokio::time::Duration::from_millis(poll_ms)).await;
                    } else {
                        for (seq, gpos) in rows {
                            push_to_view(&view_for_proj, seq as u64, view_size);
                            checkpoint = gpos;
                            processed += 1;
                            let _ = tx.send(processed);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("projection/postgres poll error: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_millis(poll_ms)).await;
                }
            }
        }
    });

    // Wait for all seed events to be processed
    {
        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= seed_count {
                break;
            }
            rx.changed().await?;
        }
    }
    let cold_start_ms = t_cold.elapsed().as_secs_f64() * 1000.0;
    info!(
        "projection/postgres: cold-start done — {:.1} ms for {} events",
        cold_start_ms, args.seed_events
    );

    // ── Live subscription-lag phase ───────────────────────────────────────────
    let mut lag_hist: Histogram<u64> = Histogram::new(4)?;
    let mut expected = seed_count;

    for i in 0..args.live_events {
        global_seq += 1;
        let payload = vec![BenchmarkEvent::new(global_seq, 0)];
        writer
            .append_batch_versioned(stream, "BenchmarkEvent", &payload, global_seq, 0)
            .await?;

        let t_ack = Instant::now();
        expected += 1;

        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= expected {
                break;
            }
            rx.changed().await?;
        }
        let lag_us = t_ack.elapsed().as_micros() as u64;
        lag_hist.record(lag_us.max(1))?;

        if i % 100 == 0 && i > 0 {
            info!(
                "projection/postgres: live-write {}/{} done, lag p50 {} µs",
                i,
                args.live_events,
                lag_hist.value_at_quantile(0.5)
            );
        }
    }

    projector.abort();

    // ── View read phase ───────────────────────────────────────────────────────
    let mut view_hist: Histogram<u64> = Histogram::new(4)?;
    for _ in 0..1_000 {
        let t = Instant::now();
        let _ = read_view(&view);
        let ns = t.elapsed().as_nanos() as u64;
        view_hist.record(ns.max(1))?;
    }

    Ok(ProjectionBenchResult {
        backend: "PostgreSQL (polling 1ms)".to_string(),
        seed_events: args.seed_events,
        view_size: args.view_size,
        cold_start_ms,
        lag_p50_us: lag_hist.value_at_quantile(0.5),
        lag_p95_us: lag_hist.value_at_quantile(0.95),
        lag_p99_us: lag_hist.value_at_quantile(0.99),
        lag_max_us: lag_hist.max(),
        view_read_p50_ns: view_hist.value_at_quantile(0.5),
        view_read_p99_ns: view_hist.value_at_quantile(0.99),
    })
}

// ── Report ────────────────────────────────────────────────────────────────────

impl ProjectionBenchResult {
    #[allow(clippy::cast_precision_loss)]
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════════════════════");
        println!("  Projection/Subscription Benchmark — {}", self.backend);
        println!(
            "  ({} events in stream, view size {})",
            self.seed_events, self.view_size
        );
        println!("══════════════════════════════════════════════════════════════");
        println!(
            "  COLD-START REBUILD  — replay {} events → view populated",
            self.seed_events
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!("  Time to ready  : {:.1} ms", self.cold_start_ms);
        println!(
            "  Throughput     : {:.0} ev/s",
            self.seed_events as f64 / (self.cold_start_ms / 1000.0)
        );
        println!();
        println!("  SUBSCRIPTION LAG  — write-ack → view-updated");
        println!("  ──────────────────────────────────────────────────────────");
        println!("  p50            : {} µs", self.lag_p50_us);
        println!("  p95            : {} µs", self.lag_p95_us);
        println!("  p99            : {} µs", self.lag_p99_us);
        println!("  max            : {} µs", self.lag_max_us);
        println!();
        println!("  VIEW READ  — 1 000 × read materialised view, zero DB queries");
        println!("  ──────────────────────────────────────────────────────────");
        println!("  p50            : {} ns", self.view_read_p50_ns);
        println!("  p99            : {} ns", self.view_read_p99_ns);
        println!("══════════════════════════════════════════════════════════════");
        println!();
    }

    pub fn print_json(&self) {
        let v = json!({
            "backend": self.backend,
            "seed_events": self.seed_events,
            "view_size": self.view_size,
            "cold_start_ms": self.cold_start_ms,
            "lag_p50_us": self.lag_p50_us,
            "lag_p95_us": self.lag_p95_us,
            "lag_p99_us": self.lag_p99_us,
            "lag_max_us": self.lag_max_us,
            "view_read_p50_ns": self.view_read_p50_ns,
            "view_read_p99_ns": self.view_read_p99_ns,
        });
        println!("{v}");
    }
}
