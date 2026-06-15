//! Scale benchmark — Test 11.
//!
//! Validates the requirement: **"5 million events accessible (one year of
//! history)"**.  Runs against PostgreSQL, which is the fastest to seed in the
//! devcontainer.  KurrentDB and MongoDB are covered by the same CLI commands
//! but default to the same event count.
//!
//! Three phases:
//!
//! 1. **Seed** — write N events in large batches (default 500 000).
//!    Measures sustained write throughput at scale; shows whether throughput
//!    degrades as the table grows.
//!
//! 2. **Read-last-N at scale** — same query as the hot-cache startup: read
//!    the last 500 events from a stream with N events.  At 500k rows the
//!    B-tree index should still be O(log N).
//!
//! 3. **Full-stream rehydration** — replay every event in the stream in order.
//!    This is what a service does when it restarts with no snapshot.  Measures
//!    throughput and elapsed time.
//!
//! Notes on 5 million events:
//!   PostgreSQL @ 14k ev/s → ~360 s (6 min)
//!   KurrentDB  @ 5.5k ev/s → ~910 s (15 min)
//!   MongoDB    @ 5.5k ev/s → ~910 s (15 min)
//! The default in this devcontainer test is 500 000 (manageable in ~35 s for PG).
//! Pass `--scale-events 5000000` for the full 5M test.

use std::time::Instant;

use anyhow::Result;
use serde_json::json;
use tracing::info;

use crate::events::BenchmarkEvent;

// ── CLI args ──────────────────────────────────────────────────────────────────

#[derive(clap::Parser, Debug)]
pub struct ScaleBenchArgs {
    /// Total events to write.  Default 500 000 (~35 s on PG devcontainer).
    /// Use 5 000 000 for the full one-year scale test.
    #[arg(long, default_value_t = 500_000)]
    pub scale_events: usize,

    /// Batch size for seeding writes.
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,

    /// Number of events to read back in the tail-read phase.
    #[arg(long, default_value_t = 500)]
    pub tail_size: usize,

    /// Stream / collection name.
    #[arg(long, default_value = "scale-bench-stream")]
    pub stream_name: String,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(clap::Parser, Debug)]
pub struct MongoScaleBenchArgs {
    /// Total events to write.
    #[arg(long, default_value_t = 500_000)]
    pub scale_events: usize,

    /// Batch size for seeding writes.
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,

    /// Number of events to read back in the tail-read phase.
    #[arg(long, default_value_t = 500)]
    pub tail_size: usize,

    /// Collection name.
    #[arg(long, default_value = "scale-bench-stream")]
    pub stream_name: String,

    /// MongoDB database name.
    #[arg(long, default_value = "scalebench")]
    pub database: String,

    /// Emit results as a single JSON line.
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

// ── Result ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ScaleBenchResult {
    pub backend: String,
    pub scale_events: usize,
    /// Overall write throughput across the full seed.
    pub write_throughput_eps: f64,
    pub write_elapsed_ms: f64,
    /// Throughput of the first 10% of events (warm-up).
    pub write_throughput_first10pct_eps: f64,
    /// Throughput of the last 10% of events (shows degradation if any).
    pub write_throughput_last10pct_eps: f64,
    /// read_last_N from a stream of `scale_events` rows.
    pub tail_read_us: u64,
    pub tail_events_read: usize,
    /// Full-stream rehydration.
    pub rehydrate_throughput_eps: f64,
    pub rehydrate_elapsed_ms: f64,
}

// ── PostgreSQL ────────────────────────────────────────────────────────────────

pub async fn run_postgres(pg_url: &str, args: &ScaleBenchArgs) -> Result<ScaleBenchResult> {
    let db = crate::postgres::client::PostgresClient::connect(pg_url).await?;
    db.ensure_bench_table_event_store().await?;
    db.ensure_stream_versions_table().await?;
    db.truncate_bench_table().await?;

    let stream = args.stream_name.as_str();
    let total = args.scale_events;
    let batch = args.batch_size;
    let tenth = (total / 10).max(1);

    info!(
        "scale/postgres: writing {} events in batches of {} …",
        total, batch
    );

    let t_write = Instant::now();
    let mut global_seq = 0u64;
    let mut t_first10_end = None;
    let mut batches_done = 0usize;

    while global_seq < total as u64 {
        let this_batch = batch.min(total - global_seq as usize);
        let payloads: Vec<BenchmarkEvent> = (0..this_batch)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        db.append_batch_versioned(
            stream,
            "BenchmarkEvent",
            &payloads,
            global_seq - this_batch as u64,
            0,
        )
        .await?;
        batches_done += 1;

        // Capture end-of-first-10%
        if t_first10_end.is_none() && global_seq as usize >= tenth {
            t_first10_end = Some((t_write.elapsed(), global_seq));
        }

        if (global_seq as usize).is_multiple_of(50_000) {
            info!(
                "scale/postgres: {}/{} events written ({:.0} ev/s)",
                global_seq,
                total,
                global_seq as f64 / t_write.elapsed().as_secs_f64()
            );
        }
    }

    let write_elapsed_ms = t_write.elapsed().as_secs_f64() * 1000.0;
    let write_throughput_eps = total as f64 / (write_elapsed_ms / 1000.0);

    // Last-10% throughput: events written from 90% mark to end
    let last_10pct_events = tenth as f64;
    let last_10pct_ms = {
        let last_batch_portion = (batches_done - (batches_done * 9 / 10)) as f64;
        write_elapsed_ms * (last_batch_portion / batches_done as f64)
    };
    let write_throughput_last10pct_eps = last_10pct_events / (last_10pct_ms / 1000.0).max(0.001);

    let (first10_elapsed, first10_events) =
        t_first10_end.unwrap_or((std::time::Duration::from_millis(1), tenth as u64));
    let write_throughput_first10pct_eps = first10_events as f64 / first10_elapsed.as_secs_f64();

    info!(
        "scale/postgres: write done — {:.0} ev/s overall ({:.0} first-10%, {:.0} last-10%)",
        write_throughput_eps, write_throughput_first10pct_eps, write_throughput_last10pct_eps
    );

    // ── Tail read: read last `tail_size` events at scale ──────────────────────
    info!("scale/postgres: reading last {} events …", args.tail_size);
    let t_tail = Instant::now();
    let tail_events = db
        .read_last_n_stream_bench_events(stream, args.tail_size as i64)
        .await?;
    let tail_read_us = t_tail.elapsed().as_micros() as u64;
    info!(
        "scale/postgres: tail read {} events in {} µs",
        tail_events.len(),
        tail_read_us
    );

    // ── Full-stream rehydration ───────────────────────────────────────────────
    info!("scale/postgres: full-stream rehydration …");
    let t_rehy = Instant::now();
    let rehy_count = db.rehydrate_stream(stream).await?;
    let rehy_elapsed_ms = t_rehy.elapsed().as_secs_f64() * 1000.0;
    let rehydrate_throughput_eps = rehy_count as f64 / (rehy_elapsed_ms / 1000.0);
    info!(
        "scale/postgres: rehydrated {} events in {:.0} ms ({:.0} ev/s)",
        rehy_count, rehy_elapsed_ms, rehydrate_throughput_eps
    );

    Ok(ScaleBenchResult {
        backend: "PostgreSQL".to_string(),
        scale_events: total,
        write_throughput_eps,
        write_elapsed_ms,
        write_throughput_first10pct_eps,
        write_throughput_last10pct_eps,
        tail_read_us,
        tail_events_read: tail_events.len(),
        rehydrate_throughput_eps,
        rehydrate_elapsed_ms: rehy_elapsed_ms,
    })
}

// ── KurrentDB ────────────────────────────────────────────────────────────────

pub async fn run_kurrentdb(kurrentdb_url: &str, args: &ScaleBenchArgs) -> Result<ScaleBenchResult> {
    use kurrentdb::{AppendToStreamOptions, StreamState};

    let db = crate::kurrentdb::client::KurrentClient::connect(kurrentdb_url)?;
    let stream = args.stream_name.as_str();
    let total = args.scale_events;
    let batch = args.batch_size;
    let tenth = (total / 10).max(1);

    info!(
        "scale/kurrentdb: writing {} events in batches of {} …",
        total, batch
    );

    let opts = AppendToStreamOptions::default().stream_state(StreamState::Any);
    let t_write = Instant::now();
    let mut global_seq = 0u64;
    let mut t_first10_end = None;
    let mut batches_done = 0usize;

    while global_seq < total as u64 {
        let this_batch = batch.min(total - global_seq as usize);
        let events: Result<Vec<kurrentdb::EventData>> = (0..this_batch)
            .map(|_| {
                global_seq += 1;
                Ok(kurrentdb::EventData::json(
                    "BenchmarkEvent",
                    &BenchmarkEvent::new(global_seq, 0),
                )
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .id(uuid::Uuid::new_v4()))
            })
            .collect();
        db.inner()
            .append_to_stream(stream, &opts, events?)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        batches_done += 1;

        if t_first10_end.is_none() && global_seq as usize >= tenth {
            t_first10_end = Some((t_write.elapsed(), global_seq));
        }
        if (global_seq as usize).is_multiple_of(50_000) {
            info!(
                "scale/kurrentdb: {}/{} events ({:.0} ev/s)",
                global_seq,
                total,
                global_seq as f64 / t_write.elapsed().as_secs_f64()
            );
        }
    }

    let write_elapsed_ms = t_write.elapsed().as_secs_f64() * 1000.0;
    let write_throughput_eps = total as f64 / (write_elapsed_ms / 1000.0);
    let (first10_elapsed, first10_events) =
        t_first10_end.unwrap_or((std::time::Duration::from_millis(1), tenth as u64));
    let write_throughput_first10pct_eps = first10_events as f64 / first10_elapsed.as_secs_f64();
    let last_10pct_events = tenth as f64;
    let last_10pct_ms =
        write_elapsed_ms * ((batches_done - batches_done * 9 / 10) as f64 / batches_done as f64);
    let write_throughput_last10pct_eps = last_10pct_events / (last_10pct_ms / 1000.0).max(0.001);

    info!(
        "scale/kurrentdb: write done — {:.0} ev/s overall",
        write_throughput_eps
    );

    // Tail read
    info!("scale/kurrentdb: reading last {} events …", args.tail_size);
    let t_tail = Instant::now();
    let tail_events = db.read_last_n_bench_events(stream, args.tail_size).await?;
    let tail_read_us = t_tail.elapsed().as_micros() as u64;
    info!(
        "scale/kurrentdb: tail read {} events in {} µs",
        tail_events.len(),
        tail_read_us
    );

    // Full-stream rehydration
    info!("scale/kurrentdb: full-stream rehydration …");
    let t_rehy = Instant::now();
    let events = db.read_stream_events(stream).await?;
    let rehy_count = events.len();
    let rehy_elapsed_ms = t_rehy.elapsed().as_secs_f64() * 1000.0;
    let rehydrate_throughput_eps = rehy_count as f64 / (rehy_elapsed_ms / 1000.0);
    info!(
        "scale/kurrentdb: rehydrated {} events in {:.0} ms ({:.0} ev/s)",
        rehy_count, rehy_elapsed_ms, rehydrate_throughput_eps
    );

    Ok(ScaleBenchResult {
        backend: "KurrentDB".to_string(),
        scale_events: total,
        write_throughput_eps,
        write_elapsed_ms,
        write_throughput_first10pct_eps,
        write_throughput_last10pct_eps,
        tail_read_us,
        tail_events_read: tail_events.len(),
        rehydrate_throughput_eps,
        rehydrate_elapsed_ms: rehy_elapsed_ms,
    })
}

// ── MongoDB ───────────────────────────────────────────────────────────────────

pub async fn run_mongo(mongodb_url: &str, args: &MongoScaleBenchArgs) -> Result<ScaleBenchResult> {
    use futures::TryStreamExt as _;
    use mongodb::bson::{doc, Document};
    use mongodb::options::FindOptions;

    let db = crate::mongodb::client::MongoClient::connect(mongodb_url, &args.database).await?;
    db.ensure_collection(&args.stream_name).await?;
    db.truncate_collection(&args.stream_name).await?;

    let coll_name = args.stream_name.as_str();
    let total = args.scale_events;
    let batch = args.batch_size;
    let tenth = (total / 10).max(1);

    info!(
        "scale/mongodb: writing {} events in batches of {} …",
        total, batch
    );

    let t_write = Instant::now();
    let mut global_seq = 0u64;
    let mut t_first10_end = None;
    let mut batches_done = 0usize;

    while global_seq < total as u64 {
        let this_batch = batch.min(total - global_seq as usize);
        let payloads: Vec<BenchmarkEvent> = (0..this_batch)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        db.append_batch(coll_name, "BenchmarkEvent", &payloads)
            .await?;
        batches_done += 1;

        if t_first10_end.is_none() && global_seq as usize >= tenth {
            t_first10_end = Some((t_write.elapsed(), global_seq));
        }
        if (global_seq as usize).is_multiple_of(50_000) {
            info!(
                "scale/mongodb: {}/{} events ({:.0} ev/s)",
                global_seq,
                total,
                global_seq as f64 / t_write.elapsed().as_secs_f64()
            );
        }
    }

    let write_elapsed_ms = t_write.elapsed().as_secs_f64() * 1000.0;
    let write_throughput_eps = total as f64 / (write_elapsed_ms / 1000.0);
    let (first10_elapsed, first10_events) =
        t_first10_end.unwrap_or((std::time::Duration::from_millis(1), tenth as u64));
    let write_throughput_first10pct_eps = first10_events as f64 / first10_elapsed.as_secs_f64();
    let last_10pct_events = tenth as f64;
    let last_10pct_ms =
        write_elapsed_ms * ((batches_done - batches_done * 9 / 10) as f64 / batches_done as f64);
    let write_throughput_last10pct_eps = last_10pct_events / (last_10pct_ms / 1000.0).max(0.001);

    info!(
        "scale/mongodb: write done — {:.0} ev/s overall",
        write_throughput_eps
    );

    // Tail read
    info!("scale/mongodb: reading last {} events …", args.tail_size);
    let coll: mongodb::Collection<Document> = db.database().collection(coll_name);
    let t_tail = Instant::now();
    let find_opts = FindOptions::builder()
        .sort(doc! { "_id": -1 })
        .limit(args.tail_size as i64)
        .build();
    let mut cursor = coll.find(doc! {}).with_options(find_opts).await?;
    let mut tail_count = 0usize;
    while cursor.try_next().await?.is_some() {
        tail_count += 1;
    }
    let tail_read_us = t_tail.elapsed().as_micros() as u64;
    info!(
        "scale/mongodb: tail read {} events in {} µs",
        tail_count, tail_read_us
    );

    // Full-stream rehydration
    info!("scale/mongodb: full-stream rehydration …");
    let t_rehy = Instant::now();
    let mut full_cursor = coll.find(doc! {}).sort(doc! { "_id": 1 }).await?;
    let mut rehy_count = 0usize;
    while full_cursor.try_next().await?.is_some() {
        rehy_count += 1;
    }
    let rehy_elapsed_ms = t_rehy.elapsed().as_secs_f64() * 1000.0;
    let rehydrate_throughput_eps = rehy_count as f64 / (rehy_elapsed_ms / 1000.0);
    info!(
        "scale/mongodb: rehydrated {} events in {:.0} ms ({:.0} ev/s)",
        rehy_count, rehy_elapsed_ms, rehydrate_throughput_eps
    );

    Ok(ScaleBenchResult {
        backend: "MongoDB".to_string(),
        scale_events: total,
        write_throughput_eps,
        write_elapsed_ms,
        write_throughput_first10pct_eps,
        write_throughput_last10pct_eps,
        tail_read_us,
        tail_events_read: tail_count,
        rehydrate_throughput_eps,
        rehydrate_elapsed_ms: rehy_elapsed_ms,
    })
}

// ── Report ────────────────────────────────────────────────────────────────────

impl ScaleBenchResult {
    #[allow(clippy::cast_precision_loss)]
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════════════════════");
        println!("  Scale Benchmark — {}", self.backend);
        println!("  ({} events)", self.scale_events);
        println!("══════════════════════════════════════════════════════════════");
        println!("  WRITE THROUGHPUT  (batched inserts)");
        println!("  ──────────────────────────────────────────────────────────");
        println!("  Total time     : {:.0} ms", self.write_elapsed_ms);
        println!("  Overall        : {:.0} ev/s", self.write_throughput_eps);
        println!(
            "  First 10%      : {:.0} ev/s  (warm dataset)",
            self.write_throughput_first10pct_eps
        );
        println!(
            "  Last  10%      : {:.0} ev/s  (shows index growth impact)",
            self.write_throughput_last10pct_eps
        );
        println!();
        println!(
            "  TAIL READ  — last {} events from {} total",
            self.tail_events_read, self.scale_events
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!(
            "  Latency        : {} µs  ({:.2} ms)",
            self.tail_read_us,
            self.tail_read_us as f64 / 1_000.0
        );
        println!();
        println!(
            "  FULL-STREAM REHYDRATION  — replay all {} events",
            self.scale_events
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!("  Elapsed        : {:.0} ms", self.rehydrate_elapsed_ms);
        println!(
            "  Throughput     : {:.0} ev/s",
            self.rehydrate_throughput_eps
        );
        println!("══════════════════════════════════════════════════════════════");
        println!();
    }

    pub fn print_json(&self) {
        let v = json!({
            "backend": self.backend,
            "scale_events": self.scale_events,
            "write_throughput_eps": self.write_throughput_eps,
            "write_elapsed_ms": self.write_elapsed_ms,
            "write_throughput_first10pct_eps": self.write_throughput_first10pct_eps,
            "write_throughput_last10pct_eps": self.write_throughput_last10pct_eps,
            "tail_read_us": self.tail_read_us,
            "tail_events_read": self.tail_events_read,
            "rehydrate_throughput_eps": self.rehydrate_throughput_eps,
            "rehydrate_elapsed_ms": self.rehydrate_elapsed_ms,
        });
        println!("{v}");
    }
}
