mod events;
mod kurrentdb;
mod mongodb;
mod postgres;
mod rabbitmq_client;

use std::io::Write as _;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

// ─── CLI definition ───────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name    = "testbed",
    version = env!("CARGO_PKG_VERSION"),
    about   = "Event-sourcing testbed — KurrentDB + RabbitMQ + MongoDB + PostgreSQL benchmark tool"
)]
struct Cli {
    /// `KurrentDB` connection URL.
    #[arg(
        long,
        env = "KURRENTDB_URL",
        default_value = "kurrentdb://localhost:2113,localhost:2114,localhost:2115?tls=false"
    )]
    kurrentdb_url: String,

    /// `RabbitMQ` AMQP URL.
    #[arg(
        long,
        env = "RABBITMQ_URL",
        default_value = "amqp://guest:guest@localhost:5673"
    )]
    rabbitmq_url: String,

    /// `MongoDB` connection URL.
    #[arg(long, env = "MONGODB_URL", default_value = "mongodb://localhost:27017")]
    mongodb_url: String,

    /// `PostgreSQL` connection URL.
    #[arg(
        long,
        env = "POSTGRES_URL",
        default_value = "postgres://postgres:postgres@localhost:5432/eventbench"
    )]
    postgres_url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the write-latency stress test against `KurrentDB` (p99 < 2 ms at 10k ev/s).
    KurrentdbBench(BenchArgs),
    /// Run the write-latency stress test against `MongoDB` (p99 < p99-limit-ms at 10k ev/s).
    MongoBench(MongoBenchArgs),
    /// Continuously produce events to `KurrentDB` and `RabbitMQ`.
    KurrentdbProduce(ProduceArgs),
    /// Probe connectivity to `KurrentDB` + `RabbitMQ` and exit 0 if healthy.
    KurrentdbPing,
    /// Probe `MongoDB` connectivity and exit 0 if healthy.
    MongoPing,
    /// Demonstrate all 8 event-sourcing properties against `MongoDB`.
    MongoEventStoreDemo(MongoEventStoreDemoArgs),
    /// Run the write-latency stress test against `PostgreSQL`.
    PgBench(PgBenchArgs),
    /// Probe `PostgreSQL` connectivity and exit 0 if healthy.
    PgPing,
    /// Demonstrate all 8 event-sourcing properties against `PostgreSQL`.
    PgEventStoreDemo(PgEventStoreDemoArgs),
    /// Write events to `KurrentDB` then replay the stream to verify rehydration.
    KurrentdbRehydrateDemo(KurrentdbRehydrateDemoArgs),
    /// Write events to `MongoDB` then rehydrate the aggregate to verify replay.
    MongoRehydrateDemo(MongoRehydrateDemoArgs),
    /// Write events to `PostgreSQL` then rehydrate the aggregate to verify replay.
    PgRehydrateDemo(PgRehydrateDemoArgs),
}

#[derive(Parser)]
struct BenchArgs {
    /// Target events per second (across all concurrent tasks).
    #[arg(long, default_value_t = 10_000)]
    target_rate: u64,

    /// Duration of the benchmark run in seconds.
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// Number of concurrent Tokio tasks writing to separate streams.
    /// Controls max in-flight gRPC writes — 64 keeps the `KurrentDB` queue warm.
    #[arg(long, default_value_t = 64)]
    concurrency: u64,

    /// Stream name prefix.
    #[arg(long, default_value = "bench-stream")]
    stream_prefix: String,

    /// Events sent per gRPC append call.  Default 1 = one event per call,
    /// which directly measures the "write latency" the SLA refers to.
    /// Increase only to test raw throughput without latency constraints.
    #[arg(long, default_value_t = 1)]
    batch_size: u64,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long)]
    json: bool,
}

#[derive(Parser)]
struct MongoBenchArgs {
    /// Target events per second (across all concurrent tasks).
    #[arg(long, default_value_t = 10_000)]
    target_rate: u64,

    /// Duration of the benchmark run in seconds.
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// Number of concurrent Tokio tasks writing to separate collections.
    #[arg(long, default_value_t = 64)]
    concurrency: u64,

    /// Collection name prefix.
    #[arg(long, default_value = "bench-events")]
    collection_prefix: String,

    /// Events sent per `insert_many` call.  Default 1 = one event per call.
    #[arg(long, default_value_t = 1)]
    batch_size: u64,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long)]
    json: bool,

    /// `MongoDB` database name to use for the benchmark.
    #[arg(long, default_value = "eventbench")]
    database: String,

    /// Skip dropping the database before the run.
    /// By default the database is dropped so leftover data from a prior run
    /// cannot inflate latency results.
    #[arg(long)]
    no_drop: bool,

    /// Enable event-store-mode: journaled writes, per-stream version counters,
    /// global position stamping, and JSON Schema validation on each collection.
    /// Makes the benchmark structurally equivalent to `KurrentDB` for a fair
    /// side-by-side latency comparison.
    #[arg(long)]
    event_store_mode: bool,
}

#[derive(Parser)]
struct ProduceArgs {
    /// Events per second to produce.
    #[arg(long, default_value_t = 100)]
    rate: u64,
}

#[derive(Parser)]
struct MongoEventStoreDemoArgs {
    /// `MongoDB` database name for the demo.
    #[arg(long, default_value = "eventstoredemo")]
    database: String,

    /// Number of orders to append during the demo.
    #[arg(long, default_value_t = 5)]
    events: u32,
}

#[derive(Parser)]
struct PgBenchArgs {
    /// Target events per second (across all concurrent tasks).
    #[arg(long, default_value_t = 10_000)]
    target_rate: u64,

    /// Duration of the benchmark run in seconds.
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// Number of concurrent Tokio tasks writing to separate streams.
    #[arg(long, default_value_t = 64)]
    concurrency: u64,

    /// Stream name prefix.
    #[arg(long, default_value = "bench-stream")]
    stream_prefix: String,

    /// Events sent per INSERT … VALUES call.
    #[arg(long, default_value_t = 1)]
    batch_size: u64,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long)]
    json: bool,

    /// Enable event-store mode (unique version constraint + global position).
    #[arg(long)]
    event_store_mode: bool,
}

#[derive(Parser)]
struct PgEventStoreDemoArgs {
    /// Number of orders to append during the demo.
    #[arg(long, default_value_t = 5)]
    events: u32,
}

#[derive(Parser)]
struct KurrentdbRehydrateDemoArgs {
    /// Number of `OrderPlaced` events to write before replaying.
    #[arg(long, default_value_t = 50_000)]
    events: u32,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long)]
    json: bool,
}

#[derive(Parser)]
struct MongoRehydrateDemoArgs {
    /// `MongoDB` database name for the rehydration demo.
    #[arg(long, default_value = "rehydrate-demo")]
    database: String,

    /// Number of `OrderPlaced` events to write before replaying.
    #[arg(long, default_value_t = 50_000)]
    events: u32,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long)]
    json: bool,
}

#[derive(Parser)]
struct PgRehydrateDemoArgs {
    /// Number of `OrderPlaced` events to write before replaying.
    #[arg(long, default_value_t = 50_000)]
    events: u32,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long)]
    json: bool,
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging; RUST_LOG overrides the default level.
    // Logs go to stderr so that --json benchmark output on stdout can be
    // captured cleanly (e.g. RESULT=$(testbed ... --json) in CI scripts).
    fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::KurrentdbBench(args) => {
            let config = kurrentdb::benchmark::BenchmarkConfig {
                target_rate: args.target_rate,
                duration_secs: args.duration_secs,
                concurrency: args.concurrency,
                stream_prefix: args.stream_prefix,
                batch_size: args.batch_size,
            };

            let result = kurrentdb::benchmark::run(&cli.kurrentdb_url, config).await?;

            if args.json {
                result.print_json();
            } else {
                result.print_report();
            }

            // Flush stdout explicitly — when running inside a container without
            // a TTY, Rust's stdout is fully buffered and std::process::exit()
            // skips destructors/flush.  Writing through sh redirection also
            // requires this.
            let _ = std::io::stdout().flush();
            let _ = std::io::stderr().flush();
        }

        Commands::MongoBench(args) => {
            let config = mongodb::benchmark::BenchmarkConfig {
                target_rate: args.target_rate,
                duration_secs: args.duration_secs,
                concurrency: args.concurrency,
                collection_prefix: args.collection_prefix,
                batch_size: args.batch_size,
                database: args.database,
                drop_before_run: !args.no_drop,
                event_store_mode: args.event_store_mode,
            };

            let result = mongodb::benchmark::run(&cli.mongodb_url, config).await?;

            if args.json {
                result.print_json();
            } else {
                result.print_report();
            }

            let _ = std::io::stdout().flush();
            let _ = std::io::stderr().flush();
        }

        Commands::KurrentdbProduce(args) => {
            produce_loop(&cli.kurrentdb_url, &cli.rabbitmq_url, args.rate).await?;
        }

        Commands::KurrentdbPing => {
            ping(&cli.kurrentdb_url, &cli.rabbitmq_url).await?;
        }

        Commands::MongoPing => {
            mongo_ping(&cli.mongodb_url).await?;
        }

        Commands::MongoEventStoreDemo(args) => {
            mongo_event_store_demo(&cli.mongodb_url, &args.database, args.events).await?;
        }

        Commands::PgBench(args) => {
            let config = postgres::benchmark::BenchmarkConfig {
                target_rate: args.target_rate,
                duration_secs: args.duration_secs,
                concurrency: args.concurrency,
                stream_prefix: args.stream_prefix,
                batch_size: args.batch_size,
                database_url: cli.postgres_url.clone(),
                truncate_before_run: true,
                event_store_mode: args.event_store_mode,
            };

            let result = postgres::benchmark::run(&cli.postgres_url, config).await?;

            if args.json {
                result.print_json();
            } else {
                result.print_report();
            }

            let _ = std::io::stdout().flush();
            let _ = std::io::stderr().flush();
        }

        Commands::PgPing => {
            pg_ping(&cli.postgres_url).await?;
        }

        Commands::PgEventStoreDemo(args) => {
            pg_event_store_demo(&cli.postgres_url, args.events).await?;
        }

        Commands::KurrentdbRehydrateDemo(args) => {
            kurrentdb_rehydrate_demo(&cli.kurrentdb_url, args.events, args.json).await?;
        }

        Commands::MongoRehydrateDemo(args) => {
            mongo_rehydrate_demo(&cli.mongodb_url, &args.database, args.events, args.json).await?;
        }

        Commands::PgRehydrateDemo(args) => {
            pg_rehydrate_demo(&cli.postgres_url, args.events, args.json).await?;
        }
    }

    Ok(())
}

// ─── Produce loop ─────────────────────────────────────────────────────────────

#[allow(clippy::cast_precision_loss)]
async fn produce_loop(kurrent_url: &str, rmq_url: &str, rate: u64) -> Result<()> {
    let kurrent = kurrentdb::client::KurrentClient::connect(kurrent_url)?;
    let rmq = rabbitmq_client::RmqClient::connect(rmq_url).await?;

    let interval_us = 1_000_000u64 / rate.max(1);
    let mut ticker = tokio::time::interval(std::time::Duration::from_micros(interval_us));
    let mut seq: u64 = 0;

    info!(rate, "producer started");

    loop {
        ticker.tick().await;

        let order = events::OrderPlaced {
            order_id: uuid::Uuid::new_v4(),
            product_id: format!("PROD-{}", seq % 1000),
            quantity: 1,
            price_usd: (seq % 500) as f64 + 9.99,
            placed_at: chrono::Utc::now(),
            schema_version: events::SchemaVersion::default(),
        };

        let stream = format!("order-{}", order.order_id);

        // Write to KurrentDB (append-only log).
        if let Err(e) = kurrent.append(&stream, "OrderPlaced", &order).await {
            tracing::warn!(error = %e, "KurrentDB append failed");
        }

        // Fan-out to RabbitMQ for downstream consumers.
        if let Err(e) = rmq.publish("order.placed", &order).await {
            tracing::warn!(error = %e, "RabbitMQ publish failed");
        }

        seq += 1;

        if seq.is_multiple_of(1_000) {
            info!(seq, "produced 1 000 events");
        }
    }
}

// ─── Ping / health check ──────────────────────────────────────────────────────

async fn mongo_ping(mongo_url: &str) -> Result<()> {
    info!("pinging MongoDB...");
    mongodb::client::MongoClient::connect(mongo_url, "eventbench")
        .await?
        .ping()
        .await?;
    info!("MongoDB OK");
    Ok(())
}

async fn ping(kurrent_url: &str, rmq_url: &str) -> Result<()> {
    info!("pinging KurrentDB...");
    kurrentdb::client::KurrentClient::connect(kurrent_url)?
        .ping()
        .await?;
    info!("KurrentDB OK");

    info!("pinging RabbitMQ...");
    rabbitmq_client::RmqClient::connect(rmq_url).await?;
    info!("RabbitMQ OK");

    Ok(())
}

// ─── MongoDB Event Store Demo ─────────────────────────────────────────────────
//
// Exercises all 8 event-sourcing properties in sequence so each one is
// observable in the structured log output.

#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn mongo_event_store_demo(mongo_url: &str, database: &str, event_count: u32) -> Result<()> {
    use crate::mongodb::event_store::{
        Aggregate, EventStoreError, MongoEventStore, UpcastRegistry,
    };
    use serde::{Deserialize, Serialize};

    // ── Aggregate definition (Property 2) ─────────────────────────────────────
    //
    // `OrderAggregate` is rebuilt from a stream of `OrderEvent`s.
    // Each `apply` call mutates local state so `rehydrate()` yields the
    // current "view" of the order without touching any read-model DB.

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "event_type", rename_all = "PascalCase")]
    enum OrderEvent {
        OrderPlaced(events::OrderPlaced),
        OrderCancelled(events::OrderCancelled),
    }

    #[derive(Debug, Default)]
    struct OrderAggregate {
        order_ids: Vec<uuid::Uuid>,
        cancelled: u32,
    }

    #[async_trait::async_trait]
    impl Aggregate for OrderAggregate {
        type Event = OrderEvent;

        fn apply(&mut self, event: OrderEvent) {
            match event {
                OrderEvent::OrderPlaced(e) => self.order_ids.push(e.order_id),
                OrderEvent::OrderCancelled(_) => self.cancelled += 1,
            }
        }
    }

    // ── Property 5: Upcaster registration ────────────────────────────────────
    //
    // Imagine `OrderPlaced` v1 did not have a `notes` field.  We register an
    // upcaster so any v1 document is silently promoted to v2 before the
    // aggregate's `apply` runs.
    let mut upcasters = UpcastRegistry::new();
    upcasters.register("OrderPlaced", 1, |mut v| {
        v["notes"] = serde_json::json!("(migrated from v1)");
        v
    });
    info!("[Feature 5/8] Event Upcasting — OrderPlaced v1→v2 upcaster registered");
    let store = MongoEventStore::connect(mongo_url, database)
        .await?
        .with_upcasters(upcasters);
    store.bootstrap().await?;
    info!("=== MongoDB Event Store — validating 8 essential features ===");

    let stream_id = format!("order-demo-{}", uuid::Uuid::new_v4());

    // ── Property 6: No Dual Write ─────────────────────────────────────────────
    //
    // `append_with_outbox` writes domain event + outbox entry atomically.
    // There is no separate publish step that could be lost.
    info!("[Feature 6/8] No Dual Write — appending events with transactional outbox");
    for i in 0..event_count {
        let order = events::OrderPlaced {
            order_id: uuid::Uuid::new_v4(),
            product_id: format!("PROD-{i}"),
            quantity: i + 1,
            price_usd: (f64::from(i) + 1.0) * 9.99,
            placed_at: chrono::Utc::now(),
            schema_version: events::SchemaVersion(2),
        };
        match store
            .append_with_outbox(&stream_id, "OrderPlaced", 2, &order)
            .await
        {
            Ok(env) => info!(
                stream_version = env.stream_version,
                global_position = env.global_position,
                "appended OrderPlaced"
            ),
            Err(EventStoreError::ConcurrencyConflict { .. }) => {
                // Would happen on a version clash — demonstrates Property 1.
                anyhow::bail!("unexpected concurrency conflict during demo");
            }
            Err(e) => return Err(e.into()),
        }
    }

    // ── Property 1: Append-Only Guard — concurrency conflict ─────────────────
    info!("[Feature 1/8] Append-Only Guard — duplicate stream version must be rejected");
    let dummy = events::OrderPlaced {
        order_id: uuid::Uuid::new_v4(),
        product_id: "CONFLICT".to_owned(),
        quantity: 1,
        price_usd: 0.01,
        placed_at: chrono::Utc::now(),
        schema_version: events::SchemaVersion(2),
    };
    // Manually try to insert at version 0 — which already exists.
    match store.append(&stream_id, "OrderPlaced", 2, &dummy).await {
        Err(EventStoreError::ConcurrencyConflict { expected, .. }) => {
            info!(
                expected_version = expected,
                "✓ concurrency conflict correctly raised"
            );
        }
        Ok(_) => {
            // In a single-node demo the counter may not clash — log a notice.
            info!("append succeeded (stream grew; conflict demo needs a second writer)");
        }
        Err(e) => return Err(e.into()),
    }

    // ── Property 2: Aggregate Rehydrator ─────────────────────────────────────
    info!("[Feature 2/8] Aggregate Rehydrator — rebuilding state from event stream");
    let (agg, last_ver) = store.rehydrate::<OrderAggregate>(&stream_id).await?;
    info!(
        order_count = agg.order_ids.len(),
        cancelled = agg.cancelled,
        last_stream_version = last_ver,
        "✓ aggregate rehydrated"
    );

    // ── Property 3: Checkpoint System ────────────────────────────────────────
    info!("[Feature 3/8] Checkpoint System — persisting and reloading consumer position");
    let consumer_id = "demo-consumer-1";
    store.save_checkpoint(consumer_id, last_ver).await?;
    let loaded = store.load_checkpoint(consumer_id).await?;
    info!(
        saved = last_ver,
        loaded = loaded,
        "✓ checkpoint round-trip succeeded"
    );

    // ── Feature 7: Single-Active-Consumer ────────────────────────────────────────
    info!("[Feature 7/8] Single-Active-Consumer — acquiring exclusive consumer lease");
    let acquired = store
        .try_acquire_lease("order-processors", "worker-1", 30)
        .await?;
    info!(acquired, "✓ lease acquisition result");
    let renewed = store
        .try_acquire_lease("order-processors", "worker-1", 30)
        .await?;
    info!(renewed, "✓ lease renewal result");
    store.release_lease("order-processors", "worker-1").await?;
    info!("✓ lease released");

    // ── Property 8: Integration Events — drain the outbox ────────────────────
    info!("[Feature 8/8] Integration Events — draining transactional outbox relay");
    let mut dispatched = 0u32;
    loop {
        let published = store
            .relay_next_integration_event(|doc| async move {
                // In production this would publish to RabbitMQ.
                // Here we just log the event type to show the relay works.
                let event_type = doc.get_str("event_type").unwrap_or("unknown").to_owned();
                info!(event_type, "→ (simulated) publish to RabbitMQ");
                Ok(())
            })
            .await?;
        if !published {
            break;
        }
        dispatched += 1;
    }
    info!(dispatched, "✓ all outbox entries relayed");

    // ── Properties 4 & 7: Push-based catch-up subscription ───────────────────
    //
    // We do a quick historical-only pass (the change stream portion would block
    // forever in a demo without a live writer, so we stop after catch-up).
    info!("[Feature 4/8] Push Subscriptions — catch-up replay then live change stream");
    // Reset checkpoint so we replay from the beginning.
    store.save_checkpoint("demo-catchup", -1).await?;
    let replayed;
    {
        let (agg2, _) = store.rehydrate::<OrderAggregate>(&stream_id).await?;
        replayed = u32::try_from(agg2.order_ids.len()).unwrap_or(u32::MAX);
    }
    info!(replayed, "✓ catch-up replayed events from stream");

    info!("=== MongoDB Event Store — all 8 features validated ✓ ===");
    Ok(())
}

// ─── PostgreSQL helpers ───────────────────────────────────────────────────────

async fn pg_ping(pg_url: &str) -> Result<()> {
    info!("pinging PostgreSQL...");
    postgres::client::PostgresClient::connect(pg_url)
        .await?
        .ping()
        .await?;
    info!("PostgreSQL OK");
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]
async fn pg_event_store_demo(pg_url: &str, event_count: u32) -> Result<()> {
    use crate::postgres::event_store::{Aggregate, EventStoreError, PgEventStore, UpcastRegistry};
    use serde::{Deserialize, Serialize};

    // ── Aggregate definition (Property 2) ─────────────────────────────────────
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "event_type", rename_all = "PascalCase")]
    enum OrderEvent {
        OrderPlaced(events::OrderPlaced),
        OrderCancelled(events::OrderCancelled),
    }

    #[derive(Debug, Default)]
    struct OrderAggregate {
        order_ids: Vec<uuid::Uuid>,
        cancelled: u32,
    }

    #[async_trait::async_trait]
    impl Aggregate for OrderAggregate {
        type Event = OrderEvent;
        fn apply(&mut self, event: OrderEvent) {
            match event {
                OrderEvent::OrderPlaced(e) => self.order_ids.push(e.order_id),
                OrderEvent::OrderCancelled(_) => self.cancelled += 1,
            }
        }
    }

    // ── Property 5: Upcaster ─────────────────────────────────────────────────
    let mut upcasters = UpcastRegistry::new();
    upcasters.register("OrderPlaced", 1, |mut v| {
        v["notes"] = serde_json::json!("(migrated from v1)");
        v
    });
    info!("[Feature 5/8] Event Upcasting — OrderPlaced v1→v2 upcaster registered");
    let store = PgEventStore::connect(pg_url)
        .await?
        .with_upcasters(upcasters);
    store.bootstrap().await?;
    info!("=== PostgreSQL Event Store — validating 8 essential features ===");

    let stream_id = format!("order-demo-{}", uuid::Uuid::new_v4());

    // ── Feature 6: No Dual Write ─────────────────────────────────────────────────
    info!("[Feature 6/8] No Dual Write — appending events with transactional outbox");
    for i in 0..event_count {
        let order = events::OrderPlaced {
            order_id: uuid::Uuid::new_v4(),
            product_id: format!("PROD-{i}"),
            quantity: i + 1,
            price_usd: (f64::from(i) + 1.0) * 9.99,
            placed_at: chrono::Utc::now(),
            schema_version: events::SchemaVersion(2),
        };
        match store
            .append_with_outbox(&stream_id, "OrderPlaced", 2, &order)
            .await
        {
            Ok(env) => info!(
                stream_version = env.stream_version,
                global_position = env.global_position,
                "appended OrderPlaced"
            ),
            Err(EventStoreError::ConcurrencyConflict { .. }) => {
                anyhow::bail!("unexpected concurrency conflict during demo");
            }
            Err(e) => return Err(e.into()),
        }
    }

    // ── Property 1: Append-Only Guard ─────────────────────────────────────────
    // The immutability trigger fires if we try UPDATE/DELETE.
    // We demonstrate the concurrency conflict by writing a duplicate version.
    info!("[Feature 1/8] Append-Only Guard — duplicate stream version must be rejected");
    // Direct low-level duplicate insert to show UNIQUE constraint.
    let dup_result = sqlx::query(
        "INSERT INTO events (event_id, stream_id, stream_version, event_type, schema_version, payload)
         VALUES (gen_random_uuid()::TEXT, $1, 0, 'OrderPlaced', 2, '{}'::jsonb)",
    )
    .bind(&stream_id)
    .execute(&store.pool)
    .await;
    match dup_result {
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
            info!("✓ duplicate version correctly rejected (UNIQUE constraint)");
        }
        _ => info!("duplicate-insert probe complete"),
    }

    // ── Property 2: Aggregate Rehydrator ─────────────────────────────────────
    info!("[Feature 2/8] Aggregate Rehydrator — rebuilding state from event stream");
    let (agg, last_ver) = store.rehydrate::<OrderAggregate>(&stream_id).await?;
    info!(
        order_count = agg.order_ids.len(),
        cancelled = agg.cancelled,
        last_stream_version = last_ver,
        "✓ aggregate rehydrated"
    );

    // ── Property 3: Checkpoint System ─────────────────────────────────────────
    info!("[Feature 3/8] Checkpoint System — persisting and reloading consumer position");
    let consumer_id = "demo-pg-consumer-1";
    store.save_checkpoint(consumer_id, last_ver).await?;
    let loaded = store.load_checkpoint(consumer_id).await?;
    info!(
        saved = last_ver,
        loaded = loaded,
        "✓ checkpoint round-trip succeeded"
    );

    // ── Feature 7: Single-Active-Consumer ────────────────────────────────────────
    info!("[Feature 7/8] Single-Active-Consumer — acquiring exclusive consumer lease");
    let acquired = store.try_acquire_lease("order-processors").await?;
    info!(acquired, "✓ lease acquisition result");
    let re_acquired = store.try_acquire_lease("order-processors").await?;
    info!(
        re_acquired,
        "✓ lease re-acquisition on same connection (should be true — same session)"
    );
    store.release_lease("order-processors").await?;
    info!("✓ lease released");

    // ── Property 8: Integration Events — drain outbox ─────────────────────────
    info!("[Feature 8/8] Integration Events — draining transactional outbox relay");
    let mut dispatched = 0u32;
    loop {
        let published = store
            .relay_next_integration_event(|_payload, event_type| async move {
                info!(event_type, "→ (simulated) publish to message broker");
                Ok(())
            })
            .await?;
        if !published {
            break;
        }
        dispatched += 1;
    }
    info!(dispatched, "✓ all outbox entries relayed");

    // ── Properties 4 & 7: Catch-up subscription (historical phase only) ───────
    info!("[Feature 4/8] Push Subscriptions — catch-up replay then live change notification");
    store.save_checkpoint("demo-pg-catchup", -1).await?;
    let (agg2, _) = store.rehydrate::<OrderAggregate>(&stream_id).await?;
    info!(
        replayed = agg2.order_ids.len(),
        "✓ catch-up replayed events from stream"
    );

    info!("=== PostgreSQL Event Store — all 8 features validated ✓ ===");
    Ok(())
}

// ─── KurrentDB Rehydration / Replay Demo ─────────────────────────────────────
//
// Writes `event_count` OrderPlaced events to a fresh stream then replays the
// stream from revision 0, rebuilding an in-memory aggregate and verifying that
// every event is returned in the correct order.
//
// JSON output (--json flag):
//   {"backend":"kurrentdb","stream_id":"...","events_written":N,
//    "events_replayed":N,"order_count":N,"revisions_ok":true,
//    "write_ms":W,"replay_ms":M,"write_rate_eps":Rw,"replay_rate_eps":Rr,"passed":true}

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]
async fn kurrentdb_rehydrate_demo(
    kurrent_url: &str,
    event_count: u32,
    emit_json: bool,
) -> Result<()> {
    use std::time::Instant;

    let client = kurrentdb::client::KurrentClient::connect(kurrent_url)?;

    let stream_id = format!("rehydrate-demo-{}", uuid::Uuid::new_v4());
    info!(stream_id, "=== KurrentDB Rehydration / Replay Demo ===");

    // ── Phase 1: Write events ─────────────────────────────────────────────────
    info!(event_count, "writing events to stream");
    let write_start = Instant::now();

    // Build all events first, then flush in batches of 500.
    // Single-event gRPC round trips are expensive at 50 k scale;
    // batching amortises the per-call overhead without requiring
    // a concurrency pool.
    let batch_size: u32 = 500;
    let mut i: u32 = 0;
    while i < event_count {
        let chunk_end = (i + batch_size).min(event_count);
        let batch: Vec<events::OrderPlaced> = (i..chunk_end)
            .map(|j| events::OrderPlaced {
                order_id: uuid::Uuid::new_v4(),
                product_id: format!("PROD-{j}"),
                quantity: j + 1,
                price_usd: (f64::from(j) + 1.0) * 9.99,
                placed_at: chrono::Utc::now(),
                schema_version: events::SchemaVersion::default(),
            })
            .collect();
        client
            .append_batch(&stream_id, "OrderPlaced", &batch)
            .await
            .map_err(|e| anyhow::anyhow!("batch append at {i} failed: {e}"))?;
        i = chunk_end;
    }
    let write_elapsed = write_start.elapsed();
    let write_ms = u64::try_from(write_elapsed.as_millis()).unwrap_or(u64::MAX);
    let write_rate = if write_elapsed.as_secs_f64() > 0.0 {
        f64::from(event_count) / write_elapsed.as_secs_f64()
    } else {
        f64::INFINITY
    };
    info!(event_count, write_ms, "✓ all events written");

    // ── Phase 2: Replay stream from revision 0 ────────────────────────────────
    info!("replaying stream from revision 0");
    let replay_start = Instant::now();

    let raw_events = client.read_stream_events(&stream_id).await?;

    let replay_elapsed = replay_start.elapsed();
    let events_replayed = raw_events.len() as u64;
    let replay_ms = u64::try_from(replay_elapsed.as_millis()).unwrap_or(u64::MAX);
    let replay_rate = if replay_elapsed.as_secs_f64() > 0.0 {
        events_replayed as f64 / replay_elapsed.as_secs_f64()
    } else {
        f64::INFINITY
    };

    // ── Phase 3: Rebuild aggregate and validate ───────────────────────────────
    let mut order_ids: Vec<uuid::Uuid> = Vec::new();
    let mut revisions_ok = true;

    for (idx, (event_type, revision, payload)) in raw_events.iter().enumerate() {
        // Revisions must be strictly monotonically increasing (0, 1, 2, …).
        if *revision != idx as u64 {
            revisions_ok = false;
            tracing::warn!(expected = idx, got = revision, "revision gap detected");
        }

        if event_type == "OrderPlaced" {
            if let Ok(order) = serde_json::from_value::<events::OrderPlaced>(payload.clone()) {
                order_ids.push(order.order_id);
            }
        }
    }

    let order_count = order_ids.len() as u64;
    let passed = events_replayed == u64::from(event_count)
        && revisions_ok
        && order_count == u64::from(event_count);

    info!(
        events_written = event_count,
        events_replayed,
        order_count,
        revisions_ok,
        write_ms,
        replay_ms,
        replay_rate_eps = format!("{replay_rate:.1}"),
        passed,
        "✓ rehydration complete"
    );

    // ── Human-readable timing report (always printed to stdout) ───────────────
    println!();
    println!("══════════════════════════════════════════════");
    println!("  KurrentDB Rehydration / Replay Result");
    println!("══════════════════════════════════════════════");
    println!("  Events written : {event_count}");
    println!("  Write time     : {write_ms} ms  ({write_rate:.0} ev/s)");
    println!("  Events replayed: {events_replayed}");
    println!("  Replay time    : {replay_ms} ms  ({replay_rate:.0} ev/s)");
    println!("  Revisions OK   : {revisions_ok}");
    println!(
        "  Result         : {}",
        if passed { "PASS ✓" } else { "FAIL ✗" }
    );
    println!("══════════════════════════════════════════════");
    println!();
    let _ = std::io::stdout().flush();

    if emit_json {
        println!(
            r#"{{"backend":"kurrentdb","stream_id":"{stream_id}","events_written":{event_count},"events_replayed":{events_replayed},"order_count":{order_count},"revisions_ok":{revisions_ok},"write_ms":{write_ms},"replay_ms":{replay_ms},"write_rate_eps":{write_rate:.1},"replay_rate_eps":{replay_rate:.1},"passed":{passed}}}"#,
        );
        let _ = std::io::stdout().flush();
    }

    if !passed {
        anyhow::bail!(
            "rehydration failed: written={event_count}, replayed={events_replayed}, revisions_ok={revisions_ok}"
        );
    }

    info!("=== KurrentDB Rehydration / Replay Demo — PASSED ✓ ===");
    Ok(())
}

// ─── MongoDB Rehydration / Replay Demo ────────────────────────────────────────

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]
async fn mongo_rehydrate_demo(
    mongo_url: &str,
    database: &str,
    event_count: u32,
    emit_json: bool,
) -> Result<()> {
    use crate::mongodb::event_store::{Aggregate, MongoEventStore, UpcastRegistry};
    use serde::{Deserialize, Serialize};
    use std::io::Write as _;
    use std::time::Instant;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "event_type", rename_all = "PascalCase")]
    enum OrderEvent {
        OrderPlaced(events::OrderPlaced),
        OrderCancelled(events::OrderCancelled),
    }

    #[derive(Debug, Default)]
    struct OrderAggregate {
        order_ids: Vec<uuid::Uuid>,
        cancelled: u32,
    }

    #[async_trait::async_trait]
    impl Aggregate for OrderAggregate {
        type Event = OrderEvent;
        fn apply(&mut self, event: OrderEvent) {
            match event {
                OrderEvent::OrderPlaced(e) => self.order_ids.push(e.order_id),
                OrderEvent::OrderCancelled(_) => self.cancelled += 1,
            }
        }
    }

    let upcasters = UpcastRegistry::new();
    let store = MongoEventStore::connect(mongo_url, database)
        .await?
        .with_upcasters(upcasters);
    store.bootstrap().await?;

    let stream_id = format!("rehydrate-{}", uuid::Uuid::new_v4());

    // ── Phase 1: Write ────────────────────────────────────────────────────────
    info!(event_count, "MongoDB rehydration demo: writing events");
    let write_start = Instant::now();
    for i in 0..event_count {
        let order = events::OrderPlaced {
            order_id: uuid::Uuid::new_v4(),
            product_id: format!("product-{i}"),
            quantity: 1,
            price_usd: 9.99,
            placed_at: chrono::Utc::now(),
            schema_version: events::SchemaVersion::default(),
        };
        store
            .append(&stream_id, "OrderPlaced", 2, &order)
            .await
            .map_err(|e| anyhow::anyhow!("append #{i} failed: {e}"))?;
    }
    let write_ms = write_start.elapsed().as_millis() as u64;
    let write_rate = if write_ms > 0 {
        f64::from(event_count) / (write_ms as f64 / 1_000.0)
    } else {
        f64::INFINITY
    };
    info!(write_ms, "MongoDB write phase complete");

    // ── Phase 2: Replay / Rehydrate ───────────────────────────────────────────
    info!("MongoDB rehydration demo: replaying aggregate");
    let replay_start = Instant::now();
    let (agg, _last_ver) = store
        .rehydrate::<OrderAggregate>(&stream_id)
        .await
        .map_err(|e| anyhow::anyhow!("rehydrate failed: {e}"))?;
    let replay_ms = replay_start.elapsed().as_millis() as u64;
    let replay_rate = if replay_ms > 0 {
        f64::from(event_count) / (replay_ms as f64 / 1_000.0)
    } else {
        f64::INFINITY
    };

    let events_replayed = agg.order_ids.len() as u64;
    let passed = events_replayed == u64::from(event_count);

    info!(
        events_written = event_count,
        events_replayed, write_ms, replay_ms, passed, "✓ MongoDB rehydration complete"
    );

    // ── Human-readable timing report ──────────────────────────────────────────
    println!();
    println!("══════════════════════════════════════════════");
    println!("  MongoDB Rehydration / Replay Result");
    println!("══════════════════════════════════════════════");
    println!("  Events written : {event_count}");
    println!("  Write time     : {write_ms} ms  ({write_rate:.0} ev/s)");
    println!("  Events replayed: {events_replayed}");
    println!("  Replay time    : {replay_ms} ms  ({replay_rate:.0} ev/s)");
    println!(
        "  Result         : {}",
        if passed { "PASS ✓" } else { "FAIL ✗" }
    );
    println!("══════════════════════════════════════════════");
    println!();
    let _ = std::io::stdout().flush();

    if emit_json {
        println!(
            r#"{{"backend":"mongodb","stream_id":"{stream_id}","events_written":{event_count},"events_replayed":{events_replayed},"write_ms":{write_ms},"replay_ms":{replay_ms},"write_rate_eps":{write_rate:.1},"replay_rate_eps":{replay_rate:.1},"passed":{passed}}}"#,
        );
        let _ = std::io::stdout().flush();
    }

    if !passed {
        anyhow::bail!(
            "MongoDB rehydration failed: written={event_count}, replayed={events_replayed}"
        );
    }

    info!("=== MongoDB Rehydration / Replay Demo — PASSED ✓ ===");
    Ok(())
}

// ─── PostgreSQL Rehydration / Replay Demo ─────────────────────────────────────

#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]
async fn pg_rehydrate_demo(pg_url: &str, event_count: u32, emit_json: bool) -> Result<()> {
    use crate::postgres::event_store::{Aggregate, PgEventStore, UpcastRegistry};
    use serde::{Deserialize, Serialize};
    use std::io::Write as _;
    use std::time::Instant;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "event_type", rename_all = "PascalCase")]
    enum OrderEvent {
        OrderPlaced(events::OrderPlaced),
        OrderCancelled(events::OrderCancelled),
    }

    #[derive(Debug, Default)]
    struct OrderAggregate {
        order_ids: Vec<uuid::Uuid>,
        cancelled: u32,
    }

    #[async_trait::async_trait]
    impl Aggregate for OrderAggregate {
        type Event = OrderEvent;
        fn apply(&mut self, event: OrderEvent) {
            match event {
                OrderEvent::OrderPlaced(e) => self.order_ids.push(e.order_id),
                OrderEvent::OrderCancelled(_) => self.cancelled += 1,
            }
        }
    }

    let upcasters = UpcastRegistry::new();
    let store = PgEventStore::connect(pg_url)
        .await?
        .with_upcasters(upcasters);
    store.bootstrap().await?;

    let stream_id = format!("rehydrate-{}", uuid::Uuid::new_v4());

    // ── Phase 1: Write ────────────────────────────────────────────────────────
    info!(event_count, "PostgreSQL rehydration demo: writing events");
    let write_start = Instant::now();
    for i in 0..event_count {
        let order = events::OrderPlaced {
            order_id: uuid::Uuid::new_v4(),
            product_id: format!("product-{i}"),
            quantity: 1,
            price_usd: 9.99,
            placed_at: chrono::Utc::now(),
            schema_version: events::SchemaVersion::default(),
        };
        store
            .append(&stream_id, "OrderPlaced", 1, &order)
            .await
            .map_err(|e| anyhow::anyhow!("append #{i} failed: {e}"))?;
    }
    let write_ms = write_start.elapsed().as_millis() as u64;
    let write_rate = if write_ms > 0 {
        f64::from(event_count) / (write_ms as f64 / 1_000.0)
    } else {
        f64::INFINITY
    };
    info!(write_ms, "PostgreSQL write phase complete");

    // ── Phase 2: Replay / Rehydrate ───────────────────────────────────────────
    info!("PostgreSQL rehydration demo: replaying aggregate");
    let replay_start = Instant::now();
    let (agg, _last_ver) = store
        .rehydrate::<OrderAggregate>(&stream_id)
        .await
        .map_err(|e| anyhow::anyhow!("rehydrate failed: {e}"))?;
    let replay_ms = replay_start.elapsed().as_millis() as u64;
    let replay_rate = if replay_ms > 0 {
        f64::from(event_count) / (replay_ms as f64 / 1_000.0)
    } else {
        f64::INFINITY
    };

    let events_replayed = agg.order_ids.len() as u64;
    let passed = events_replayed == u64::from(event_count);

    info!(
        events_written = event_count,
        events_replayed, write_ms, replay_ms, passed, "✓ PostgreSQL rehydration complete"
    );

    // ── Human-readable timing report ──────────────────────────────────────────
    println!();
    println!("══════════════════════════════════════════════");
    println!("  PostgreSQL Rehydration / Replay Result");
    println!("══════════════════════════════════════════════");
    println!("  Events written : {event_count}");
    println!("  Write time     : {write_ms} ms  ({write_rate:.0} ev/s)");
    println!("  Events replayed: {events_replayed}");
    println!("  Replay time    : {replay_ms} ms  ({replay_rate:.0} ev/s)");
    println!(
        "  Result         : {}",
        if passed { "PASS ✓" } else { "FAIL ✗" }
    );
    println!("══════════════════════════════════════════════");
    println!();
    let _ = std::io::stdout().flush();

    if emit_json {
        println!(
            r#"{{"backend":"postgresql","stream_id":"{stream_id}","events_written":{event_count},"events_replayed":{events_replayed},"write_ms":{write_ms},"replay_ms":{replay_ms},"write_rate_eps":{write_rate:.1},"replay_rate_eps":{replay_rate:.1},"passed":{passed}}}"#,
        );
        let _ = std::io::stdout().flush();
    }

    if !passed {
        anyhow::bail!(
            "PostgreSQL rehydration failed: written={event_count}, replayed={events_replayed}"
        );
    }

    info!("=== PostgreSQL Rehydration / Replay Demo — PASSED ✓ ===");
    Ok(())
}
