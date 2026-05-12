mod benchmark;
mod events;
mod eventstore_client;
mod mongodb_benchmark;
mod mongodb_client;
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
    about   = "Event-sourcing testbed — EventStoreDB + RabbitMQ benchmark tool"
)]
struct Cli {
    /// EventStoreDB connection URL.
    #[arg(
        long,
        env = "EVENTSTORE_URL",
        default_value = "esdb://localhost:2113,localhost:2114,localhost:2115?tls=false"
    )]
    eventstore_url: String,

    /// RabbitMQ AMQP URL.
    #[arg(
        long,
        env = "RABBITMQ_URL",
        default_value = "amqp://guest:guest@localhost:5673"
    )]
    rabbitmq_url: String,

    /// MongoDB connection URL.
    #[arg(long, env = "MONGODB_URL", default_value = "mongodb://localhost:27017")]
    mongodb_url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the write-latency stress test against EventStoreDB (p99 < 2 ms at 10k ev/s).
    Bench(BenchArgs),
    /// Run the write-latency stress test against MongoDB (p99 < p99-limit-ms at 10k ev/s).
    MongoBench(MongoBenchArgs),
    /// Continuously produce events to both EventStoreDB and RabbitMQ.
    Produce(ProduceArgs),
    /// Probe connectivity to both backends and exit 0 if healthy.
    Ping,
    /// Probe MongoDB connectivity and exit 0 if healthy.
    MongoPing,
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
    /// Controls max in-flight gRPC writes — 64 keeps the ES queue warm.
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

    /// p99 latency limit in milliseconds — benchmark FAILs if exceeded.
    #[arg(long, default_value_t = 2)]
    p99_limit_ms: u64,
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

    /// p99 latency limit in milliseconds — benchmark FAILs if exceeded.
    #[arg(long, default_value_t = 2)]
    p99_limit_ms: u64,

    /// MongoDB database name to use for the benchmark.
    #[arg(long, default_value = "eventbench")]
    database: String,

    /// Skip dropping the database before the run.
    /// By default the database is dropped so leftover data from a prior run
    /// cannot inflate latency results.
    #[arg(long)]
    no_drop: bool,
}

#[derive(Parser)]
struct ProduceArgs {
    /// Events per second to produce.
    #[arg(long, default_value_t = 100)]
    rate: u64,
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging; RUST_LOG overrides the default level.
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Bench(args) => {
            let config = benchmark::BenchmarkConfig {
                target_rate: args.target_rate,
                duration_secs: args.duration_secs,
                concurrency: args.concurrency,
                stream_prefix: args.stream_prefix,
                batch_size: args.batch_size,
                p99_limit_us: args.p99_limit_ms * 1_000,
            };

            let result = benchmark::run(&cli.eventstore_url, config).await?;

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
            let config = mongodb_benchmark::BenchmarkConfig {
                target_rate: args.target_rate,
                duration_secs: args.duration_secs,
                concurrency: args.concurrency,
                collection_prefix: args.collection_prefix,
                batch_size: args.batch_size,
                p99_limit_us: args.p99_limit_ms * 1_000,
                database: args.database,
                drop_before_run: !args.no_drop,
            };

            let result = mongodb_benchmark::run(&cli.mongodb_url, config).await?;

            if args.json {
                result.print_json();
            } else {
                result.print_report();
            }

            let _ = std::io::stdout().flush();
            let _ = std::io::stderr().flush();

            if !result.passed {
                std::process::exit(1);
            }
        }

        Commands::Produce(args) => {
            produce_loop(&cli.eventstore_url, &cli.rabbitmq_url, args.rate).await?;
        }

        Commands::Ping => {
            ping(&cli.eventstore_url, &cli.rabbitmq_url).await?;
        }

        Commands::MongoPing => {
            mongo_ping(&cli.mongodb_url).await?;
        }
    }

    Ok(())
}

// ─── Produce loop ─────────────────────────────────────────────────────────────

async fn produce_loop(es_url: &str, rmq_url: &str, rate: u64) -> Result<()> {
    let es = eventstore_client::EsClient::connect(es_url).await?;
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
        };

        let stream = format!("order-{}", order.order_id);

        // Write to EventStoreDB (append-only log).
        if let Err(e) = es.append(&stream, "OrderPlaced", &order).await {
            tracing::warn!(error = %e, "EventStoreDB append failed");
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
    mongodb_client::MongoClient::connect(mongo_url, "eventbench")
        .await?
        .ping()
        .await?;
    info!("MongoDB OK");
    Ok(())
}

async fn ping(es_url: &str, rmq_url: &str) -> Result<()> {
    info!("pinging EventStoreDB...");
    eventstore_client::EsClient::connect(es_url)
        .await?
        .ping()
        .await?;
    info!("EventStoreDB OK");

    info!("pinging RabbitMQ...");
    rabbitmq_client::RmqClient::connect(rmq_url).await?;
    info!("RabbitMQ OK");

    Ok(())
}
