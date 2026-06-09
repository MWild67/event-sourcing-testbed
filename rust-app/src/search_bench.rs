//! Search-index projection benchmark — Test 10.
//!
//! Validates the requirement: **"all events are searchable"**.
//!
//! A projector subscribes to each event-store backend and writes every event
//! into a PostgreSQL full-text search table (`search_index`).  The BFF
//! queries that table for search results — never the event store.
//!
//! Three metrics are measured:
//!
//! 1. **Indexing throughput** — how fast the projector can write 50 000
//!    events from the event store into the search index.
//!
//! 2. **Indexing lag** — for 300 live writes: time from event-store
//!    write-ack → search index updated (p50 / p99).
//!
//! 3. **Query latency** — four query patterns against the fully-indexed
//!    dataset, each run 200 times: exact match, prefix, full-text, date range.

use std::time::Instant;

use anyhow::Result;
use hdrhistogram::Histogram;
use serde_json::json;
use sqlx::Row as _;
use tracing::info;

use crate::events::BenchmarkEvent;

// ── CLI args ──────────────────────────────────────────────────────────────────

#[derive(clap::Parser, Debug)]
pub struct SearchBenchArgs {
    /// Events to seed into the event store and then project into the index.
    #[arg(long, default_value_t = 50_000)]
    pub seed_events: usize,

    /// Additional live events to write during the lag phase.
    #[arg(long, default_value_t = 300)]
    pub live_events: usize,

    /// Stream name in the event store.
    #[arg(long, default_value = "search-bench-stream")]
    pub stream_name: String,

    /// Polling interval for the PostgreSQL projector (ms).
    #[arg(long, default_value_t = 1)]
    pub poll_interval_ms: u64,

    /// Emit results as a single JSON line (for CI parsing).
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(clap::Parser, Debug)]
pub struct MongoSearchBenchArgs {
    /// Events to seed.
    #[arg(long, default_value_t = 50_000)]
    pub seed_events: usize,

    /// Additional live events.
    #[arg(long, default_value_t = 300)]
    pub live_events: usize,

    /// Collection name.
    #[arg(long, default_value = "search-bench-stream")]
    pub stream_name: String,

    /// MongoDB database name.
    #[arg(long, default_value = "searchbench")]
    pub database: String,

    /// Emit results as a single JSON line.
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

// ── Result ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SearchBenchResult {
    pub backend: String,
    pub indexed_events: usize,
    pub index_build_ms: f64,
    pub index_throughput_eps: f64,
    pub lag_p50_us: u64,
    pub lag_p99_us: u64,
    pub query_exact_p50_us: u64,
    pub query_exact_p99_us: u64,
    pub query_prefix_p50_us: u64,
    pub query_prefix_p99_us: u64,
    pub query_fts_p50_us: u64,
    pub query_fts_p99_us: u64,
    pub query_range_p50_us: u64,
    pub query_range_p99_us: u64,
}

// ── Search index schema helpers (PostgreSQL) ──────────────────────────────────

pub async fn ensure_search_index_table(pool: &sqlx::PgPool) -> Result<()> {
    // A single table that all three backend projectors write to.
    // In production this would be per-tenant/per-service; here it is global
    // for simplicity.
    sqlx::query(
        r"
        CREATE TABLE IF NOT EXISTS search_index (
            id            TEXT         NOT NULL PRIMARY KEY,
            stream_id     TEXT         NOT NULL,
            event_type    TEXT         NOT NULL,
            seq           BIGINT       NOT NULL,
            order_id      TEXT,
            product_id    TEXT,
            status        TEXT,
            full_text     TEXT         NOT NULL,
            fts_vector    TSVECTOR     GENERATED ALWAYS AS (to_tsvector('english', full_text)) STORED,
            indexed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            event_ts      TIMESTAMPTZ  NOT NULL
        )
        ",
    )
    .execute(pool)
    .await
    .context("failed to create search_index table")?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_si_stream   ON search_index (stream_id)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_si_type     ON search_index (event_type)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_si_order    ON search_index (order_id)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_si_fts      ON search_index USING GIN (fts_vector)",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_si_event_ts ON search_index (event_ts)")
        .execute(pool)
        .await?;

    Ok(())
}

use anyhow::Context as _;

pub async fn truncate_search_index(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query("TRUNCATE TABLE search_index")
        .execute(pool)
        .await
        .context("truncate search_index")?;
    Ok(())
}

/// Insert a batch of `BenchmarkEvent`s into `search_index`.
/// Simulates what a real projector would do: extract searchable fields and
/// write a denormalised row so the BFF never has to touch the event store.
pub async fn index_batch(
    pool: &sqlx::PgPool,
    stream_id: &str,
    events: &[(i64, BenchmarkEvent)],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO search_index \
         (id, stream_id, event_type, seq, order_id, product_id, status, full_text, event_ts) ",
    );
    qb.push_values(events, |mut b, (gpos, ev)| {
        let order_id = format!("ORD-{}", ev.seq % 10_000);
        let product_id = format!("PROD-{}", ev.seq % 1_000);
        let status = if ev.seq % 5 == 0 {
            "completed".to_string()
        } else {
            "active".to_string()
        };
        let full_text = format!(
            "order {} product {} status {} task {}",
            order_id, product_id, status, ev.task_id
        );
        b.push_bind(format!("{stream_id}-{gpos}"))
            .push_bind(stream_id.to_string())
            .push_bind("BenchmarkEvent")
            .push_bind(ev.seq as i64)
            .push_bind(order_id)
            .push_bind(product_id)
            .push_bind(status)
            .push_bind(full_text)
            .push_bind(ev.created_at);
    });
    qb.push(" ON CONFLICT (id) DO NOTHING");
    qb.build()
        .execute(pool)
        .await
        .context("index_batch insert failed")?;
    Ok(())
}

/// Fetch up to `limit` events from `bench_events` WHERE `global_position > after`
/// and return them together with their global position.
pub async fn poll_events_for_indexing(
    pool: &sqlx::PgPool,
    stream_id: &str,
    after: i64,
    limit: i64,
) -> Result<Vec<(i64, BenchmarkEvent)>> {
    let rows = sqlx::query(
        "SELECT seq, task_id, payload, created_at, global_position \
         FROM bench_events \
         WHERE stream_id = $1 AND global_position > $2 \
         ORDER BY global_position LIMIT $3",
    )
    .bind(stream_id)
    .bind(after)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("poll_events_for_indexing failed")?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let seq: i64 = r.get("seq");
            let task_id: i64 = r.get("task_id");
            let payload: Vec<u8> = r.get("payload");
            let created_at: chrono::DateTime<chrono::Utc> = r.get("created_at");
            let gpos: i64 = r.get("global_position");
            (
                gpos,
                BenchmarkEvent {
                    seq: seq as u64,
                    task_id: task_id as u64,
                    payload,
                    created_at,
                },
            )
        })
        .collect())
}

// ── Query helpers ─────────────────────────────────────────────────────────────

/// Exact lookup by order_id.
pub async fn query_exact(pool: &sqlx::PgPool, order_id: &str) -> Result<usize> {
    let row = sqlx::query("SELECT COUNT(*) FROM search_index WHERE order_id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await?;
    Ok(row.get::<i64, _>(0) as usize)
}

/// Prefix match: all products starting with a prefix.
pub async fn query_prefix(pool: &sqlx::PgPool, prefix: &str) -> Result<usize> {
    let row = sqlx::query("SELECT COUNT(*) FROM search_index WHERE product_id LIKE $1")
        .bind(format!("{prefix}%"))
        .fetch_one(pool)
        .await?;
    Ok(row.get::<i64, _>(0) as usize)
}

/// Full-text search.
pub async fn query_fts(pool: &sqlx::PgPool, term: &str) -> Result<usize> {
    let row = sqlx::query(
        "SELECT COUNT(*) FROM search_index WHERE fts_vector @@ plainto_tsquery('english', $1)",
    )
    .bind(term)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>(0) as usize)
}

/// Date-range query: events indexed in the last N seconds.
pub async fn query_range(pool: &sqlx::PgPool, last_secs: i64) -> Result<usize> {
    let row = sqlx::query(
        "SELECT COUNT(*) FROM search_index \
         WHERE event_ts >= NOW() - ($1 || ' seconds')::INTERVAL",
    )
    .bind(last_secs)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>(0) as usize)
}

// ── KurrentDB runner ──────────────────────────────────────────────────────────

pub async fn run_kurrentdb(
    kurrentdb_url: &str,
    pg_url: &str,
    args: &SearchBenchArgs,
) -> Result<SearchBenchResult> {
    use kurrentdb::{AppendToStreamOptions, StreamPosition, StreamState, SubscribeToStreamOptions};

    let writer = crate::kurrentdb::client::KurrentClient::connect(kurrentdb_url)?;
    let pg = crate::postgres::client::PostgresClient::connect(pg_url).await?;
    ensure_search_index_table(&pg.pool).await?;
    truncate_search_index(&pg.pool).await?;

    let stream = args.stream_name.as_str();

    info!("search/kurrentdb: seeding {} events …", args.seed_events);
    let batch_size = 200usize;
    let mut global_seq = 0u64;
    let opts = AppendToStreamOptions::default().stream_state(StreamState::Any);

    for _ in 0..(args.seed_events / batch_size) {
        let events: Result<Vec<kurrentdb::EventData>> = (0..batch_size)
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
        writer
            .inner()
            .append_to_stream(stream, &opts, events?)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    let rem = args.seed_events % batch_size;
    if rem > 0 {
        let events: Result<Vec<kurrentdb::EventData>> = (0..rem)
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
        writer
            .inner()
            .append_to_stream(stream, &opts, events?)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    info!("search/kurrentdb: building index via catch-up subscription …");

    // ── Index build: catch-up subscription → batch insert into search_index ──
    let pg_pool = pg.pool.clone();
    let sub_client = writer.inner().clone();
    let sub_stream = args.stream_name.clone();
    let seed_count = args.seed_events;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0usize);
    let tx = progress_tx.clone();

    let t_index = Instant::now();

    let indexer = tokio::spawn(async move {
        let sub_opts = SubscribeToStreamOptions::default().start_from(StreamPosition::Start);
        let mut sub = sub_client
            .subscribe_to_stream(sub_stream.as_str(), &sub_opts)
            .await;
        let mut buf: Vec<(i64, BenchmarkEvent)> = Vec::with_capacity(200);
        let mut processed = 0usize;
        let mut gpos = 0i64;
        loop {
            match sub.next().await {
                Ok(ev) => {
                    let recorded = ev.get_original_event();
                    gpos += 1;
                    if let Ok(bench_ev) = serde_json::from_slice::<BenchmarkEvent>(&recorded.data) {
                        buf.push((gpos, bench_ev));
                        if buf.len() >= 200 {
                            let _ = index_batch(&pg_pool, sub_stream.as_str(), &buf).await;
                            processed += buf.len();
                            buf.clear();
                            let _ = tx.send(processed);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("search/kurrentdb subscription error: {e}");
                    break;
                }
            }
        }
    });

    // Wait for all seed events to be indexed
    {
        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= seed_count {
                break;
            }
            rx.changed().await?;
        }
    }
    let index_build_ms = t_index.elapsed().as_secs_f64() * 1000.0;
    let index_throughput_eps = args.seed_events as f64 / (index_build_ms / 1000.0);
    info!(
        "search/kurrentdb: index built in {:.0} ms ({:.0} ev/s)",
        index_build_ms, index_throughput_eps
    );

    // ── Indexing lag phase ────────────────────────────────────────────────────
    let mut lag_hist: Histogram<u64> = Histogram::new(4)?;
    let mut expected = seed_count;

    for _ in 0..args.live_events {
        global_seq += 1;
        let ev_data =
            kurrentdb::EventData::json("BenchmarkEvent", &BenchmarkEvent::new(global_seq, 0))
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .id(uuid::Uuid::new_v4());
        writer
            .inner()
            .append_to_stream(stream, &opts, vec![ev_data])
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
        lag_hist.record(t_ack.elapsed().as_micros() as u64)?;
    }

    indexer.abort();

    run_queries(
        &pg.pool,
        args.seed_events,
        "KurrentDB → PostgreSQL FTS",
        index_build_ms,
        index_throughput_eps,
        lag_hist,
    )
    .await
}

// ── MongoDB runner ────────────────────────────────────────────────────────────

pub async fn run_mongo(
    mongodb_url: &str,
    pg_url: &str,
    args: &MongoSearchBenchArgs,
) -> Result<SearchBenchResult> {
    use futures::StreamExt as _;
    use mongodb::bson::{doc, Document};

    let writer = crate::mongodb::client::MongoClient::connect(mongodb_url, &args.database).await?;
    let pg = crate::postgres::client::PostgresClient::connect(pg_url).await?;
    ensure_search_index_table(&pg.pool).await?;
    truncate_search_index(&pg.pool).await?;

    let coll_name = &args.stream_name;
    writer.ensure_collection(coll_name).await?;
    writer.truncate_collection(coll_name).await?;

    info!("search/mongodb: seeding {} events …", args.seed_events);
    let batch_size = 200usize;
    let mut global_seq = 0u64;

    for _ in 0..(args.seed_events / batch_size) {
        let payloads: Vec<BenchmarkEvent> = (0..batch_size)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        writer
            .append_batch(coll_name, "BenchmarkEvent", &payloads)
            .await?;
    }
    let rem = args.seed_events % batch_size;
    if rem > 0 {
        let payloads: Vec<BenchmarkEvent> = (0..rem)
            .map(|_| {
                global_seq += 1;
                BenchmarkEvent::new(global_seq, 0)
            })
            .collect();
        writer
            .append_batch(coll_name, "BenchmarkEvent", &payloads)
            .await?;
    }

    info!("search/mongodb: building index via cursor + change stream …");

    let pg_pool = pg.pool.clone();
    let db_url = mongodb_url.to_string();
    let db_name = args.database.clone();
    let coll_n = coll_name.clone();
    let seed_count = args.seed_events;
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0usize);
    let tx = progress_tx.clone();

    let t_index = Instant::now();

    let indexer = tokio::spawn(async move {
        let client = crate::mongodb::client::MongoClient::connect(&db_url, &db_name)
            .await
            .expect("indexer mongo connect");
        let coll: mongodb::Collection<Document> = client.database().collection(&coll_n);

        // Phase A: replay historical docs
        let mut cursor = coll
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .await
            .expect("find");
        let mut processed = 0usize;
        let mut buf: Vec<(i64, BenchmarkEvent)> = Vec::with_capacity(200);
        let mut gpos = 0i64;
        while let Some(Ok(doc)) = cursor.next().await {
            gpos += 1;
            if let (Ok(seq), Ok(task_id)) = (doc.get_i64("seq"), doc.get_i64("task_id")) {
                buf.push((
                    gpos,
                    BenchmarkEvent {
                        seq: seq as u64,
                        task_id: task_id as u64,
                        payload: vec![],
                        created_at: chrono::Utc::now(),
                    },
                ));
                if buf.len() >= 200 {
                    let _ = index_batch(&pg_pool, &coll_n, &buf).await;
                    processed += buf.len();
                    buf.clear();
                    let _ = tx.send(processed);
                }
            }
        }
        if !buf.is_empty() {
            let _ = index_batch(&pg_pool, &coll_n, &buf).await;
            processed += buf.len();
            buf.clear();
            let _ = tx.send(processed);
        }

        // Phase B: live change stream
        let mut cs = coll.watch().await.expect("watch");
        while let Some(Ok(event)) = cs.next().await {
            if let Some(doc) = event.full_document {
                gpos += 1;
                if let (Ok(seq), Ok(task_id)) = (doc.get_i64("seq"), doc.get_i64("task_id")) {
                    let ev = BenchmarkEvent {
                        seq: seq as u64,
                        task_id: task_id as u64,
                        payload: vec![],
                        created_at: chrono::Utc::now(),
                    };
                    let _ = index_batch(&pg_pool, &coll_n, &[(gpos, ev)]).await;
                    processed += 1;
                    let _ = tx.send(processed);
                }
            }
        }
    });

    {
        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= seed_count {
                break;
            }
            rx.changed().await?;
        }
    }
    let index_build_ms = t_index.elapsed().as_secs_f64() * 1000.0;
    let index_throughput_eps = args.seed_events as f64 / (index_build_ms / 1000.0);
    info!(
        "search/mongodb: index built in {:.0} ms ({:.0} ev/s)",
        index_build_ms, index_throughput_eps
    );

    let mut lag_hist: Histogram<u64> = Histogram::new(4)?;
    let mut expected = seed_count;
    for _ in 0..args.live_events {
        global_seq += 1;
        writer
            .append(
                coll_name,
                "BenchmarkEvent",
                &BenchmarkEvent::new(global_seq, 0),
            )
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
        lag_hist.record(t_ack.elapsed().as_micros() as u64)?;
    }

    indexer.abort();

    let backend = "MongoDB → PostgreSQL FTS".to_string();
    run_queries_named(
        &pg.pool,
        args.seed_events,
        &backend,
        index_build_ms,
        index_throughput_eps,
        lag_hist,
    )
    .await
}

// ── PostgreSQL runner ─────────────────────────────────────────────────────────

pub async fn run_postgres(pg_url: &str, args: &SearchBenchArgs) -> Result<SearchBenchResult> {
    let writer = crate::postgres::client::PostgresClient::connect(pg_url).await?;
    writer.ensure_bench_table_event_store().await?;
    writer.ensure_stream_versions_table().await?;
    writer.truncate_bench_table().await?;
    ensure_search_index_table(&writer.pool).await?;
    truncate_search_index(&writer.pool).await?;

    let stream = args.stream_name.as_str();
    info!("search/postgres: seeding {} events …", args.seed_events);

    let batch_size = 200usize;
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
    let rem = args.seed_events % batch_size;
    if rem > 0 {
        let payloads: Vec<BenchmarkEvent> = (0..rem)
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
                global_seq - rem as u64,
                0,
            )
            .await?;
    }

    info!("search/postgres: building index via polling projector …");

    let pg_url_s = pg_url.to_string();
    let stream_name = args.stream_name.clone();
    let seed_count = args.seed_events;
    let poll_ms = args.poll_interval_ms;
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0usize);
    let tx = progress_tx.clone();

    let t_index = Instant::now();

    let indexer = tokio::spawn(async move {
        let client = crate::postgres::client::PostgresClient::connect(&pg_url_s)
            .await
            .expect("indexer pg connect");
        let mut checkpoint: i64 = 0;
        let mut processed = 0usize;
        loop {
            match poll_events_for_indexing(&client.pool, &stream_name, checkpoint, 200).await {
                Ok(rows) if rows.is_empty() => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(poll_ms)).await;
                }
                Ok(rows) => {
                    let last_gpos = rows.last().map(|(g, _)| *g).unwrap_or(checkpoint);
                    let _ = index_batch(&client.pool, &stream_name, &rows).await;
                    processed += rows.len();
                    checkpoint = last_gpos;
                    let _ = tx.send(processed);
                }
                Err(e) => {
                    tracing::warn!("search/postgres poll error: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_millis(poll_ms)).await;
                }
            }
        }
    });

    {
        let mut rx = progress_rx.clone();
        loop {
            if *rx.borrow() >= seed_count {
                break;
            }
            rx.changed().await?;
        }
    }
    let index_build_ms = t_index.elapsed().as_secs_f64() * 1000.0;
    let index_throughput_eps = args.seed_events as f64 / (index_build_ms / 1000.0);
    info!(
        "search/postgres: index built in {:.0} ms ({:.0} ev/s)",
        index_build_ms, index_throughput_eps
    );

    let mut lag_hist: Histogram<u64> = Histogram::new(4)?;
    let mut expected = seed_count;
    for _ in 0..args.live_events {
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
        lag_hist.record(t_ack.elapsed().as_micros() as u64)?;
    }

    indexer.abort();

    run_queries(
        &writer.pool,
        args.seed_events,
        "PostgreSQL → PostgreSQL FTS",
        index_build_ms,
        index_throughput_eps,
        lag_hist,
    )
    .await
}

// ── Shared query runner ───────────────────────────────────────────────────────

trait BenchName {
    fn backend_name(&self) -> String;
}
impl BenchName for SearchBenchArgs {
    fn backend_name(&self) -> String {
        "KurrentDB → PostgreSQL FTS".to_string()
    }
}

async fn run_queries(
    pool: &sqlx::PgPool,
    indexed: usize,
    backend: &str,
    build_ms: f64,
    throughput: f64,
    lag_hist: Histogram<u64>,
) -> Result<SearchBenchResult> {
    run_queries_named(pool, indexed, backend, build_ms, throughput, lag_hist).await
}

async fn run_queries_named(
    pool: &sqlx::PgPool,
    indexed: usize,
    backend: &str,
    build_ms: f64,
    throughput: f64,
    lag_hist: Histogram<u64>,
) -> Result<SearchBenchResult> {
    info!(
        "search: running query benchmarks ({} events indexed) …",
        indexed
    );

    let reps = 200usize;

    // Exact: order_id = "ORD-42"
    let mut exact_hist: Histogram<u64> = Histogram::new(4)?;
    for i in 0..reps {
        let oid = format!("ORD-{}", i % 100);
        let t = Instant::now();
        let _ = query_exact(pool, &oid).await?;
        exact_hist.record(t.elapsed().as_micros() as u64 + 1)?;
    }

    // Prefix: product_id LIKE 'PROD-1%'
    let mut prefix_hist: Histogram<u64> = Histogram::new(4)?;
    for i in 0..reps {
        let prefix = format!("PROD-{}", i % 10);
        let t = Instant::now();
        let _ = query_prefix(pool, &prefix).await?;
        prefix_hist.record(t.elapsed().as_micros() as u64 + 1)?;
    }

    // Full-text: keyword search
    let terms = ["active", "completed", "order", "product", "task"];
    let mut fts_hist: Histogram<u64> = Histogram::new(4)?;
    for i in 0..reps {
        let term = terms[i % terms.len()];
        let t = Instant::now();
        let _ = query_fts(pool, term).await?;
        fts_hist.record(t.elapsed().as_micros() as u64 + 1)?;
    }

    // Date range: events from last 3600 seconds
    let mut range_hist: Histogram<u64> = Histogram::new(4)?;
    for _ in 0..reps {
        let t = Instant::now();
        let _ = query_range(pool, 3600).await?;
        range_hist.record(t.elapsed().as_micros() as u64 + 1)?;
    }

    info!(
        "search: queries done — exact p50 {} µs, FTS p50 {} µs",
        exact_hist.value_at_quantile(0.5),
        fts_hist.value_at_quantile(0.5)
    );

    Ok(SearchBenchResult {
        backend: backend.to_string(),
        indexed_events: indexed,
        index_build_ms: build_ms,
        index_throughput_eps: throughput,
        lag_p50_us: lag_hist.value_at_quantile(0.5),
        lag_p99_us: lag_hist.value_at_quantile(0.99),
        query_exact_p50_us: exact_hist.value_at_quantile(0.5),
        query_exact_p99_us: exact_hist.value_at_quantile(0.99),
        query_prefix_p50_us: prefix_hist.value_at_quantile(0.5),
        query_prefix_p99_us: prefix_hist.value_at_quantile(0.99),
        query_fts_p50_us: fts_hist.value_at_quantile(0.5),
        query_fts_p99_us: fts_hist.value_at_quantile(0.99),
        query_range_p50_us: range_hist.value_at_quantile(0.5),
        query_range_p99_us: range_hist.value_at_quantile(0.99),
    })
}

// ── Report ────────────────────────────────────────────────────────────────────

impl SearchBenchResult {
    #[allow(clippy::cast_precision_loss)]
    pub fn print_report(&self) {
        println!();
        println!("══════════════════════════════════════════════════════════════");
        println!("  Search-Index Projection Benchmark — {}", self.backend);
        println!("  ({} events indexed)", self.indexed_events);
        println!("══════════════════════════════════════════════════════════════");
        println!("  INDEX BUILD  — event-store → PostgreSQL FTS table");
        println!("  ──────────────────────────────────────────────────────────");
        println!("  Time to ready  : {:.0} ms", self.index_build_ms);
        println!("  Throughput     : {:.0} ev/s", self.index_throughput_eps);
        println!();
        println!("  INDEXING LAG  — write-ack → search-index updated");
        println!("  ──────────────────────────────────────────────────────────");
        println!("  p50            : {} µs", self.lag_p50_us);
        println!("  p99            : {} µs", self.lag_p99_us);
        println!();
        println!(
            "  QUERY LATENCY  — 200 queries each, {} events in index",
            self.indexed_events
        );
        println!("  ──────────────────────────────────────────────────────────");
        println!(
            "  Exact (order_id=)   p50 {:>6} µs  p99 {:>6} µs",
            self.query_exact_p50_us, self.query_exact_p99_us
        );
        println!(
            "  Prefix (LIKE x%)    p50 {:>6} µs  p99 {:>6} µs",
            self.query_prefix_p50_us, self.query_prefix_p99_us
        );
        println!(
            "  Full-text (FTS)     p50 {:>6} µs  p99 {:>6} µs",
            self.query_fts_p50_us, self.query_fts_p99_us
        );
        println!(
            "  Date range          p50 {:>6} µs  p99 {:>6} µs",
            self.query_range_p50_us, self.query_range_p99_us
        );
        println!("══════════════════════════════════════════════════════════════");
        println!();
    }

    pub fn print_json(&self) {
        let v = json!({
            "backend": self.backend,
            "indexed_events": self.indexed_events,
            "index_build_ms": self.index_build_ms,
            "index_throughput_eps": self.index_throughput_eps,
            "lag_p50_us": self.lag_p50_us,
            "lag_p99_us": self.lag_p99_us,
            "query_exact_p50_us": self.query_exact_p50_us,
            "query_exact_p99_us": self.query_exact_p99_us,
            "query_prefix_p50_us": self.query_prefix_p50_us,
            "query_prefix_p99_us": self.query_prefix_p99_us,
            "query_fts_p50_us": self.query_fts_p50_us,
            "query_fts_p99_us": self.query_fts_p99_us,
            "query_range_p50_us": self.query_range_p50_us,
            "query_range_p99_us": self.query_range_p99_us,
        });
        println!("{v}");
    }
}
