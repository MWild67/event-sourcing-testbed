//! Thin async wrapper around sqlx's PostgreSQL connection pool.
//!
//! Mirrors the interface of [`crate::mongodb::client::MongoClient`] so the
//! benchmark harness can swap backends without structural changes.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use uuid::Uuid;

// ─── Client ──────────────────────────────────────────────────────────────────

pub struct PostgresClient {
    pub pool: PgPool,
}

impl PostgresClient {
    /// Connect and return a pooled client.
    ///
    /// `url` example: `"postgres://postgres:postgres@localhost:5432/eventbench"`
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(128)
            // Short connect timeout so the readiness retry loop fails fast.
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .with_context(|| format!("failed to connect to PostgreSQL: {url}"))?;
        Ok(Self { pool })
    }

    // ── Health ────────────────────────────────────────────────────────────────

    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("PostgreSQL ping failed")?;
        Ok(())
    }

    // ── Schema bootstrap ──────────────────────────────────────────────────────

    /// Create the simple benchmark table (no version / global-position columns).
    pub async fn ensure_bench_table(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS bench_events (
                event_id    TEXT        NOT NULL PRIMARY KEY,
                stream_id   TEXT        NOT NULL,
                event_type  TEXT        NOT NULL,
                seq         BIGINT      NOT NULL,
                task_id     BIGINT      NOT NULL,
                payload     BYTEA       NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to create bench_events table")?;

        // Index on stream_id so per-stream reads are fast.
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_stream ON bench_events (stream_id)")
            .execute(&self.pool)
            .await
            .context("failed to create stream index")?;

        Ok(())
    }

    /// Create the event-store-mode table: adds the unique version constraint
    /// and a `global_position` identity column — mirrors KurrentDB semantics.
    pub async fn ensure_bench_table_event_store(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS bench_events (
                event_id        TEXT        NOT NULL PRIMARY KEY,
                stream_id       TEXT        NOT NULL,
                stream_version  BIGINT      NOT NULL,
                global_position BIGINT      GENERATED ALWAYS AS IDENTITY,
                event_type      TEXT        NOT NULL,
                seq             BIGINT      NOT NULL,
                task_id         BIGINT      NOT NULL,
                payload         BYTEA       NOT NULL,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                -- Append-Only Guard: duplicate (stream_id, stream_version) is
                -- a conflict = WrongExpectedVersion equivalent.
                UNIQUE (stream_id, stream_version)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to create bench_events (event-store) table")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_bench_global ON bench_events (global_position)",
        )
        .execute(&self.pool)
        .await
        .context("failed to create global_position index")?;

        Ok(())
    }

    /// Truncate the bench table so each run starts from a clean slate.
    /// Faster than DROP+CREATE because it keeps the schema/indexes in place.
    pub async fn truncate_bench_table(&self) -> Result<()> {
        sqlx::query("TRUNCATE TABLE bench_events RESTART IDENTITY CASCADE")
            .execute(&self.pool)
            .await
            .context("failed to truncate bench_events")?;
        Ok(())
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Insert a batch of events in a single round-trip using `INSERT … VALUES`.
    pub async fn append_batch<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        payloads: &[T],
        base_seq: u64,
        task_id: u64,
    ) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }
        // Use sqlx QueryBuilder for safe parameterised batch inserts.
        let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO bench_events (event_id, stream_id, event_type, seq, task_id, payload) ",
        );
        qb.push_values(payloads.iter().enumerate(), |mut b, (i, p)| {
            b.push_bind(Uuid::new_v4().to_string())
                .push_bind(stream_id)
                .push_bind(event_type)
                .push_bind((base_seq + i as u64) as i64)
                .push_bind(task_id as i64)
                .push_bind(serde_json::to_vec(p).unwrap_or_default());
        });
        qb.build()
            .execute(&self.pool)
            .await
            .context("batch insert failed")?;
        Ok(())
    }

    /// Insert a batch with monotonic `stream_version` and `global_position`.
    /// A unique-constraint violation on `(stream_id, stream_version)` surfaces
    /// as an `anyhow` error wrapping the duplicate-key DB error — equivalent to
    /// KurrentDB's `WrongExpectedVersion`.
    pub async fn append_batch_versioned<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        payloads: &[T],
        base_seq: u64,
        task_id: u64,
    ) -> Result<i64> {
        if payloads.is_empty() {
            return Ok(0);
        }
        let batch_len = payloads.len() as i64;

        if batch_len == 1 {
            // ── Single-event fast path: one CTE round trip ──────────────────
            //
            // The entire version-bump + insert executes as a single SQL
            // statement.  This keeps the stream_versions row lock for only
            // the statement duration (microseconds) and uses exactly one
            // pool connection instead of two, eliminating both lock
            // contention and connection-pool pressure under high concurrency.
            let payload_bytes = serde_json::to_vec(&payloads[0]).unwrap_or_default();
            sqlx::query(
                r#"
                WITH ver AS (
                    INSERT INTO stream_versions (stream_id, version)
                    VALUES ($1, 1)
                    ON CONFLICT (stream_id) DO UPDATE
                        SET version = stream_versions.version + 1
                    RETURNING version - 1 AS start_ver
                )
                INSERT INTO bench_events
                    (event_id, stream_id, stream_version, event_type, seq, task_id, payload)
                SELECT $2, $1, ver.start_ver, $3, $4, $5, $6
                FROM ver
                "#,
            )
            .bind(stream_id)
            .bind(Uuid::new_v4().to_string())
            .bind(event_type)
            .bind(base_seq as i64)
            .bind(task_id as i64)
            .bind(payload_bytes)
            .execute(&self.pool)
            .await
            .with_context(|| format!("single-event versioned insert failed for '{stream_id}'"))?;

            return Ok(1);
        }

        // ── Batch path (batch_size > 1): two separate autocommit queries ──────
        //
        // The UPSERT on stream_versions is autocommit — the row lock is
        // released the instant the statement completes, so concurrent tasks
        // for the same stream only block for microseconds before proceeding.
        // Each query uses one pool connection sequentially, so peak concurrent
        // connection count equals concurrency (64), well within the pool limit.
        let row = sqlx::query(
            r#"
            INSERT INTO stream_versions (stream_id, version)
            VALUES ($1, $2)
            ON CONFLICT (stream_id) DO UPDATE
                SET version = stream_versions.version + $2
            RETURNING version - $2  -- version before this batch
            "#,
        )
        .bind(stream_id)
        .bind(batch_len)
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("version counter update failed for '{stream_id}'"))?;
        let start_version: i64 = row.get(0);

        // ── Build and execute the batch insert ───────────────────────────────
        let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO bench_events \
             (event_id, stream_id, stream_version, event_type, seq, task_id, payload) ",
        );
        qb.push_values(payloads.iter().enumerate(), |mut b, (i, p)| {
            b.push_bind(Uuid::new_v4().to_string())
                .push_bind(stream_id)
                .push_bind(start_version + i as i64)
                .push_bind(event_type)
                .push_bind((base_seq + i as u64) as i64)
                .push_bind(task_id as i64)
                .push_bind(serde_json::to_vec(p).unwrap_or_default());
        });
        qb.build()
            .execute(&self.pool)
            .await
            .context("versioned batch insert failed")?;

        Ok(batch_len)
    }

    /// Create the `stream_versions` counter table used in event-store mode.
    pub async fn ensure_stream_versions_table(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS stream_versions \
             (stream_id TEXT NOT NULL PRIMARY KEY, version BIGINT NOT NULL DEFAULT 0)",
        )
        .execute(&self.pool)
        .await
        .context("failed to create stream_versions table")?;
        Ok(())
    }

    /// Pre-warm stream-version rows so the first insert doesn't pay an upsert
    /// overhead inside the timed window.
    pub async fn init_stream_versions(&self, stream_ids: &[String]) -> Result<()> {
        if stream_ids.is_empty() {
            return Ok(());
        }
        let mut qb: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new("INSERT INTO stream_versions (stream_id, version) ");
        qb.push_values(stream_ids.iter(), |mut b, id| {
            b.push_bind(id).push_bind(0i64);
        });
        qb.push(" ON CONFLICT DO NOTHING");
        qb.build()
            .execute(&self.pool)
            .await
            .context("failed to pre-warm stream versions")?;
        Ok(())
    }
}
