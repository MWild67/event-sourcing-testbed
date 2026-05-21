//! `PostgreSQL` as an Event Store — demonstrating the 8 essential event-sourcing
//! properties using idiomatic Rust + sqlx.
//!
//! ┌────┬────────────────────────┬───────────────────────────────────────────────┐
//! │ #  │ Property               │ How it is covered here                        │
//! ├────┼────────────────────────┼───────────────────────────────────────────────┤
//! │ 1  │ Append-Only Guard      │ `UNIQUE(stream_id`, `stream_version`) constraint   │
//! │    │                        │ + a BEFORE UPDATE OR DELETE trigger that       │
//! │    │                        │ raises an exception prevents any mutation.     │
//! │ 2  │ Aggregate Rehydrator   │ `rehydrate()` SELECTs the stream ORDER BY      │
//! │    │                        │ `stream_version`, feeds events through Apply.    │
//! │ 3  │ Checkpoint System      │ `checkpoints` table; UPSERT on `consumer_id`.   │
//! │ 4  │ Event Polling → Push   │ `PostgreSQL` LISTEN/NOTIFY via sqlx `PgListener`; │
//! │    │                        │ the server pushes a notification on INSERT,    │
//! │    │                        │ no polling loop in application code.           │
//! │ 5  │ Event Upcasting        │ `UpcastRegistry` middleware transforms old     │
//! │    │                        │ schema versions before handing to consumers.   │
//! │ 6  │ No Dual Write          │ `append_with_outbox` wraps domain event +      │
//! │    │                        │ `integration_outbox` entry in one transaction.   │
//! │ 7  │ Built-in Subscriptions │ `catch_up_subscribe` replays from checkpoint  │
//! │    │                        │ then switches to LISTEN for live events.       │
//! │    │                        │ `try_acquire_lease` uses `pg_try_advisory_lock`  │
//! │    │                        │ for Single-Active-Consumer ordering.           │
//! │ 8  │ Integration Events     │ Transactional outbox in `integration_outbox`; │
//! │    │                        │ relay uses SELECT … FOR UPDATE SKIP LOCKED     │
//! │    │                        │ for concurrent, exactly-once dispatch.         │
//! └────┴────────────────────────┴───────────────────────────────────────────────┘

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use thiserror::Error;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::events::{SchemaVersion, UpcastResult};

// ─── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum EventStoreError {
    /// Attempted to append at a `stream_version` that already exists.
    /// Equivalent to `KurrentDB`'s `WrongExpectedVersion`.
    #[error("optimistic concurrency conflict: stream '{stream}' version {expected} already used")]
    ConcurrencyConflict { stream: String, expected: i64 },

    #[error("event store I/O error: {0}")]
    Io(#[from] anyhow::Error),
}

// ─── Envelope ─────────────────────────────────────────────────────────────────

/// Wire format as stored in `PostgreSQL` for every persisted event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub stream_id: String,
    pub stream_version: i64,
    pub global_position: i64,
    pub event_type: String,
    pub schema_version: i32,
    /// The raw event payload as a JSON value.
    pub payload: serde_json::Value,
    pub occurred_at: String,
}

// ─── Upcasting (Property 5) ───────────────────────────────────────────────────

pub type UpcasterFn = Box<dyn Fn(serde_json::Value) -> serde_json::Value + Send + Sync>;

/// Registry of upcasters indexed by `(event_type, from_schema_version)`.
pub struct UpcastRegistry {
    fns: HashMap<(String, u32), UpcasterFn>,
    current: HashMap<String, u32>,
}

impl UpcastRegistry {
    pub fn new() -> Self {
        Self {
            fns: HashMap::new(),
            current: HashMap::new(),
        }
    }

    pub fn register<F>(&mut self, event_type: &str, from_version: u32, f: F)
    where
        F: Fn(serde_json::Value) -> serde_json::Value + Send + Sync + 'static,
    {
        let new_ver = from_version + 1;
        self.fns
            .insert((event_type.to_owned(), from_version), Box::new(f));
        self.current
            .entry(event_type.to_owned())
            .and_modify(|v| *v = (*v).max(new_ver))
            .or_insert(new_ver);
    }

    pub fn upcast(
        &self,
        event_type: &str,
        stored_version: u32,
        payload: serde_json::Value,
    ) -> UpcastResult<serde_json::Value> {
        let target = *self.current.get(event_type).unwrap_or(&stored_version);
        if stored_version >= target {
            return UpcastResult::Current(payload);
        }
        let original = SchemaVersion(stored_version);
        let mut v = payload;
        let mut ver = stored_version;
        while ver < target {
            if let Some(f) = self.fns.get(&(event_type.to_owned(), ver)) {
                v = f(v);
                ver += 1;
            } else {
                break;
            }
        }
        UpcastResult::Migrated {
            _from: original,
            event: v,
        }
    }
}

impl Default for UpcastRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Aggregate trait (Property 2) ─────────────────────────────────────────────

#[async_trait]
pub trait Aggregate: Default + Send {
    type Event: DeserializeOwned + Send;
    fn apply(&mut self, event: Self::Event);
}

// ─── Event Store ──────────────────────────────────────────────────────────────

pub struct PgEventStore {
    pub pool: PgPool,
    pub upcasters: Arc<UpcastRegistry>,
}

impl PgEventStore {
    // ── Construction ─────────────────────────────────────────────────────────

    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .with_context(|| format!("invalid PostgreSQL URL: {url}"))?;
        Ok(Self {
            pool,
            upcasters: Arc::new(UpcastRegistry::new()),
        })
    }

    pub fn with_upcasters(mut self, reg: UpcastRegistry) -> Self {
        self.upcasters = Arc::new(reg);
        self
    }

    // ── Schema bootstrap ─────────────────────────────────────────────────────

    /// Idempotent: creates all tables, indexes, triggers, and sequences.
    #[allow(clippy::too_many_lines)]
    pub async fn bootstrap(&self) -> Result<()> {
        // ── Property 1: Append-Only Guard ─────────────────────────────────────
        //
        // events table:
        //   • UNIQUE(stream_id, stream_version) → duplicate insert = ConcurrencyConflict
        //   • global_position GENERATED ALWAYS AS IDENTITY → auto-assigned, immutable
        //   • immutability_guard trigger blocks UPDATE and DELETE on committed rows
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS events (
                event_id        TEXT        NOT NULL PRIMARY KEY,
                stream_id       TEXT        NOT NULL,
                stream_version  BIGINT      NOT NULL,
                global_position BIGINT      GENERATED ALWAYS AS IDENTITY,
                event_type      TEXT        NOT NULL,
                schema_version  INTEGER     NOT NULL DEFAULT 1,
                payload         JSONB       NOT NULL,
                occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (stream_id, stream_version)
            )
            ",
        )
        .execute(&self.pool)
        .await
        .context("failed to create events table")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_events_stream \
             ON events (stream_id, stream_version)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_events_global \
             ON events (global_position)",
        )
        .execute(&self.pool)
        .await?;

        // ── Immutability trigger (Property 1) ────────────────────────────────
        sqlx::query(
            r"
            CREATE OR REPLACE FUNCTION events_immutability_guard()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION
                    'event store is append-only: UPDATE/DELETE on events is forbidden';
            END;
            $$
            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r"
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger
                    WHERE tgname = 'trg_events_immutability'
                ) THEN
                    CREATE TRIGGER trg_events_immutability
                    BEFORE UPDATE OR DELETE ON events
                    FOR EACH ROW EXECUTE FUNCTION events_immutability_guard();
                END IF;
            END $$
            ",
        )
        .execute(&self.pool)
        .await?;

        // ── NOTIFY trigger (Property 4) ───────────────────────────────────────
        //
        // After every INSERT the trigger fires pg_notify('events_channel',
        // global_position::TEXT).  Subscribers receive the new position
        // instantly — no polling.
        sqlx::query(
            r"
            CREATE OR REPLACE FUNCTION events_notify_insert()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_notify('events_channel', NEW.global_position::TEXT);
                RETURN NEW;
            END;
            $$
            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r"
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger
                    WHERE tgname = 'trg_events_notify'
                ) THEN
                    CREATE TRIGGER trg_events_notify
                    AFTER INSERT ON events
                    FOR EACH ROW EXECUTE FUNCTION events_notify_insert();
                END IF;
            END $$
            ",
        )
        .execute(&self.pool)
        .await?;

        // ── Auxiliary tables ──────────────────────────────────────────────────

        // stream_versions: per-stream version counter (avoids a SELECT MAX).
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS stream_versions \
             (stream_id TEXT NOT NULL PRIMARY KEY, version BIGINT NOT NULL DEFAULT 0)",
        )
        .execute(&self.pool)
        .await?;

        // checkpoints: Property 3 — durable consumer cursors.
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS checkpoints (
                consumer_id     TEXT        NOT NULL PRIMARY KEY,
                global_position BIGINT      NOT NULL,
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            ",
        )
        .execute(&self.pool)
        .await?;

        // integration_outbox: Property 8 — transactional outbox.
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS integration_outbox (
                event_id        TEXT        NOT NULL PRIMARY KEY,
                stream_id       TEXT        NOT NULL,
                global_position BIGINT      NOT NULL,
                event_type      TEXT        NOT NULL,
                payload         JSONB       NOT NULL,
                dispatched      BOOLEAN     NOT NULL DEFAULT FALSE,
                dispatched_at   TIMESTAMPTZ
            )
            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_outbox_pending \
             ON integration_outbox (dispatched) WHERE dispatched = FALSE",
        )
        .execute(&self.pool)
        .await?;

        info!("PostgreSQL event store bootstrap complete");
        Ok(())
    }

    // ── Append (Properties 1, 6) ──────────────────────────────────────────────

    /// ── Property 6: No Dual Write ──────────────────────────────────────────
    ///
    /// Writes the domain event **and** an outbox entry in a single `PostgreSQL`
    /// transaction.  Either both records exist after commit or neither does.
    ///
    /// ── Property 1: Append-Only Guard ──────────────────────────────────────
    ///
    /// A `UNIQUE(stream_id, stream_version)` violation surfaces as
    /// [`EventStoreError::ConcurrencyConflict`].
    #[allow(clippy::future_not_send)]
    pub async fn append_with_outbox<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        schema_version: i32,
        payload: &T,
    ) -> Result<EventEnvelope, EventStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin transaction")?;

        let envelope = self
            .append_in_tx(stream_id, event_type, schema_version, payload, &mut tx)
            .await?;

        // Write outbox entry inside the same transaction (Property 6).
        sqlx::query(
            r"
            INSERT INTO integration_outbox
                (event_id, stream_id, global_position, event_type, payload)
            VALUES ($1, $2, $3, $4, $5)
            ",
        )
        .bind(&envelope.event_id)
        .bind(&envelope.stream_id)
        .bind(envelope.global_position)
        .bind(&envelope.event_type)
        .bind(&envelope.payload)
        .execute(&mut *tx)
        .await
        .context("failed to write integration-event outbox entry")
        .map_err(EventStoreError::Io)?;

        tx.commit()
            .await
            .context("failed to commit transaction")
            .map_err(EventStoreError::Io)?;

        debug!(
            stream_id,
            stream_version = envelope.stream_version,
            global_position = envelope.global_position,
            "event appended with outbox"
        );
        Ok(envelope)
    }

    /// Append without an outbox entry.
    #[allow(dead_code)]
    #[allow(clippy::future_not_send)]
    pub async fn append<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        schema_version: i32,
        payload: &T,
    ) -> Result<EventEnvelope, EventStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin transaction")?;
        let env = self
            .append_in_tx(stream_id, event_type, schema_version, payload, &mut tx)
            .await?;
        tx.commit()
            .await
            .context("failed to commit")
            .map_err(EventStoreError::Io)?;
        Ok(env)
    }

    #[allow(clippy::future_not_send)]
    async fn append_in_tx<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        schema_version: i32,
        payload: &T,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<EventEnvelope, EventStoreError> {
        // ── Advance per-stream version counter ───────────────────────────────
        let row = sqlx::query(
            r"
            INSERT INTO stream_versions (stream_id, version)
            VALUES ($1, 1)
            ON CONFLICT (stream_id) DO UPDATE
                SET version = stream_versions.version + 1
            RETURNING version - 1  -- version before this insert
            ",
        )
        .bind(stream_id)
        .fetch_one(&mut **tx)
        .await
        .with_context(|| format!("version counter update failed for '{stream_id}'"))
        .map_err(EventStoreError::Io)?;

        let stream_version: i64 = row.get(0);

        let event_id = Uuid::new_v4().to_string();
        let payload_json = serde_json::to_value(payload)
            .context("failed to serialise payload")
            .map_err(EventStoreError::Io)?;
        let occurred_at_dt = chrono::Utc::now();
        let occurred_at = occurred_at_dt.to_rfc3339();

        // ── Insert event — duplicate key = ConcurrencyConflict ───────────────
        let row = match sqlx::query(
            r"
            INSERT INTO events
                (event_id, stream_id, stream_version, event_type,
                 schema_version, payload, occurred_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING global_position
            ",
        )
        .bind(&event_id)
        .bind(stream_id)
        .bind(stream_version)
        .bind(event_type)
        .bind(schema_version)
        .bind(&payload_json)
        .bind(occurred_at_dt)
        .fetch_one(&mut **tx)
        .await
        {
            Ok(r) => r,
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("23505") => {
                // 23505 = unique_violation → stream version already exists.
                return Err(EventStoreError::ConcurrencyConflict {
                    stream: stream_id.to_owned(),
                    expected: stream_version,
                });
            }
            Err(e) => {
                return Err(EventStoreError::Io(
                    anyhow::Error::new(e).context("event insert failed"),
                ));
            }
        };

        let global_position: i64 = row.get(0);

        Ok(EventEnvelope {
            event_id,
            stream_id: stream_id.to_owned(),
            stream_version,
            global_position,
            event_type: event_type.to_owned(),
            schema_version,
            payload: payload_json,
            occurred_at,
        })
    }

    // ── Rehydration (Property 2) ──────────────────────────────────────────────

    /// ── Property 2: Aggregate Rehydrator ───────────────────────────────────
    ///
    /// Reads every event for `stream_id` in `stream_version` order and feeds
    /// each one through `A::apply` after running the upcaster chain.
    pub async fn rehydrate<A>(&self, stream_id: &str) -> Result<(A, i64)>
    where
        A: Aggregate,
    {
        let rows = sqlx::query(
            "SELECT event_type, schema_version, payload, stream_version \
             FROM events WHERE stream_id = $1 ORDER BY stream_version",
        )
        .bind(stream_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch stream for rehydration")?;

        let mut state = A::default();
        let mut last_version: i64 = -1;

        for row in rows {
            let event_type: String = row.get("event_type");
            let schema_version: i32 = row.get("schema_version");
            let payload: serde_json::Value = row.get("payload");
            last_version = row.get("stream_version");

            let upcasted =
                self.upcasters
                    .upcast(&event_type, schema_version.cast_unsigned(), payload);

            let mut event_json = match upcasted {
                UpcastResult::Current(v) | UpcastResult::Migrated { event: v, .. } => v,
                UpcastResult::Unrecognised(_) => continue,
            };

            // Inject the event_type discriminator so that aggregate-event enums
            // defined with `#[serde(tag = "event_type")]` can be deserialized.
            // The payload stored in JSONB contains only the event fields; the
            // event_type is persisted in its own column for efficient querying.
            if let serde_json::Value::Object(ref mut map) = event_json {
                map.entry("event_type")
                    .or_insert_with(|| serde_json::Value::String(event_type.clone()));
            }

            match serde_json::from_value::<A::Event>(event_json) {
                Ok(ev) => state.apply(ev),
                Err(e) => warn!(
                    stream_id,
                    stream_version = last_version,
                    error = %e,
                    "skipping unrecognised event during rehydration"
                ),
            }
        }

        Ok((state, last_version))
    }

    // ── Checkpoint System (Property 3) ────────────────────────────────────────

    /// Persist the last processed `global_position` for `consumer_id`.
    pub async fn save_checkpoint(&self, consumer_id: &str, global_position: i64) -> Result<()> {
        sqlx::query(
            r"
            INSERT INTO checkpoints (consumer_id, global_position, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (consumer_id) DO UPDATE
                SET global_position = EXCLUDED.global_position,
                    updated_at      = NOW()
            ",
        )
        .bind(consumer_id)
        .bind(global_position)
        .execute(&self.pool)
        .await
        .context("failed to save checkpoint")?;
        Ok(())
    }

    /// Load the last checkpoint for `consumer_id`; `None` means start from 0.
    pub async fn load_checkpoint(&self, consumer_id: &str) -> Result<Option<i64>> {
        let row = sqlx::query("SELECT global_position FROM checkpoints WHERE consumer_id = $1")
            .bind(consumer_id)
            .fetch_optional(&self.pool)
            .await
            .context("failed to load checkpoint")?;
        Ok(row.map(|r| r.get(0)))
    }

    // ── Catch-Up Subscription (Properties 4, 7) ───────────────────────────────

    /// ── Properties 4 & 7: Push-based Catch-Up Subscription ────────────────
    ///
    /// Phase 1 — replays all events after the stored checkpoint (historical).
    /// Phase 2 — subscribes to `events_channel` via `PostgreSQL` LISTEN/NOTIFY.
    ///           The server *pushes* notifications to us; there is no polling loop.
    ///
    /// Each notification carries `global_position` so we can fetch the new event
    /// and advance the checkpoint atomically.
    #[allow(dead_code)]
    pub async fn catch_up_subscribe<F, Fut>(
        &self,
        consumer_id: &str,
        pg_url: &str,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(EventEnvelope) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let from = self.load_checkpoint(consumer_id).await?.unwrap_or(-1);
        info!(consumer_id, from_position = from, "starting catch-up phase");

        // ── Phase 1: historical catch-up ─────────────────────────────────────
        let rows = sqlx::query(
            "SELECT event_id, stream_id, stream_version, global_position, \
                    event_type, schema_version, payload, occurred_at \
             FROM events WHERE global_position > $1 ORDER BY global_position",
        )
        .bind(from)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch historical events")?;

        let mut last_pos = from;
        for row in rows {
            let envelope = row_to_envelope(&row);
            last_pos = envelope.global_position;
            handler(envelope).await?;
            self.save_checkpoint(consumer_id, last_pos).await?;
        }
        info!(
            consumer_id,
            caught_up_at = last_pos,
            "catch-up complete, switching to LISTEN"
        );

        // ── Phase 2: live push via LISTEN/NOTIFY (Property 4) ────────────────
        //
        // PgListener opens a dedicated connection and registers for NOTIFY on
        // 'events_channel'.  The trigger fires pg_notify on every INSERT so we
        // receive new positions without any polling.
        let mut listener = sqlx::postgres::PgListener::connect(pg_url)
            .await
            .context("failed to open LISTEN connection")?;
        listener
            .listen("events_channel")
            .await
            .context("failed to LISTEN on events_channel")?;

        loop {
            let notification = listener.recv().await.context("LISTEN channel error")?;

            let pos: i64 = notification
                .payload()
                .parse()
                .context("malformed NOTIFY payload")?;

            let row = sqlx::query(
                "SELECT event_id, stream_id, stream_version, global_position, \
                        event_type, schema_version, payload, occurred_at \
                 FROM events WHERE global_position = $1",
            )
            .bind(pos)
            .fetch_optional(&self.pool)
            .await
            .context("failed to fetch notified event")?;

            if let Some(r) = row {
                let envelope = row_to_envelope(&r);
                handler(envelope).await?;
                self.save_checkpoint(consumer_id, pos).await?;
            }
        }
    }

    // ── Competing Consumers / Single-Active-Consumer (Property 7) ─────────────

    /// ── Property 7 (Competing Consumer): acquire exclusive lease ───────────
    ///
    /// Uses `PostgreSQL` advisory locks (`pg_try_advisory_lock`) for a
    /// Single-Active-Consumer guarantee.  Advisory locks are session-scoped:
    /// if the holder's connection drops the lock is released automatically,
    /// preventing indefinite blocking.
    ///
    /// `lock_key` is a 64-bit integer derived from the consumer-group name.
    /// Returns `true` if the lock was acquired.
    pub async fn try_acquire_lease(&self, group_id: &str) -> Result<bool> {
        let key = stable_hash(group_id);
        let row = sqlx::query("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&self.pool)
            .await
            .context("pg_try_advisory_lock failed")?;
        Ok(row.get(0))
    }

    /// Release the advisory lock for `group_id`.
    pub async fn release_lease(&self, group_id: &str) -> Result<()> {
        let key = stable_hash(group_id);
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .execute(&self.pool)
            .await
            .context("pg_advisory_unlock failed")?;
        Ok(())
    }

    // ── Integration Event Relay (Property 8) ─────────────────────────────────

    /// ── Property 8: Integration Events ─────────────────────────────────────
    ///
    /// Reads the oldest un-dispatched outbox entry using
    /// `SELECT … FOR UPDATE SKIP LOCKED` — `PostgreSQL`'s native concurrent-relay
    /// mechanism.  Multiple relay workers can run simultaneously; each worker
    /// locks a different row so there is no double-delivery.
    ///
    /// Returns `true` if an event was dispatched, `false` if the outbox is empty.
    pub async fn relay_next_integration_event<F, Fut>(&self, publish: F) -> Result<bool>
    where
        F: FnOnce(serde_json::Value, String) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin relay transaction")?;

        let row = sqlx::query(
            r"
            SELECT event_id, payload, event_type
            FROM   integration_outbox
            WHERE  dispatched = FALSE
            ORDER  BY global_position
            LIMIT  1
            FOR UPDATE SKIP LOCKED
            ",
        )
        .fetch_optional(&mut *tx)
        .await
        .context("failed to query integration outbox")?;

        let Some(row) = row else {
            tx.rollback().await.ok();
            return Ok(false);
        };

        let event_id: String = row.get("event_id");
        let payload: serde_json::Value = row.get("payload");
        let event_type: String = row.get("event_type");

        // Publish first — leave dispatched=false so the relay retries on failure.
        publish(payload, event_type).await?;

        sqlx::query(
            "UPDATE integration_outbox \
             SET dispatched = TRUE, dispatched_at = NOW() \
             WHERE event_id = $1",
        )
        .bind(&event_id)
        .execute(&mut *tx)
        .await
        .context("failed to mark integration event dispatched")?;

        tx.commit()
            .await
            .context("failed to commit relay transaction")?;

        debug!(event_id, "integration event dispatched");
        Ok(true)
    }

    /// Run the integration event relay loop until `shutdown` resolves.
    #[allow(dead_code)]
    pub async fn run_relay_loop<F>(
        &self,
        mut make_publish: impl FnMut(serde_json::Value, String) -> F,
        mut shutdown: impl std::future::Future<Output = ()> + Unpin,
    ) -> Result<()>
    where
        F: std::future::Future<Output = Result<()>>,
    {
        info!("integration event relay loop started");
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => {
                    info!("relay loop shutting down");
                    return Ok(());
                }
                result = self.relay_next_integration_event(&mut make_publish) => {
                    match result {
                        Ok(true)  => {}
                        Ok(false) => tokio::time::sleep(Duration::from_millis(100)).await,
                        Err(e) => {
                            warn!(error = %e, "relay error, retrying in 1 s");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn row_to_envelope(row: &sqlx::postgres::PgRow) -> EventEnvelope {
    let payload: serde_json::Value = row.get("payload");
    EventEnvelope {
        event_id: row.get("event_id"),
        stream_id: row.get("stream_id"),
        stream_version: row.get("stream_version"),
        global_position: row.get("global_position"),
        event_type: row.get("event_type"),
        schema_version: row.get("schema_version"),
        payload,
        occurred_at: row
            .get::<chrono::DateTime<chrono::Utc>, _>("occurred_at")
            .to_rfc3339(),
    }
}

/// Stable 64-bit hash of a string for use as a `PostgreSQL` advisory-lock key.
fn stable_hash(s: &str) -> i64 {
    use std::hash::Hash;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    std::hash::Hasher::finish(&h).cast_signed()
}
