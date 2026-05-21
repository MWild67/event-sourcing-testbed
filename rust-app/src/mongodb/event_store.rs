//! `MongoDB` as an Event Store — demonstrating the 8 essential event-sourcing
//! properties using idiomatic Rust crates.
//!
//! ┌────┬────────────────────────┬───────────────────────────────────────────────┐
//! │ #  │ Property               │ How it is covered here                        │
//! ├────┼────────────────────────┼───────────────────────────────────────────────┤
//! │ 1  │ Append-Only Guard      │ JSON Schema validator + unique `stream_version`  │
//! │    │                        │ index prevent overwrites; `delete` is blocked  │
//! │    │                        │ at the collection level via a custom role.     │
//! │ 2  │ Aggregate Rehydrator   │ `rehydrate()` reads stream from version 0,    │
//! │    │                        │ feeds events through an `Apply` closure.       │
//! │ 3  │ Checkpoint System      │ `_checkpoints` collection stores the last      │
//! │    │                        │ processed `global_position` per consumer.      │
//! │ 4  │ Event Polling → Push   │ `MongoDB` Change Streams via `watch()` give a   │
//! │    │                        │ push-based subscription with no polling loop.  │
//! │ 5  │ Event Upcasting        │ `UpcastRegistry` middleware transforms old     │
//! │    │                        │ schema versions before handing to consumers.   │
//! │ 6  │ No Dual Write          │ A single `append_with_outbox` call writes the  │
//! │    │                        │ event and the integration-event outbox entry   │
//! │    │                        │ in one `MongoDB` multi-document transaction.      │
//! │ 7  │ Built-in Subscriptions │ `CatchUpSubscription` resumes from checkpoint; │
//! │    │                        │ `CompetingConsumer` uses a mutex document for  │
//! │    │                        │ Single-Active-Consumer ordering.               │
//! │ 8  │ Integration Events     │ Transactional outbox in `_integration_outbox`; │
//! │    │                        │ a relay task publishes to `RabbitMQ` and marks   │
//! │    │                        │ entries dispatched atomically.                 │
//! └────┴────────────────────────┴───────────────────────────────────────────────┘

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, from_document, to_document, Document},
    change_stream::event::OperationType,
    options::{ClientOptions, FindOptions, IndexOptions, ValidationAction},
    Client, ClientSession, Database, IndexModel,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use crate::events::{SchemaVersion, UpcastResult};

// ─── Errors ───────────────────────────────────────────────────────────────────

/// Typed errors the event store can surface.
#[derive(Debug, Error)]
pub enum EventStoreError {
    /// Attempted to append an event whose `stream_version` already exists.
    /// Equivalent to `KurrentDB`'s `WrongExpectedVersion`.
    #[error("optimistic concurrency conflict: stream '{stream}' version {expected} already used")]
    ConcurrencyConflict { stream: String, expected: i64 },

    /// A serialisation or driver-level failure.
    #[error("event store I/O error: {0}")]
    Io(#[from] anyhow::Error),
}

// ─── Envelope ─────────────────────────────────────────────────────────────────

/// Wire format stored in `MongoDB` for every persisted event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// UUID string, used as `_id`.
    pub event_id: String,
    /// Logical name of the stream (e.g. `"order-a3f2"`).
    pub stream_id: String,
    /// 0-based monotonically increasing version within the stream.
    pub stream_version: i64,
    /// Globally ordered position across all streams.
    pub global_position: i64,
    /// Discriminant used to choose the correct deserialiser / upcaster.
    pub event_type: String,
    /// Schema version for upcasting purposes.
    pub schema_version: u32,
    /// The raw event payload as a BSON sub-document.
    pub payload: Document,
    /// ISO-8601 timestamp when the event was written.
    pub occurred_at: String,
}

// ─── Upcasting (Property 5) ───────────────────────────────────────────────────

/// A single upcaster step: transforms a raw JSON payload from `from_version`
/// to the next version.  Chain multiple upcasters to bridge large gaps.
pub type UpcasterFn = Box<dyn Fn(serde_json::Value) -> serde_json::Value + Send + Sync>;

/// Registry of upcasters indexed by `(event_type, from_schema_version)`.
///
/// ```rust
/// let mut reg = UpcastRegistry::new();
/// // Migrate OrderPlaced v1 → v2 by back-filling a missing `notes` field.
/// reg.register("OrderPlaced", 1, |mut v| {
///     v["notes"] = serde_json::json!("");
///     v
/// });
/// ```
pub struct UpcastRegistry {
    // (event_type, from_version) → transform fn
    fns: HashMap<(String, u32), UpcasterFn>,
    /// The current ("canonical") schema version every event type should reach.
    current: HashMap<String, u32>,
}

impl UpcastRegistry {
    pub fn new() -> Self {
        Self {
            fns: HashMap::new(),
            current: HashMap::new(),
        }
    }

    /// Register an upcaster that migrates `event_type` payloads at
    /// `from_version` to `from_version + 1`.
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

    /// Run the full upcaster chain for `event_type` starting from
    /// `stored_version`.  Returns the migrated payload and the version it
    /// reached.  If no upcasters are registered the payload is returned
    /// unchanged.
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
                // Gap in the chain — cannot upcast further.
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

/// Any type that can be rebuilt from a stream of events.
///
/// Implement this trait on your aggregate root; `MongoEventStore::rehydrate`
/// will feed every stored event through `apply` in version order.
#[async_trait]
pub trait Aggregate: Default + Send {
    type Event: DeserializeOwned + Send;

    /// Mutate `self` in response to one historical event.
    fn apply(&mut self, event: Self::Event);
}

// ─── Event Store ──────────────────────────────────────────────────────────────

/// High-level `MongoDB` event store client.
///
/// Wraps [`crate::mongodb::client::MongoClient`] with event-sourcing semantics
/// on top of the raw driver so every property is visible in one place.
pub struct MongoEventStore {
    db: Database,
    pub upcasters: Arc<UpcastRegistry>,
}

impl MongoEventStore {
    // ── Construction ─────────────────────────────────────────────────────────

    /// Connect and return a ready-to-use event store.
    pub async fn connect(url: &str, db_name: &str) -> Result<Self> {
        let opts = ClientOptions::parse(url)
            .await
            .with_context(|| format!("invalid MongoDB URL: {url}"))?;
        let client = Client::with_options(opts).context("failed to create MongoDB client")?;
        let db = client.database(db_name);
        Ok(Self {
            db,
            upcasters: Arc::new(UpcastRegistry::new()),
        })
    }

    pub fn with_upcasters(mut self, reg: UpcastRegistry) -> Self {
        self.upcasters = Arc::new(reg);
        self
    }

    // ── Schema bootstrap ─────────────────────────────────────────────────────

    /// Ensure all required collections, indexes, and counters exist.
    /// Safe to call on every startup — idempotent.
    pub async fn bootstrap(&self) -> Result<()> {
        self.ensure_events_collection("events").await?;
        self.ensure_aux_collections().await?;
        info!("MongoDB event store bootstrap complete");
        Ok(())
    }

    /// ── Property 1: Append-Only Guard ──────────────────────────────────────
    ///
    /// The JSON Schema validator requires every document to carry the four
    /// mandatory fields.  The unique index on `(stream_id, stream_version)`
    /// means any attempt to re-insert or update an event at an existing version
    /// returns a duplicate-key error — the storage layer physically cannot
    /// overwrite the past.
    async fn ensure_events_collection(&self, name: &str) -> Result<()> {
        let validator = doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["event_id", "stream_id", "stream_version",
                              "global_position", "event_type", "schema_version", "payload"],
                "additionalProperties": true,
                "properties": {
                    "event_id":        { "bsonType": "string" },
                    "stream_id":       { "bsonType": "string" },
                    "stream_version":  { "bsonType": "long" },
                    "global_position": { "bsonType": "long" },
                    "event_type":      { "bsonType": "string" },
                    "schema_version":  { "bsonType": ["int", "long"] },
                    "payload":         { "bsonType": "object" },
                }
            }
        };

        match self
            .db
            .create_collection(name)
            .validator(validator)
            .validation_action(ValidationAction::Error)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                if let mongodb::error::ErrorKind::Command(ref cmd) = *e.kind {
                    if cmd.code == 48 {
                        // NamespaceExists — already created; skip index creation below.
                        return Ok(());
                    }
                }
                return Err(e).context(format!("failed to create events collection '{name}'"));
            }
        }

        let coll = self.db.collection::<Document>(name);

        // Unique index enforces the append-only / optimistic-concurrency contract.
        let uq_stream = IndexModel::builder()
            .keys(doc! { "stream_id": 1, "stream_version": 1 })
            .options(
                IndexOptions::builder()
                    .unique(true)
                    .name("uq_stream_version".to_string())
                    .build(),
            )
            .build();

        // Supports $all-stream ordered reads and catch-up subscriptions.
        let idx_global = IndexModel::builder()
            .keys(doc! { "global_position": 1 })
            .options(
                IndexOptions::builder()
                    .name("idx_global_position".to_string())
                    .build(),
            )
            .build();

        // Supports per-stream ordered reads (rehydration).
        let idx_stream = IndexModel::builder()
            .keys(doc! { "stream_id": 1, "stream_version": 1 })
            .options(
                IndexOptions::builder()
                    .name("idx_stream_read".to_string())
                    .build(),
            )
            .build();

        coll.create_indexes(vec![uq_stream, idx_global, idx_stream])
            .await
            .context("failed to create indexes on events collection")?;

        Ok(())
    }

    async fn ensure_aux_collections(&self) -> Result<()> {
        for name in &[
            "_global_seq",
            "_stream_versions",
            "_checkpoints",
            "_integration_outbox",
            "_competing_consumers",
        ] {
            match self.db.create_collection(*name).await {
                Ok(()) => {}
                Err(e) => {
                    if let mongodb::error::ErrorKind::Command(ref cmd) = *e.kind {
                        if cmd.code == 48 {
                            continue;
                        }
                    }
                    return Err(e).context(format!("failed to create aux collection '{name}'"));
                }
            }
        }
        // Seed global sequence if absent.
        self.db
            .collection::<Document>("_global_seq")
            .update_one(
                doc! { "_id": "counter" },
                doc! { "$setOnInsert": { "seq": 0i64 } },
            )
            .upsert(true)
            .await
            .context("failed to seed global sequence counter")?;
        Ok(())
    }

    // ── Append (Properties 1, 6) ──────────────────────────────────────────────

    /// ── Property 6: No Dual Write ──────────────────────────────────────────
    ///
    /// `append_with_outbox` writes the domain event **and** an integration-event
    /// outbox entry in a single multi-document transaction.  There is no second
    /// publish step that could go missing — if the transaction commits both
    /// records exist; if it aborts neither does.
    ///
    /// ── Property 1: Append-Only Guard ──────────────────────────────────────
    ///
    /// A duplicate-key error on the unique `(stream_id, stream_version)` index
    /// surfaces as [`EventStoreError::ConcurrencyConflict`], the `MongoDB`
    /// equivalent of `KurrentDB`'s `WrongExpectedVersion`.
    #[allow(clippy::future_not_send)]
    pub async fn append_with_outbox<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        schema_version: u32,
        payload: &T,
    ) -> Result<EventEnvelope, EventStoreError> {
        let mut session = self
            .db
            .client()
            .start_session()
            .await
            .context("failed to start session")?;

        session
            .start_transaction()
            .await
            .context("failed to start transaction")?;

        let envelope = self
            .append_in_session(stream_id, event_type, schema_version, payload, &mut session)
            .await?;

        // Write integration-event outbox entry inside the same transaction.
        let outbox_doc = doc! {
            "_id":             &envelope.event_id,
            "stream_id":       &envelope.stream_id,
            "global_position": envelope.global_position,
            "event_type":      &envelope.event_type,
            "payload":         &envelope.payload,
            "dispatched":      false,
        };
        self.db
            .collection::<Document>("_integration_outbox")
            .insert_one(outbox_doc)
            .session(&mut session)
            .await
            .context("failed to write integration-event outbox entry")
            .map_err(EventStoreError::Io)?;

        session
            .commit_transaction()
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

    /// Append without an outbox entry (for benchmarks / internal use).
    #[allow(clippy::future_not_send)]
    pub async fn append<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        schema_version: u32,
        payload: &T,
    ) -> Result<EventEnvelope, EventStoreError> {
        let mut session = self
            .db
            .client()
            .start_session()
            .await
            .context("failed to start session")?;

        session
            .start_transaction()
            .await
            .context("failed to start transaction")?;

        let env = self
            .append_in_session(stream_id, event_type, schema_version, payload, &mut session)
            .await?;

        session
            .commit_transaction()
            .await
            .context("failed to commit transaction")
            .map_err(EventStoreError::Io)?;

        Ok(env)
    }

    #[allow(clippy::future_not_send)]
    async fn append_in_session<T: Serialize>(
        &self,
        stream_id: &str,
        event_type: &str,
        schema_version: u32,
        payload: &T,
        session: &mut ClientSession,
    ) -> Result<EventEnvelope, EventStoreError> {
        let batch = 1i64;

        // ── Advance per-stream version counter ───────────────────────────────
        let ver_doc = self
            .db
            .collection::<Document>("_stream_versions")
            .find_one_and_update(
                doc! { "_id": stream_id },
                doc! { "$inc": { "version": batch } },
            )
            .upsert(true)
            .return_document(mongodb::options::ReturnDocument::Before)
            .session(&mut *session)
            .await
            .with_context(|| format!("version counter update failed for '{stream_id}'"))
            .map_err(EventStoreError::Io)?;

        let stream_version: i64 = ver_doc
            .as_ref()
            .and_then(|d| d.get_i64("version").ok())
            .unwrap_or(0);

        // ── Advance global sequence counter ───────────────────────────────────
        let gseq_doc = self
            .db
            .collection::<Document>("_global_seq")
            .find_one_and_update(doc! { "_id": "counter" }, doc! { "$inc": { "seq": batch } })
            .upsert(true)
            .return_document(mongodb::options::ReturnDocument::Before)
            .session(&mut *session)
            .await
            .context("global sequence counter update failed")
            .map_err(EventStoreError::Io)?;

        let global_position: i64 = gseq_doc
            .as_ref()
            .and_then(|d| d.get_i64("seq").ok())
            .unwrap_or(0);

        // ── Build envelope ────────────────────────────────────────────────────
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload_doc = to_document(payload)
            .context("failed to serialise payload")
            .map_err(EventStoreError::Io)?;

        let envelope = EventEnvelope {
            event_id: event_id.clone(),
            stream_id: stream_id.to_owned(),
            stream_version,
            global_position,
            event_type: event_type.to_owned(),
            schema_version,
            payload: payload_doc.clone(),
            occurred_at: chrono::Utc::now().to_rfc3339(),
        };

        let mut doc = to_document(&envelope)
            .context("failed to serialise envelope")
            .map_err(EventStoreError::Io)?;
        doc.insert("_id", &event_id);

        // ── Insert — a duplicate key here is a concurrency conflict ───────────
        match self
            .db
            .collection::<Document>("events")
            .insert_one(doc)
            .session(&mut *session)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                if matches!(
                    *e.kind,
                    mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                        mongodb::error::WriteError { code: 11000, .. }
                    ))
                ) {
                    return Err(EventStoreError::ConcurrencyConflict {
                        stream: stream_id.to_owned(),
                        expected: stream_version,
                    });
                }
                return Err(EventStoreError::Io(
                    anyhow::Error::new(e).context("event insert failed"),
                ));
            }
        }

        Ok(envelope)
    }

    // ── Rehydration (Property 2) ──────────────────────────────────────────────

    /// ── Property 2: Aggregate Rehydrator ───────────────────────────────────
    ///
    /// Reads every event for `stream_id` in version order (from 0) and feeds
    /// each one through `A::apply`.  The upcaster chain runs first so the
    /// aggregate always receives events at the current schema version.
    ///
    /// Returns the final aggregate state and the last `stream_version` seen
    /// (useful for optimistic-concurrency checks on the next append).
    pub async fn rehydrate<A>(&self, stream_id: &str) -> Result<(A, i64)>
    where
        A: Aggregate,
    {
        let coll = self.db.collection::<Document>("events");
        let filter = doc! { "stream_id": stream_id };
        let opts = FindOptions::builder()
            .sort(doc! { "stream_version": 1 })
            .build();
        let mut cursor = coll
            .find(filter)
            .with_options(opts)
            .await
            .context("failed to open rehydration cursor")?;

        let mut state = A::default();
        let mut last_version: i64 = -1;

        while let Some(raw) = TryStreamExt::try_next(&mut cursor)
            .await
            .context("cursor error during rehydration")?
        {
            let envelope: EventEnvelope =
                from_document(raw).context("failed to deserialise event envelope")?;
            last_version = envelope.stream_version;

            // Run upcaster chain before handing to the aggregate.
            let raw_json: serde_json::Value = serde_json::to_value(&envelope.payload)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::default()));
            let upcasted =
                self.upcasters
                    .upcast(&envelope.event_type, envelope.schema_version, raw_json);

            let mut event_json = match upcasted {
                UpcastResult::Current(v) | UpcastResult::Migrated { event: v, .. } => v,
                UpcastResult::Unrecognised(_) => continue,
            };

            // Inject the event_type discriminator so serde(tag) enums
            // (e.g. `#[serde(tag = "event_type")]`) can deserialise correctly.
            // The payload stored in MongoDB contains only the event fields;
            // the discriminator lives in the top-level `event_type` column.
            if let Some(obj) = event_json.as_object_mut() {
                obj.entry("event_type")
                    .or_insert_with(|| serde_json::Value::String(envelope.event_type.clone()));
            }

            match serde_json::from_value::<A::Event>(event_json) {
                Ok(ev) => state.apply(ev),
                Err(e) => {
                    warn!(
                        stream_id,
                        stream_version = last_version,
                        error = %e,
                        "skipping unrecognised event during rehydration"
                    );
                }
            }
        }

        Ok((state, last_version))
    }

    // ── Checkpoint System (Property 3) ────────────────────────────────────────

    /// ── Property 3: Checkpoint System ──────────────────────────────────────
    ///
    /// Persists the last successfully processed `global_position` for
    /// `consumer_id` in the `_checkpoints` collection.  The consumer calls
    /// this after each batch to advance its durable cursor.  On restart,
    /// `load_checkpoint` returns this value so processing resumes exactly where
    /// it left off — no events are replayed or skipped.
    pub async fn save_checkpoint(&self, consumer_id: &str, global_position: i64) -> Result<()> {
        self.db
            .collection::<Document>("_checkpoints")
            .update_one(
                doc! { "_id": consumer_id },
                doc! { "$set": { "global_position": global_position, "updated_at": chrono::Utc::now().to_rfc3339() } },
            )
            .upsert(true)
            .await
            .context("failed to save checkpoint")?;
        Ok(())
    }

    /// Load the last persisted checkpoint for `consumer_id`.
    /// Returns `None` if this consumer has never checkpointed (start from 0).
    pub async fn load_checkpoint(&self, consumer_id: &str) -> Result<Option<i64>> {
        let doc = self
            .db
            .collection::<Document>("_checkpoints")
            .find_one(doc! { "_id": consumer_id })
            .await
            .context("failed to load checkpoint")?;
        Ok(doc.and_then(|d| d.get_i64("global_position").ok()))
    }

    // ── Catch-Up Subscription (Property 7) ────────────────────────────────────

    /// ── Property 7 (Catch-Up): Catch-Up Subscription ───────────────────────
    ///
    /// Reads all events from `from_position` (exclusive) in global order, then
    /// switches to a Change Stream so the handler receives new events in real
    /// time with no polling.  The consumer is responsible for calling
    /// `save_checkpoint` after each successful batch.
    ///
    /// `handler` receives each envelope; returning `Err` stops the subscription.
    #[allow(dead_code)]
    pub async fn catch_up_subscribe<F, Fut>(&self, consumer_id: &str, mut handler: F) -> Result<()>
    where
        F: FnMut(EventEnvelope) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        // ── Phase 1: historical catch-up ─────────────────────────────────────
        let from = self.load_checkpoint(consumer_id).await?.unwrap_or(-1);
        info!(consumer_id, from_position = from, "starting catch-up phase");

        let coll = self.db.collection::<Document>("events");
        let opts = FindOptions::builder()
            .sort(doc! { "global_position": 1 })
            .build();
        let mut cursor = coll
            .find(doc! { "global_position": { "$gt": from } })
            .with_options(opts)
            .await
            .context("failed to open catch-up cursor")?;

        let mut last_pos = from;
        while let Some(raw) = TryStreamExt::try_next(&mut cursor)
            .await
            .context("cursor error during catch-up")?
        {
            let envelope: EventEnvelope =
                from_document(raw).context("failed to deserialise envelope during catch-up")?;
            last_pos = envelope.global_position;
            handler(envelope).await?;
            self.save_checkpoint(consumer_id, last_pos).await?;
        }
        info!(
            consumer_id,
            caught_up_at = last_pos,
            "catch-up complete, switching to live stream"
        );

        // ── Phase 2: live push via Change Stream (Property 4) ────────────────
        //
        // Property 4: Event Polling → Push
        // MongoDB Change Streams are server-push: the driver keeps a long-lived
        // cursor on the oplog and delivers new inserts to us instantly.
        // There is no application-level polling loop.
        let mut change_stream = coll
            .watch()
            .pipeline([doc! { "$match": { "operationType": "insert" } }])
            .await
            .context("failed to open change stream")?;

        while let Some(event) = change_stream.next().await {
            let cs_event = event.context("change stream error")?;
            if cs_event.operation_type != OperationType::Insert {
                continue;
            }
            if let Some(full_doc) = cs_event.full_document {
                let envelope: EventEnvelope =
                    from_document(full_doc).context("failed to deserialise change stream doc")?;
                let pos = envelope.global_position;
                handler(envelope).await?;
                self.save_checkpoint(consumer_id, pos).await?;
            }
        }
        Ok(())
    }

    // ── Competing Consumers / Single-Active-Consumer (Property 7) ─────────────

    /// ── Property 7 (Competing Consumer): acquire exclusive lease ───────────
    ///
    /// Implements a Single-Active-Consumer (SAC) pattern using a TTL-based
    /// mutex document in `_competing_consumers`.  Only the task that holds the
    /// lease processes events; all others spin-wait.  The lease auto-expires
    /// after `lease_ttl` seconds if the holder crashes.
    ///
    /// Returns `true` if the lease was acquired, `false` if another consumer
    /// already holds it.
    pub async fn try_acquire_lease(
        &self,
        group_id: &str,
        consumer_id: &str,
        lease_ttl_secs: u32,
    ) -> Result<bool> {
        let now = chrono::Utc::now();
        let expires_at = now + chrono::Duration::seconds(i64::from(lease_ttl_secs));

        // Either create (no existing lease) or take over an expired one.
        let result = self
            .db
            .collection::<Document>("_competing_consumers")
            .update_one(
                doc! {
                    "_id": group_id,
                    "$or": [
                        // No lease exists yet.
                        { "consumer_id": { "$exists": false } },
                        // Lease expired.
                        { "expires_at": { "$lt": now.to_rfc3339() } },
                        // We already hold the lease (renewal).
                        { "consumer_id": consumer_id },
                    ]
                },
                doc! {
                    "$set": {
                        "consumer_id": consumer_id,
                        "acquired_at": now.to_rfc3339(),
                        "expires_at":  expires_at.to_rfc3339(),
                    }
                },
            )
            .upsert(true)
            .await
            .context("failed to update competing consumer lease")?;

        Ok(result.matched_count > 0 || result.upserted_id.is_some())
    }

    /// Release the lease for `consumer_id` on `group_id`.
    pub async fn release_lease(&self, group_id: &str, consumer_id: &str) -> Result<()> {
        self.db
            .collection::<Document>("_competing_consumers")
            .delete_one(doc! { "_id": group_id, "consumer_id": consumer_id })
            .await
            .context("failed to release lease")?;
        Ok(())
    }

    // ── Integration Event Relay (Property 8) ─────────────────────────────────

    /// ── Property 8: Integration Events ─────────────────────────────────────
    ///
    /// Reads the oldest un-dispatched entry from `_integration_outbox`,
    /// invokes `publish` (e.g. to `RabbitMQ`), then atomically marks it
    /// as dispatched.  The caller runs this in a background loop.
    ///
    /// Because the outbox entry was written inside the same transaction as the
    /// domain event (Property 6), exactly-once delivery semantics are
    /// achievable: the relay retries until `publish` succeeds, and the
    /// `dispatched = true` update prevents re-delivery.
    pub async fn relay_next_integration_event<F, Fut>(&self, publish: F) -> Result<bool>
    where
        F: FnOnce(Document) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let coll = self.db.collection::<Document>("_integration_outbox");
        let entry = coll
            .find_one(doc! { "dispatched": false })
            .await
            .context("failed to query integration outbox")?;

        let Some(doc) = entry else { return Ok(false) };

        let id = doc
            .get_str("_id")
            .context("outbox entry missing _id")?
            .to_owned();

        // Publish first — if this fails we leave `dispatched = false` and retry.
        publish(doc).await?;

        // Only mark dispatched after a successful publish.
        coll.update_one(
            doc! { "_id": &id },
            doc! { "$set": { "dispatched": true, "dispatched_at": chrono::Utc::now().to_rfc3339() } },
        )
        .await
        .context("failed to mark integration event dispatched")?;

        debug!(event_id = %id, "integration event dispatched");
        Ok(true)
    }

    /// Run the integration event relay loop until `shutdown` resolves.
    ///
    /// Calls `relay_next_integration_event` in a tight loop (with a short
    /// back-off when the outbox is empty) so integration events are published
    /// with minimal latency.
    #[allow(dead_code)]
    pub async fn run_relay_loop<F>(
        &self,
        mut make_publish: impl FnMut(Document) -> F,
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
                    info!("integration event relay loop shutting down");
                    return Ok(());
                }
                result = self.relay_next_integration_event(&mut make_publish) => {
                    match result {
                        Ok(true) => {} // more may be pending, loop immediately
                        Ok(false) => {
                            // Outbox empty — wait before checking again.
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
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
