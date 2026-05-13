//! Thin async wrapper around the official MongoDB Rust driver.
//!
//! Mirrors the interface of [`crate::kurrentdb::client::KurrentClient`] so the
//! benchmark harness can swap backends without structural changes.

use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::try_join_all;
use mongodb::{
    bson::{doc, to_document, Document},
    options::{
        Acknowledgment, ClientOptions, IndexOptions, ReturnDocument, ValidationAction, WriteConcern,
    },
    Client, Database, IndexModel,
};
use serde::Serialize;
use uuid::Uuid;

// ─── Client ──────────────────────────────────────────────────────────────────

pub struct MongoClient {
    db: Database,
}

impl MongoClient {
    /// Connect to MongoDB and select a database.
    ///
    /// `url` examples:
    ///   "mongodb://localhost:27017"
    ///   "mongodb://user:pass@mongo:27017/eventbench?authSource=admin"
    pub async fn connect(url: &str, db_name: &str) -> Result<Self> {
        let opts = ClientOptions::parse(url)
            .await
            .with_context(|| format!("invalid MongoDB URL: {url}"))?;
        let client = Client::with_options(opts).context("failed to create MongoDB client")?;
        let db = client.database(db_name);
        Ok(Self { db })
    }

    /// Insert a single JSON-serialisable event into `collection_name`.
    pub async fn append<T: Serialize>(
        &self,
        collection_name: &str,
        event_type: &str,
        payload: &T,
    ) -> Result<()> {
        let coll = self.db.collection::<Document>(collection_name);
        let mut document =
            to_document(payload).with_context(|| "failed to serialise event to BSON")?;
        document.insert("_id", Uuid::new_v4().to_string());
        document.insert("event_type", event_type);
        coll.insert_one(document)
            .await
            .with_context(|| format!("insert to collection '{collection_name}' failed"))?;
        Ok(())
    }

    /// Insert a pre-built batch of events in a single round-trip.
    /// More efficient at high throughput than one-by-one inserts.
    pub async fn append_batch<T: Serialize>(
        &self,
        collection_name: &str,
        event_type: &str,
        payloads: &[T],
    ) -> Result<()> {
        let coll = self.db.collection::<Document>(collection_name);
        let docs: Result<Vec<Document>> = payloads
            .iter()
            .map(|p| {
                let mut doc =
                    to_document(p).with_context(|| "failed to serialise event to BSON")?;
                doc.insert("_id", Uuid::new_v4().to_string());
                doc.insert("event_type", event_type);
                Ok(doc)
            })
            .collect();
        coll.insert_many(docs?)
            .await
            .with_context(|| format!("batch insert to '{collection_name}' failed"))?;
        Ok(())
    }

    // ── Event-store-mode additions ─────────────────────────────────────────

    /// Create `collection_name` with:
    ///  1. A JSON Schema validator requiring `stream_id`, `stream_version`, and
    ///     `global_position` on every inserted document (structural immutability).
    ///  2. A unique compound index `{ stream_id, stream_version }` — a duplicate
    ///     key error on insert mirrors KurrentDB's `WrongExpectedVersion`.
    ///  3. An index on `global_position` to support `$all`-stream queries.
    pub async fn ensure_collection_event_store(&self, collection_name: &str) -> Result<()> {
        let validator = doc! {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["_id", "event_type", "stream_id", "stream_version", "global_position"],
                "properties": {
                    "_id":             { "bsonType": "string" },
                    "event_type":      { "bsonType": "string" },
                    "stream_id":       { "bsonType": "string" },
                    "stream_version":  { "bsonType": "long" },
                    "global_position": { "bsonType": "long" },
                }
            }
        };

        match self
            .db
            .create_collection(collection_name)
            .validator(validator)
            .validation_action(ValidationAction::Error)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                if let mongodb::error::ErrorKind::Command(ref cmd) = *e.kind {
                    if cmd.code == 48 {
                        return Ok(());
                    }
                }
                return Err(e).context(format!("failed to create collection '{collection_name}'"));
            }
        }

        let coll = self.db.collection::<Document>(collection_name);

        // Unique compound index — provides optimistic-concurrency enforcement.
        let stream_ver_idx = IndexModel::builder()
            .keys(doc! { "stream_id": 1, "stream_version": 1 })
            .options(
                IndexOptions::builder()
                    .unique(true)
                    .name("uq_stream_version".to_string())
                    .build(),
            )
            .build();

        // Index on global_position supports efficient $all-stream reads.
        let global_pos_idx = IndexModel::builder()
            .keys(doc! { "global_position": 1 })
            .options(
                IndexOptions::builder()
                    .name("idx_global_position".to_string())
                    .build(),
            )
            .build();

        coll.create_indexes(vec![stream_ver_idx, global_pos_idx])
            .await
            .with_context(|| format!("failed to create indexes on '{collection_name}'"))?;

        Ok(())
    }

    /// Pre-warm the per-stream version counters and the global sequence counter
    /// so their documents exist before the timed benchmark window starts.
    /// Uses `$setOnInsert` — a no-op if the document already exists.
    pub async fn init_event_store_counters(&self, stream_names: &[String]) -> Result<()> {
        // Global sequence counter (single document shared across all streams).
        let gseq_coll = self.db.collection::<Document>("_global_seq");
        gseq_coll
            .update_one(
                doc! { "_id": "counter" },
                doc! { "$setOnInsert": { "seq": 0i64 } },
            )
            .upsert(true)
            .await
            .context("failed to init global sequence counter")?;

        // Per-stream version counters — initialise all in parallel.
        let versions_coll = self.db.collection::<Document>("_stream_versions");
        let futs: Vec<_> = stream_names
            .iter()
            .map(|name| {
                let coll = versions_coll.clone();
                let name = name.clone();
                async move {
                    coll.update_one(
                        doc! { "_id": &name },
                        doc! { "$setOnInsert": { "version": 0i64 } },
                    )
                    .upsert(true)
                    .await
                    .context("failed to init stream version counter")
                }
            })
            .collect();
        try_join_all(futs).await?;
        Ok(())
    }

    /// Insert a batch of events stamped with a monotonic `stream_version` and a
    /// globally-ordered `global_position` — mirroring KurrentDB's per-stream
    /// version and global `$all`-stream position.
    ///
    /// Two sequential atomic `findOneAndUpdate` increments advance the
    /// `_stream_versions` and `_global_seq` control collections, then the
    /// actual `insertMany` writes all events in one round-trip.
    pub async fn append_batch_versioned<T: Serialize>(
        &self,
        collection_name: &str,
        event_type: &str,
        payloads: &[T],
    ) -> Result<()> {
        let batch_len = payloads.len() as i64;

        // ── 1. Atomically advance the per-stream version counter ────────────
        let versions_coll = self.db.collection::<Document>("_stream_versions");
        let ver_before = versions_coll
            .find_one_and_update(
                doc! { "_id": collection_name },
                doc! { "$inc": { "version": batch_len } },
            )
            .upsert(true)
            .return_document(ReturnDocument::Before)
            .await
            .with_context(|| format!("version counter update failed for '{collection_name}'"))?;

        let start_version: i64 = ver_before
            .as_ref()
            .and_then(|d| d.get_i64("version").ok())
            .unwrap_or(0);

        // ── 2. Atomically advance the global sequence counter ───────────────
        let gseq_coll = self.db.collection::<Document>("_global_seq");
        let gseq_before = gseq_coll
            .find_one_and_update(
                doc! { "_id": "counter" },
                doc! { "$inc": { "seq": batch_len } },
            )
            .upsert(true)
            .return_document(ReturnDocument::Before)
            .await
            .context("global sequence counter update failed")?;

        let global_start: i64 = gseq_before
            .as_ref()
            .and_then(|d| d.get_i64("seq").ok())
            .unwrap_or(0);

        // ── 3. Build documents and insert ───────────────────────────────────
        let coll = self.db.collection::<Document>(collection_name);
        let docs: Result<Vec<Document>> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let mut doc =
                    to_document(p).with_context(|| "failed to serialise event to BSON")?;
                doc.insert("_id", Uuid::new_v4().to_string());
                doc.insert("event_type", event_type);
                doc.insert("stream_id", collection_name);
                doc.insert("stream_version", start_version + i as i64);
                doc.insert("global_position", global_start + i as i64);
                Ok(doc)
            })
            .collect();

        // Apply journaled write concern on the event insert only — the fsync
        // cost here is what makes this comparable to KurrentDB's durable append.
        // Applying j:true at the ClientOptions level would interfere with server
        // selection and cause connection timeouts on some MongoDB configurations.
        let journal_wc = WriteConcern::builder()
            .w(Acknowledgment::Nodes(1))
            .journal(true)
            .build();

        coll.insert_many(docs?)
            .write_concern(journal_wc)
            .await
            .with_context(|| format!("versioned batch insert to '{collection_name}' failed"))?;

        Ok(())
    }

    /// Drop the entire database (removes all collections and their data).
    /// Called at the start of each benchmark run to guarantee a clean slate.
    pub async fn drop_database(&self) -> Result<()> {
        self.db
            .drop()
            .await
            .context("failed to drop MongoDB database")?;
        Ok(())
    }

    /// Create a collection if it does not already exist.
    /// Ignores NamespaceExists (code 48) so it is safe to call on a collection
    /// that was already created by a concurrent task or a `--no-drop` run.
    pub async fn ensure_collection(&self, collection_name: &str) -> Result<()> {
        match self.db.create_collection(collection_name).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let mongodb::error::ErrorKind::Command(ref cmd) = *e.kind {
                    if cmd.code == 48 {
                        // NamespaceExists — another task beat us, not an error
                        return Ok(());
                    }
                }
                Err(e).context(format!("failed to create collection '{collection_name}'"))
            }
        }
    }

    /// Cheap health probe — issues a server-level `ping` command.
    ///
    /// Wraps the call in a 5-second hard timeout so the benchmark's readiness
    /// retry loop fails quickly (5 s per attempt) rather than waiting the
    /// driver's default 30-second server-selection timeout.  30 retries × 5 s
    /// = 2.5 minutes maximum wait, vs 30 × 30 s = 15 minutes without this.
    pub async fn ping(&self) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.db.run_command(doc! { "ping": 1 }),
        )
        .await
        .context("MongoDB ping timed out after 5 s")?
        .context("MongoDB ping failed")?;
        Ok(())
    }
}
