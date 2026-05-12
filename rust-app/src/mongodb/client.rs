//! Thin async wrapper around the official MongoDB Rust driver.
//!
//! Mirrors the interface of [`crate::kurrentdb::client::KurrentClient`] so the
//! benchmark harness can swap backends without structural changes.

use anyhow::{Context, Result};
use mongodb::{
    bson::{doc, to_document, Document},
    options::ClientOptions,
    Client, Database,
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
    pub async fn ping(&self) -> Result<()> {
        self.db
            .run_command(doc! { "ping": 1 })
            .await
            .context("MongoDB ping failed")?;
        Ok(())
    }
}
