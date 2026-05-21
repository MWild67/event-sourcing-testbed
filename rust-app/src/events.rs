use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── Upcast support ───────────────────────────────────────────────────────────

/// Version tag embedded in every persisted envelope.
/// Increment this when a breaking schema change is made to a domain event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersion(pub u32);

impl Default for SchemaVersion {
    fn default() -> Self {
        Self(1)
    }
}

/// Result after the upcaster chain runs.  The variant tells the consumer what
/// happened so integration-event publishing decisions can be made cleanly.
#[derive(Debug)]
#[allow(dead_code)]
pub enum UpcastResult<T> {
    /// Payload was already at the current version — no transform needed.
    Current(T),
    /// Payload was migrated from an older schema version to `T`.
    Migrated { _from: SchemaVersion, event: T },
    /// The raw BSON/JSON could not be deserialised even after all upcasters ran.
    Unrecognised(serde_json::Value),
}

// ─── Domain events ────────────────────────────────────────────────────────────

/// An order was placed in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderPlaced {
    pub order_id: Uuid,
    pub product_id: String,
    pub quantity: u32,
    pub price_usd: f64,
    pub placed_at: DateTime<Utc>,
    #[serde(default)]
    pub schema_version: SchemaVersion,
}

/// An order was cancelled.  Added in schema v2 — demonstrates upcasting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCancelled {
    pub order_id: Uuid,
    pub reason: String,
    pub cancelled_at: DateTime<Utc>,
    #[serde(default)]
    pub schema_version: SchemaVersion,
}

/// Discriminated union of all domain events this app can process.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "PascalCase")]
#[allow(dead_code)]
pub enum DomainEvent {
    OrderPlaced(OrderPlaced),
    OrderCancelled(OrderCancelled),
}

// ─── Benchmark payload ───────────────────────────────────────────────────────

/// Minimal synthetic event used during the stress test.
/// Kept small so disk I/O is the bottleneck, not serialisation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkEvent {
    pub seq: u64,
    pub task_id: u64,
    pub payload: Vec<u8>, // ~256 bytes of fixed payload
    pub created_at: DateTime<Utc>,
}

impl BenchmarkEvent {
    pub fn new(seq: u64, task_id: u64) -> Self {
        Self {
            seq,
            task_id,
            payload: vec![0xAB; 256],
            created_at: Utc::now(),
        }
    }
}
