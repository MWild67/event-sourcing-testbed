use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── Domain events ────────────────────────────────────────────────────────────

/// An order was placed in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderPlaced {
    pub order_id: Uuid,
    pub product_id: String,
    pub quantity: u32,
    pub price_usd: f64,
    pub placed_at: DateTime<Utc>,
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
