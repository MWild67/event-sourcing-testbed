//! Snapshot demo: write 1000 domain events to KurrentDB (3 types), take an ES
//! snapshot every 55 events, then stop and fully restore aggregate state by
//! rehydrating from the latest snapshot plus the trailing events that followed.
//!
//! Stream layout
//! ─────────────
//!   Main stream  : `snapshot-demo-{id}`          – domain events
//!   Snapshot stream: `snapshot-demo-{id}-snapshots` – periodic snapshots
//!
//! Snapshot cadence
//! ────────────────
//!   A snapshot is appended after the event whose 1-based sequence number is a
//!   multiple of 55.  With 1000 events this produces 18 snapshots (at revisions
//!   54, 109, 164 … 989).  The final 10 events (revisions 990–999) are not yet
//!   covered by a snapshot and must be replayed during rehydration.
//!
//! Rehydration algorithm
//! ──────────────────────
//!   1. Read the latest snapshot   → `InventoryState` + `at_revision`
//!   2. Read domain events from    `at_revision + 1` to the end of stream
//!   3. Fold those events over the snapshot state
//!   4. Assert the result equals the state we recorded at the end of the write
//!      phase.

use std::time::Instant;

use anyhow::Result;
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::kurrentdb::client::KurrentClient;

// ─── Configuration ────────────────────────────────────────────────────────────

// ─── Domain events (3 types) ─────────────────────────────────────────────────

/// Stock was added to the inventory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemAdded {
    pub item_id: Uuid,
    pub sku: String,
    pub quantity: u32,
    pub unit_price_usd: f64,
    pub added_at: DateTime<Utc>,
}

/// The unit price of an inventory item was updated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemPriceUpdated {
    pub item_id: Uuid,
    pub old_price_usd: f64,
    pub new_price_usd: f64,
    pub updated_at: DateTime<Utc>,
}

/// Stock was removed from the inventory (sold, scrapped, …).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemRemoved {
    pub item_id: Uuid,
    pub quantity: u32,
    pub reason: String,
    pub removed_at: DateTime<Utc>,
}

// ─── Aggregate ───────────────────────────────────────────────────────────────

/// Running state of the inventory stream.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventoryState {
    /// Net units currently in stock (additions minus removals).
    pub total_stock: i64,
    /// Cumulative number of `ItemAdded` events processed.
    pub total_added: u64,
    /// Cumulative number of `ItemPriceUpdated` events processed.
    pub total_price_updates: u64,
    /// Cumulative number of `ItemRemoved` events processed.
    pub total_removals: u64,
    /// Stream revision of the last event included in this state.
    /// Set to `u64::MAX` when the state is freshly default-constructed.
    pub at_revision: u64,
}

impl InventoryState {
    /// Fold a single domain event into the aggregate.
    pub fn apply(&mut self, event_type: &str, payload: &serde_json::Value, revision: u64) {
        match event_type {
            "ItemAdded" => {
                let qty = payload["quantity"].as_u64().unwrap_or(0) as i64;
                self.total_stock += qty;
                self.total_added += 1;
            }
            "ItemPriceUpdated" => {
                self.total_price_updates += 1;
            }
            "ItemRemoved" => {
                let qty = payload["quantity"].as_u64().unwrap_or(0) as i64;
                self.total_stock -= qty;
                self.total_removals += 1;
            }
            other => {
                tracing::warn!(event_type = other, "unknown event type — skipping");
            }
        }
        self.at_revision = revision;
    }

    pub fn print_summary(&self, label: &str) {
        println!();
        println!("  ── {label} ──────────────────────────────────────");
        println!("  total_stock       : {}", self.total_stock);
        println!("  total_added       : {}", self.total_added);
        println!("  total_price_upd   : {}", self.total_price_updates);
        println!("  total_removals    : {}", self.total_removals);
        println!("  at_revision       : {}", self.at_revision);
    }
}

// ─── Snapshot envelope ───────────────────────────────────────────────────────

/// The blob persisted to the snapshot stream.
#[derive(Debug, Serialize, Deserialize)]
struct SnapshotEnvelope {
    /// The aggregate state at the time of the snapshot.
    pub state: InventoryState,
    /// Which stream revision in the *main* stream this snapshot covers up to
    /// (inclusive).  Used to fast-forward reads during rehydration.
    pub at_stream_revision: u64,
    pub taken_at: DateTime<Utc>,
}

// ─── Public entry point ──────────────────────────────────────────────────────

/// Run the full snapshot demo end-to-end.
pub async fn run(kurrent_url: &str, event_count: u32, snapshot_every: u32) -> Result<()> {
    // Wait for KurrentDB to be ready.
    // A fresh client is created on every attempt: once the internal gossip
    // discovery fails, the kurrentdb client enters a broken state and never
    // recovers — reconnecting is the only reliable way to retry.
    info!("waiting for KurrentDB to become ready...");
    let mut ready = false;
    for attempt in 1..=30 {
        match KurrentClient::connect(kurrent_url) {
            Err(e) => {
                tracing::warn!(attempt, error = %e, "failed to create client, retrying...");
            }
            Ok(probe) => match probe.ping().await {
                Ok(()) => {
                    ready = true;
                    break;
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "not ready yet, retrying...");
                }
            },
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    if !ready {
        anyhow::bail!("KurrentDB did not become ready within 30 s");
    }
    info!("KurrentDB ready");

    // Now that the cluster is up, create the long-lived client for the demo.
    let client = KurrentClient::connect(kurrent_url)?;

    let run_id = Uuid::new_v4();
    let stream_name = format!("snapshot-demo-{run_id}");
    let snapshot_stream = format!("{stream_name}-snapshots");

    println!();
    println!("══════════════════════════════════════════════════════════════");
    println!("  KurrentDB Snapshot Demo");
    println!("══════════════════════════════════════════════════════════════");
    println!("  stream          : {stream_name}");
    println!("  snapshot stream : {snapshot_stream}");
    println!("  events          : {event_count}");
    println!("  snapshot every  : {snapshot_every} events");
    println!("══════════════════════════════════════════════════════════════");

    // ── Phase 1: write events and take periodic snapshots ─────────────────────
    println!();
    println!("  Phase 1 — Writing {event_count} events with snapshots every {snapshot_every}…");
    let write_start = Instant::now();

    let mut live_state = InventoryState {
        at_revision: u64::MAX,
        ..Default::default()
    };
    let mut snapshot_count = 0u32;

    // Re-use a small set of item IDs so the stream represents a realistic
    // inventory rather than one unique item per event.
    let item_ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
    let mut rng = rand::thread_rng();

    // Events are written in batches of `snapshot_every` per gRPC call.
    // This collapses 1000 individual round-trips (each ~15 ms on Windows/Podman
    // due to HyperV VM scheduling latency) into ~18 calls, making the demo
    // run in ~1 s instead of ~15 s on Windows.
    //
    // Each batch is a single `append_batch` call carrying all events in the
    // window. After the batch is ACK'd by KurrentDB, we compute the per-event
    // revision (base revision + offset within the batch) and fold into the
    // in-memory state, then write the snapshot.
    let mut seq: u32 = 0;

    while seq < event_count {
        let window_end = (seq + snapshot_every).min(event_count);
        let window_len = (window_end - seq) as usize;

        // Build the batch payload for this window.
        // We store each event as a typed enum so we can serialise homogeneous
        // batches per event type. KurrentDB accepts mixed-type batches via
        // individual EventData items — we use `append_raw_batch` which takes
        // pre-built EventData values.
        let mut event_data_batch: Vec<kurrentdb::EventData> = Vec::with_capacity(window_len);
        // Also store lightweight apply info so we can fold without re-parsing.
        let mut apply_log: Vec<(&'static str, serde_json::Value)> = Vec::with_capacity(window_len);

        for i in seq..window_end {
            let item_id = item_ids[(i as usize) % item_ids.len()];
            match i % 3 {
                0 => {
                    let qty: u32 = rng.gen_range(1..=5);
                    let payload = ItemAdded {
                        item_id,
                        sku: format!("SKU-{:04}", (i as usize) % item_ids.len()),
                        quantity: qty,
                        unit_price_usd: 9.99 + (i % 50) as f64,
                        added_at: Utc::now(),
                    };
                    event_data_batch.push(
                        kurrentdb::EventData::json("ItemAdded", &payload)
                            .map_err(|e| anyhow::anyhow!("{e}"))?
                            .id(uuid::Uuid::new_v4()),
                    );
                    apply_log.push(("ItemAdded", serde_json::json!({"quantity": qty})));
                }
                1 => {
                    let old_price = 9.99 + (i % 50) as f64;
                    let payload = ItemPriceUpdated {
                        item_id,
                        old_price_usd: old_price,
                        new_price_usd: old_price * 1.05,
                        updated_at: Utc::now(),
                    };
                    event_data_batch.push(
                        kurrentdb::EventData::json("ItemPriceUpdated", &payload)
                            .map_err(|e| anyhow::anyhow!("{e}"))?
                            .id(uuid::Uuid::new_v4()),
                    );
                    apply_log.push(("ItemPriceUpdated", serde_json::json!({})));
                }
                _ => {
                    let qty: u32 = rng.gen_range(1..=5);
                    let payload = ItemRemoved {
                        item_id,
                        quantity: qty,
                        reason: if i % 7 == 0 {
                            "scrapped".to_string()
                        } else {
                            "sold".to_string()
                        },
                        removed_at: Utc::now(),
                    };
                    event_data_batch.push(
                        kurrentdb::EventData::json("ItemRemoved", &payload)
                            .map_err(|e| anyhow::anyhow!("{e}"))?
                            .id(uuid::Uuid::new_v4()),
                    );
                    apply_log.push(("ItemRemoved", serde_json::json!({"quantity": qty})));
                }
            }
        }

        // Append the whole window in one gRPC call.
        let opts =
            kurrentdb::AppendToStreamOptions::default().stream_state(kurrentdb::StreamState::Any);
        let result = client
            .inner()
            .append_to_stream(stream_name.as_str(), &opts, event_data_batch)
            .await
            .map_err(|e| anyhow::anyhow!("batch append failed: {e}"))?;

        // The server returns `next_expected_version` which equals the revision
        // of the LAST event in the batch.
        let last_revision = result.next_expected_version;
        let first_revision = last_revision - (window_len as u64 - 1);

        // Fold each event into the live state.
        for (offset, (event_type, apply_payload)) in apply_log.iter().enumerate() {
            live_state.apply(event_type, apply_payload, first_revision + offset as u64);
        }

        // Write snapshot after this batch (unless it's a partial tail window
        // smaller than snapshot_every — we still snapshot for completeness).
        let envelope = SnapshotEnvelope {
            state: live_state.clone(),
            at_stream_revision: last_revision,
            taken_at: Utc::now(),
        };
        client
            .append(&snapshot_stream, "InventorySnapshot", &envelope)
            .await?;
        snapshot_count += 1;
        info!(
            batch_first = seq,
            batch_last = window_end - 1,
            stream_revision = last_revision,
            snapshot_count,
            total_stock = live_state.total_stock,
            "batch written + snapshot taken"
        );

        seq = window_end;
    }

    let write_elapsed = write_start.elapsed();
    println!(
        "  ✓ Wrote {event_count} events and {snapshot_count} snapshots in {:.2}s",
        write_elapsed.as_secs_f64()
    );
    live_state.print_summary("Live state after write phase");

    // ── Phase 2: stop — simulate a process restart ─────────────────────────
    println!();
    println!("  Phase 2 — Simulating process restart (dropping in-memory state)…");
    let _ = live_state; // explicit ownership boundary before rehydration

    // ── Phase 3: rehydrate from snapshot + trailing events ─────────────────
    println!();
    println!("  Phase 3 — Rehydrating from latest snapshot + trailing events…");
    let rehydrate_start = Instant::now();

    // Step 3a: load the most recent snapshot.
    let (mut restored_state, snapshot_revision) =
        match client.read_last_event(&snapshot_stream).await? {
            None => {
                // No snapshot exists — cold replay from the very beginning.
                info!("no snapshot found; performing full cold replay");
                (
                    InventoryState {
                        at_revision: u64::MAX,
                        ..Default::default()
                    },
                    None,
                )
            }
            Some((_, _snap_stream_rev, payload)) => {
                let envelope: SnapshotEnvelope = serde_json::from_value(payload)
                    .map_err(|e| anyhow::anyhow!("failed to deserialise snapshot: {e}"))?;
                let covers_up_to = envelope.at_stream_revision;
                info!(
                    at_stream_revision = covers_up_to,
                    total_stock = envelope.state.total_stock,
                    "loaded latest snapshot"
                );
                (envelope.state, Some(covers_up_to))
            }
        };

    // Step 3b: read the trailing events that are not yet covered by the snapshot.
    let start_from = snapshot_revision.map_or(0, |r| r + 1);
    let trailing = client
        .read_stream_from_revision(&stream_name, start_from)
        .await?;
    let trailing_count = trailing.len();
    info!(
        trailing_event_count = trailing_count,
        start_from, "replaying trailing events"
    );

    for (event_type, revision, payload) in &trailing {
        restored_state.apply(event_type, payload, *revision);
    }

    let rehydrate_elapsed = rehydrate_start.elapsed();
    println!(
        "  ✓ Rehydrated in {:.2}s  (snapshot + {trailing_count} trailing events)",
        rehydrate_elapsed.as_secs_f64()
    );
    restored_state.print_summary("Restored state after rehydration");

    // ── Phase 4: verify ─────────────────────────────────────────────────────
    println!();
    println!("  Phase 4 — Verification…");

    // Recompute expected state from scratch so we have a ground truth without
    // relying on the in-memory state that was dropped above.
    let all_events = client.read_stream_events(&stream_name).await?;
    let mut expected = InventoryState {
        at_revision: u64::MAX,
        ..Default::default()
    };
    for (event_type, revision, payload) in &all_events {
        expected.apply(event_type, payload, *revision);
    }

    let ok = restored_state == expected;
    if ok {
        println!("  ✓ Restored state matches full cold-replay — rehydration correct!");
    } else {
        println!("  ✗ MISMATCH — restored state does NOT match cold replay!");
        restored_state.print_summary("Restored");
        expected.print_summary("Expected (full replay)");
    }

    // ── Phase 5: stream content report ──────────────────────────────────────
    println!();
    println!("  Phase 5 — Stream content report…");

    // Count by event type from the already-fetched full event list.
    let mut count_added = 0u64;
    let mut count_price = 0u64;
    let mut count_removed = 0u64;
    for (event_type, _, _) in &all_events {
        match event_type.as_str() {
            "ItemAdded" => count_added += 1,
            "ItemPriceUpdated" => count_price += 1,
            "ItemRemoved" => count_removed += 1,
            _ => {}
        }
    }

    // Read all snapshots to report their contents.
    let all_snapshots = client.read_stream_events(&snapshot_stream).await?;
    let snap_total = all_snapshots.len();

    println!();
    println!("  ── Domain event stream : {stream_name}");
    println!("  total events in stream  : {}", all_events.len());
    println!("    ItemAdded             : {count_added}");
    println!("    ItemPriceUpdated      : {count_price}");
    println!("    ItemRemoved           : {count_removed}");
    println!("  first revision          : 0");
    println!("  last  revision          : {}", expected.at_revision);

    println!();
    println!("  ── Snapshot stream : {snapshot_stream}");
    println!("  total snapshots         : {snap_total}");
    for (idx, (_, snap_rev, payload)) in all_snapshots.iter().enumerate() {
        if let Ok(env) = serde_json::from_value::<SnapshotEnvelope>(payload.clone()) {
            println!(
                "    [{:>2}] snap_stream_rev={snap_rev:<4}  covers_domain_rev={:<4}  stock={:<6}  added={:<4}  removed={:<4}",
                idx + 1,
                env.at_stream_revision,
                env.state.total_stock,
                env.state.total_added,
                env.state.total_removals,
            );
        }
    }

    println!();
    println!("══════════════════════════════════════════════════════════════");
    println!("  Summary");
    println!("══════════════════════════════════════════════════════════════");
    println!("  domain events written : {event_count}");
    println!("  snapshots taken       : {snapshot_count}");
    println!("  trailing events (post-last-snapshot) : {trailing_count}");
    println!("  write time  : {:.3}s", write_elapsed.as_secs_f64());
    println!("  rehydration : {:.3}s", rehydrate_elapsed.as_secs_f64());
    println!("  result      : {}", if ok { "✓ PASS" } else { "✗ FAIL" });
    println!("══════════════════════════════════════════════════════════════");

    if !ok {
        anyhow::bail!("rehydration verification failed");
    }

    Ok(())
}
