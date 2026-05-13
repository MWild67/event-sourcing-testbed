//! KurrentDB gRPC client wrapper.
//!
//! Thin async wrapper around the official `kurrentdb` crate, exposing only
//! the append and health-probe operations needed by the benchmark harness.

use anyhow::{Context, Result};
use kurrentdb::{AppendToStreamOptions, Client, ClientSettings, EventData, StreamState};
use serde::Serialize;
use uuid::Uuid;

pub struct KurrentClient {
    inner: Client,
}

impl KurrentClient {
    /// Connect to KurrentDB.
    ///
    /// `url` examples:
    ///   "kurrentdb://localhost:2113?tls=false"
    ///   "kurrentdb://kurrent-0:2113,kurrent-1:2113,kurrent-2:2113?tls=false"
    pub async fn connect(url: &str) -> Result<Self> {
        let settings: ClientSettings = url
            .parse()
            .with_context(|| format!("invalid KurrentDB URL: {url}"))?;
        let inner = Client::new(settings)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("failed to create KurrentDB client")?;
        Ok(Self { inner })
    }

    /// Append a single JSON-encoded event to `stream_name`.
    /// Returns the log position of the written event.
    pub async fn append<T: Serialize>(
        &self,
        stream_name: &str,
        event_type: &str,
        payload: &T,
    ) -> Result<u64> {
        let event = EventData::json(event_type, payload)
            .with_context(|| format!("failed to serialise event type '{event_type}'"))?
            .id(Uuid::new_v4());

        let opts = AppendToStreamOptions::default().stream_state(StreamState::Any);

        let result = self
            .inner
            .append_to_stream(stream_name, &opts, event)
            .await
            .with_context(|| format!("append to stream '{stream_name}' failed"))?;

        Ok(result.next_expected_version)
    }

    /// Append a pre-built batch of events in a single gRPC call.
    /// More efficient at high throughput than one-by-one appends.
    pub async fn append_batch<T: Serialize>(
        &self,
        stream_name: &str,
        event_type: &str,
        payloads: &[T],
    ) -> Result<u64> {
        let events: Result<Vec<EventData>> = payloads
            .iter()
            .map(|p| {
                Ok(EventData::json(event_type, p)
                    .with_context(|| "serialise failed")?
                    .id(Uuid::new_v4()))
            })
            .collect();

        let opts = AppendToStreamOptions::default().stream_state(StreamState::Any);

        let result = self
            .inner
            .append_to_stream(stream_name, &opts, events?)
            .await
            .with_context(|| format!("batch append to '{stream_name}' failed"))?;

        Ok(result.next_expected_version)
    }

    /// Cheap health probe — checks whether the gRPC endpoint responds.
    /// Returns an error if the server is not reachable or not yet ready.
    pub async fn ping(&self) -> Result<()> {
        // We use read_stream and check the error — a transport error means not
        // ready; a stream-not-found or access-denied error means KurrentDB is
        // up and accepting requests (stream just doesn't exist yet).
        match self
            .inner
            .read_stream("$ping-probe", &Default::default())
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("StreamNotFound")
                    || msg.contains("stream not found")
                    || msg.contains("AccessDenied")
                    || msg.contains("not found")
                {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("KurrentDB not ready: {}", msg))
                }
            }
        }
    }
}
