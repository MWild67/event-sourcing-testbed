//! `KurrentDB` gRPC client wrapper.
//!
//! Thin async wrapper around the official `kurrentdb` crate, exposing only
//! the append and health-probe operations needed by the benchmark harness.

use anyhow::{Context, Result};
use kurrentdb::{
    AppendToStreamOptions, Client, ClientSettings, EventData, ReadStreamOptions, StreamPosition,
    StreamState,
};
use serde::Serialize;
use uuid::Uuid;

pub struct KurrentClient {
    inner: Client,
}

impl KurrentClient {
    /// Expose the raw `kurrentdb::Client` for callers that need to build
    /// custom `EventData` batches (e.g. the snapshot demo batch writer).
    pub fn inner(&self) -> &Client {
        &self.inner
    }

    /// Connect to `KurrentDB`.
    ///
    /// `url` examples:
    ///   "<kurrentdb://localhost:2113?tls=false>"
    ///   "kurrentdb://kurrent-0:2113,kurrent-1:2113,kurrent-2:2113?tls=false"
    pub fn connect(url: &str) -> Result<Self> {
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
    #[allow(clippy::future_not_send)]
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
    #[allow(clippy::future_not_send)]
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

    /// Read all events from `stream_name` in chronological order (position 0
    /// to end).  Returns a `Vec` of `(event_type, revision, payload)` tuples.
    ///
    /// Returns an empty `Vec` if the stream does not exist yet.
    pub async fn read_stream_events(
        &self,
        stream_name: &str,
    ) -> Result<Vec<(String, u64, serde_json::Value)>> {
        use kurrentdb::ReadStreamOptions;

        let opts = ReadStreamOptions::default();
        let mut read_stream = match self.inner.read_stream(stream_name, &opts).await {
            Ok(s) => s,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("StreamNotFound")
                    || msg.contains("stream not found")
                    || msg.contains("not found")
                {
                    return Ok(Vec::new());
                }
                return Err(anyhow::anyhow!("read_stream '{stream_name}' failed: {msg}"));
            }
        };

        let mut events = Vec::new();
        loop {
            match read_stream.next().await {
                Ok(Some(resolved)) => {
                    let recorded = resolved.get_original_event();
                    let event_type = recorded.event_type.clone();
                    let revision = recorded.revision;
                    let payload: serde_json::Value =
                        serde_json::from_slice(&recorded.data).unwrap_or(serde_json::Value::Null);
                    events.push((event_type, revision, payload));
                }
                Ok(None) => break,
                Err(e) => return Err(anyhow::anyhow!("error reading '{stream_name}': {e}")),
            }
        }
        Ok(events)
    }

    /// Read events from `stream_name` starting at `from_revision` (inclusive).
    ///
    /// Useful for rehydrating an aggregate from just after a snapshot position.
    /// Returns an empty `Vec` if the stream does not exist or has no events
    /// at or after the requested revision.
    pub async fn read_stream_from_revision(
        &self,
        stream_name: &str,
        from_revision: u64,
    ) -> Result<Vec<(String, u64, serde_json::Value)>> {
        let opts = ReadStreamOptions::default().position(StreamPosition::Position(from_revision));
        let mut read_stream = match self.inner.read_stream(stream_name, &opts).await {
            Ok(s) => s,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("StreamNotFound")
                    || msg.contains("stream not found")
                    || msg.contains("not found")
                {
                    return Ok(Vec::new());
                }
                return Err(anyhow::anyhow!(
                    "read_stream_from_revision '{stream_name}'@{from_revision} failed: {msg}"
                ));
            }
        };

        let mut events = Vec::new();
        loop {
            match read_stream.next().await {
                Ok(Some(resolved)) => {
                    let recorded = resolved.get_original_event();
                    let event_type = recorded.event_type.clone();
                    let revision = recorded.revision;
                    let payload: serde_json::Value =
                        serde_json::from_slice(&recorded.data).unwrap_or(serde_json::Value::Null);
                    events.push((event_type, revision, payload));
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "error reading '{stream_name}'@{from_revision}: {e}"
                    ))
                }
            }
        }
        Ok(events)
    }

    /// Read the last (most recent) event from `stream_name`.
    ///
    /// Returns `None` if the stream does not exist or is empty.
    pub async fn read_last_event(
        &self,
        stream_name: &str,
    ) -> Result<Option<(String, u64, serde_json::Value)>> {
        let opts = ReadStreamOptions::default()
            .backwards()
            .position(StreamPosition::End)
            .max_count(1);
        let mut read_stream = match self.inner.read_stream(stream_name, &opts).await {
            Ok(s) => s,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("StreamNotFound")
                    || msg.contains("stream not found")
                    || msg.contains("not found")
                {
                    return Ok(None);
                }
                return Err(anyhow::anyhow!(
                    "read_last_event '{stream_name}' failed: {msg}"
                ));
            }
        };

        match read_stream.next().await {
            Ok(Some(resolved)) => {
                let recorded = resolved.get_original_event();
                let event_type = recorded.event_type.clone();
                let revision = recorded.revision;
                let payload: serde_json::Value =
                    serde_json::from_slice(&recorded.data).unwrap_or(serde_json::Value::Null);
                Ok(Some((event_type, revision, payload)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(anyhow::anyhow!(
                "error reading last event '{stream_name}': {e}"
            )),
        }
    }

    /// Read the last `n` events from `stream_name` as [`crate::events::BenchmarkEvent`]
    /// payloads, returned **oldest-first**.
    ///
    /// Uses a backwards read with `max_count = n` so only one gRPC round-trip is
    /// needed regardless of stream length.  Events that cannot be deserialised
    /// (wrong type) are silently skipped.
    pub async fn read_last_n_bench_events(
        &self,
        stream_name: &str,
        n: usize,
    ) -> Result<Vec<crate::events::BenchmarkEvent>> {
        let opts = ReadStreamOptions::default()
            .backwards()
            .position(StreamPosition::End)
            .max_count(n);

        let mut read_stream = match self.inner.read_stream(stream_name, &opts).await {
            Ok(s) => s,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("StreamNotFound")
                    || msg.contains("stream not found")
                    || msg.contains("not found")
                {
                    return Ok(Vec::new());
                }
                return Err(anyhow::anyhow!("read_last_n '{stream_name}' failed: {msg}"));
            }
        };

        let mut events = Vec::with_capacity(n);
        loop {
            match read_stream.next().await {
                Ok(Some(resolved)) => {
                    let recorded = resolved.get_original_event();
                    if let Ok(ev) =
                        serde_json::from_slice::<crate::events::BenchmarkEvent>(&recorded.data)
                    {
                        events.push(ev);
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(anyhow::anyhow!("error reading '{stream_name}': {e}")),
            }
        }
        // Backwards read returns newest-first; reverse to get oldest-first.
        events.reverse();
        Ok(events)
    }

    /// Cheap health probe — checks whether the gRPC endpoint responds.
    /// Returns an error if the server is not reachable or not yet ready.
    pub async fn ping(&self) -> Result<()> {
        // We use read_stream and check the error — a transport error means not
        // ready; a stream-not-found or access-denied error means KurrentDB is
        // up and accepting requests (stream just doesn't exist yet).
        match self
            .inner
            .read_stream("$ping-probe", &ReadStreamOptions::default())
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
                    Err(anyhow::anyhow!("KurrentDB not ready: {msg}"))
                }
            }
        }
    }
}
