use anyhow::{Context, Result};
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use serde::Serialize;

const EXCHANGE: &str = "events";
const QUEUE: &str = "events.all";
const ROUTING: &str = "#";

pub struct RmqClient {
    channel: Channel,
}

impl RmqClient {
    pub async fn connect(url: &str) -> Result<Self> {
        let conn = Connection::connect(url, ConnectionProperties::default())
            .await
            .with_context(|| format!("failed to connect to RabbitMQ at {url}"))?;

        let channel = conn
            .create_channel()
            .await
            .context("failed to open AMQP channel")?;

        // Declare a durable topic exchange so consumers can filter by event type.
        channel
            .exchange_declare(
                EXCHANGE,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("exchange_declare failed")?;

        // Durable catch-all queue bound with "#" routing key.
        channel
            .queue_declare(
                QUEUE,
                QueueDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("queue_declare failed")?;

        channel
            .queue_bind(
                QUEUE,
                EXCHANGE,
                ROUTING,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("queue_bind failed")?;

        Ok(Self { channel })
    }

    /// Publish a single JSON-encoded event to the topic exchange.
    /// `routing_key` should be the event type (e.g. "order.placed").
    #[allow(clippy::future_not_send)]
    pub async fn publish<T: Serialize>(&self, routing_key: &str, payload: &T) -> Result<()> {
        let body = serde_json::to_vec(payload)
            .with_context(|| format!("serialise failed for routing key '{routing_key}'"))?;

        self.channel
            .basic_publish(
                EXCHANGE,
                routing_key,
                BasicPublishOptions::default(),
                &body,
                BasicProperties::default()
                    .with_delivery_mode(2) // persistent
                    .with_content_type("application/json".into()),
            )
            .await
            .with_context(|| format!("basic_publish failed for '{routing_key}'"))?
            .await
            .context("broker confirm failed")?;

        Ok(())
    }
}
