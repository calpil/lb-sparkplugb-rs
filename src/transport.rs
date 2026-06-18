//! MQTT transport abstraction (Phase 2+).
//!
//! The Edge/Host engines drive an [`MqttTransport` ] so the library can sit on
//! any MQTT client. A `rumqttc`-backed implementation (with TLS/HA) lands in
//! Phase 4; tests use an in-memory transport. The QoS / retain / will rules are
//! enforced by the edge/host layers, not the transport.

use bytes::Bytes;

use crate::error::Result;

/// MQTT Quality of Service level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Qos {
    /// At most once (`0`) — Sparkplug data/cmd/birth/death.
    AtMostOnce,
    /// At least once (`1`) — Sparkplug will (NDEATH) and STATE.
    AtLeastOnce,
    /// Exactly once (`2`).
    ExactlyOnce,
}

/// TLS configuration for a transport (PEM-encoded material).
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    /// Trusted CA chain (PEM).
    pub ca_pem: Option<Vec<u8>>,
    /// Client certificate for mTLS (PEM).
    pub client_cert_pem: Option<Vec<u8>>,
    /// Client private key for mTLS (PEM).
    pub client_key_pem: Option<Vec<u8>>,
}

/// A message to publish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundMessage {
    /// The MQTT topic.
    pub topic: String,
    /// The QoS level.
    pub qos: Qos,
    /// The retain flag.
    pub retain: bool,
    /// The raw payload bytes.
    pub payload: Bytes,
}

/// A message received from the broker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncomingMessage {
    /// The MQTT topic the message arrived on.
    pub topic: String,
    /// The raw payload bytes.
    pub payload: Bytes,
}

/// Connection options, including the Last-Will-and-Testament.
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// MQTT client id.
    pub client_id: String,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: u16,
    /// Keep-alive interval, seconds.
    pub keep_alive_secs: u16,
    /// MQTT 3.1.1 Clean Session / MQTT 5.0 Clean Start (Sparkplug requires `true`).
    pub clean_start: bool,
    /// The Last-Will-and-Testament (the Edge Node's NDEATH, QoS 1, retain=false).
    pub will: Option<OutboundMessage>,
    /// Optional TLS configuration.
    pub tls: Option<TlsConfig>,
}

/// The MQTT transport the edge/host engines drive.
///
/// Implementations are used via static dispatch (the engines are generic over
/// `T: MqttTransport`), so the auto-trait-bound caveat of `async fn` in traits
/// does not apply here.
#[allow(async_fn_in_trait)]
pub trait MqttTransport {
    /// Connect to the broker with the given options (registering the will).
    ///
    /// # Errors
    /// Returns an error if the connection cannot be established.
    async fn connect(&mut self, opts: &ConnectOptions) -> Result<()>;

    /// Subscribe to a topic filter at the given QoS.
    ///
    /// # Errors
    /// Returns an error if the subscription fails.
    async fn subscribe(&mut self, topic_filter: &str, qos: Qos) -> Result<()>;

    /// Publish a message.
    ///
    /// # Errors
    /// Returns an error if publishing fails.
    async fn publish(&mut self, message: &OutboundMessage) -> Result<()>;

    /// Disconnect gracefully (the broker must NOT deliver the will).
    ///
    /// # Errors
    /// Returns an error if the disconnect fails.
    async fn disconnect(&mut self) -> Result<()>;

    /// Await the next inbound message, or `None` once the stream is closed.
    ///
    /// # Errors
    /// Returns an error if the transport fails while receiving.
    async fn recv(&mut self) -> Result<Option<IncomingMessage>>;
}
