//! MQTT transport abstraction — **Phase 4 scaffold**.
//!
//! The Edge/Host engines drive an [`MqttTransport`] so the library can sit on
//! `rumqttc` (a default-feature impl in Phase 4) or any MQTT client. The QoS /
//! retain / will rules are enforced by the edge/host layers, not the transport.
//! In Phase 4 the trait methods become `async` (Tokio) and gain an inbound
//! message stream.

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
#[derive(Clone, Debug)]
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
    /// The Last-Will-and-Testament (the Edge Node's NDEATH).
    pub will: Option<OutboundMessage>,
    /// Optional TLS configuration.
    pub tls: Option<TlsConfig>,
}

/// The MQTT transport the edge/host engines drive.
///
/// Phase 4 turns these into `async fn` and adds an inbound message stream.
pub trait MqttTransport {
    /// Connect to the broker with the given options.
    ///
    /// # Errors
    /// Returns an error if the connection cannot be established.
    fn connect(&mut self, opts: &ConnectOptions) -> Result<()>;

    /// Subscribe to a topic filter at the given QoS.
    ///
    /// # Errors
    /// Returns an error if the subscription fails.
    fn subscribe(&mut self, topic_filter: &str, qos: Qos) -> Result<()>;

    /// Publish a message.
    ///
    /// # Errors
    /// Returns an error if publishing fails.
    fn publish(&mut self, message: &OutboundMessage) -> Result<()>;
}
