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
    /// Returns an error if the transport fails while receiving. An error may be
    /// **transient** (e.g. a reconnecting client): a run-loop should generally
    /// log/back-off and call `recv` again rather than treat it as terminal.
    async fn recv(&mut self) -> Result<Option<IncomingMessage>>;
}

#[cfg(feature = "transport-rumqttc")]
mod rumqtt_impl {
    use std::time::{Duration, Instant};

    use rumqttc::v5::mqttbytes::QoS as RumqttQos;
    use rumqttc::v5::mqttbytes::v5::{ConnectProperties, LastWill};
    use rumqttc::v5::{AsyncClient, ConnectionError, Event, EventLoop, Incoming, MqttOptions};

    use super::{ConnectOptions, IncomingMessage, MqttTransport, OutboundMessage, Qos};
    use crate::error::{Result, SparkplugError};

    const fn to_rumqtt_qos(qos: Qos) -> RumqttQos {
        match qos {
            Qos::AtMostOnce => RumqttQos::AtMostOnce,
            Qos::AtLeastOnce => RumqttQos::AtLeastOnce,
            Qos::ExactlyOnce => RumqttQos::ExactlyOnce,
        }
    }

    fn transport_err(e: impl ToString) -> SparkplugError {
        SparkplugError::Transport(e.to_string())
    }

    /// A [`MqttTransport`] backed by the `rumqttc` MQTT v5 async client.
    ///
    /// **The event loop must be pumped.** `subscribe`/`publish` only *enqueue*
    /// requests onto a bounded channel; nothing reaches the broker until
    /// [`MqttTransport::recv`] polls the event loop. A caller MUST therefore drive
    /// `recv` continuously (e.g. the edge/host engines' `recv_and_handle` loop) —
    /// otherwise queued messages are never sent and, once the channel fills,
    /// `publish`/`subscribe` will block. `connect` only polls until the CONNACK.
    ///
    /// TLS is not yet wired: if [`super::ConnectOptions::tls`] is `Some`, `connect`
    /// fails loudly rather than silently downgrading to plaintext.
    pub struct RumqttcTransport {
        client: Option<AsyncClient>,
        eventloop: Option<EventLoop>,
        connect_timeout: Duration,
        channel_capacity: usize,
    }

    impl RumqttcTransport {
        /// A new, unconnected transport (call [`MqttTransport::connect`]).
        #[must_use]
        pub fn new() -> Self {
            Self {
                client: None,
                eventloop: None,
                connect_timeout: Duration::from_secs(10),
                channel_capacity: 256,
            }
        }

        /// Override how long [`MqttTransport::connect`] waits for the CONNACK.
        #[must_use]
        pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
            self.connect_timeout = timeout;
            self
        }

        fn client(&self) -> Result<&AsyncClient> {
            self.client
                .as_ref()
                .ok_or_else(|| SparkplugError::Transport("not connected".to_owned()))
        }
    }

    impl Default for RumqttcTransport {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MqttTransport for RumqttcTransport {
        async fn connect(&mut self, opts: &ConnectOptions) -> Result<()> {
            if opts.tls.is_some() {
                // Fail loud rather than silently connecting in the clear.
                return Err(SparkplugError::Transport(
                    "TLS is not yet wired in the rumqttc backend; would connect in plaintext"
                        .to_owned(),
                ));
            }
            let mut options =
                MqttOptions::new(opts.client_id.clone(), opts.host.clone(), opts.port);
            options.set_keep_alive(Duration::from_secs(u64::from(opts.keep_alive_secs)));
            options.set_clean_start(opts.clean_start);
            // MQTT 5.0: Sparkplug requires Clean Start = true AND Session Expiry
            // Interval = 0 (tck-id-principles-persistence-clean-session-50). Set the
            // property explicitly so it is present on the wire for strict brokers/TCK.
            let mut props = ConnectProperties::new();
            props.session_expiry_interval = Some(0);
            options.set_connect_properties(props);
            if let Some(will) = &opts.will {
                options.set_last_will(LastWill::new(
                    will.topic.clone(),
                    will.payload.to_vec(),
                    to_rumqtt_qos(will.qos),
                    will.retain,
                    None,
                ));
            }

            let (client, mut eventloop) = AsyncClient::new(options, self.channel_capacity);

            // Drive the event loop until the CONNACK; the broker may still be
            // binding, so a poll error is retried until the deadline.
            let deadline = Instant::now() + self.connect_timeout;
            loop {
                match tokio::time::timeout(Duration::from_secs(1), eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Incoming::ConnAck(_)))) => break,
                    Ok(Ok(_)) => {}
                    // A refused CONNACK or non-CONNACK first packet is fatal —
                    // fail fast with the reason instead of spinning to the deadline.
                    Ok(Err(
                        e
                        @ (ConnectionError::ConnectionRefused(_) | ConnectionError::NotConnAck(_)),
                    )) => return Err(transport_err(e)),
                    Ok(Err(_)) => tokio::time::sleep(Duration::from_millis(50)).await,
                    Err(_elapsed) => {}
                }
                if Instant::now() >= deadline {
                    return Err(SparkplugError::Transport(
                        "timed out waiting for CONNACK".to_owned(),
                    ));
                }
            }

            self.client = Some(client);
            self.eventloop = Some(eventloop);
            Ok(())
        }

        async fn subscribe(&mut self, topic_filter: &str, qos: Qos) -> Result<()> {
            self.client()?
                .subscribe(topic_filter, to_rumqtt_qos(qos))
                .await
                .map_err(transport_err)
        }

        async fn publish(&mut self, message: &OutboundMessage) -> Result<()> {
            self.client()?
                .publish(
                    message.topic.clone(),
                    to_rumqtt_qos(message.qos),
                    message.retain,
                    message.payload.to_vec(),
                )
                .await
                .map_err(transport_err)
        }

        async fn disconnect(&mut self) -> Result<()> {
            if let Some(client) = &self.client {
                client.disconnect().await.map_err(transport_err)?;
            }
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<IncomingMessage>> {
            let eventloop = self
                .eventloop
                .as_mut()
                .ok_or_else(|| SparkplugError::Transport("not connected".to_owned()))?;
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        let topic = String::from_utf8(publish.topic.to_vec())
                            .map_err(|_| SparkplugError::InvalidUtf8)?;
                        return Ok(Some(IncomingMessage {
                            topic,
                            payload: bytes::Bytes::from(publish.payload.to_vec()),
                        }));
                    }
                    Ok(_) => {}
                    Err(e) => return Err(transport_err(e)),
                }
            }
        }
    }
}

#[cfg(feature = "transport-rumqttc")]
pub use rumqtt_impl::RumqttcTransport;
