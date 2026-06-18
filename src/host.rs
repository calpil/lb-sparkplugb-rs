//! Host Application + Primary Host (Phase 3).
//!
//! [`HostApplication`] is an async engine, generic over an [`MqttTransport`],
//! that implements the Sparkplug Host Application / Primary Host role: publish a
//! retained STATE birth (and register the offline STATE as the will, sharing one
//! timestamp), subscribe to the data namespace, and consume Edge Node traffic —
//! binding aliases on birth, validating the per-Edge-Node sequence number,
//! resolving alias-only DATA back to names, gating NDEATH on the `bdSeq`, and
//! requesting a (debounced) rebirth on a sequence gap or data-before-birth.
//!
//! Like the edge engine it spawns no tasks and uses no runtime timers; the caller
//! drives [`HostApplication::recv_and_handle`]. A reorder buffer (the spec's
//! optional in-order delivery window) is a future enhancement — the base
//! behavior is the spec's fail-fast-and-rebirth.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::alias::AliasRegistry;
use crate::codec::{EncodeOptions, decode, encode};
use crate::error::{Result, SparkplugError};
use crate::model::{Metric, Payload};
use crate::state::StatePayload;
use crate::topic::{DeviceId, EdgeNodeId, GroupId, MessageType, SparkplugTopic};
use crate::transport::{ConnectOptions, IncomingMessage, MqttTransport, OutboundMessage, Qos};
use crate::value::MetricValue;
use crate::{BDSEQ_METRIC_NAME, NODE_CONTROL_REBIRTH};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Configuration for a [`HostApplication`].
#[derive(Clone, Debug)]
pub struct HostConfig {
    /// The Host Application ID (its STATE topic is `spBv1.0/STATE/<host_id>`).
    pub host_id: String,
    /// Sparkplug data-namespace topic filters to subscribe (default `spBv1.0/#`).
    pub group_subscriptions: Vec<String>,
    /// MQTT client id.
    pub client_id: String,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: u16,
    /// Keep-alive interval, seconds.
    pub keep_alive_secs: u16,
    /// Debounce window for rebirth requests, per Edge Node.
    pub rebirth_debounce: Duration,
}

impl HostConfig {
    /// A config for `host_id` subscribing the whole Sparkplug B namespace.
    #[must_use]
    pub fn new(host_id: &str) -> Self {
        Self {
            host_id: host_id.to_owned(),
            group_subscriptions: vec!["spBv1.0/#".to_owned()],
            client_id: format!("host-{host_id}"),
            host: "localhost".to_owned(),
            port: 1883,
            keep_alive_secs: 30,
            rebirth_debounce: Duration::from_secs(5),
        }
    }

    fn state_topic(&self) -> String {
        format!("spBv1.0/STATE/{}", self.host_id)
    }
}

/// An event surfaced by [`HostApplication::handle_incoming`].
#[derive(Clone, Debug)]
pub enum HostEvent {
    /// An Edge Node birth was processed; `metrics` carry names + datatypes.
    NodeBirth {
        /// Group ID.
        group: String,
        /// Edge Node ID.
        edge: String,
        /// The birth metrics.
        metrics: Vec<Metric>,
    },
    /// Edge Node data (alias-only metrics resolved back to names where possible).
    NodeData {
        /// Group ID.
        group: String,
        /// Edge Node ID.
        edge: String,
        /// The data metrics.
        metrics: Vec<Metric>,
    },
    /// An Edge Node death (its metrics, and its devices, are now stale).
    NodeDeath {
        /// Group ID.
        group: String,
        /// Edge Node ID.
        edge: String,
    },
    /// A Device birth.
    DeviceBirth {
        /// Group ID.
        group: String,
        /// Edge Node ID.
        edge: String,
        /// Device ID.
        device: String,
        /// The birth metrics.
        metrics: Vec<Metric>,
    },
    /// Device data (alias-resolved).
    DeviceData {
        /// Group ID.
        group: String,
        /// Edge Node ID.
        edge: String,
        /// Device ID.
        device: String,
        /// The data metrics.
        metrics: Vec<Metric>,
    },
    /// A Device death (its metrics are now stale).
    DeviceDeath {
        /// Group ID.
        group: String,
        /// Edge Node ID.
        edge: String,
        /// Device ID.
        device: String,
    },
    /// The host requested a rebirth (published an NCMD) for an Edge Node.
    RebirthRequested {
        /// Group ID.
        group: String,
        /// Edge Node ID.
        edge: String,
    },
    /// A message that did not require action (own command echo, foreign/own STATE,
    /// a stale NDEATH, or a debounced rebirth).
    Ignored,
}

#[derive(Default)]
struct DeviceState {
    online: bool,
    aliases: AliasRegistry,
}

struct NodeState {
    online: bool,
    bd_seq: Option<i64>,
    expected_seq: u8,
    aliases: AliasRegistry,
    devices: HashMap<String, DeviceState>,
    last_rebirth: Option<Instant>,
}

impl NodeState {
    fn new() -> Self {
        Self {
            online: false,
            bd_seq: None,
            expected_seq: 0,
            aliases: AliasRegistry::new(),
            devices: HashMap::new(),
            last_rebirth: None,
        }
    }

    /// Check the rebirth debounce and, if allowed, record the attempt.
    fn take_rebirth_slot(&mut self, debounce: Duration, now: Instant) -> bool {
        let allowed = self
            .last_rebirth
            .is_none_or(|at| now.duration_since(at) >= debounce);
        if allowed {
            self.last_rebirth = Some(now);
        }
        allowed
    }
}

/// Extract the `bdSeq` metric value from a payload (INT64/UInt64).
fn bdseq_of(payload: &Payload) -> Option<i64> {
    payload
        .metrics
        .iter()
        .find(|m| m.name.as_deref() == Some(BDSEQ_METRIC_NAME))
        .and_then(|m| match &m.value {
            MetricValue::Int64(v) => Some(*v),
            MetricValue::UInt64(v) => i64::try_from(*v).ok(),
            _ => None,
        })
}

/// Fill in metric names from an alias registry for alias-only DATA metrics.
fn resolve_names(aliases: &AliasRegistry, mut metrics: Vec<Metric>) -> Vec<Metric> {
    for metric in &mut metrics {
        if metric.name.is_none()
            && let Some(alias) = metric.alias
            && let Some(name) = aliases.name_for_alias(alias)
        {
            metric.name = Some(name.to_owned());
        }
    }
    metrics
}

/// The internal decision a per-node handler reaches without holding a borrow
/// across an `await`.
enum Step {
    Event(HostEvent),
    Rebirth,
}

/// The Host Application engine.
pub struct HostApplication<T> {
    config: HostConfig,
    transport: T,
    nodes: HashMap<String, NodeState>,
    state_ts: i64,
}

impl<T: MqttTransport> HostApplication<T> {
    /// Create the engine. Call [`HostApplication::start`] to connect + birth.
    pub fn new(config: HostConfig, transport: T) -> Self {
        Self {
            config,
            transport,
            nodes: HashMap::new(),
            state_ts: 0,
        }
    }

    /// Connect (registering the offline STATE will), subscribe to the own STATE
    /// topic (QoS 1) and the data namespace (QoS 0), then publish the retained
    /// online STATE birth that shares the will's timestamp.
    ///
    /// # Errors
    /// Propagates any transport error.
    pub async fn start(&mut self) -> Result<()> {
        self.state_ts = now_ms();
        let state_topic = self.config.state_topic();
        let will = OutboundMessage {
            topic: state_topic.clone(),
            qos: Qos::AtLeastOnce,
            retain: true,
            payload: Bytes::from(StatePayload::new(false, self.state_ts).to_json()),
        };
        let opts = ConnectOptions {
            client_id: self.config.client_id.clone(),
            host: self.config.host.clone(),
            port: self.config.port,
            keep_alive_secs: self.config.keep_alive_secs,
            clean_start: true,
            will: Some(will),
            tls: None,
        };
        self.transport.connect(&opts).await?;

        // Own STATE at QoS 1, then the data namespace at QoS 0.
        self.transport
            .subscribe(&state_topic, Qos::AtLeastOnce)
            .await?;
        let subs = self.config.group_subscriptions.clone();
        for filter in &subs {
            self.transport.subscribe(filter, Qos::AtMostOnce).await?;
        }
        self.publish_state_birth().await
    }

    async fn publish_state_birth(&mut self) -> Result<()> {
        let msg = OutboundMessage {
            topic: self.config.state_topic(),
            qos: Qos::AtLeastOnce,
            retain: true,
            payload: Bytes::from(StatePayload::new(true, self.state_ts).to_json()),
        };
        self.transport.publish(&msg).await
    }

    /// Publish a retained offline STATE and disconnect.
    ///
    /// # Errors
    /// Propagates any transport error.
    pub async fn shutdown(&mut self) -> Result<()> {
        let msg = OutboundMessage {
            topic: self.config.state_topic(),
            qos: Qos::AtLeastOnce,
            retain: true,
            payload: Bytes::from(StatePayload::new(false, now_ms()).to_json()),
        };
        self.transport.publish(&msg).await?;
        self.transport.disconnect().await
    }

    /// Publish an NCMD to an Edge Node (QoS 0, retain=false).
    ///
    /// # Errors
    /// Propagates an invalid-id or transport error.
    pub async fn publish_node_command(
        &mut self,
        group: &str,
        edge: &str,
        metrics: Vec<Metric>,
    ) -> Result<()> {
        let topic = SparkplugTopic::node(
            GroupId::new(group)?,
            EdgeNodeId::new(edge)?,
            MessageType::NCmd,
        )?
        .to_string();
        let payload = Payload {
            timestamp: Some(u64::try_from(now_ms()).unwrap_or(0)),
            metrics,
            seq: None,
            uuid: None,
            body: None,
        };
        self.publish_raw(topic, encode(&payload, EncodeOptions::data()))
            .await
    }

    /// Publish a DCMD to a Device (QoS 0, retain=false).
    ///
    /// # Errors
    /// Propagates an invalid-id or transport error.
    pub async fn publish_device_command(
        &mut self,
        group: &str,
        edge: &str,
        device: &str,
        metrics: Vec<Metric>,
    ) -> Result<()> {
        let topic = SparkplugTopic::device(
            GroupId::new(group)?,
            EdgeNodeId::new(edge)?,
            DeviceId::new(device)?,
            MessageType::DCmd,
        )?
        .to_string();
        let payload = Payload {
            timestamp: Some(u64::try_from(now_ms()).unwrap_or(0)),
            metrics,
            seq: None,
            uuid: None,
            body: None,
        };
        self.publish_raw(topic, encode(&payload, EncodeOptions::data()))
            .await
    }

    async fn publish_raw(&mut self, topic: String, payload: Bytes) -> Result<()> {
        self.transport
            .publish(&OutboundMessage {
                topic,
                qos: Qos::AtMostOnce,
                retain: false,
                payload,
            })
            .await
    }

    /// Request a rebirth: publish an NCMD with `Node Control/Rebirth = true`.
    async fn send_rebirth(&mut self, group: &GroupId, edge: &EdgeNodeId) -> Result<()> {
        let topic = SparkplugTopic::Node {
            group: group.clone(),
            edge: edge.clone(),
            ty: MessageType::NCmd,
        }
        .to_string();
        let payload = Payload {
            timestamp: Some(u64::try_from(now_ms()).unwrap_or(0)),
            metrics: vec![Metric::new(
                NODE_CONTROL_REBIRTH,
                MetricValue::Boolean(true),
            )],
            seq: None,
            uuid: None,
            body: None,
        };
        self.publish_raw(topic, encode(&payload, EncodeOptions::data()))
            .await
    }

    /// Receive the next inbound message and handle it; `None` if the stream closed.
    ///
    /// # Errors
    /// Propagates transport/decode errors.
    pub async fn recv_and_handle(&mut self) -> Result<Option<HostEvent>> {
        match self.transport.recv().await? {
            Some(message) => Ok(Some(self.handle_incoming(&message).await?)),
            None => Ok(None),
        }
    }

    /// Handle one inbound message, advancing the per-Edge-Node state machine.
    ///
    /// # Errors
    /// Returns a decode/transport error; malformed topics/payloads are surfaced
    /// rather than panicking.
    pub async fn handle_incoming(&mut self, message: &IncomingMessage) -> Result<HostEvent> {
        let topic = SparkplugTopic::parse(&message.topic)?;
        match topic {
            SparkplugTopic::HostState { host_id } => {
                // Self-STATE repair: a retained OFFLINE for our own host id means
                // the broker still holds our prior will; re-assert ONLINE.
                if host_id == self.config.host_id {
                    let state = StatePayload::parse(
                        std::str::from_utf8(&message.payload)
                            .map_err(|_| SparkplugError::InvalidUtf8)?,
                    )?;
                    if !state.online {
                        self.publish_state_birth().await?;
                    }
                }
                Ok(HostEvent::Ignored)
            }
            SparkplugTopic::Node { group, edge, ty } => match ty {
                MessageType::NBirth => self.on_node_birth(&group, &edge, &message.payload).await,
                MessageType::NData => self.on_node_data(&group, &edge, &message.payload).await,
                MessageType::NDeath => Ok(self.on_node_death(&group, &edge, &message.payload)?),
                // NCMD is our own outbound echo.
                MessageType::NCmd => Ok(HostEvent::Ignored),
                _ => Ok(HostEvent::Ignored),
            },
            SparkplugTopic::Device {
                group,
                edge,
                device,
                ty,
            } => match ty {
                MessageType::DBirth => {
                    self.on_device_birth(&group, &edge, &device, &message.payload)
                        .await
                }
                MessageType::DData => {
                    self.on_device_data(&group, &edge, &device, &message.payload)
                        .await
                }
                MessageType::DDeath => {
                    self.on_device_death(&group, &edge, &device, &message.payload)
                        .await
                }
                MessageType::DCmd => Ok(HostEvent::Ignored),
                _ => Ok(HostEvent::Ignored),
            },
        }
    }

    fn key(group: &GroupId, edge: &EdgeNodeId) -> String {
        format!("{group}/{edge}")
    }

    async fn on_node_birth(
        &mut self,
        group: &GroupId,
        edge: &EdgeNodeId,
        payload: &[u8],
    ) -> Result<HostEvent> {
        let payload = decode(payload, None)?;
        let key = Self::key(group, edge);
        let mut duplicate_alias = false;
        {
            let node = self.nodes.entry(key).or_insert_with(NodeState::new);
            node.online = true;
            node.bd_seq = bdseq_of(&payload);
            node.expected_seq = payload.seq.unwrap_or(0).wrapping_add(1);
            node.aliases.clear();
            node.devices.clear();
            for m in &payload.metrics {
                if let Some(name) = &m.name
                    && node
                        .aliases
                        .try_bind(name, m.alias, m.value.datatype())
                        .is_err()
                {
                    duplicate_alias = true;
                }
            }
        }
        if duplicate_alias {
            return self.rebirth(group, edge).await;
        }
        Ok(HostEvent::NodeBirth {
            group: group.as_str().to_owned(),
            edge: edge.as_str().to_owned(),
            metrics: payload.metrics,
        })
    }

    async fn on_node_data(
        &mut self,
        group: &GroupId,
        edge: &EdgeNodeId,
        payload: &[u8],
    ) -> Result<HostEvent> {
        let key = Self::key(group, edge);
        let step = match self.nodes.get_mut(&key) {
            Some(node) if node.online => {
                let payload = decode(payload, Some(&node.aliases))?;
                if payload.seq == Some(node.expected_seq) {
                    node.expected_seq = node.expected_seq.wrapping_add(1);
                    Step::Event(HostEvent::NodeData {
                        group: group.as_str().to_owned(),
                        edge: edge.as_str().to_owned(),
                        metrics: resolve_names(&node.aliases, payload.metrics),
                    })
                } else {
                    node.online = false; // a gap invalidates the session until rebirth
                    Step::Rebirth
                }
            }
            _ => Step::Rebirth, // data before/without a birth
        };
        self.finish(group, edge, step).await
    }

    fn on_node_death(
        &mut self,
        group: &GroupId,
        edge: &EdgeNodeId,
        payload: &[u8],
    ) -> Result<HostEvent> {
        let incoming = bdseq_of(&decode(payload, None)?);
        let key = Self::key(group, edge);
        if let Some(node) = self.nodes.get_mut(&key)
            && node.online
            && node.bd_seq == incoming
        {
            node.online = false;
            for device in node.devices.values_mut() {
                device.online = false;
            }
            return Ok(HostEvent::NodeDeath {
                group: group.as_str().to_owned(),
                edge: edge.as_str().to_owned(),
            });
        }
        // Unknown node, already offline, or bdSeq mismatch (stale death) -> ignore.
        Ok(HostEvent::Ignored)
    }

    async fn on_device_birth(
        &mut self,
        group: &GroupId,
        edge: &EdgeNodeId,
        device: &DeviceId,
        payload: &[u8],
    ) -> Result<HostEvent> {
        let key = Self::key(group, edge);
        let dev = device.as_str().to_owned();
        let step = match self.nodes.get_mut(&key) {
            Some(node) if node.online => {
                let payload = decode(payload, None)?;
                if payload.seq == Some(node.expected_seq) {
                    node.expected_seq = node.expected_seq.wrapping_add(1);
                    let device_state = node.devices.entry(dev.clone()).or_default();
                    device_state.online = true;
                    device_state.aliases.clear();
                    let mut dup = false;
                    for m in &payload.metrics {
                        if let Some(name) = &m.name
                            && device_state
                                .aliases
                                .try_bind(name, m.alias, m.value.datatype())
                                .is_err()
                        {
                            dup = true;
                        }
                    }
                    if dup {
                        Step::Rebirth
                    } else {
                        Step::Event(HostEvent::DeviceBirth {
                            group: group.as_str().to_owned(),
                            edge: edge.as_str().to_owned(),
                            device: dev.clone(),
                            metrics: payload.metrics,
                        })
                    }
                } else {
                    node.online = false;
                    Step::Rebirth
                }
            }
            _ => Step::Rebirth,
        };
        self.finish(group, edge, step).await
    }

    async fn on_device_data(
        &mut self,
        group: &GroupId,
        edge: &EdgeNodeId,
        device: &DeviceId,
        payload: &[u8],
    ) -> Result<HostEvent> {
        let key = Self::key(group, edge);
        let dev = device.as_str().to_owned();
        let step = match self.nodes.get_mut(&key) {
            Some(node) if node.online && node.devices.get(&dev).is_some_and(|d| d.online) => {
                let device_state = node.devices.get(&dev).expect("checked present");
                let payload = decode(payload, Some(&device_state.aliases))?;
                if payload.seq == Some(node.expected_seq) {
                    node.expected_seq = node.expected_seq.wrapping_add(1);
                    let device_state = node.devices.get(&dev).expect("checked present");
                    Step::Event(HostEvent::DeviceData {
                        group: group.as_str().to_owned(),
                        edge: edge.as_str().to_owned(),
                        device: dev.clone(),
                        metrics: resolve_names(&device_state.aliases, payload.metrics),
                    })
                } else {
                    node.online = false;
                    Step::Rebirth
                }
            }
            _ => Step::Rebirth,
        };
        self.finish(group, edge, step).await
    }

    async fn on_device_death(
        &mut self,
        group: &GroupId,
        edge: &EdgeNodeId,
        device: &DeviceId,
        payload: &[u8],
    ) -> Result<HostEvent> {
        let key = Self::key(group, edge);
        let dev = device.as_str().to_owned();
        let step = match self.nodes.get_mut(&key) {
            Some(node) if node.online => {
                let payload = decode(payload, None)?;
                if payload.seq == Some(node.expected_seq) {
                    node.expected_seq = node.expected_seq.wrapping_add(1);
                    if let Some(device_state) = node.devices.get_mut(&dev) {
                        device_state.online = false;
                    }
                    Step::Event(HostEvent::DeviceDeath {
                        group: group.as_str().to_owned(),
                        edge: edge.as_str().to_owned(),
                        device: dev.clone(),
                    })
                } else {
                    node.online = false;
                    Step::Rebirth
                }
            }
            _ => Step::Rebirth,
        };
        self.finish(group, edge, step).await
    }

    /// Turn a [`Step`] into an event, sending a (debounced) rebirth NCMD when needed.
    async fn finish(
        &mut self,
        group: &GroupId,
        edge: &EdgeNodeId,
        step: Step,
    ) -> Result<HostEvent> {
        match step {
            Step::Event(event) => Ok(event),
            Step::Rebirth => self.rebirth(group, edge).await,
        }
    }

    async fn rebirth(&mut self, group: &GroupId, edge: &EdgeNodeId) -> Result<HostEvent> {
        let key = Self::key(group, edge);
        let allowed = {
            let node = self.nodes.entry(key).or_insert_with(NodeState::new);
            node.take_rebirth_slot(self.config.rebirth_debounce, Instant::now())
        };
        if !allowed {
            return Ok(HostEvent::Ignored);
        }
        self.send_rebirth(group, edge).await?;
        Ok(HostEvent::RebirthRequested {
            group: group.as_str().to_owned(),
            edge: edge.as_str().to_owned(),
        })
    }
}
