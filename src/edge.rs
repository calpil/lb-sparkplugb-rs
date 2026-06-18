//! Edge Node + Device lifecycle (Phase 2).
//!
//! [`EdgeNode`] is an async engine, generic over an [`MqttTransport`] and a
//! [`BdSeqStore`], that implements the Sparkplug Edge Node session: register the
//! NDEATH as the MQTT will, connect, subscribe to NCMD/DCMD, publish
//! NBIRTH (`seq = 0`, the `bdSeq` metric, and `Node Control/Rebirth = false`)
//! then each Device's DBIRTH, publish NDATA/DDATA by exception, handle an
//! NCMD-driven (debounced) rebirth that reuses the connection's `bdSeq`, and
//! publish an NDEATH on graceful disconnect.
//!
//! The engine spawns no tasks and uses no runtime timers, so it is deterministic
//! and runtime-agnostic: the caller drives [`EdgeNode::recv_and_handle`] (or
//! calls [`EdgeNode::handle_incoming`] directly) and the publish methods. The
//! Phase 4 `rumqttc` transport supplies the real network loop.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::alias::AliasRegistry;
use crate::codec::{EncodeOptions, decode, encode};
use crate::error::{Result, SparkplugError};
use crate::model::{Metric, Payload};
use crate::sequence::{BdSeqStore, Seq};
use crate::state::StatePayload;
use crate::topic::{DeviceId, EdgeNodeId, GroupId, MessageType, SparkplugTopic};
use crate::transport::{ConnectOptions, IncomingMessage, MqttTransport, OutboundMessage, Qos};
use crate::value::MetricValue;
use crate::{BDSEQ_METRIC_NAME, NODE_CONTROL_REBIRTH};

/// Milliseconds since the Unix epoch (UTC), for payload/metric timestamps.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Stamp every metric lacking a timestamp with `ts`. The spec requires a
/// per-metric timestamp on every NBIRTH/DBIRTH/NDATA/DDATA metric
/// (`tck-id-payloads-name-birth-data-requirement`), and some hosts (e.g. srad)
/// reject an NBIRTH whose metrics are unstamped.
fn stamp(metrics: &mut [Metric], ts: u64) {
    for metric in metrics.iter_mut() {
        if metric.timestamp.is_none() {
            metric.timestamp = Some(ts);
        }
    }
}

/// Edge Node lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeState {
    /// Not connected.
    Disconnected,
    /// Connected; awaiting the configured primary host's online STATE before birthing.
    WaitingForPrimaryHost,
    /// Births published; publishing data by exception.
    Online,
}

/// Configuration for an [`EdgeNode`].
#[derive(Clone, Debug)]
pub struct EdgeNodeConfig {
    /// Group ID.
    pub group: GroupId,
    /// Edge Node ID.
    pub edge: EdgeNodeId,
    /// Devices reported by this Edge Node.
    pub devices: Vec<DeviceId>,
    /// Optional primary Host Application ID to gate online/offline on.
    pub primary_host_id: Option<String>,
    /// Assign and use metric aliases on DATA messages.
    pub use_aliases: bool,
    /// Debounce window collapsing a storm of rebirth requests into one.
    pub rebirth_debounce: Duration,
    /// MQTT client id.
    pub client_id: String,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: u16,
    /// Keep-alive interval, seconds.
    pub keep_alive_secs: u16,
}

impl EdgeNodeConfig {
    /// Build a config from string identifiers, validating the Group/Edge/Device IDs.
    ///
    /// # Errors
    /// Returns [`SparkplugError::InvalidId`] if any identifier is empty or
    /// contains a reserved character.
    pub fn new(group: &str, edge: &str, devices: &[&str]) -> Result<Self> {
        Ok(Self {
            group: GroupId::new(group)?,
            edge: EdgeNodeId::new(edge)?,
            devices: devices
                .iter()
                .map(|d| DeviceId::new(*d))
                .collect::<Result<_>>()?,
            primary_host_id: None,
            use_aliases: false,
            rebirth_debounce: Duration::from_secs(5),
            client_id: format!("{group}-{edge}"),
            host: "localhost".to_owned(),
            port: 1883,
            keep_alive_secs: 30,
        })
    }
}

/// What the application supplies to the engine: the metrics to publish in births.
pub trait DataSource {
    /// The Edge Node's metrics for an NBIRTH (every metric it will ever report).
    fn node_birth_metrics(&self) -> Vec<Metric>;

    /// A Device's metrics for a DBIRTH.
    fn device_birth_metrics(&self, device: &str) -> Vec<Metric>;
}

/// An event surfaced by [`EdgeNode::handle_incoming`].
#[derive(Clone, Debug)]
pub enum EdgeEvent {
    /// A rebirth was requested and the birth sequence was re-published.
    Rebirthed,
    /// A rebirth was requested but suppressed by the debounce window.
    RebirthDebounced,
    /// An Edge Node command (NCMD) other than a rebirth.
    NodeCommand(Payload),
    /// A Device command (DCMD).
    DeviceCommand {
        /// Target device id.
        device: String,
        /// The command payload.
        payload: Payload,
    },
    /// A primary-host STATE update.
    PrimaryHostState(StatePayload),
    /// A message that did not require engine action.
    Ignored,
}

/// The Edge Node engine.
pub struct EdgeNode<T, S> {
    config: EdgeNodeConfig,
    transport: T,
    bdseq_store: S,
    seq: Seq,
    bd_seq: u8,
    aliases: AliasRegistry,
    next_alias: u64,
    born_devices: Vec<String>,
    last_rebirth: Option<Instant>,
    last_state_ts: Option<i64>,
    state: EdgeState,
}

impl<T: MqttTransport, S: BdSeqStore> EdgeNode<T, S> {
    /// Create an engine. Call [`EdgeNode::connect`] to start the session.
    pub fn new(config: EdgeNodeConfig, transport: T, bdseq_store: S) -> Self {
        Self {
            config,
            transport,
            bdseq_store,
            seq: Seq::new(),
            bd_seq: 0,
            aliases: AliasRegistry::new(),
            next_alias: 0,
            born_devices: Vec::new(),
            last_rebirth: None,
            last_state_ts: None,
            state: EdgeState::Disconnected,
        }
    }

    /// The current lifecycle state.
    #[must_use]
    pub fn state(&self) -> EdgeState {
        self.state
    }

    /// The `bdSeq` value bound to the current connection.
    #[must_use]
    pub fn bd_seq(&self) -> u8 {
        self.bd_seq
    }

    fn node_topic(&self, ty: MessageType) -> String {
        SparkplugTopic::Node {
            group: self.config.group.clone(),
            edge: self.config.edge.clone(),
            ty,
        }
        .to_string()
    }

    fn device_topic(&self, device: &DeviceId, ty: MessageType) -> String {
        SparkplugTopic::Device {
            group: self.config.group.clone(),
            edge: self.config.edge.clone(),
            device: device.clone(),
            ty,
        }
        .to_string()
    }

    /// The NDEATH payload (a single `bdSeq` INT64 metric).
    fn ndeath_payload(&self) -> Payload {
        let ts = now_ms();
        let mut metrics = vec![Metric::new(
            BDSEQ_METRIC_NAME,
            MetricValue::Int64(i64::from(self.bd_seq)),
        )];
        stamp(&mut metrics, ts);
        Payload {
            timestamp: Some(ts),
            metrics,
            seq: None, // NDEATH carries no sequence number
            uuid: None,
            body: None,
        }
    }

    async fn publish_raw(
        &mut self,
        topic: String,
        payload: Bytes,
        qos: Qos,
        retain: bool,
    ) -> Result<()> {
        self.transport
            .publish(&OutboundMessage {
                topic,
                qos,
                retain,
                payload,
            })
            .await
    }

    /// Connect to the broker (registering the NDEATH will), subscribe to NCMD /
    /// per-device DCMD (and the primary host's STATE if configured), then publish
    /// the birth sequence unless gated on a primary host.
    ///
    /// # Errors
    /// Propagates any transport error.
    pub async fn connect<D: DataSource>(&mut self, source: &D) -> Result<()> {
        // The bdSeq for THIS connection, then persist the next one.
        self.bd_seq = self.bdseq_store.load_next_death()?;
        self.bdseq_store
            .store_next_death(self.bd_seq.wrapping_add(1))?;

        let will_payload = encode(&self.ndeath_payload(), EncodeOptions::birth());
        let will = OutboundMessage {
            topic: self.node_topic(MessageType::NDeath),
            qos: Qos::AtLeastOnce, // will QoS MUST be 1
            retain: false,         // will retain MUST be false
            payload: will_payload,
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

        // NCMD/DCMD subscriptions MUST be QoS 1 so rebirth/command requests are
        // not silently dropped (tck-id-message-flow-edge-node-ncmd-subscribe,
        // tck-id-message-flow-device-dcmd-subscribe).
        let ncmd_topic = self.node_topic(MessageType::NCmd);
        self.transport
            .subscribe(&ncmd_topic, Qos::AtLeastOnce)
            .await?;
        let devices = self.config.devices.clone();
        for device in &devices {
            let dcmd_topic = self.device_topic(device, MessageType::DCmd);
            self.transport
                .subscribe(&dcmd_topic, Qos::AtLeastOnce)
                .await?;
        }

        if let Some(host_id) = self.config.primary_host_id.clone() {
            let state_topic = format!("spBv1.0/STATE/{host_id}");
            self.transport
                .subscribe(&state_topic, Qos::AtLeastOnce)
                .await?;
            self.state = EdgeState::WaitingForPrimaryHost;
        } else {
            self.publish_birth_sequence(source).await?;
        }
        Ok(())
    }

    /// Publish NBIRTH (`seq = 0`) followed by each Device's DBIRTH.
    ///
    /// # Errors
    /// Propagates any transport error.
    pub async fn publish_birth_sequence<D: DataSource>(&mut self, source: &D) -> Result<()> {
        self.seq.reset();
        if self.config.use_aliases {
            self.aliases.clear();
            self.next_alias = 0;
        }
        self.born_devices.clear();

        let ts = now_ms();
        let mut metrics = self.build_node_birth_metrics(source);
        stamp(&mut metrics, ts);
        let payload = Payload {
            timestamp: Some(ts),
            metrics,
            seq: Some(self.seq.next_value()), // NBIRTH carries seq = 0
            uuid: None,
            body: None,
        };
        let bytes = encode(&payload, EncodeOptions::birth());
        self.publish_raw(
            self.node_topic(MessageType::NBirth),
            bytes,
            Qos::AtMostOnce,
            false,
        )
        .await?;

        let devices = self.config.devices.clone();
        for device in &devices {
            self.publish_device_birth(source, device).await?;
        }
        self.state = EdgeState::Online;
        Ok(())
    }

    fn build_node_birth_metrics<D: DataSource>(&mut self, source: &D) -> Vec<Metric> {
        let mut metrics = source.node_birth_metrics();
        if !metrics
            .iter()
            .any(|m| m.name.as_deref() == Some(NODE_CONTROL_REBIRTH))
        {
            metrics.push(Metric::new(
                NODE_CONTROL_REBIRTH,
                MetricValue::Boolean(false),
            ));
        }
        if !metrics
            .iter()
            .any(|m| m.name.as_deref() == Some(BDSEQ_METRIC_NAME))
        {
            metrics.push(Metric::new(
                BDSEQ_METRIC_NAME,
                MetricValue::Int64(i64::from(self.bd_seq)),
            ));
        }
        self.assign_aliases(&mut metrics);
        metrics
    }

    async fn publish_device_birth<D: DataSource>(
        &mut self,
        source: &D,
        device: &DeviceId,
    ) -> Result<()> {
        let ts = now_ms();
        let mut metrics = source.device_birth_metrics(device.as_str());
        self.assign_aliases(&mut metrics);
        stamp(&mut metrics, ts);
        let payload = Payload {
            timestamp: Some(ts),
            metrics,
            seq: Some(self.seq.next_value()),
            uuid: None,
            body: None,
        };
        let bytes = encode(&payload, EncodeOptions::birth());
        self.publish_raw(
            self.device_topic(device, MessageType::DBirth),
            bytes,
            Qos::AtMostOnce,
            false,
        )
        .await?;
        self.born_devices.push(device.as_str().to_owned());
        Ok(())
    }

    /// Assign aliases to data metrics (never to the control metrics), recording
    /// the name↔alias↔datatype binding for later DATA resolution.
    fn assign_aliases(&mut self, metrics: &mut [Metric]) {
        if !self.config.use_aliases {
            return;
        }
        for metric in metrics.iter_mut() {
            let Some(name) = metric.name.clone() else {
                continue;
            };
            // The rebirth and bdSeq control metrics MUST NOT be aliased.
            if name == NODE_CONTROL_REBIRTH || name == BDSEQ_METRIC_NAME {
                continue;
            }
            let alias = self.next_alias;
            self.next_alias += 1;
            metric.alias = Some(alias);
            self.aliases
                .bind(&name, Some(alias), metric.value.datatype());
        }
    }

    /// Map data metrics to their on-wire form: alias-only when aliasing is on
    /// and a binding exists, otherwise by name.
    fn to_data_metrics(&self, metrics: Vec<Metric>) -> Vec<Metric> {
        if !self.config.use_aliases {
            return metrics;
        }
        metrics
            .into_iter()
            .map(|mut m| {
                if let Some(name) = m.name.as_deref()
                    && let Some(alias) = self.aliases.alias_for_name(name)
                {
                    m.alias = Some(alias);
                    m.name = None;
                }
                m
            })
            .collect()
    }

    /// Publish an NDATA (report-by-exception) with the next sequence number.
    ///
    /// # Errors
    /// Returns [`SparkplugError::InvalidTopic`] if not online, or a transport error.
    pub async fn publish_node_data(&mut self, metrics: Vec<Metric>) -> Result<()> {
        if self.state != EdgeState::Online {
            return Err(SparkplugError::InvalidTopic(
                "cannot publish NDATA before NBIRTH".to_owned(),
            ));
        }
        let ts = now_ms();
        let mut data = self.to_data_metrics(metrics);
        stamp(&mut data, ts);
        let payload = Payload {
            timestamp: Some(ts),
            metrics: data,
            seq: Some(self.seq.next_value()),
            uuid: None,
            body: None,
        };
        let bytes = encode(&payload, EncodeOptions::data());
        self.publish_raw(
            self.node_topic(MessageType::NData),
            bytes,
            Qos::AtMostOnce,
            false,
        )
        .await
    }

    /// Publish a DDATA for a born device.
    ///
    /// # Errors
    /// Returns an error if the device is not born or on transport failure.
    pub async fn publish_device_data(&mut self, device: &str, metrics: Vec<Metric>) -> Result<()> {
        if !self.born_devices.iter().any(|d| d == device) {
            return Err(SparkplugError::InvalidTopic(format!(
                "device {device:?} is not born; cannot publish DDATA"
            )));
        }
        let device_id = DeviceId::new(device)?;
        let ts = now_ms();
        let mut data = self.to_data_metrics(metrics);
        stamp(&mut data, ts);
        let payload = Payload {
            timestamp: Some(ts),
            metrics: data,
            seq: Some(self.seq.next_value()),
            uuid: None,
            body: None,
        };
        let bytes = encode(&payload, EncodeOptions::data());
        self.publish_raw(
            self.device_topic(&device_id, MessageType::DData),
            bytes,
            Qos::AtMostOnce,
            false,
        )
        .await
    }

    /// Publish a DDEATH for a born device and mark it offline. DDEATH carries a
    /// sequence number (`tck-id-payloads-ddeath-seq`) at QoS 0, retain=false.
    ///
    /// # Errors
    /// Returns an error if the device is not born, or on transport failure.
    pub async fn publish_device_death(&mut self, device: &str) -> Result<()> {
        let pos = self
            .born_devices
            .iter()
            .position(|d| d == device)
            .ok_or_else(|| {
                SparkplugError::InvalidTopic(format!(
                    "device {device:?} is not born; cannot publish DDEATH"
                ))
            })?;
        let device_id = DeviceId::new(device)?;
        let payload = Payload {
            timestamp: Some(now_ms()),
            metrics: Vec::new(),
            seq: Some(self.seq.next_value()),
            uuid: None,
            body: None,
        };
        let bytes = encode(&payload, EncodeOptions::birth());
        self.publish_raw(
            self.device_topic(&device_id, MessageType::DDeath),
            bytes,
            Qos::AtMostOnce,
            false,
        )
        .await?;
        self.born_devices.remove(pos);
        Ok(())
    }

    fn rebirth_allowed(&self) -> bool {
        match self.last_rebirth {
            None => true,
            Some(at) => at.elapsed() >= self.config.rebirth_debounce,
        }
    }

    /// Handle one inbound message, driving rebirth / primary-host transitions.
    ///
    /// A `Node Control/Rebirth = true` NCMD re-publishes the birth sequence
    /// (resetting `seq` to 0 and **reusing** the connection's `bdSeq`), subject to
    /// the rebirth debounce.
    ///
    /// # Errors
    /// Returns a decode/transport error; a malformed topic/payload is surfaced
    /// rather than panicking.
    pub async fn handle_incoming<D: DataSource>(
        &mut self,
        message: &IncomingMessage,
        source: &D,
    ) -> Result<EdgeEvent> {
        let topic = SparkplugTopic::parse(&message.topic)?;
        match topic {
            SparkplugTopic::Node {
                ty: MessageType::NCmd,
                ..
            } => {
                let payload = decode(&message.payload, Some(&self.aliases))?;
                let is_rebirth = payload.metrics.iter().any(|m| {
                    m.name.as_deref() == Some(NODE_CONTROL_REBIRTH)
                        && matches!(m.value, MetricValue::Boolean(true))
                });
                if is_rebirth {
                    if self.rebirth_allowed() {
                        self.publish_birth_sequence(source).await?;
                        self.last_rebirth = Some(Instant::now());
                        return Ok(EdgeEvent::Rebirthed);
                    }
                    return Ok(EdgeEvent::RebirthDebounced);
                }
                Ok(EdgeEvent::NodeCommand(payload))
            }
            SparkplugTopic::Device {
                device,
                ty: MessageType::DCmd,
                ..
            } => {
                let payload = decode(&message.payload, Some(&self.aliases))?;
                Ok(EdgeEvent::DeviceCommand {
                    device: device.as_str().to_owned(),
                    payload,
                })
            }
            SparkplugTopic::HostState { host_id } => {
                if self.config.primary_host_id.as_deref() != Some(host_id.as_str()) {
                    return Ok(EdgeEvent::Ignored);
                }
                let state = StatePayload::parse(
                    std::str::from_utf8(&message.payload)
                        .map_err(|_| SparkplugError::InvalidUtf8)?,
                )?;
                // Ignore a STATE older than the last accepted one (the edge
                // drops stale/out-of-order primary-host STATE messages).
                if self
                    .last_state_ts
                    .is_some_and(|last| state.timestamp < last)
                {
                    return Ok(EdgeEvent::Ignored);
                }
                self.last_state_ts = Some(state.timestamp);
                if state.online {
                    if self.state == EdgeState::WaitingForPrimaryHost {
                        self.publish_birth_sequence(source).await?;
                    }
                } else if self.state == EdgeState::Online {
                    // Primary host went offline: terminate this session (a
                    // graceful disconnect publishes NDEATH first). Re-homing to
                    // another MQTT server is the Phase 4 multi-server concern.
                    self.disconnect().await?;
                }
                Ok(EdgeEvent::PrimaryHostState(state))
            }
            _ => Ok(EdgeEvent::Ignored),
        }
    }

    /// Receive the next inbound message and handle it; `None` if the stream closed.
    ///
    /// # Errors
    /// Propagates transport/decode errors.
    pub async fn recv_and_handle<D: DataSource>(
        &mut self,
        source: &D,
    ) -> Result<Option<EdgeEvent>> {
        match self.transport.recv().await? {
            Some(message) => Ok(Some(self.handle_incoming(&message, source).await?)),
            None => Ok(None),
        }
    }

    /// Gracefully disconnect: publish the NDEATH explicitly (so the broker need
    /// not deliver the will), then disconnect.
    ///
    /// # Errors
    /// Propagates any transport error.
    pub async fn disconnect(&mut self) -> Result<()> {
        let bytes = encode(&self.ndeath_payload(), EncodeOptions::birth());
        self.publish_raw(
            self.node_topic(MessageType::NDeath),
            bytes,
            Qos::AtLeastOnce,
            false,
        )
        .await?;
        self.transport.disconnect().await?;
        self.state = EdgeState::Disconnected;
        Ok(())
    }
}
