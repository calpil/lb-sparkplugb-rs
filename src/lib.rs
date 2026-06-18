//! # `sparkplug_b` — Eclipse Sparkplug B 3.0.0 in Rust
//!
//! A standalone, spec-driven implementation of the [Eclipse Sparkplug B 3.0.0]
//! specification: the wire payload model + codec, the topic namespace, the
//! sequence/`bdSeq` machinery, and (phased) the Edge Node and Host Application
//! roles.
//!
//! This crate (package `lb-sparkplugb-rs`, library `sparkplug_b`) has **no
//! dependency** on any SCADA framework. Phase 1 — the payload model, the
//! `protoc`-free proto2 codec, the topic namespace, and the seq/bdSeq layer —
//! is implemented and tested. The Edge/Host/transport layers are designed in
//! `docs/plan-lb-sparkplugb-rs-sparkplug-b.md` and scaffolded in [`edge`],
//! [`host`], and [`transport`].
//!
//! ## Example: build, encode, and decode an NBIRTH-style payload
//!
//! ```
//! use sparkplug_b::{decode, encode, EncodeOptions, Metric, MetricValue, Payload};
//!
//! let payload = Payload::new()
//!     .with_timestamp(1_700_000_000_000)
//!     .with_seq(0) // NBIRTH carries seq = 0 (tck-id-topics-nbirth-seq-num)
//!     .with_metric(Metric::new("Temperature", MetricValue::Double(21.5)))
//!     .with_metric(Metric::new("Online", MetricValue::Boolean(true)));
//!
//! let bytes = encode(&payload, EncodeOptions::birth());
//! let decoded = decode(&bytes, None).expect("valid payload round-trips");
//! assert_eq!(decoded, payload);
//! ```
//!
//! [Eclipse Sparkplug B 3.0.0]: https://sparkplug.eclipse.org/specification/version/3.0/documents/sparkplug-specification-3.0.0.pdf

pub mod alias;
pub mod codec;
pub mod datatype;
pub mod edge;
pub mod error;
pub mod host;
pub mod model;
pub mod sequence;
pub mod state;
pub mod topic;
pub mod transport;
pub mod value;

#[cfg(feature = "compression")]
pub mod compress;

pub use alias::{AliasRegistry, MetricKey};
pub use codec::{EncodeOptions, decode, encode};
pub use datatype::DataType;
pub use edge::{DataSource, EdgeEvent, EdgeNode, EdgeNodeConfig, EdgeState};
pub use error::{Result, SparkplugError};
pub use host::{HostApplication, HostConfig, HostEvent};
pub use model::{
    DataSet, MetaData, Metric, Parameter, Payload, PropertySet, PropertySetList, Template,
};
pub use sequence::{BdSeq, BdSeqStore, FileBdSeqStore, InMemoryBdSeqStore, Seq};
pub use state::StatePayload;
pub use topic::{DeviceId, EdgeNodeId, GroupId, MessageType, SparkplugTopic};
pub use transport::{
    ConnectOptions, IncomingMessage, MqttTransport, OutboundMessage, Qos, TlsConfig,
};
pub use value::{DataSetValue, MetricValue, ParameterValue, PropertyValue};

/// The Sparkplug B namespace token (`spBv1.0`, `tck-id-topic-structure-namespace-a`).
pub const SPARKPLUG_B_NAMESPACE: &str = topic::NAMESPACE;

/// The reserved `Node Control/Rebirth` metric name (`tck-id-topics-nbirth-rebirth-metric`).
pub const NODE_CONTROL_REBIRTH: &str = "Node Control/Rebirth";

/// The `bdSeq` metric name carried in NBIRTH/NDEATH (`tck-id-payloads-nbirth-bdseq`).
pub const BDSEQ_METRIC_NAME: &str = "bdSeq";

/// The `uuid` sentinel marking a compression-envelope payload.
pub const COMPRESSED_PAYLOAD_UUID: &str = "SPBV1.0_COMPRESSED";
