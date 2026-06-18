//! Host Application + Primary Host — **Phase 3 scaffold**.
//!
//! Sketches the public shape of the Host Application designed in
//! `docs/plan-lb-sparkplugb-rs-sparkplug-b.md` §7: STATE birth/will (retained,
//! QoS 1, shared timestamp), per-node sequence validation + reorder buffer,
//! alias binding, NDEATH bdSeq gating, and a [`HostEvent`] stream. The full
//! async engine lands in Phase 3.

use crate::model::Metric;
use crate::state::StatePayload;
use crate::topic::SparkplugTopic;

/// Configuration for a Host Application.
#[derive(Clone, Debug)]
pub struct HostConfig {
    /// The Host Application ID (its STATE topic is `spBv1.0/STATE/<host_id>`).
    pub host_id: String,
    /// The Sparkplug data-namespace subscriptions to make (e.g. `spBv1.0/#`).
    pub group_subscriptions: Vec<String>,
}

/// An event surfaced by the Phase 3 Host Application to its consumer (delivered
/// over a channel rather than via Tahu's 18-method callback interface).
#[derive(Clone, Debug)]
pub enum HostEvent {
    /// An Edge Node birth was received and processed.
    NodeBirth {
        /// The NBIRTH topic.
        topic: SparkplugTopic,
    },
    /// An Edge Node data metric update.
    NodeData {
        /// The NDATA topic.
        topic: SparkplugTopic,
        /// The (alias-resolved) metric.
        metric: Metric,
    },
    /// An Edge Node death (all its metrics become stale).
    NodeDeath {
        /// The NDEATH topic.
        topic: SparkplugTopic,
    },
    /// A Device birth.
    DeviceBirth {
        /// The DBIRTH topic.
        topic: SparkplugTopic,
    },
    /// A Device data metric update.
    DeviceData {
        /// The DDATA topic.
        topic: SparkplugTopic,
        /// The (alias-resolved) metric.
        metric: Metric,
    },
    /// A Device death.
    DeviceDeath {
        /// The DDEATH topic.
        topic: SparkplugTopic,
    },
    /// A metric was marked stale (after a death).
    Stale {
        /// The owning entity's topic.
        topic: SparkplugTopic,
    },
    /// A primary-host STATE change observed on `spBv1.0/STATE/<host_id>`.
    PrimaryHostState(StatePayload),
}
