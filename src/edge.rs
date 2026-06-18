//! Edge Node + Device lifecycle — **Phase 2 scaffold**.
//!
//! This module sketches the public shape of the Edge Node engine designed in
//! `docs/plan-lb-sparkplugb-rs-sparkplug-b.md` §6. The full async (Tokio) state
//! machine — connect → subscribe → NBIRTH(`seq=0`)/DBIRTH → RBE NDATA → NDEATH
//! (LWT), with NCMD-driven debounced rebirth and multi-server primary-host
//! failover — lands in Phase 2. The types here compile today so downstream code
//! can be written against the intended API.

use std::time::Duration;

use crate::error::Result;
use crate::model::Payload;

/// Edge Node lifecycle state (the Phase 2 state machine, ADR-aligned).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeState {
    /// Not connected to any MQTT server.
    Disconnected,
    /// MQTT connect in progress.
    Connecting,
    /// Connected; waiting for the configured primary host's online STATE before birthing.
    WaitingForPrimaryHost,
    /// Births published; publishing data by exception.
    Online,
}

/// Configuration for an Edge Node.
#[derive(Clone, Debug)]
pub struct EdgeNodeConfig {
    /// Group ID.
    pub group_id: String,
    /// Edge Node ID.
    pub edge_node_id: String,
    /// Optional primary Host Application ID to gate online/offline on.
    pub primary_host_id: Option<String>,
    /// Whether to assign and use metric aliases on DATA messages.
    pub use_aliases: bool,
    /// Debounce window collapsing a storm of rebirth requests into one.
    pub rebirth_debounce: Duration,
}

/// The SPI the Phase 2 Edge Node engine calls into for payloads and writes.
///
/// In Phase 2 these become `async fn` driven by the engine's Tokio task.
pub trait MetricHandler {
    /// Build the NDEATH payload (a single `bdSeq` metric) to register as the
    /// MQTT will (`tck-id-message-flow-edge-node-birth-publish-will-message-payload`).
    ///
    /// # Errors
    /// Returns an error if the death payload cannot be produced.
    fn death_payload(&self) -> Result<Payload>;

    /// Publish the full birth sequence: NBIRTH (seq=0, bdSeq, `Node Control/Rebirth`)
    /// then each Device's DBIRTH.
    ///
    /// # Errors
    /// Returns an error if a birth cannot be published.
    fn publish_birth_sequence(&mut self) -> Result<()>;

    /// Whether `name` is a metric this Edge Node (or one of its Devices) reports.
    fn has_metric(&self, name: &str) -> bool;
}
