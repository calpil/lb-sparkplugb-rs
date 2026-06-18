//! Topic namespace, message types, and validated identifiers (spec §4).
//!
//! `namespace/group_id/message_type/edge_node_id/[device_id]`
//! (`tck-id-topic-structure`), with the constant namespace `spBv1.0`. We model
//! a topic as an enum (ADR-3) so the token-count ↔ message-type invariant is
//! enforced by the type system, and we expose a single canonical parser
//! (Eclipse Tahu ships two divergent ones).

use std::fmt;
use std::str::FromStr;

use crate::error::{Result, SparkplugError};

/// The Sparkplug B namespace token.
pub const NAMESPACE: &str = "spBv1.0";

/// The STATE topic token (`spBv1.0/STATE/<host_id>`).
pub const STATE_TOKEN: &str = "STATE";

/// A Sparkplug message type (the topic's `message_type` token).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// Edge Node birth certificate.
    NBirth,
    /// Edge Node death certificate.
    NDeath,
    /// Edge Node data.
    NData,
    /// Edge Node command.
    NCmd,
    /// Device birth certificate.
    DBirth,
    /// Device death certificate.
    DDeath,
    /// Device data.
    DData,
    /// Device command.
    DCmd,
    /// Host Application state.
    State,
}

impl MessageType {
    /// The wire token (`"NBIRTH"`, `"DDATA"`, …).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NBirth => "NBIRTH",
            Self::NDeath => "NDEATH",
            Self::NData => "NDATA",
            Self::NCmd => "NCMD",
            Self::DBirth => "DBIRTH",
            Self::DDeath => "DDEATH",
            Self::DData => "DDATA",
            Self::DCmd => "DCMD",
            Self::State => "STATE",
        }
    }

    /// Whether this message type addresses a Device (carries a `device_id`).
    #[must_use]
    pub const fn has_device(self) -> bool {
        matches!(self, Self::DBirth | Self::DDeath | Self::DData | Self::DCmd)
    }

    /// Whether a payload of this type carries a sequence number. NDEATH, NCMD,
    /// DCMD, and STATE do not (`tck-id-payloads-{ndeath,ncmd,dcmd}-seq`).
    #[must_use]
    pub const fn carries_seq(self) -> bool {
        matches!(
            self,
            Self::NBirth | Self::NData | Self::DBirth | Self::DData | Self::DDeath
        )
    }

    /// Whether this is a command (NCMD/DCMD).
    #[must_use]
    pub const fn is_command(self) -> bool {
        matches!(self, Self::NCmd | Self::DCmd)
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MessageType {
    type Err = SparkplugError;

    fn from_str(s: &str) -> Result<Self> {
        let ty = match s {
            "NBIRTH" => Self::NBirth,
            "NDEATH" => Self::NDeath,
            "NDATA" => Self::NData,
            "NCMD" => Self::NCmd,
            "DBIRTH" => Self::DBirth,
            "DDEATH" => Self::DDeath,
            "DDATA" => Self::DData,
            "DCMD" => Self::DCmd,
            "STATE" => Self::State,
            other => {
                return Err(SparkplugError::InvalidTopic(format!(
                    "unknown message type {other:?}"
                )));
            }
        };
        Ok(ty)
    }
}

/// Validate a Sparkplug identifier token: non-empty and free of the reserved
/// MQTT characters `+`, `/`, `#` (`tck-id-topic-structure-namespace-valid-*`).
fn validate_id(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(SparkplugError::InvalidId("identifier is empty".to_owned()));
    }
    if s.contains(['+', '/', '#']) {
        return Err(SparkplugError::InvalidId(format!(
            "{s:?} contains a reserved character (+, /, or #)"
        )));
    }
    Ok(())
}

/// Define a validated identifier newtype.
macro_rules! id_newtype {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            /// Create the identifier, validating reserved characters.
            ///
            /// # Errors
            /// Returns [`SparkplugError::InvalidId`] if empty or containing `+`, `/`, `#`.
            pub fn new(s: impl Into<String>) -> Result<Self> {
                let s = s.into();
                validate_id(&s)?;
                Ok(Self(s))
            }

            /// The identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

id_newtype!(
    /// A Sparkplug Group ID.
    GroupId
);
id_newtype!(
    /// A Sparkplug Edge Node ID.
    EdgeNodeId
);
id_newtype!(
    /// A Sparkplug Device ID.
    DeviceId
);

/// A parsed Sparkplug B topic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SparkplugTopic {
    /// An Edge-Node-level topic (4 tokens; `N*` message types).
    Node {
        /// Group ID.
        group: GroupId,
        /// Edge Node ID.
        edge: EdgeNodeId,
        /// Message type (one of NBIRTH/NDEATH/NDATA/NCMD).
        ty: MessageType,
    },
    /// A Device-level topic (5 tokens; `D*` message types).
    Device {
        /// Group ID.
        group: GroupId,
        /// Edge Node ID.
        edge: EdgeNodeId,
        /// Device ID.
        device: DeviceId,
        /// Message type (one of DBIRTH/DDEATH/DDATA/DCMD).
        ty: MessageType,
    },
    /// A Host Application STATE topic (`spBv1.0/STATE/<host_id>`).
    HostState {
        /// Host Application ID.
        host_id: String,
    },
}

impl SparkplugTopic {
    /// Build an Edge-Node topic; the message type must be a non-device `N*` type.
    ///
    /// # Errors
    /// Returns [`SparkplugError::InvalidTopic`] if `ty` is a device or STATE type.
    pub fn node(group: GroupId, edge: EdgeNodeId, ty: MessageType) -> Result<Self> {
        if ty.has_device() || ty == MessageType::State {
            return Err(SparkplugError::InvalidTopic(format!(
                "{ty} is not an Edge-Node message type"
            )));
        }
        Ok(Self::Node { group, edge, ty })
    }

    /// Build a Device topic; the message type must be a `D*` type.
    ///
    /// # Errors
    /// Returns [`SparkplugError::InvalidTopic`] if `ty` is not a device type.
    pub fn device(
        group: GroupId,
        edge: EdgeNodeId,
        device: DeviceId,
        ty: MessageType,
    ) -> Result<Self> {
        if !ty.has_device() {
            return Err(SparkplugError::InvalidTopic(format!(
                "{ty} is not a Device message type"
            )));
        }
        Ok(Self::Device {
            group,
            edge,
            device,
            ty,
        })
    }

    /// The message type of this topic.
    #[must_use]
    pub fn message_type(&self) -> MessageType {
        match self {
            Self::Node { ty, .. } | Self::Device { ty, .. } => *ty,
            Self::HostState { .. } => MessageType::State,
        }
    }

    /// Parse a topic string into a validated [`SparkplugTopic`].
    ///
    /// # Errors
    /// Returns [`SparkplugError::InvalidTopic`] / [`SparkplugError::InvalidId`]
    /// for a bad namespace, wrong token count, reserved characters, or a
    /// message type that does not match the topic arity.
    pub fn parse(topic: &str) -> Result<Self> {
        let parts: Vec<&str> = topic.split('/').collect();
        if parts.first() != Some(&NAMESPACE) {
            return Err(SparkplugError::InvalidTopic(format!(
                "topic must start with {NAMESPACE:?}: {topic:?}"
            )));
        }
        match parts.as_slice() {
            [_, token, host] if *token == STATE_TOKEN => {
                validate_id(host)?;
                Ok(Self::HostState {
                    host_id: (*host).to_owned(),
                })
            }
            [_, group, ty_token, edge] => {
                let ty = MessageType::from_str(ty_token)?;
                if ty.has_device() || ty == MessageType::State {
                    return Err(SparkplugError::InvalidTopic(format!(
                        "{ty} requires a different token count"
                    )));
                }
                Self::node(GroupId::new(*group)?, EdgeNodeId::new(*edge)?, ty)
            }
            [_, group, ty_token, edge, device] => {
                let ty = MessageType::from_str(ty_token)?;
                if !ty.has_device() {
                    return Err(SparkplugError::InvalidTopic(format!(
                        "{ty} must not include a device_id"
                    )));
                }
                Self::device(
                    GroupId::new(*group)?,
                    EdgeNodeId::new(*edge)?,
                    DeviceId::new(*device)?,
                    ty,
                )
            }
            _ => Err(SparkplugError::InvalidTopic(format!(
                "unexpected token count in topic {topic:?}"
            ))),
        }
    }
}

impl fmt::Display for SparkplugTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node { group, edge, ty } => {
                write!(f, "{NAMESPACE}/{group}/{ty}/{edge}")
            }
            Self::Device {
                group,
                edge,
                device,
                ty,
            } => write!(f, "{NAMESPACE}/{group}/{ty}/{edge}/{device}"),
            Self::HostState { host_id } => write!(f, "{NAMESPACE}/{STATE_TOKEN}/{host_id}"),
        }
    }
}
