//! Host Application STATE payload (spec §6.4.27).
//!
//! Unlike the Sparkplug data payloads (protobuf), STATE is JSON UTF-8 with
//! exactly two keys: `online` (bool) and `timestamp` (epoch milliseconds UTC)
//! (`tck-id-host-topic-phid-birth-payload`). It is published retained at QoS 1
//! on `spBv1.0/STATE/<host_id>`; the birth (online) reuses the will (offline)
//! timestamp (`tck-id-host-topic-phid-birth-payload-timestamp`).
//!
//! The format is small and fixed, so we serialize/parse it directly (no serde
//! dependency — the foundation crate stays at `bytes` + `thiserror`).

use crate::error::{Result, SparkplugError};

/// A Host Application STATE message body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatePayload {
    /// Whether the Host Application is online.
    pub online: bool,
    /// Epoch milliseconds (UTC) of this state.
    pub timestamp: i64,
}

impl StatePayload {
    /// Construct a STATE payload.
    #[must_use]
    pub const fn new(online: bool, timestamp: i64) -> Self {
        Self { online, timestamp }
    }

    /// Serialize to the canonical JSON form `{"online":<bool>,"timestamp":<ms>}`.
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\"online\":{},\"timestamp\":{}}}",
            self.online, self.timestamp
        )
    }

    /// Parse a STATE JSON payload.
    ///
    /// Tolerant of whitespace and key order; requires both `online` and
    /// `timestamp`. The STATE object has no nested structure, so this small
    /// hand parser is sufficient and total.
    ///
    /// # Errors
    /// Returns [`SparkplugError::InvalidState`] for malformed JSON, a missing
    /// key, or an out-of-form value.
    pub fn parse(json: &str) -> Result<Self> {
        let trimmed = json.trim();
        let inner = trimmed
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| SparkplugError::InvalidState("expected a JSON object".to_owned()))?;

        let mut online: Option<bool> = None;
        let mut timestamp: Option<i64> = None;

        for field in inner.split(',') {
            if field.trim().is_empty() {
                continue;
            }
            let (key, value) = field.split_once(':').ok_or_else(|| {
                SparkplugError::InvalidState(format!("expected key:value, got {field:?}"))
            })?;
            let key = key.trim().trim_matches('"');
            let value = value.trim();
            match key {
                "online" => {
                    online = Some(match value {
                        "true" => true,
                        "false" => false,
                        other => {
                            return Err(SparkplugError::InvalidState(format!(
                                "online must be true/false, got {other:?}"
                            )));
                        }
                    });
                }
                "timestamp" => {
                    timestamp = Some(value.parse::<i64>().map_err(|e| {
                        SparkplugError::InvalidState(format!("timestamp not an integer: {e}"))
                    })?);
                }
                // Ignore unknown keys for forward compatibility.
                _ => {}
            }
        }

        match (online, timestamp) {
            (Some(online), Some(timestamp)) => Ok(Self { online, timestamp }),
            _ => Err(SparkplugError::InvalidState(
                "missing 'online' or 'timestamp'".to_owned(),
            )),
        }
    }
}
