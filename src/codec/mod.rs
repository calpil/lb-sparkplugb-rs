//! Sparkplug B protobuf (proto2) codec — hand-written and `protoc`-free (ADR-1).
//!
//! The codec is pure: it has no notion of seq/bdSeq/STATE/transport (those live
//! in higher layers). Encoding is total; decoding never panics on hostile input.

mod arrays;
mod decode;
mod encode;
mod wire;

use bytes::Bytes;

use crate::alias::AliasRegistry;
use crate::error::Result;
use crate::model::Payload;

/// Options controlling how a payload is encoded.
#[derive(Clone, Copy, Debug, Default)]
pub struct EncodeOptions {
    /// When `true`, per-metric `datatype` fields are omitted (DATA/CMD messages,
    /// `tck-id-payloads-metric-datatype-not-req`). Decoding such a payload then
    /// requires an [`AliasRegistry`] built from the corresponding birth.
    pub strip_datatypes: bool,
}

impl EncodeOptions {
    /// Options for a BIRTH/DEATH message — datatypes included.
    #[must_use]
    pub const fn birth() -> Self {
        Self {
            strip_datatypes: false,
        }
    }

    /// Options for a DATA/CMD message — datatypes stripped.
    #[must_use]
    pub const fn data() -> Self {
        Self {
            strip_datatypes: true,
        }
    }
}

/// Encode a [`Payload`] to Sparkplug B protobuf bytes.
#[must_use]
pub fn encode(payload: &Payload, opts: EncodeOptions) -> Bytes {
    encode::encode_payload(payload, opts.strip_datatypes)
}

/// Decode Sparkplug B protobuf bytes into a [`Payload`].
///
/// Pass `types` (an [`AliasRegistry`] built from the birth) to recover datatypes
/// for stripped DATA/CMD metrics; pass `None` for BIRTH/DEATH payloads, which
/// carry their own datatypes.
///
/// # Errors
/// Returns a [`crate::SparkplugError`] for malformed input: truncation, invalid
/// wire/field types, unknown datatypes, a stripped metric whose datatype cannot
/// be recovered, an inconsistent DataSet/PropertySet, or excessive nesting.
pub fn decode(bytes: &[u8], types: Option<&AliasRegistry>) -> Result<Payload> {
    decode::decode_payload(bytes, types)
}
