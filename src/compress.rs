//! Optional payload compression envelope (`compression` feature).
//!
//! Mirrors the Tahu/spec convention: a compressed payload is a *new* payload
//! whose `uuid` is the [`crate::COMPRESSED_PAYLOAD_UUID`] sentinel and whose
//! `body` holds the compressed bytes of the inner payload. A `String` metric
//! named `algorithm` selects GZIP; its absence implies DEFLATE.

use std::io::{Read, Write};

use bytes::Bytes;
use flate2::Compression as FlateLevel;
use flate2::read::{DeflateDecoder, GzDecoder};
use flate2::write::{DeflateEncoder, GzEncoder};

use crate::COMPRESSED_PAYLOAD_UUID;
use crate::alias::AliasRegistry;
use crate::codec::{self, EncodeOptions};
use crate::error::{Result, SparkplugError};
use crate::model::{Metric, Payload};
use crate::value::MetricValue;

/// The compression algorithm used for a payload envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    /// Raw DEFLATE (the default when no `algorithm` metric is present).
    Deflate,
    /// GZIP.
    Gzip,
}

/// Encode `payload` with `opts`, compress the bytes, and wrap them in a
/// compression-envelope [`Payload`].
///
/// # Errors
/// Returns [`SparkplugError::Io`] if compression fails.
pub fn compress(payload: &Payload, opts: EncodeOptions, algo: Compression) -> Result<Payload> {
    let raw = codec::encode(payload, opts);
    let body = match algo {
        Compression::Deflate => deflate(&raw)?,
        Compression::Gzip => gzip(&raw)?,
    };
    let mut envelope = Payload::new();
    envelope.uuid = Some(COMPRESSED_PAYLOAD_UUID.to_owned());
    envelope.seq = payload.seq;
    envelope.body = Some(body);
    if algo == Compression::Gzip {
        envelope.metrics.push(Metric::new(
            "algorithm",
            MetricValue::String("GZIP".to_owned()),
        ));
    }
    Ok(envelope)
}

/// Reverse [`compress`]: decompress an envelope's body and decode the inner payload.
///
/// # Errors
/// Returns [`SparkplugError`] if `envelope` is not a compression envelope, lacks
/// a body, or fails to decompress/decode.
pub fn decompress(envelope: &Payload, types: Option<&AliasRegistry>) -> Result<Payload> {
    if envelope.uuid.as_deref() != Some(COMPRESSED_PAYLOAD_UUID) {
        return Err(SparkplugError::ValueTypeMismatch(
            "payload is not a SPBV1.0_COMPRESSED envelope".to_owned(),
        ));
    }
    let body = envelope.body.as_ref().ok_or_else(|| {
        SparkplugError::ValueTypeMismatch("compressed envelope has no body".to_owned())
    })?;
    let algo = if envelope.metrics.iter().any(|m| {
        m.name.as_deref() == Some("algorithm")
            && matches!(&m.value, MetricValue::String(s) if s.eq_ignore_ascii_case("GZIP"))
    }) {
        Compression::Gzip
    } else {
        Compression::Deflate
    };
    let raw = match algo {
        Compression::Deflate => inflate(body)?,
        Compression::Gzip => gunzip(body)?,
    };
    codec::decode(&raw, types)
}

fn deflate(data: &[u8]) -> Result<Bytes> {
    let mut encoder = DeflateEncoder::new(Vec::new(), FlateLevel::default());
    encoder.write_all(data)?;
    Ok(Bytes::from(encoder.finish()?))
}

fn inflate(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = DeflateDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

fn gzip(data: &[u8]) -> Result<Bytes> {
    let mut encoder = GzEncoder::new(Vec::new(), FlateLevel::default());
    encoder.write_all(data)?;
    Ok(Bytes::from(encoder.finish()?))
}

fn gunzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}
