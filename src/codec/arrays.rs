//! Sparkplug array packing into a single `bytes_value` field (spec §6.4.17).
//!
//! Rules (from the spec + the Tahu reference, which is the canonical encoder):
//! - numeric/temporal arrays: fixed-width **little-endian** elements
//!   (1/2/4/8 bytes); `DateTime` is epoch-ms `i64` (8 bytes).
//! - `BooleanArray`: a 4-byte **little-endian element count**, then `ceil(n/8)`
//!   bytes, bits packed **MSB-first** within each byte (`byte |= 128 >> bit`).
//! - `StringArray`: each UTF-8 string followed by a single `0x00` NUL terminator.

use bytes::{BufMut, Bytes, BytesMut};

use crate::datatype::DataType;
use crate::error::{Result, SparkplugError};
use crate::value::MetricValue;

/// Pack an array-typed [`MetricValue`] into its `bytes_value` representation.
/// Non-array values yield empty bytes (the encoder only calls this for arrays).
pub(crate) fn pack(value: &MetricValue) -> Bytes {
    let mut buf = BytesMut::new();
    match value {
        MetricValue::Int8Array(v) => {
            for &x in v {
                buf.put_i8(x);
            }
        }
        MetricValue::UInt8Array(v) => buf.put_slice(v),
        MetricValue::Int16Array(v) => {
            for &x in v {
                buf.put_i16_le(x);
            }
        }
        MetricValue::UInt16Array(v) => {
            for &x in v {
                buf.put_u16_le(x);
            }
        }
        MetricValue::Int32Array(v) => {
            for &x in v {
                buf.put_i32_le(x);
            }
        }
        MetricValue::UInt32Array(v) => {
            for &x in v {
                buf.put_u32_le(x);
            }
        }
        MetricValue::Int64Array(v) | MetricValue::DateTimeArray(v) => {
            for &x in v {
                buf.put_i64_le(x);
            }
        }
        MetricValue::UInt64Array(v) => {
            for &x in v {
                buf.put_u64_le(x);
            }
        }
        MetricValue::FloatArray(v) => {
            for &x in v {
                buf.put_f32_le(x);
            }
        }
        MetricValue::DoubleArray(v) => {
            for &x in v {
                buf.put_f64_le(x);
            }
        }
        MetricValue::BooleanArray(v) => {
            buf.put_u32_le(v.len() as u32);
            for chunk in v.chunks(8) {
                let mut byte = 0u8;
                for (bit, &b) in chunk.iter().enumerate() {
                    if b {
                        byte |= 128 >> bit;
                    }
                }
                buf.put_u8(byte);
            }
        }
        MetricValue::StringArray(v) => {
            for s in v {
                buf.put_slice(s.as_bytes());
                buf.put_u8(0);
            }
        }
        _ => {}
    }
    buf.freeze()
}

/// Unpack a `bytes_value` slice into an array-typed [`MetricValue`] given its
/// declared [`DataType`].
///
/// # Errors
/// Returns [`SparkplugError::ArrayLength`] if a numeric array's length is not a
/// multiple of the element width, [`SparkplugError::Truncated`] for a short
/// boolean array, or [`SparkplugError::InvalidUtf8`] for a bad string array.
pub(crate) fn unpack(dt: DataType, data: &[u8]) -> Result<MetricValue> {
    match dt {
        DataType::BooleanArray => unpack_bool(data),
        DataType::StringArray => unpack_strings(data),
        _ => unpack_numeric(dt, data),
    }
}

fn unpack_numeric(dt: DataType, data: &[u8]) -> Result<MetricValue> {
    let width = dt.array_element_width().ok_or_else(|| {
        SparkplugError::ValueTypeMismatch(format!("{dt:?} is not a numeric array"))
    })?;
    if !data.len().is_multiple_of(width) {
        return Err(SparkplugError::ArrayLength {
            len: data.len(),
            width,
        });
    }
    let value = match dt {
        DataType::Int8Array => MetricValue::Int8Array(data.iter().map(|&b| b as i8).collect()),
        DataType::UInt8Array => MetricValue::UInt8Array(data.to_vec()),
        DataType::Int16Array => MetricValue::Int16Array(
            data.chunks_exact(2)
                .map(|c| i16::from_le_bytes([c[0], c[1]]))
                .collect(),
        ),
        DataType::UInt16Array => MetricValue::UInt16Array(
            data.chunks_exact(2)
                .map(|c| u16::from_le_bytes([c[0], c[1]]))
                .collect(),
        ),
        DataType::Int32Array => MetricValue::Int32Array(
            data.chunks_exact(4)
                .map(|c| i32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
        ),
        DataType::UInt32Array => MetricValue::UInt32Array(
            data.chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
        ),
        DataType::FloatArray => MetricValue::FloatArray(
            data.chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
        ),
        DataType::Int64Array => MetricValue::Int64Array(
            data.chunks_exact(8)
                .map(|c| i64::from_le_bytes(c.try_into().expect("8-byte chunk")))
                .collect(),
        ),
        DataType::UInt64Array => MetricValue::UInt64Array(
            data.chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().expect("8-byte chunk")))
                .collect(),
        ),
        DataType::DoubleArray => MetricValue::DoubleArray(
            data.chunks_exact(8)
                .map(|c| f64::from_le_bytes(c.try_into().expect("8-byte chunk")))
                .collect(),
        ),
        DataType::DateTimeArray => MetricValue::DateTimeArray(
            data.chunks_exact(8)
                .map(|c| i64::from_le_bytes(c.try_into().expect("8-byte chunk")))
                .collect(),
        ),
        other => {
            return Err(SparkplugError::ValueTypeMismatch(format!(
                "{other:?} is not a numeric array"
            )));
        }
    };
    Ok(value)
}

fn unpack_bool(data: &[u8]) -> Result<MetricValue> {
    if data.len() < 4 {
        return Err(SparkplugError::Truncated);
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let bits = &data[4..];
    let needed = count.div_ceil(8);
    if bits.len() < needed {
        return Err(SparkplugError::Truncated);
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let byte = bits[i / 8];
        out.push(byte & (128 >> (i % 8)) != 0);
    }
    Ok(MetricValue::BooleanArray(out))
}

fn unpack_strings(data: &[u8]) -> Result<MetricValue> {
    if data.is_empty() {
        return Ok(MetricValue::StringArray(Vec::new()));
    }
    let mut segments: Vec<&[u8]> = data.split(|&b| b == 0).collect();
    // Each element is NUL-terminated, so a trailing NUL yields an empty segment.
    if data.last() == Some(&0) {
        segments.pop();
    }
    let mut out = Vec::with_capacity(segments.len());
    for seg in segments {
        out.push(
            std::str::from_utf8(seg)
                .map_err(|_| SparkplugError::InvalidUtf8)?
                .to_owned(),
        );
    }
    Ok(MetricValue::StringArray(out))
}
