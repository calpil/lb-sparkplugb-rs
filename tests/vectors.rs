//! Golden byte vector (validates the wire format against hand-computed protobuf
//! bytes, independent of our own encoder) plus deterministic round-trips for the
//! tricky encodings (two's-complement, boolean bit-packing, NUL-delimited strings).

use bytes::Bytes;
use sparkplug_b::value::MetricValue;
use sparkplug_b::{DataType, EncodeOptions, Metric, Payload, decode, encode};

/// A hand-computed NBIRTH-style payload: one Int32 metric named "x" = 5, seq = 0.
///
/// Wire layout:
/// - field 2 (metrics, len-delimited): tag `0x12`, len `0x07`, body:
///   - name "x":        `0x0A 0x01 0x78`
///   - datatype Int32=3: `0x20 0x03`
///   - int_value 5:      `0x50 0x05`
/// - field 3 (seq, varint): tag `0x18`, value `0x00`
const GOLDEN: &[u8] = &[
    0x12, 0x07, 0x0A, 0x01, 0x78, 0x20, 0x03, 0x50, 0x05, 0x18, 0x00,
];

fn golden_payload() -> Payload {
    Payload {
        timestamp: None,
        metrics: vec![Metric::new("x", MetricValue::Int32(5))],
        seq: Some(0),
        uuid: None,
        body: None,
    }
}

#[test]
fn encodes_to_golden_bytes() {
    let bytes = encode(&golden_payload(), EncodeOptions::birth());
    assert_eq!(bytes.as_ref(), GOLDEN);
}

#[test]
fn decodes_golden_bytes() {
    let decoded = decode(GOLDEN, None).expect("golden vector decodes");
    assert_eq!(decoded, golden_payload());
}

/// Each value round-trips through encode(birth) / decode.
fn roundtrip(value: MetricValue) {
    let payload = Payload::new().with_metric(Metric::new("m", value.clone()));
    let bytes = encode(&payload, EncodeOptions::birth());
    let decoded = decode(&bytes, None).expect("round-trips");
    assert_eq!(decoded.metrics[0].value, value);
}

#[test]
fn signed_integers_roundtrip_via_twos_complement() {
    roundtrip(MetricValue::Int8(-1));
    roundtrip(MetricValue::Int8(i8::MIN));
    roundtrip(MetricValue::Int16(-12345));
    roundtrip(MetricValue::Int32(i32::MIN));
    roundtrip(MetricValue::Int64(i64::MIN));
    roundtrip(MetricValue::UInt8(255));
    roundtrip(MetricValue::UInt64(u64::MAX));
}

#[test]
fn boolean_array_bit_packing_roundtrips() {
    roundtrip(MetricValue::BooleanArray(vec![true, false, true]));
    roundtrip(MetricValue::BooleanArray(vec![])); // 4-byte count prefix, no data bytes
    roundtrip(MetricValue::BooleanArray(vec![true; 17])); // spans 3 bytes
}

#[test]
fn string_array_nul_framing_roundtrips() {
    roundtrip(MetricValue::StringArray(vec![
        "alpha".to_owned(),
        String::new(), // empty element
        "δ-ünïcode".to_owned(),
    ]));
    roundtrip(MetricValue::StringArray(vec![]));
}

#[test]
fn null_value_roundtrips_with_declared_type() {
    roundtrip(MetricValue::Null(DataType::Double));
    roundtrip(MetricValue::Null(DataType::Template));
}

#[test]
fn empty_numeric_array_roundtrips() {
    roundtrip(MetricValue::Int32Array(vec![]));
    roundtrip(MetricValue::DoubleArray(vec![1.5, -2.25, 0.0]));
}

#[test]
fn bytes_and_file_are_distinguished_by_datatype() {
    roundtrip(MetricValue::Bytes(Bytes::from_static(b"\x00\x01\xff")));
    roundtrip(MetricValue::File(Bytes::from_static(b"PDF")));
}
