//! Property-based round-trip tests: `decode(encode(p)) == p` across every
//! scalar/array datatype, a stripped-DATA + `AliasRegistry` recovery round-trip,
//! and a "decode never panics on arbitrary bytes" property.
//!
//! Strategies are grammatical (valid payloads), not `.*` over the wire — the
//! lesson from the SCADA project's parser-fuzzing work (feat#23). NaN floats are
//! excluded because `PartialEq` is not reflexive for NaN (the round-trip
//! preserves the bits regardless). Composite types (DataSet/Template/PropertySet)
//! are covered by deterministic round-trips in `composites.rs`.

use bytes::Bytes;
use proptest::prelude::*;
use sparkplug_b::value::MetricValue;
use sparkplug_b::{AliasRegistry, DataType, EncodeOptions, Metric, Payload, decode, encode};

fn finite_f32() -> impl Strategy<Value = f32> {
    any::<f32>().prop_filter("exclude NaN", |f| !f.is_nan())
}

fn finite_f64() -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("exclude NaN", |f| !f.is_nan())
}

/// The datatypes a metric value may legally declare (excludes Unknown and the
/// property-only PropertySet/PropertySetList).
fn valid_metric_datatype() -> impl Strategy<Value = DataType> {
    prop::sample::select(vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float,
        DataType::Double,
        DataType::Boolean,
        DataType::String,
        DataType::DateTime,
        DataType::Text,
        DataType::Uuid,
        DataType::Bytes,
        DataType::File,
        DataType::Int8Array,
        DataType::Int16Array,
        DataType::Int32Array,
        DataType::Int64Array,
        DataType::UInt8Array,
        DataType::UInt16Array,
        DataType::UInt32Array,
        DataType::UInt64Array,
        DataType::FloatArray,
        DataType::DoubleArray,
        DataType::BooleanArray,
        DataType::StringArray,
        DataType::DateTimeArray,
    ])
}

fn metric_value() -> impl Strategy<Value = MetricValue> {
    let small = 0usize..12;
    prop_oneof![
        any::<i8>().prop_map(MetricValue::Int8),
        any::<i16>().prop_map(MetricValue::Int16),
        any::<i32>().prop_map(MetricValue::Int32),
        any::<i64>().prop_map(MetricValue::Int64),
        any::<u8>().prop_map(MetricValue::UInt8),
        any::<u16>().prop_map(MetricValue::UInt16),
        any::<u32>().prop_map(MetricValue::UInt32),
        any::<u64>().prop_map(MetricValue::UInt64),
        finite_f32().prop_map(MetricValue::Float),
        finite_f64().prop_map(MetricValue::Double),
        any::<bool>().prop_map(MetricValue::Boolean),
        ".*".prop_map(MetricValue::String),
        ".*".prop_map(MetricValue::Text),
        "[0-9a-fA-F-]{0,36}".prop_map(MetricValue::Uuid),
        any::<i64>().prop_map(MetricValue::DateTime),
        prop::collection::vec(any::<u8>(), small.clone())
            .prop_map(|v| MetricValue::Bytes(Bytes::from(v))),
        prop::collection::vec(any::<u8>(), small.clone())
            .prop_map(|v| MetricValue::File(Bytes::from(v))),
        prop::collection::vec(any::<i8>(), small.clone()).prop_map(MetricValue::Int8Array),
        prop::collection::vec(any::<i16>(), small.clone()).prop_map(MetricValue::Int16Array),
        prop::collection::vec(any::<i32>(), small.clone()).prop_map(MetricValue::Int32Array),
        prop::collection::vec(any::<i64>(), small.clone()).prop_map(MetricValue::Int64Array),
        prop::collection::vec(any::<u8>(), small.clone()).prop_map(MetricValue::UInt8Array),
        prop::collection::vec(any::<u16>(), small.clone()).prop_map(MetricValue::UInt16Array),
        prop::collection::vec(any::<u32>(), small.clone()).prop_map(MetricValue::UInt32Array),
        prop::collection::vec(any::<u64>(), small.clone()).prop_map(MetricValue::UInt64Array),
        prop::collection::vec(finite_f32(), small.clone()).prop_map(MetricValue::FloatArray),
        prop::collection::vec(finite_f64(), small.clone()).prop_map(MetricValue::DoubleArray),
        prop::collection::vec(any::<bool>(), 0usize..20).prop_map(MetricValue::BooleanArray),
        prop::collection::vec("[^\u{0}]{0,8}", small.clone()).prop_map(MetricValue::StringArray),
        prop::collection::vec(any::<i64>(), small).prop_map(MetricValue::DateTimeArray),
        valid_metric_datatype().prop_map(MetricValue::Null),
    ]
}

fn metric() -> impl Strategy<Value = Metric> {
    (
        proptest::option::of("[A-Za-z0-9_/]{1,16}"),
        proptest::option::of(any::<u64>()),
        proptest::option::of(any::<u64>()),
        metric_value(),
        proptest::option::of(any::<bool>()),
        proptest::option::of(any::<bool>()),
    )
        .prop_map(
            |(name, alias, timestamp, value, is_historical, is_transient)| Metric {
                name,
                alias,
                timestamp,
                value,
                is_historical,
                is_transient,
                metadata: None,
                properties: None,
            },
        )
}

fn payload() -> impl Strategy<Value = Payload> {
    (
        proptest::option::of(any::<u64>()),
        proptest::option::of(any::<u8>()),
        prop::collection::vec(metric(), 0..8),
    )
        .prop_map(|(timestamp, seq, metrics)| Payload {
            timestamp,
            metrics,
            seq,
            uuid: None,
            body: None,
        })
}

/// A payload of named+aliased metrics with unique names, suitable for the
/// strip-and-recover path (an `AliasRegistry` is built from these metrics).
fn recoverable_payload() -> impl Strategy<Value = Payload> {
    prop::collection::vec((any::<u64>(), metric_value()), 0..6).prop_map(|items| {
        let metrics = items
            .into_iter()
            .enumerate()
            .map(|(i, (alias, value))| Metric {
                name: Some(format!("m{i}")), // unique names so name-based recovery is unambiguous
                alias: Some(alias),
                timestamp: None,
                value,
                is_historical: None,
                is_transient: None,
                metadata: None,
                properties: None,
            })
            .collect();
        Payload {
            timestamp: Some(1),
            metrics,
            seq: Some(0),
            uuid: None,
            body: None,
        }
    })
}

proptest! {
    /// Encoding with datatypes included (BIRTH) and decoding without a registry
    /// reproduces the original payload exactly.
    #[test]
    fn birth_roundtrip(p in payload()) {
        let bytes = encode(&p, EncodeOptions::birth());
        let decoded = decode(&bytes, None)?;
        prop_assert_eq!(decoded, p);
    }

    /// Encoding with datatypes stripped (DATA) and decoding with an AliasRegistry
    /// built from the metrics recovers every datatype and reproduces the payload.
    #[test]
    fn data_roundtrip_with_registry(p in recoverable_payload()) {
        let mut reg = AliasRegistry::new();
        for m in &p.metrics {
            reg.bind(m.name.as_deref().unwrap(), m.alias, m.value.datatype());
        }
        let bytes = encode(&p, EncodeOptions::data());
        let decoded = decode(&bytes, Some(&reg))?;
        prop_assert_eq!(decoded, p);
    }

    /// The decoder never panics on arbitrary bytes (it returns Ok or a typed Err).
    #[test]
    fn decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = decode(&bytes, None);
    }
}
