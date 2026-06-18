//! Negative / hardening tests: malformed inputs map to the right error variant,
//! the recursion guard fires, and the strip-datatypes recovery path behaves.

use sparkplug_b::value::MetricValue;
use sparkplug_b::{
    AliasRegistry, DataType, EncodeOptions, Metric, Parameter, Payload, PropertySet, PropertyValue,
    SparkplugError, Template, decode, encode,
};

#[test]
fn truncated_length_delimited_field() {
    // metrics field (2): claims 5 bytes but only 2 follow.
    let bytes = [0x12, 0x05, 0x00, 0x00];
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::Truncated)
    ));
}

#[test]
fn varint_overflows_64_bits() {
    // timestamp field (1) followed by 10 continuation bytes => > 64 bits.
    let mut bytes = vec![0x08];
    bytes.extend([0xFF; 10]);
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::VarintOverflow)
    ));
}

#[test]
fn unknown_datatype_code_in_metric() {
    // one metric whose datatype field (4) = 35 (out of range).
    let bytes = [0x12, 0x02, 0x20, 0x23];
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::UnknownDataType(35))
    ));
}

#[test]
fn numeric_array_length_not_multiple_of_width() {
    // metric datatype 23 (Int16Array) + a 3-byte bytes_value (not a multiple of 2).
    let bytes = [0x12, 0x08, 0x20, 0x17, 0x82, 0x01, 0x03, 0x00, 0x00, 0x00];
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::ArrayLength { len: 3, width: 2 })
    ));
}

#[test]
fn invalid_utf8_string_field() {
    // metric datatype 12 (String) + a string_value (field 15) of one byte 0xFF.
    let bytes = [0x12, 0x05, 0x20, 0x0C, 0x7A, 0x01, 0xFF];
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::InvalidUtf8)
    ));
}

#[test]
fn propertyset_key_value_count_mismatch() {
    // Boolean metric, is_null=true, properties with 1 key and 0 values.
    let bytes = [
        0x12, 0x09, 0x20, 0x0B, 0x38, 0x01, 0x4A, 0x03, 0x0A, 0x01, 0x6B,
    ];
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::ValueTypeMismatch(_))
    ));
}

#[test]
fn deeply_nested_propertyset_hits_recursion_limit() {
    // Build a PropertySet nested far deeper than MAX_DEPTH (32).
    let mut value = PropertyValue::Int32(0);
    for _ in 0..50 {
        value = PropertyValue::PropertySet(PropertySet::new().with("n", value));
    }
    let props = PropertySet::new().with("root", value);
    let payload = Payload::new()
        .with_seq(0)
        .with_metric(Metric::new("m", MetricValue::Boolean(true)).with_properties(props));
    let bytes = encode(&payload, EncodeOptions::birth());
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::RecursionLimit)
    ));
}

#[test]
fn shallowly_nested_propertyset_decodes_ok() {
    let mut value = PropertyValue::Int32(0);
    for _ in 0..4 {
        value = PropertyValue::PropertySet(PropertySet::new().with("n", value));
    }
    let props = PropertySet::new().with("root", value);
    let payload = Payload::new()
        .with_seq(0)
        .with_metric(Metric::new("m", MetricValue::Boolean(true)).with_properties(props));
    let bytes = encode(&payload, EncodeOptions::birth());
    let decoded = decode(&bytes, None).expect("shallow nesting decodes");
    assert_eq!(decoded, payload);
}

#[test]
fn stripped_payload_without_registry_errors() {
    let payload = Payload::new()
        .with_seq(0)
        .with_metric(Metric::new("x", MetricValue::Int32(1)));
    let bytes = encode(&payload, EncodeOptions::data()); // datatype stripped
    assert!(matches!(
        decode(&bytes, None),
        Err(SparkplugError::MissingDataType(_))
    ));
}

#[test]
fn name_based_datatype_recovery() {
    let payload = Payload::new()
        .with_seq(0)
        .with_metric(Metric::new("x", MetricValue::Int32(42)));
    let bytes = encode(&payload, EncodeOptions::data());
    let mut reg = AliasRegistry::new();
    reg.bind("x", None, DataType::Int32);
    let decoded = decode(&bytes, Some(&reg)).expect("recovers by name");
    assert_eq!(decoded, payload);
}

#[test]
fn alias_based_datatype_recovery() {
    let payload = Payload::new()
        .with_seq(0)
        .with_metric(Metric::aliased(9, MetricValue::Int32(7)));
    let bytes = encode(&payload, EncodeOptions::data());
    let mut reg = AliasRegistry::new();
    reg.bind("x", Some(9), DataType::Int32);
    let decoded = decode(&bytes, Some(&reg)).expect("recovers by alias");
    assert_eq!(decoded, payload);
}

#[test]
fn stripped_template_member_without_binding_errors_as_documented() {
    // Phase-1 limitation (documented in decode.rs): a stripped template member
    // is recovered by bare name; if the registry only binds the outer metric,
    // the member's datatype cannot be recovered -> MissingDataType (not silent).
    let instance = Template {
        version: None,
        template_ref: Some("T".into()),
        is_definition: false,
        metrics: vec![Metric::new("member", MetricValue::Int32(7))],
        parameters: Vec::<Parameter>::new(),
    };
    let payload = Payload::new().with_seq(0).with_metric(Metric::new(
        "inst",
        MetricValue::Template(Box::new(instance)),
    ));
    let bytes = encode(&payload, EncodeOptions::data());
    let mut reg = AliasRegistry::new();
    reg.bind("inst", None, DataType::Template); // outer only; member not bound
    assert!(matches!(
        decode(&bytes, Some(&reg)),
        Err(SparkplugError::MissingDataType(_))
    ));
}
