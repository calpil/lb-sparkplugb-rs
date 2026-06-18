//! Deterministic round-trips for the composite payload types and the new
//! validation behaviours. Complements the scalar/array proptest in
//! `codec_roundtrip.rs` (which cannot easily build heterogeneous DataSets).

use bytes::Bytes;
use sparkplug_b::value::{DataSetValue, MetricValue, ParameterValue, PropertyValue};
use sparkplug_b::{
    AliasRegistry, DataSet, DataType, EncodeOptions, MetaData, Metric, Parameter, Payload,
    PropertySet, PropertySetList, Template, decode, encode,
};

/// BIRTH-encode then decode (no registry).
fn rt(payload: &Payload) -> Payload {
    decode(&encode(payload, EncodeOptions::birth()), None).expect("round-trips")
}

fn one(value: MetricValue) -> Payload {
    Payload::new()
        .with_seq(0)
        .with_metric(Metric::new("m", value))
}

#[test]
fn dataset_roundtrips_all_cell_types_and_nulls() {
    let ds = DataSet::new(
        vec![
            "i8".into(),
            "i64".into(),
            "u32".into(),
            "f64".into(),
            "b".into(),
            "s".into(),
            "ts".into(),
        ],
        vec![
            DataType::Int8,
            DataType::Int64,
            DataType::UInt32,
            DataType::Double,
            DataType::Boolean,
            DataType::String,
            DataType::DateTime,
        ],
        vec![
            vec![
                DataSetValue::Int8(-5),
                DataSetValue::Int64(i64::MIN),
                DataSetValue::UInt32(7),
                DataSetValue::Double(1.5),
                DataSetValue::Boolean(true),
                DataSetValue::String("row0".into()),
                DataSetValue::DateTime(1_700_000_000_000),
            ],
            vec![
                DataSetValue::Int8(0),
                DataSetValue::Null, // a null cell
                DataSetValue::UInt32(0),
                DataSetValue::Double(-0.25),
                DataSetValue::Boolean(false),
                DataSetValue::String(String::new()),
                DataSetValue::DateTime(0),
            ],
        ],
    )
    .expect("valid dataset");
    let p = one(MetricValue::DataSet(ds));
    assert_eq!(rt(&p), p);
}

#[test]
fn dataset_new_rejects_cell_type_not_matching_column() {
    // Int8 column with a UInt16 cell — both use protobuf field 1 (varint), so a
    // missing check would silently corrupt the value on decode.
    let bad = DataSet::new(
        vec!["x".into()],
        vec![DataType::Int8],
        vec![vec![DataSetValue::UInt16(300)]],
    );
    assert!(bad.is_err(), "mismatched cell type must be rejected");
}

#[test]
fn dataset_new_rejects_non_basic_column_type() {
    let bad = DataSet::new(vec!["x".into()], vec![DataType::DataSet], vec![]);
    assert!(bad.is_err());
}

#[test]
fn propertyset_roundtrips_including_nested_and_list_and_null() {
    let inner = PropertySet::new()
        .with("lo", PropertyValue::Double(0.0))
        .with("hi", PropertyValue::Double(100.0));
    let list = PropertySetList {
        sets: vec![
            PropertySet::new().with("a", PropertyValue::Int32(1)),
            PropertySet::new().with("b", PropertyValue::Boolean(false)),
        ],
    };
    let props = PropertySet::new()
        .with("engUnit", PropertyValue::String("kPa".into()))
        .with("Quality", PropertyValue::Int32(192))
        .with("limits", PropertyValue::PropertySet(inner))
        .with("history", PropertyValue::PropertySetList(list))
        .with("missing", PropertyValue::Null(DataType::Double));

    let p = Payload::new()
        .with_seq(0)
        .with_metric(Metric::new("pressure", MetricValue::Double(42.0)).with_properties(props));
    assert_eq!(rt(&p), p);
}

#[test]
fn template_definition_and_instance_roundtrip() {
    let definition = Template {
        version: Some("1.0".into()),
        template_ref: None,
        is_definition: true,
        metrics: vec![
            Metric::new("rpm", MetricValue::Int32(0)),
            Metric::new("running", MetricValue::Boolean(false)),
        ],
        parameters: vec![
            Parameter {
                name: "poles".into(),
                datatype: DataType::Int32,
                value: Some(ParameterValue::Int32(4)),
            },
            Parameter {
                name: "label".into(),
                datatype: DataType::String,
                value: None, // a definition may omit the value
            },
        ],
    };
    let instance = Template {
        version: Some("1.0".into()),
        template_ref: Some("Motor".into()),
        is_definition: false,
        metrics: vec![
            Metric::new("rpm", MetricValue::Int32(1500)),
            Metric::new("running", MetricValue::Boolean(true)),
        ],
        parameters: vec![Parameter {
            name: "poles".into(),
            datatype: DataType::Int32,
            value: Some(ParameterValue::Int32(6)),
        }],
    };
    let p = Payload::new()
        .with_seq(0)
        .with_metric(Metric::new(
            "Motor",
            MetricValue::Template(Box::new(definition)),
        ))
        .with_metric(Metric::new(
            "Motor/1",
            MetricValue::Template(Box::new(instance)),
        ));
    assert_eq!(rt(&p), p);
}

#[test]
fn metadata_and_file_roundtrip() {
    let md = MetaData {
        is_multi_part: Some(true),
        content_type: Some("application/pdf".into()),
        size: Some(1234),
        seq: Some(2),
        file_name: Some("report.pdf".into()),
        file_type: Some("pdf".into()),
        md5: Some("d41d8cd98f00b204e9800998ecf8427e".into()),
        description: Some("a file".into()),
    };
    let mut metric = Metric::new("doc", MetricValue::File(Bytes::from_static(b"%PDF-1.7")));
    metric.metadata = Some(md);
    let p = Payload::new().with_seq(0).with_metric(metric);
    assert_eq!(rt(&p), p);
}

#[test]
fn string_text_uuid_disambiguated_by_datatype() {
    // All three use protobuf field 15 (string); the datatype field disambiguates.
    let p = Payload::new()
        .with_seq(0)
        .with_metric(Metric::new("s", MetricValue::String("x".into())))
        .with_metric(Metric::new("t", MetricValue::Text("x".into())))
        .with_metric(Metric::new(
            "u",
            MetricValue::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
        ));
    let decoded = rt(&p);
    assert!(matches!(decoded.metrics[0].value, MetricValue::String(_)));
    assert!(matches!(decoded.metrics[1].value, MetricValue::Text(_)));
    assert!(matches!(decoded.metrics[2].value, MetricValue::Uuid(_)));
    assert_eq!(decoded, p);
}

#[test]
fn metric_value_null_constructor_rejects_invalid_datatypes() {
    assert!(MetricValue::null(DataType::Double).is_ok());
    assert!(MetricValue::null(DataType::Int32Array).is_ok());
    assert!(MetricValue::null(DataType::Unknown).is_err());
    assert!(MetricValue::null(DataType::PropertySet).is_err());
    assert!(MetricValue::null(DataType::PropertySetList).is_err());
}

#[test]
fn alias_registry_try_bind_rejects_duplicate_alias() {
    let mut reg = AliasRegistry::new();
    assert!(reg.try_bind("a", Some(5), DataType::Int32).is_ok());
    // Same alias, different name -> fatal.
    assert!(reg.try_bind("b", Some(5), DataType::Int32).is_err());
    // Same alias, same name -> fine (idempotent rebind).
    assert!(reg.try_bind("a", Some(5), DataType::Int32).is_ok());
    // A fresh alias -> fine.
    assert!(reg.try_bind("c", Some(6), DataType::Boolean).is_ok());
    assert_eq!(reg.name_for_alias(5), Some("a"));
}
