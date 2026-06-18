//! Domain model → Sparkplug B protobuf bytes.

use bytes::Bytes;

use super::arrays;
use super::wire::Writer;
use crate::model::{
    DataSet, MetaData, Metric, Parameter, Payload, PropertySet, PropertySetList, Template,
};
use crate::value::{DataSetValue, MetricValue, ParameterValue, PropertyValue};

/// Encode a full payload. When `strip` is true, per-metric datatype fields are
/// omitted (DATA/CMD messages); decoding then requires a birth-derived registry.
pub(crate) fn encode_payload(payload: &Payload, strip: bool) -> Bytes {
    let mut w = Writer::new();
    if let Some(ts) = payload.timestamp {
        w.uint64_field(1, ts);
    }
    for metric in &payload.metrics {
        let body = encode_metric(metric, strip);
        w.message_field(2, &body);
    }
    if let Some(seq) = payload.seq {
        w.uint64_field(3, u64::from(seq));
    }
    if let Some(uuid) = &payload.uuid {
        w.string_field(4, uuid);
    }
    if let Some(body) = &payload.body {
        w.bytes_field(5, body);
    }
    w.into_bytes()
}

fn encode_metric(metric: &Metric, strip: bool) -> Bytes {
    let mut w = Writer::new();
    if let Some(name) = &metric.name {
        w.string_field(1, name);
    }
    if let Some(alias) = metric.alias {
        w.uint64_field(2, alias);
    }
    if let Some(ts) = metric.timestamp {
        w.uint64_field(3, ts);
    }
    if !strip {
        w.uint32_field(4, metric.value.datatype().as_u32());
    }
    if let Some(h) = metric.is_historical {
        w.bool_field(5, h);
    }
    if let Some(t) = metric.is_transient {
        w.bool_field(6, t);
    }
    if metric.value.is_null() {
        w.bool_field(7, true);
    }
    if let Some(md) = &metric.metadata {
        let body = encode_metadata(md);
        w.message_field(8, &body);
    }
    if let Some(props) = &metric.properties {
        let body = encode_propertyset(props);
        w.message_field(9, &body);
    }
    if !metric.value.is_null() {
        encode_metric_value(&mut w, &metric.value, strip);
    }
    w.into_bytes()
}

fn encode_metric_value(w: &mut Writer, value: &MetricValue, strip: bool) {
    match value {
        MetricValue::Int8(v) => w.uint32_field(10, i32::from(*v) as u32),
        MetricValue::Int16(v) => w.uint32_field(10, i32::from(*v) as u32),
        MetricValue::Int32(v) => w.uint32_field(10, *v as u32),
        MetricValue::UInt8(v) => w.uint32_field(10, u32::from(*v)),
        MetricValue::UInt16(v) => w.uint32_field(10, u32::from(*v)),
        MetricValue::UInt32(v) => w.uint32_field(10, *v),
        MetricValue::Int64(v) => w.uint64_field(11, *v as u64),
        MetricValue::UInt64(v) => w.uint64_field(11, *v),
        MetricValue::DateTime(v) => w.uint64_field(11, *v as u64),
        MetricValue::Float(v) => w.float_field(12, *v),
        MetricValue::Double(v) => w.double_field(13, *v),
        MetricValue::Boolean(v) => w.bool_field(14, *v),
        MetricValue::String(s) | MetricValue::Text(s) | MetricValue::Uuid(s) => {
            w.string_field(15, s);
        }
        MetricValue::Bytes(b) | MetricValue::File(b) => w.bytes_field(16, b),
        MetricValue::DataSet(ds) => {
            let body = encode_dataset(ds);
            w.message_field(17, &body);
        }
        MetricValue::Template(t) => {
            let body = encode_template(t, strip);
            w.message_field(18, &body);
        }
        // All array variants pack into the bytes_value (field 16).
        MetricValue::Int8Array(_)
        | MetricValue::Int16Array(_)
        | MetricValue::Int32Array(_)
        | MetricValue::Int64Array(_)
        | MetricValue::UInt8Array(_)
        | MetricValue::UInt16Array(_)
        | MetricValue::UInt32Array(_)
        | MetricValue::UInt64Array(_)
        | MetricValue::FloatArray(_)
        | MetricValue::DoubleArray(_)
        | MetricValue::BooleanArray(_)
        | MetricValue::StringArray(_)
        | MetricValue::DateTimeArray(_) => {
            let packed = arrays::pack(value);
            w.bytes_field(16, &packed);
        }
        MetricValue::Null(_) => {} // handled by caller via is_null
    }
}

fn encode_metadata(md: &MetaData) -> Bytes {
    let mut w = Writer::new();
    if let Some(v) = md.is_multi_part {
        w.bool_field(1, v);
    }
    if let Some(v) = &md.content_type {
        w.string_field(2, v);
    }
    if let Some(v) = md.size {
        w.uint64_field(3, v);
    }
    if let Some(v) = md.seq {
        w.uint64_field(4, v);
    }
    if let Some(v) = &md.file_name {
        w.string_field(5, v);
    }
    if let Some(v) = &md.file_type {
        w.string_field(6, v);
    }
    if let Some(v) = &md.md5 {
        w.string_field(7, v);
    }
    if let Some(v) = &md.description {
        w.string_field(8, v);
    }
    w.into_bytes()
}

fn encode_propertyset(ps: &PropertySet) -> Bytes {
    let mut w = Writer::new();
    for (key, _) in &ps.entries {
        w.string_field(1, key);
    }
    for (_, value) in &ps.entries {
        let body = encode_propertyvalue(value);
        w.message_field(2, &body);
    }
    w.into_bytes()
}

fn encode_propertyvalue(value: &PropertyValue) -> Bytes {
    let mut w = Writer::new();
    w.uint32_field(1, value.datatype().as_u32());
    match value {
        PropertyValue::Null(_) => w.bool_field(2, true),
        PropertyValue::Int8(v) => w.uint32_field(3, i32::from(*v) as u32),
        PropertyValue::Int16(v) => w.uint32_field(3, i32::from(*v) as u32),
        PropertyValue::Int32(v) => w.uint32_field(3, *v as u32),
        PropertyValue::UInt8(v) => w.uint32_field(3, u32::from(*v)),
        PropertyValue::UInt16(v) => w.uint32_field(3, u32::from(*v)),
        PropertyValue::UInt32(v) => w.uint32_field(3, *v),
        PropertyValue::Int64(v) => w.uint64_field(4, *v as u64),
        PropertyValue::UInt64(v) => w.uint64_field(4, *v),
        PropertyValue::DateTime(v) => w.uint64_field(4, *v as u64),
        PropertyValue::Float(v) => w.float_field(5, *v),
        PropertyValue::Double(v) => w.double_field(6, *v),
        PropertyValue::Boolean(v) => w.bool_field(7, *v),
        PropertyValue::String(s) | PropertyValue::Text(s) => w.string_field(8, s),
        PropertyValue::PropertySet(ps) => {
            let body = encode_propertyset(ps);
            w.message_field(9, &body);
        }
        PropertyValue::PropertySetList(list) => {
            let body = encode_propertysetlist(list);
            w.message_field(10, &body);
        }
    }
    w.into_bytes()
}

fn encode_propertysetlist(list: &PropertySetList) -> Bytes {
    let mut w = Writer::new();
    for set in &list.sets {
        let body = encode_propertyset(set);
        w.message_field(1, &body);
    }
    w.into_bytes()
}

fn encode_dataset(ds: &DataSet) -> Bytes {
    let mut w = Writer::new();
    w.uint64_field(1, ds.num_of_columns());
    for col in ds.columns() {
        w.string_field(2, col);
    }
    for ty in ds.types() {
        w.uint32_field(3, ty.as_u32());
    }
    for row in ds.rows() {
        let body = encode_row(row);
        w.message_field(4, &body);
    }
    w.into_bytes()
}

fn encode_row(row: &[DataSetValue]) -> Bytes {
    let mut w = Writer::new();
    for cell in row {
        let body = encode_datasetvalue(cell);
        w.message_field(1, &body);
    }
    w.into_bytes()
}

fn encode_datasetvalue(value: &DataSetValue) -> Bytes {
    let mut w = Writer::new();
    match value {
        DataSetValue::Null => {}
        DataSetValue::Int8(v) => w.uint32_field(1, i32::from(*v) as u32),
        DataSetValue::Int16(v) => w.uint32_field(1, i32::from(*v) as u32),
        DataSetValue::Int32(v) => w.uint32_field(1, *v as u32),
        DataSetValue::UInt8(v) => w.uint32_field(1, u32::from(*v)),
        DataSetValue::UInt16(v) => w.uint32_field(1, u32::from(*v)),
        DataSetValue::UInt32(v) => w.uint32_field(1, *v),
        DataSetValue::Int64(v) => w.uint64_field(2, *v as u64),
        DataSetValue::UInt64(v) => w.uint64_field(2, *v),
        DataSetValue::DateTime(v) => w.uint64_field(2, *v as u64),
        DataSetValue::Float(v) => w.float_field(3, *v),
        DataSetValue::Double(v) => w.double_field(4, *v),
        DataSetValue::Boolean(v) => w.bool_field(5, *v),
        DataSetValue::String(s) | DataSetValue::Text(s) => w.string_field(6, s),
    }
    w.into_bytes()
}

fn encode_template(t: &Template, strip: bool) -> Bytes {
    let mut w = Writer::new();
    if let Some(v) = &t.version {
        w.string_field(1, v);
    }
    for metric in &t.metrics {
        let body = encode_metric(metric, strip);
        w.message_field(2, &body);
    }
    for param in &t.parameters {
        let body = encode_parameter(param);
        w.message_field(3, &body);
    }
    if let Some(r) = &t.template_ref {
        w.string_field(4, r);
    }
    w.bool_field(5, t.is_definition);
    w.into_bytes()
}

fn encode_parameter(p: &Parameter) -> Bytes {
    let mut w = Writer::new();
    w.string_field(1, &p.name);
    w.uint32_field(2, p.datatype.as_u32());
    match &p.value {
        None => {}
        Some(ParameterValue::Int8(v)) => w.uint32_field(3, i32::from(*v) as u32),
        Some(ParameterValue::Int16(v)) => w.uint32_field(3, i32::from(*v) as u32),
        Some(ParameterValue::Int32(v)) => w.uint32_field(3, *v as u32),
        Some(ParameterValue::UInt8(v)) => w.uint32_field(3, u32::from(*v)),
        Some(ParameterValue::UInt16(v)) => w.uint32_field(3, u32::from(*v)),
        Some(ParameterValue::UInt32(v)) => w.uint32_field(3, *v),
        Some(ParameterValue::Int64(v)) => w.uint64_field(4, *v as u64),
        Some(ParameterValue::UInt64(v)) => w.uint64_field(4, *v),
        Some(ParameterValue::DateTime(v)) => w.uint64_field(4, *v as u64),
        Some(ParameterValue::Float(v)) => w.float_field(5, *v),
        Some(ParameterValue::Double(v)) => w.double_field(6, *v),
        Some(ParameterValue::Boolean(v)) => w.bool_field(7, *v),
        Some(ParameterValue::String(s)) | Some(ParameterValue::Text(s)) => w.string_field(8, s),
    }
    w.into_bytes()
}
