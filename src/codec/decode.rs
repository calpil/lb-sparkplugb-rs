//! Sparkplug B protobuf bytes → domain model.
//!
//! Never panics on hostile input: truncation, bad tags, unknown datatypes, and
//! over-deep nesting all return typed [`SparkplugError`]s.

use bytes::Bytes;

use super::arrays;
use super::wire::{Reader, WIRE_I32, WIRE_I64, WIRE_LEN, WIRE_VARINT};
use crate::alias::AliasRegistry;
use crate::datatype::DataType;
use crate::error::{Result, SparkplugError};
use crate::model::{
    DataSet, MetaData, Metric, Parameter, Payload, PropertySet, PropertySetList, Template,
};
use crate::value::{DataSetValue, MetricValue, ParameterValue, PropertyValue};

/// Maximum nesting depth (templates / property sets) before bailing out.
const MAX_DEPTH: usize = 32;

/// Decode a Sparkplug B payload.
pub(crate) fn decode_payload(bytes: &[u8], types: Option<&AliasRegistry>) -> Result<Payload> {
    let mut r = Reader::new(bytes);
    let mut payload = Payload::default();
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_VARINT) => payload.timestamp = Some(r.read_varint()?),
            (2, WIRE_LEN) => {
                let body = r.read_len_slice()?;
                payload.metrics.push(decode_metric(body, types, 0)?);
            }
            (3, WIRE_VARINT) => payload.seq = Some(r.read_varint()? as u8),
            (4, WIRE_LEN) => payload.uuid = Some(r.read_string()?),
            (5, WIRE_LEN) => payload.body = Some(r.read_bytes()?),
            _ => r.skip(wire)?,
        }
    }
    Ok(payload)
}

/// The raw value-oneof slots collected from a metric before we know the datatype.
#[derive(Default)]
struct RawValue<'a> {
    int_v: Option<u32>,
    long_v: Option<u64>,
    float_v: Option<f32>,
    double_v: Option<f64>,
    bool_v: Option<bool>,
    string_v: Option<String>,
    bytes_v: Option<Bytes>,
    dataset_v: Option<&'a [u8]>,
    template_v: Option<&'a [u8]>,
}

fn mismatch(msg: impl Into<String>) -> SparkplugError {
    SparkplugError::ValueTypeMismatch(msg.into())
}

fn decode_metric(bytes: &[u8], types: Option<&AliasRegistry>, depth: usize) -> Result<Metric> {
    if depth > MAX_DEPTH {
        return Err(SparkplugError::RecursionLimit);
    }
    let mut r = Reader::new(bytes);
    let mut name = None;
    let mut alias = None;
    let mut timestamp = None;
    let mut datatype: Option<u32> = None;
    let mut is_historical = None;
    let mut is_transient = None;
    let mut is_null = false;
    let mut metadata = None;
    let mut properties = None;
    let mut raw = RawValue::default();

    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_LEN) => name = Some(r.read_string()?),
            (2, WIRE_VARINT) => alias = Some(r.read_varint()?),
            (3, WIRE_VARINT) => timestamp = Some(r.read_varint()?),
            (4, WIRE_VARINT) => datatype = Some(r.read_varint()? as u32),
            (5, WIRE_VARINT) => is_historical = Some(r.read_varint()? != 0),
            (6, WIRE_VARINT) => is_transient = Some(r.read_varint()? != 0),
            (7, WIRE_VARINT) => is_null = r.read_varint()? != 0,
            (8, WIRE_LEN) => metadata = Some(decode_metadata(r.read_len_slice()?)?),
            (9, WIRE_LEN) => properties = Some(decode_propertyset(r.read_len_slice()?, depth + 1)?),
            (10, WIRE_VARINT) => raw.int_v = Some(r.read_varint()? as u32),
            (11, WIRE_VARINT) => raw.long_v = Some(r.read_varint()?),
            (12, WIRE_I32) => raw.float_v = Some(r.read_f32()?),
            (13, WIRE_I64) => raw.double_v = Some(r.read_f64()?),
            (14, WIRE_VARINT) => raw.bool_v = Some(r.read_varint()? != 0),
            (15, WIRE_LEN) => raw.string_v = Some(r.read_string()?),
            (16, WIRE_LEN) => raw.bytes_v = Some(r.read_bytes()?),
            (17, WIRE_LEN) => raw.dataset_v = Some(r.read_len_slice()?),
            (18, WIRE_LEN) => raw.template_v = Some(r.read_len_slice()?),
            _ => r.skip(wire)?,
        }
    }

    let dt = match datatype {
        Some(code) => DataType::from_u32(code)?,
        None => recover_datatype(types, name.as_deref(), alias)
            .ok_or_else(|| SparkplugError::MissingDataType(describe(name.as_deref(), alias)))?,
    };

    let value = if is_null {
        MetricValue::Null(dt)
    } else {
        build_metric_value(dt, raw, types, depth)?
    };

    Ok(Metric {
        name,
        alias,
        timestamp,
        value,
        is_historical,
        is_transient,
        metadata,
        properties,
    })
}

fn recover_datatype(
    types: Option<&AliasRegistry>,
    name: Option<&str>,
    alias: Option<u64>,
) -> Option<DataType> {
    let reg = types?;
    if let Some(n) = name {
        reg.datatype_for_name(n)
    } else if let Some(a) = alias {
        reg.datatype_for_alias(a)
    } else {
        None
    }
}

fn describe(name: Option<&str>, alias: Option<u64>) -> String {
    match (name, alias) {
        (Some(n), _) => format!("name {n:?}"),
        (None, Some(a)) => format!("alias {a}"),
        (None, None) => "anonymous metric".to_owned(),
    }
}

fn build_metric_value(
    dt: DataType,
    raw: RawValue<'_>,
    types: Option<&AliasRegistry>,
    depth: usize,
) -> Result<MetricValue> {
    let int_v = || {
        raw.int_v
            .ok_or_else(|| mismatch(format!("{dt:?} expected int_value")))
    };
    let long_v = || {
        raw.long_v
            .ok_or_else(|| mismatch(format!("{dt:?} expected long_value")))
    };
    let value = match dt {
        DataType::Int8 => MetricValue::Int8(int_v()? as i8),
        DataType::Int16 => MetricValue::Int16(int_v()? as i16),
        DataType::Int32 => MetricValue::Int32(int_v()? as i32),
        DataType::UInt8 => MetricValue::UInt8(int_v()? as u8),
        DataType::UInt16 => MetricValue::UInt16(int_v()? as u16),
        DataType::UInt32 => MetricValue::UInt32(int_v()?),
        DataType::Int64 => MetricValue::Int64(long_v()? as i64),
        DataType::UInt64 => MetricValue::UInt64(long_v()?),
        DataType::DateTime => MetricValue::DateTime(long_v()? as i64),
        DataType::Float => MetricValue::Float(
            raw.float_v
                .ok_or_else(|| mismatch("Float expected float_value"))?,
        ),
        DataType::Double => MetricValue::Double(
            raw.double_v
                .ok_or_else(|| mismatch("Double expected double_value"))?,
        ),
        DataType::Boolean => MetricValue::Boolean(
            raw.bool_v
                .ok_or_else(|| mismatch("Boolean expected boolean_value"))?,
        ),
        DataType::String => MetricValue::String(string(raw.string_v, dt)?),
        DataType::Text => MetricValue::Text(string(raw.string_v, dt)?),
        DataType::Uuid => MetricValue::Uuid(string(raw.string_v, dt)?),
        DataType::Bytes => MetricValue::Bytes(bytes(raw.bytes_v, dt)?),
        DataType::File => MetricValue::File(bytes(raw.bytes_v, dt)?),
        DataType::DataSet => {
            let slice = raw
                .dataset_v
                .ok_or_else(|| mismatch("DataSet expected dataset_value"))?;
            MetricValue::DataSet(decode_dataset(slice)?)
        }
        DataType::Template => {
            let slice = raw
                .template_v
                .ok_or_else(|| mismatch("Template expected template_value"))?;
            MetricValue::Template(Box::new(decode_template(slice, types, depth + 1)?))
        }
        dt if dt.is_array() => {
            let slice = raw
                .bytes_v
                .ok_or_else(|| mismatch(format!("{dt:?} expected bytes_value")))?;
            arrays::unpack(dt, &slice)?
        }
        DataType::Unknown | DataType::PropertySet | DataType::PropertySetList => {
            return Err(mismatch(format!("{dt:?} is not a valid metric value type")));
        }
        // Unreachable: all remaining variants are handled above; kept for exhaustiveness.
        other => return Err(mismatch(format!("unhandled datatype {other:?}"))),
    };
    Ok(value)
}

fn string(value: Option<String>, dt: DataType) -> Result<String> {
    value.ok_or_else(|| mismatch(format!("{dt:?} expected string_value")))
}

fn bytes(value: Option<Bytes>, dt: DataType) -> Result<Bytes> {
    value.ok_or_else(|| mismatch(format!("{dt:?} expected bytes_value")))
}

fn decode_metadata(bytes: &[u8]) -> Result<MetaData> {
    let mut r = Reader::new(bytes);
    let mut md = MetaData::default();
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_VARINT) => md.is_multi_part = Some(r.read_varint()? != 0),
            (2, WIRE_LEN) => md.content_type = Some(r.read_string()?),
            (3, WIRE_VARINT) => md.size = Some(r.read_varint()?),
            (4, WIRE_VARINT) => md.seq = Some(r.read_varint()?),
            (5, WIRE_LEN) => md.file_name = Some(r.read_string()?),
            (6, WIRE_LEN) => md.file_type = Some(r.read_string()?),
            (7, WIRE_LEN) => md.md5 = Some(r.read_string()?),
            (8, WIRE_LEN) => md.description = Some(r.read_string()?),
            _ => r.skip(wire)?,
        }
    }
    Ok(md)
}

fn decode_propertyset(bytes: &[u8], depth: usize) -> Result<PropertySet> {
    if depth > MAX_DEPTH {
        return Err(SparkplugError::RecursionLimit);
    }
    let mut r = Reader::new(bytes);
    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<PropertyValue> = Vec::new();
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_LEN) => keys.push(r.read_string()?),
            (2, WIRE_LEN) => values.push(decode_propertyvalue(r.read_len_slice()?, depth + 1)?),
            _ => r.skip(wire)?,
        }
    }
    if keys.len() != values.len() {
        return Err(mismatch(format!(
            "PropertySet has {} keys but {} values",
            keys.len(),
            values.len()
        )));
    }
    Ok(PropertySet {
        entries: keys.into_iter().zip(values).collect(),
    })
}

fn decode_propertyvalue(bytes: &[u8], depth: usize) -> Result<PropertyValue> {
    if depth > MAX_DEPTH {
        return Err(SparkplugError::RecursionLimit);
    }
    let mut r = Reader::new(bytes);
    let mut type_code: Option<u32> = None;
    let mut is_null = false;
    let mut int_v = None;
    let mut long_v = None;
    let mut float_v = None;
    let mut double_v = None;
    let mut bool_v = None;
    let mut string_v = None;
    let mut ps_slice = None;
    let mut psl_slice = None;
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_VARINT) => type_code = Some(r.read_varint()? as u32),
            (2, WIRE_VARINT) => is_null = r.read_varint()? != 0,
            (3, WIRE_VARINT) => int_v = Some(r.read_varint()? as u32),
            (4, WIRE_VARINT) => long_v = Some(r.read_varint()?),
            (5, WIRE_I32) => float_v = Some(r.read_f32()?),
            (6, WIRE_I64) => double_v = Some(r.read_f64()?),
            (7, WIRE_VARINT) => bool_v = Some(r.read_varint()? != 0),
            (8, WIRE_LEN) => string_v = Some(r.read_string()?),
            (9, WIRE_LEN) => ps_slice = Some(r.read_len_slice()?),
            (10, WIRE_LEN) => psl_slice = Some(r.read_len_slice()?),
            _ => r.skip(wire)?,
        }
    }
    let dt = DataType::from_u32(type_code.ok_or_else(|| mismatch("PropertyValue missing type"))?)?;
    if is_null {
        return Ok(PropertyValue::Null(dt));
    }
    let value = match dt {
        DataType::Int8 => PropertyValue::Int8(int_v.ok_or_else(|| mismatch("Int8"))? as i8),
        DataType::Int16 => PropertyValue::Int16(int_v.ok_or_else(|| mismatch("Int16"))? as i16),
        DataType::Int32 => PropertyValue::Int32(int_v.ok_or_else(|| mismatch("Int32"))? as i32),
        DataType::UInt8 => PropertyValue::UInt8(int_v.ok_or_else(|| mismatch("UInt8"))? as u8),
        DataType::UInt16 => PropertyValue::UInt16(int_v.ok_or_else(|| mismatch("UInt16"))? as u16),
        DataType::UInt32 => PropertyValue::UInt32(int_v.ok_or_else(|| mismatch("UInt32"))?),
        DataType::Int64 => PropertyValue::Int64(long_v.ok_or_else(|| mismatch("Int64"))? as i64),
        DataType::UInt64 => PropertyValue::UInt64(long_v.ok_or_else(|| mismatch("UInt64"))?),
        DataType::DateTime => {
            PropertyValue::DateTime(long_v.ok_or_else(|| mismatch("DateTime"))? as i64)
        }
        DataType::Float => PropertyValue::Float(float_v.ok_or_else(|| mismatch("Float"))?),
        DataType::Double => PropertyValue::Double(double_v.ok_or_else(|| mismatch("Double"))?),
        DataType::Boolean => PropertyValue::Boolean(bool_v.ok_or_else(|| mismatch("Boolean"))?),
        DataType::String => PropertyValue::String(string_v.ok_or_else(|| mismatch("String"))?),
        DataType::Text => PropertyValue::Text(string_v.ok_or_else(|| mismatch("Text"))?),
        DataType::PropertySet => {
            let slice = ps_slice.ok_or_else(|| mismatch("PropertySet value missing"))?;
            PropertyValue::PropertySet(decode_propertyset(slice, depth + 1)?)
        }
        DataType::PropertySetList => {
            let slice = psl_slice.ok_or_else(|| mismatch("PropertySetList value missing"))?;
            PropertyValue::PropertySetList(decode_propertysetlist(slice, depth + 1)?)
        }
        other => {
            return Err(mismatch(format!(
                "{other:?} is not a valid property value type"
            )));
        }
    };
    Ok(value)
}

fn decode_propertysetlist(bytes: &[u8], depth: usize) -> Result<PropertySetList> {
    if depth > MAX_DEPTH {
        return Err(SparkplugError::RecursionLimit);
    }
    let mut r = Reader::new(bytes);
    let mut sets = Vec::new();
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_LEN) => sets.push(decode_propertyset(r.read_len_slice()?, depth + 1)?),
            _ => r.skip(wire)?,
        }
    }
    Ok(PropertySetList { sets })
}

fn decode_dataset(bytes: &[u8]) -> Result<DataSet> {
    let mut r = Reader::new(bytes);
    let mut columns: Vec<String> = Vec::new();
    let mut type_codes: Vec<u32> = Vec::new();
    let mut row_slices: Vec<&[u8]> = Vec::new();
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_VARINT) => {
                r.read_varint()?; // num_of_columns — derived from columns.len() instead
            }
            (2, WIRE_LEN) => columns.push(r.read_string()?),
            (3, WIRE_VARINT) => type_codes.push(r.read_varint()? as u32),
            (3, WIRE_LEN) => {
                // Packed repeated `types` (proto3-style) — tolerate it.
                let mut sub = Reader::new(r.read_len_slice()?);
                while !sub.is_empty() {
                    type_codes.push(sub.read_varint()? as u32);
                }
            }
            (4, WIRE_LEN) => row_slices.push(r.read_len_slice()?),
            _ => r.skip(wire)?,
        }
    }
    let types = type_codes
        .iter()
        .map(|&c| DataType::from_u32(c))
        .collect::<Result<Vec<_>>>()?;
    let mut rows = Vec::with_capacity(row_slices.len());
    for slice in row_slices {
        rows.push(decode_row(slice, &types)?);
    }
    DataSet::new(columns, types, rows)
}

fn decode_row(bytes: &[u8], types: &[DataType]) -> Result<Vec<DataSetValue>> {
    let mut r = Reader::new(bytes);
    let mut cells = Vec::new();
    let mut idx = 0usize;
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_LEN) => {
                let slice = r.read_len_slice()?;
                let dt = types.get(idx).copied().ok_or_else(|| {
                    SparkplugError::DataSetShape("row wider than column types".to_owned())
                })?;
                cells.push(decode_datasetvalue(slice, dt)?);
                idx += 1;
            }
            _ => r.skip(wire)?,
        }
    }
    Ok(cells)
}

fn decode_datasetvalue(bytes: &[u8], dt: DataType) -> Result<DataSetValue> {
    let mut r = Reader::new(bytes);
    let mut int_v = None;
    let mut long_v = None;
    let mut float_v = None;
    let mut double_v = None;
    let mut bool_v = None;
    let mut string_v = None;
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_VARINT) => int_v = Some(r.read_varint()? as u32),
            (2, WIRE_VARINT) => long_v = Some(r.read_varint()?),
            (3, WIRE_I32) => float_v = Some(r.read_f32()?),
            (4, WIRE_I64) => double_v = Some(r.read_f64()?),
            (5, WIRE_VARINT) => bool_v = Some(r.read_varint()? != 0),
            (6, WIRE_LEN) => string_v = Some(r.read_string()?),
            _ => r.skip(wire)?,
        }
    }
    let any = int_v.is_some()
        || long_v.is_some()
        || float_v.is_some()
        || double_v.is_some()
        || bool_v.is_some()
        || string_v.is_some();
    if !any {
        return Ok(DataSetValue::Null);
    }
    let value = match dt {
        DataType::Int8 => DataSetValue::Int8(int_v.ok_or_else(|| mismatch("Int8 cell"))? as i8),
        DataType::Int16 => DataSetValue::Int16(int_v.ok_or_else(|| mismatch("Int16 cell"))? as i16),
        DataType::Int32 => DataSetValue::Int32(int_v.ok_or_else(|| mismatch("Int32 cell"))? as i32),
        DataType::UInt8 => DataSetValue::UInt8(int_v.ok_or_else(|| mismatch("UInt8 cell"))? as u8),
        DataType::UInt16 => {
            DataSetValue::UInt16(int_v.ok_or_else(|| mismatch("UInt16 cell"))? as u16)
        }
        DataType::UInt32 => DataSetValue::UInt32(int_v.ok_or_else(|| mismatch("UInt32 cell"))?),
        DataType::Int64 => {
            DataSetValue::Int64(long_v.ok_or_else(|| mismatch("Int64 cell"))? as i64)
        }
        DataType::UInt64 => DataSetValue::UInt64(long_v.ok_or_else(|| mismatch("UInt64 cell"))?),
        DataType::DateTime => {
            DataSetValue::DateTime(long_v.ok_or_else(|| mismatch("DateTime cell"))? as i64)
        }
        DataType::Float => DataSetValue::Float(float_v.ok_or_else(|| mismatch("Float cell"))?),
        DataType::Double => DataSetValue::Double(double_v.ok_or_else(|| mismatch("Double cell"))?),
        DataType::Boolean => DataSetValue::Boolean(bool_v.ok_or_else(|| mismatch("Boolean cell"))?),
        DataType::String => DataSetValue::String(string_v.ok_or_else(|| mismatch("String cell"))?),
        DataType::Text => DataSetValue::Text(string_v.ok_or_else(|| mismatch("Text cell"))?),
        other => {
            return Err(SparkplugError::DataSetShape(format!(
                "{other:?} is not a valid DataSet column type"
            )));
        }
    };
    Ok(value)
}

fn decode_template(bytes: &[u8], types: Option<&AliasRegistry>, depth: usize) -> Result<Template> {
    if depth > MAX_DEPTH {
        return Err(SparkplugError::RecursionLimit);
    }
    let mut r = Reader::new(bytes);
    let mut t = Template::default();
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_LEN) => t.version = Some(r.read_string()?),
            (2, WIRE_LEN) => t
                .metrics
                .push(decode_metric(r.read_len_slice()?, types, depth + 1)?),
            (3, WIRE_LEN) => t.parameters.push(decode_parameter(r.read_len_slice()?)?),
            (4, WIRE_LEN) => t.template_ref = Some(r.read_string()?),
            (5, WIRE_VARINT) => t.is_definition = r.read_varint()? != 0,
            _ => r.skip(wire)?,
        }
    }
    Ok(t)
}

fn decode_parameter(bytes: &[u8]) -> Result<Parameter> {
    let mut r = Reader::new(bytes);
    let mut name = None;
    let mut type_code: Option<u32> = None;
    let mut int_v = None;
    let mut long_v = None;
    let mut float_v = None;
    let mut double_v = None;
    let mut bool_v = None;
    let mut string_v = None;
    while !r.is_empty() {
        let (field, wire) = r.read_tag()?;
        match (field, wire) {
            (1, WIRE_LEN) => name = Some(r.read_string()?),
            (2, WIRE_VARINT) => type_code = Some(r.read_varint()? as u32),
            (3, WIRE_VARINT) => int_v = Some(r.read_varint()? as u32),
            (4, WIRE_VARINT) => long_v = Some(r.read_varint()?),
            (5, WIRE_I32) => float_v = Some(r.read_f32()?),
            (6, WIRE_I64) => double_v = Some(r.read_f64()?),
            (7, WIRE_VARINT) => bool_v = Some(r.read_varint()? != 0),
            (8, WIRE_LEN) => string_v = Some(r.read_string()?),
            _ => r.skip(wire)?,
        }
    }
    let name = name.ok_or_else(|| mismatch("Parameter missing name"))?;
    let dt = DataType::from_u32(type_code.ok_or_else(|| mismatch("Parameter missing type"))?)?;
    let any = int_v.is_some()
        || long_v.is_some()
        || float_v.is_some()
        || double_v.is_some()
        || bool_v.is_some()
        || string_v.is_some();
    let value = if any {
        Some(build_parameter_value(
            dt, int_v, long_v, float_v, double_v, bool_v, string_v,
        )?)
    } else {
        None
    };
    Ok(Parameter {
        name,
        datatype: dt,
        value,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_parameter_value(
    dt: DataType,
    int_v: Option<u32>,
    long_v: Option<u64>,
    float_v: Option<f32>,
    double_v: Option<f64>,
    bool_v: Option<bool>,
    string_v: Option<String>,
) -> Result<ParameterValue> {
    let value = match dt {
        DataType::Int8 => ParameterValue::Int8(int_v.ok_or_else(|| mismatch("Int8 param"))? as i8),
        DataType::Int16 => {
            ParameterValue::Int16(int_v.ok_or_else(|| mismatch("Int16 param"))? as i16)
        }
        DataType::Int32 => {
            ParameterValue::Int32(int_v.ok_or_else(|| mismatch("Int32 param"))? as i32)
        }
        DataType::UInt8 => {
            ParameterValue::UInt8(int_v.ok_or_else(|| mismatch("UInt8 param"))? as u8)
        }
        DataType::UInt16 => {
            ParameterValue::UInt16(int_v.ok_or_else(|| mismatch("UInt16 param"))? as u16)
        }
        DataType::UInt32 => ParameterValue::UInt32(int_v.ok_or_else(|| mismatch("UInt32 param"))?),
        DataType::Int64 => {
            ParameterValue::Int64(long_v.ok_or_else(|| mismatch("Int64 param"))? as i64)
        }
        DataType::UInt64 => ParameterValue::UInt64(long_v.ok_or_else(|| mismatch("UInt64 param"))?),
        DataType::DateTime => {
            ParameterValue::DateTime(long_v.ok_or_else(|| mismatch("DateTime param"))? as i64)
        }
        DataType::Float => ParameterValue::Float(float_v.ok_or_else(|| mismatch("Float param"))?),
        DataType::Double => {
            ParameterValue::Double(double_v.ok_or_else(|| mismatch("Double param"))?)
        }
        DataType::Boolean => {
            ParameterValue::Boolean(bool_v.ok_or_else(|| mismatch("Boolean param"))?)
        }
        DataType::String => {
            ParameterValue::String(string_v.ok_or_else(|| mismatch("String param"))?)
        }
        DataType::Text => ParameterValue::Text(string_v.ok_or_else(|| mismatch("Text param"))?),
        other => {
            return Err(mismatch(format!("{other:?} is not a valid parameter type")));
        }
    };
    Ok(value)
}
