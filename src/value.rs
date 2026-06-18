//! Typed value enums — the heart of the “make illegal states unrepresentable”
//! design (ADR-2). A [`MetricValue`] fuses the Sparkplug datatype with its
//! payload, so the runtime `checkType` of the Java reference is unnecessary.

use bytes::Bytes;

use crate::datatype::DataType;
use crate::error::{Result, SparkplugError};
use crate::model::{DataSet, PropertySet, PropertySetList, Template};

/// The value carried by a [`crate::model::Metric`].
///
/// The active variant determines the on-wire [`DataType`]; the sole exception is
/// [`MetricValue::Null`], which carries its declared datatype explicitly (a null
/// metric still has a declared type, expressed on the wire as `is_null = true`).
#[derive(Clone, Debug, PartialEq)]
#[allow(missing_docs)] // variant names mirror the spec datatype names
pub enum MetricValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    Text(String),
    Uuid(String),
    /// Epoch milliseconds, UTC (`tck-id-payloads-timestamp-in-UTC`).
    DateTime(i64),
    Bytes(Bytes),
    File(Bytes),
    DataSet(DataSet),
    Template(Box<Template>),
    Int8Array(Vec<i8>),
    Int16Array(Vec<i16>),
    Int32Array(Vec<i32>),
    Int64Array(Vec<i64>),
    UInt8Array(Vec<u8>),
    UInt16Array(Vec<u16>),
    UInt32Array(Vec<u32>),
    UInt64Array(Vec<u64>),
    FloatArray(Vec<f32>),
    DoubleArray(Vec<f64>),
    BooleanArray(Vec<bool>),
    StringArray(Vec<String>),
    DateTimeArray(Vec<i64>),
    /// A null value with its declared datatype (encoded as `is_null = true`).
    Null(DataType),
}

impl MetricValue {
    /// The Sparkplug [`DataType`] this value declares on the wire.
    #[must_use]
    pub fn datatype(&self) -> DataType {
        match self {
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt8(_) => DataType::UInt8,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float(_) => DataType::Float,
            Self::Double(_) => DataType::Double,
            Self::Boolean(_) => DataType::Boolean,
            Self::String(_) => DataType::String,
            Self::Text(_) => DataType::Text,
            Self::Uuid(_) => DataType::Uuid,
            Self::DateTime(_) => DataType::DateTime,
            Self::Bytes(_) => DataType::Bytes,
            Self::File(_) => DataType::File,
            Self::DataSet(_) => DataType::DataSet,
            Self::Template(_) => DataType::Template,
            Self::Int8Array(_) => DataType::Int8Array,
            Self::Int16Array(_) => DataType::Int16Array,
            Self::Int32Array(_) => DataType::Int32Array,
            Self::Int64Array(_) => DataType::Int64Array,
            Self::UInt8Array(_) => DataType::UInt8Array,
            Self::UInt16Array(_) => DataType::UInt16Array,
            Self::UInt32Array(_) => DataType::UInt32Array,
            Self::UInt64Array(_) => DataType::UInt64Array,
            Self::FloatArray(_) => DataType::FloatArray,
            Self::DoubleArray(_) => DataType::DoubleArray,
            Self::BooleanArray(_) => DataType::BooleanArray,
            Self::StringArray(_) => DataType::StringArray,
            Self::DateTimeArray(_) => DataType::DateTimeArray,
            Self::Null(dt) => *dt,
        }
    }

    /// Whether this is a null value (`is_null = true` on the wire).
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null(_))
    }

    /// Construct a checked null value, rejecting datatypes that are invalid for
    /// a metric (`Unknown`, and the property-only `PropertySet`/`PropertySetList`).
    /// This prevents emitting a non-conformant `datatype` field on the wire.
    ///
    /// # Errors
    /// Returns [`SparkplugError::ValueTypeMismatch`] if `dt` is not a valid
    /// metric datatype.
    pub fn null(dt: DataType) -> Result<Self> {
        match dt {
            DataType::Unknown | DataType::PropertySet | DataType::PropertySetList => Err(
                SparkplugError::ValueTypeMismatch(format!("{dt:?} is not a valid metric datatype")),
            ),
            _ => Ok(Self::Null(dt)),
        }
    }
}

/// A single cell value inside a [`DataSet`] row. Only basic scalar types are
/// valid; a `Null` cell encodes as an unset value oneof.
#[derive(Clone, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum DataSetValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    Text(String),
    DateTime(i64),
    Null,
}

impl DataSetValue {
    /// The Sparkplug [`DataType`] this cell declares, or `None` for `Null`.
    #[must_use]
    pub fn datatype(&self) -> Option<DataType> {
        let dt = match self {
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt8(_) => DataType::UInt8,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float(_) => DataType::Float,
            Self::Double(_) => DataType::Double,
            Self::Boolean(_) => DataType::Boolean,
            Self::String(_) => DataType::String,
            Self::Text(_) => DataType::Text,
            Self::DateTime(_) => DataType::DateTime,
            Self::Null => return None,
        };
        Some(dt)
    }

    /// Whether this cell is null (unset value oneof on the wire).
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

/// A value inside a [`PropertySet`] — basic scalars plus recursive property sets.
#[derive(Clone, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum PropertyValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    Text(String),
    DateTime(i64),
    PropertySet(PropertySet),
    PropertySetList(PropertySetList),
    /// A null property value with its declared datatype.
    Null(DataType),
}

impl PropertyValue {
    /// The Sparkplug [`DataType`] this property value declares on the wire.
    #[must_use]
    pub fn datatype(&self) -> DataType {
        match self {
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt8(_) => DataType::UInt8,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float(_) => DataType::Float,
            Self::Double(_) => DataType::Double,
            Self::Boolean(_) => DataType::Boolean,
            Self::String(_) => DataType::String,
            Self::Text(_) => DataType::Text,
            Self::DateTime(_) => DataType::DateTime,
            Self::PropertySet(_) => DataType::PropertySet,
            Self::PropertySetList(_) => DataType::PropertySetList,
            Self::Null(dt) => *dt,
        }
    }
}

/// A template parameter value — basic scalar types only
/// (`tck-id-payloads-template-parameter-value`).
#[derive(Clone, Debug, PartialEq)]
#[allow(missing_docs)]
pub enum ParameterValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(String),
    Text(String),
    DateTime(i64),
}

impl ParameterValue {
    /// The Sparkplug [`DataType`] this parameter value declares on the wire.
    #[must_use]
    pub fn datatype(&self) -> DataType {
        match self {
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt8(_) => DataType::UInt8,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float(_) => DataType::Float,
            Self::Double(_) => DataType::Double,
            Self::Boolean(_) => DataType::Boolean,
            Self::String(_) => DataType::String,
            Self::Text(_) => DataType::Text,
            Self::DateTime(_) => DataType::DateTime,
        }
    }
}
