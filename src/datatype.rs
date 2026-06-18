//! Sparkplug B data types (spec §6.4.16 “Data Types”, codes 0..=34).
//!
//! The integer codes are shared across the metric, DataSet-column, template-
//! parameter, and property-value sub-domains; this single [`DataType`] models
//! all of them and exposes classifiers (`is_basic`, `is_array`, …) instead of
//! Tahu's four parallel enums.

use crate::error::{Result, SparkplugError};

/// A Sparkplug B data type code.
///
/// Values `1..=14` are the *basic* scalar types (valid as DataSet columns,
/// template parameters, and property values). `15..=19` add UUID/DataSet/Bytes/
/// File/Template. `20..=21` are property-value-only (`PropertySet`/list).
/// `22..=34` are the array types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
#[allow(missing_docs)] // variant names are self-describing; documented as a group above
pub enum DataType {
    Unknown = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    UInt8 = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,
    Float = 9,
    Double = 10,
    Boolean = 11,
    String = 12,
    DateTime = 13,
    Text = 14,
    Uuid = 15,
    DataSet = 16,
    Bytes = 17,
    File = 18,
    Template = 19,
    PropertySet = 20,
    PropertySetList = 21,
    Int8Array = 22,
    Int16Array = 23,
    Int32Array = 24,
    Int64Array = 25,
    UInt8Array = 26,
    UInt16Array = 27,
    UInt32Array = 28,
    UInt64Array = 29,
    FloatArray = 30,
    DoubleArray = 31,
    BooleanArray = 32,
    StringArray = 33,
    DateTimeArray = 34,
}

impl DataType {
    /// The integer code as it appears on the wire (`datatype` field, an unsigned
    /// 32-bit integer per `tck-id-payloads-metric-datatype-value-type`).
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self as i32 as u32
    }

    /// Parse a wire datatype code, rejecting unknown values
    /// (`tck-id-payloads-metric-datatype-value`).
    ///
    /// # Errors
    /// Returns [`SparkplugError::UnknownDataType`] for codes outside `0..=34`.
    pub const fn from_u32(v: u32) -> Result<Self> {
        let ty = match v {
            0 => Self::Unknown,
            1 => Self::Int8,
            2 => Self::Int16,
            3 => Self::Int32,
            4 => Self::Int64,
            5 => Self::UInt8,
            6 => Self::UInt16,
            7 => Self::UInt32,
            8 => Self::UInt64,
            9 => Self::Float,
            10 => Self::Double,
            11 => Self::Boolean,
            12 => Self::String,
            13 => Self::DateTime,
            14 => Self::Text,
            15 => Self::Uuid,
            16 => Self::DataSet,
            17 => Self::Bytes,
            18 => Self::File,
            19 => Self::Template,
            20 => Self::PropertySet,
            21 => Self::PropertySetList,
            22 => Self::Int8Array,
            23 => Self::Int16Array,
            24 => Self::Int32Array,
            25 => Self::Int64Array,
            26 => Self::UInt8Array,
            27 => Self::UInt16Array,
            28 => Self::UInt32Array,
            29 => Self::UInt64Array,
            30 => Self::FloatArray,
            31 => Self::DoubleArray,
            32 => Self::BooleanArray,
            33 => Self::StringArray,
            34 => Self::DateTimeArray,
            other => return Err(SparkplugError::UnknownDataType(other)),
        };
        Ok(ty)
    }

    /// `true` for the basic scalar types `1..=14`, the only types permitted as
    /// DataSet columns and template parameters (`tck-id-payloads-dataset-types-value`).
    #[must_use]
    pub const fn is_basic(self) -> bool {
        matches!(self as i32, 1..=14)
    }

    /// `true` for the array types `22..=34`.
    #[must_use]
    pub const fn is_array(self) -> bool {
        matches!(self as i32, 22..=34)
    }

    /// For numeric/temporal array types, the fixed little-endian element width
    /// in bytes. `BooleanArray`/`StringArray` use special framing and return
    /// `None`, as do non-array types.
    #[must_use]
    pub const fn array_element_width(self) -> Option<usize> {
        let w = match self {
            Self::Int8Array | Self::UInt8Array => 1,
            Self::Int16Array | Self::UInt16Array => 2,
            Self::Int32Array | Self::UInt32Array | Self::FloatArray => 4,
            Self::Int64Array | Self::UInt64Array | Self::DoubleArray | Self::DateTimeArray => 8,
            _ => return None,
        };
        Some(w)
    }
}
