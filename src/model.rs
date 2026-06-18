//! The Sparkplug B payload object model (spec §6.4 “Payload Component
//! Definitions”). Plain data structs — no Java-style getters/setters; values
//! are typed via the enums in [`crate::value`].

use bytes::Bytes;

use crate::datatype::DataType;
use crate::error::{Result, SparkplugError};
use crate::value::{DataSetValue, MetricValue, ParameterValue, PropertyValue};

/// A complete Sparkplug B payload (the body of an N/D BIRTH/DATA/DEATH/CMD message).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Payload {
    /// Message send time, epoch milliseconds UTC.
    pub timestamp: Option<u64>,
    /// The metrics carried by this payload.
    pub metrics: Vec<Metric>,
    /// The Sparkplug sequence number (`0..=255`); absent on NDEATH/NCMD/DCMD.
    pub seq: Option<u8>,
    /// Optional schema UUID; also the `SPBV1.0_COMPRESSED` sentinel for the
    /// compression envelope.
    pub uuid: Option<String>,
    /// Optional opaque body (used by the compression envelope).
    pub body: Option<Bytes>,
}

impl Payload {
    /// Create an empty payload.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder-style setter for the payload timestamp.
    #[must_use]
    pub fn with_timestamp(mut self, ts: u64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Builder-style setter for the sequence number.
    #[must_use]
    pub fn with_seq(mut self, seq: u8) -> Self {
        self.seq = Some(seq);
        self
    }

    /// Append a metric, consuming and returning `self` for chaining.
    #[must_use]
    pub fn with_metric(mut self, metric: Metric) -> Self {
        self.metrics.push(metric);
        self
    }
}

/// A single named/aliased typed value — the unit of all Sparkplug data.
///
/// `name` and `alias` are both optional: BIRTH metrics carry the name (and,
/// when aliasing, an alias); DATA metrics may carry only the alias
/// (`tck-id-payloads-alias-data-cmd-requirement`).
#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
    /// Metric name (required on BIRTH unless aliasing — `tck-id-payloads-name-requirement`).
    pub name: Option<String>,
    /// Metric alias (unique per Edge Node — `tck-id-payloads-alias-uniqueness`).
    pub alias: Option<u64>,
    /// Acquisition timestamp, epoch milliseconds UTC.
    pub timestamp: Option<u64>,
    /// The typed value (carries the datatype; `Null` carries the declared type).
    pub value: MetricValue,
    /// Whether this is historical data (should not update the live tag).
    pub is_historical: Option<bool>,
    /// Whether this value should not be stored as a tag.
    pub is_transient: Option<bool>,
    /// Optional metadata (esp. for `Bytes`/`File`/multi-part transfers).
    pub metadata: Option<MetaData>,
    /// Optional property set (engineering units, quality, …).
    pub properties: Option<PropertySet>,
}

impl Metric {
    /// A named metric with the given value and no other fields set.
    #[must_use]
    pub fn new(name: impl Into<String>, value: MetricValue) -> Self {
        Self {
            name: Some(name.into()),
            alias: None,
            timestamp: None,
            value,
            is_historical: None,
            is_transient: None,
            metadata: None,
            properties: None,
        }
    }

    /// An alias-only metric (no name), as used in DATA/CMD messages.
    #[must_use]
    pub fn aliased(alias: u64, value: MetricValue) -> Self {
        Self {
            name: None,
            alias: Some(alias),
            timestamp: None,
            value,
            is_historical: None,
            is_transient: None,
            metadata: None,
            properties: None,
        }
    }

    /// Builder-style setter for the alias.
    #[must_use]
    pub fn with_alias(mut self, alias: u64) -> Self {
        self.alias = Some(alias);
        self
    }

    /// Builder-style setter for the timestamp (epoch ms).
    #[must_use]
    pub fn with_timestamp(mut self, ts: u64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Builder-style setter for the property set.
    #[must_use]
    pub fn with_properties(mut self, props: PropertySet) -> Self {
        self.properties = Some(props);
        self
    }

    /// The datatype this metric declares on the wire.
    #[must_use]
    pub fn datatype(&self) -> DataType {
        self.value.datatype()
    }
}

/// Optional per-metric metadata, especially for `File`/`Bytes`/multi-part transfers.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct MetaData {
    /// Whether the payload is one part of a multi-part transfer.
    pub is_multi_part: Option<bool>,
    /// Content/media type.
    pub content_type: Option<String>,
    /// File/string/multi-part size in bytes.
    pub size: Option<u64>,
    /// Multi-part sequence number (distinct from the payload-level Sparkplug seq).
    pub seq: Option<u64>,
    /// File name (for `File` metrics).
    pub file_name: Option<String>,
    /// File type (e.g. `xml`, `json`).
    pub file_type: Option<String>,
    /// MD5 of the data.
    pub md5: Option<String>,
    /// Free-form description.
    pub description: Option<String>,
}

/// A set of named properties attached to a metric. Order is preserved (it is
/// significant on the wire, where keys and values are parallel arrays).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct PropertySet {
    /// The (key, value) entries, in wire order.
    pub entries: Vec<(String, PropertyValue)>,
}

impl PropertySet {
    /// An empty property set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a property, returning `self` for chaining.
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, value: PropertyValue) -> Self {
        self.entries.push((key.into(), value));
        self
    }

    /// Look up a property value by key (first match).
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&PropertyValue> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Number of properties.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// A list of property sets (`PropertySetList` datatype).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct PropertySetList {
    /// The property sets.
    pub sets: Vec<PropertySet>,
}

/// A column-oriented table value (the `DataSet` datatype).
///
/// Construct via [`DataSet::new`], which validates that `columns`, `types`, and
/// every row share the same width (`tck-id-payloads-dataset-*`).
#[derive(Clone, Debug, PartialEq)]
pub struct DataSet {
    columns: Vec<String>,
    types: Vec<DataType>,
    rows: Vec<Vec<DataSetValue>>,
}

impl DataSet {
    /// Build and validate a DataSet.
    ///
    /// # Errors
    /// Returns [`SparkplugError::DataSetShape`] if `columns.len() != types.len()`,
    /// any column type is not a basic scalar, or any row's width differs from
    /// the column count.
    pub fn new(
        columns: Vec<String>,
        types: Vec<DataType>,
        rows: Vec<Vec<DataSetValue>>,
    ) -> Result<Self> {
        if columns.len() != types.len() {
            return Err(SparkplugError::DataSetShape(format!(
                "{} columns but {} types",
                columns.len(),
                types.len()
            )));
        }
        for ty in &types {
            if !ty.is_basic() {
                return Err(SparkplugError::DataSetShape(format!(
                    "column type {ty:?} is not a basic scalar type"
                )));
            }
        }
        for (i, row) in rows.iter().enumerate() {
            if row.len() != columns.len() {
                return Err(SparkplugError::DataSetShape(format!(
                    "row {i} has {} cells but there are {} columns",
                    row.len(),
                    columns.len()
                )));
            }
            // Each non-null cell's type must match its column type, so a value
            // can never be decoded under a different type than it was built with.
            for (col, (cell, col_ty)) in row.iter().zip(&types).enumerate() {
                if let Some(cell_ty) = cell.datatype()
                    && cell_ty != *col_ty
                {
                    return Err(SparkplugError::DataSetShape(format!(
                        "row {i} column {col}: cell type {cell_ty:?} does not match column type {col_ty:?}"
                    )));
                }
            }
        }
        Ok(Self {
            columns,
            types,
            rows,
        })
    }

    /// The number of columns (`num_of_columns` on the wire).
    #[must_use]
    pub fn num_of_columns(&self) -> u64 {
        self.columns.len() as u64
    }

    /// The column names.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The per-column datatypes.
    #[must_use]
    pub fn types(&self) -> &[DataType] {
        &self.types
    }

    /// The rows (each row has one [`DataSetValue`] per column).
    #[must_use]
    pub fn rows(&self) -> &[Vec<DataSetValue>] {
        &self.rows
    }
}

/// A user-defined type (UDT) definition or instance (the `Template` datatype).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Template {
    /// Optional template version string.
    pub version: Option<String>,
    /// Reference to the definition's name — set on instances, omitted on
    /// definitions (`tck-id-payloads-template-ref-*`).
    pub template_ref: Option<String>,
    /// `true` for a definition, `false` for an instance
    /// (`tck-id-payloads-template-is-definition`).
    pub is_definition: bool,
    /// Member metrics.
    pub metrics: Vec<Metric>,
    /// Template parameters.
    pub parameters: Vec<Parameter>,
}

/// A named, typed template parameter.
#[derive(Clone, Debug, PartialEq)]
pub struct Parameter {
    /// Parameter name (required — `tck-id-payloads-template-parameter-name-required`).
    pub name: String,
    /// Parameter datatype (a basic scalar — `tck-id-payloads-template-parameter-type-value`).
    pub datatype: DataType,
    /// Parameter value; a definition may omit it.
    pub value: Option<ParameterValue>,
}
