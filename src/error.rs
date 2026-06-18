//! The crate-wide error type and result alias.
//!
//! A single [`SparkplugError`] models every fallible operation (parsing,
//! encoding, decoding, persistence). Per the Rust API guidelines we never
//! `panic!` on untrusted input — every decode failure is a typed variant.

/// Convenient result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, SparkplugError>;

/// Errors produced while building, encoding, decoding, or parsing Sparkplug B data.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SparkplugError {
    /// A topic string was not a valid Sparkplug B topic.
    #[error("invalid Sparkplug topic: {0}")]
    InvalidTopic(String),

    /// A Group/Edge-Node/Device identifier was empty or contained a reserved
    /// character (`+`, `/`, `#`).
    #[error("invalid Sparkplug identifier: {0}")]
    InvalidId(String),

    /// The protobuf byte stream ended unexpectedly.
    #[error("unexpected end of input while decoding protobuf")]
    Truncated,

    /// A protobuf varint exceeded 64 bits.
    #[error("protobuf varint overflowed 64 bits")]
    VarintOverflow,

    /// A protobuf field used a wire type the schema does not allow.
    #[error("unsupported protobuf wire type {0}")]
    InvalidWireType(u8),

    /// A string field was not valid UTF-8.
    #[error("invalid UTF-8 in string field")]
    InvalidUtf8,

    /// A datatype code on the wire was not one of the enumerated Sparkplug data types.
    #[error("unknown Sparkplug data type code {0}")]
    UnknownDataType(u32),

    /// A DATA/CMD metric omitted its datatype and it could not be recovered
    /// from a prior birth (by name or alias).
    #[error("missing datatype for metric (no birth/alias mapping): {0}")]
    MissingDataType(String),

    /// A packed numeric array had a length that is not a multiple of the element width.
    #[error("packed array length {len} is not a multiple of element width {width}")]
    ArrayLength {
        /// The byte length found on the wire.
        len: usize,
        /// The expected per-element width in bytes.
        width: usize,
    },

    /// A DataSet's `columns`/`types`/`rows` shapes were inconsistent.
    #[error("invalid DataSet shape: {0}")]
    DataSetShape(String),

    /// A metric's value did not match the field set expected for its datatype.
    #[error("value/datatype mismatch: {0}")]
    ValueTypeMismatch(String),

    /// Decoding hit the maximum nesting depth (defends against hostile inputs).
    #[error("maximum nesting depth exceeded while decoding")]
    RecursionLimit,

    /// A STATE JSON payload was malformed.
    #[error("invalid STATE payload: {0}")]
    InvalidState(String),

    /// An I/O error from bdSeq persistence.
    #[error("bdSeq store I/O error: {0}")]
    Io(#[from] std::io::Error),
}
