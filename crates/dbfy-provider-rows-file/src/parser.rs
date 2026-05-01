//! Pluggable line parser interface — a `LineParser` turns a contiguous byte
//! range (zero or more complete lines) into a typed Arrow `RecordBatch`.
//!
//! Built-in parsers live under [`crate::parsers`] and are gated behind cargo
//! features. Third parties register custom formats by implementing this
//! trait directly and passing the parser to `RowsFileTable::with_parser`.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

pub type ParseError = Box<dyn std::error::Error + Send + Sync>;
pub type ParseResult<T> = std::result::Result<T, ParseError>;

/// Implement to expose a new line-delimited record format to the indexed
/// scan engine.
pub trait LineParser: Send + Sync {
    /// Schema of the rows produced by this parser. Constant for the lifetime
    /// of one provider; the index file pins this schema and rebuilds when
    /// it changes.
    fn schema(&self) -> SchemaRef;

    /// Parse a contiguous byte slice that ends at a chunk boundary (or EOF)
    /// into a single record batch.
    ///
    /// `bytes` may contain zero or more complete records. Trailing newline
    /// at end of `bytes` is permitted and ignored.
    fn parse_chunk(&self, bytes: &[u8]) -> ParseResult<RecordBatch>;

    /// Return the offset of the last safe split point inside `bytes`, or
    /// `None` if `bytes` does not contain a complete record.
    ///
    /// Default implementation splits on `\n`, which is correct for jsonl,
    /// logfmt, syslog, CSV without quoted multi-line fields, and most regex
    /// patterns. Override for formats with multi-line records.
    fn chunk_boundary(&self, bytes: &[u8]) -> Option<usize> {
        memchr::memrchr(b'\n', bytes).map(|i| i + 1)
    }
}

/// Type-erased parser handle held by `RowsFileTable`.
pub type DynParser = Arc<dyn LineParser>;

/// Subset of Arrow types the built-in parsers (jsonl, regex, logfmt, syslog,
/// csv) emit. Each maps to a single Arrow `DataType` and a single typed
/// builder. Float and Decimal are not yet wired into all parsers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellType {
    Int64,
    Float64,
    /// UTF-8 string.
    String,
    /// RFC 3339 microseconds, UTC.
    Timestamp,
}

impl CellType {
    pub fn arrow(self) -> arrow_schema::DataType {
        match self {
            CellType::Int64 => arrow_schema::DataType::Int64,
            CellType::Float64 => arrow_schema::DataType::Float64,
            CellType::String => arrow_schema::DataType::Utf8,
            CellType::Timestamp => arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
        }
    }
}
