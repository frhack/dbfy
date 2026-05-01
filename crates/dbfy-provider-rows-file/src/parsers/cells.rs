//! Shared helpers for `regex`/`logfmt`/future parsers that extract `&str`
//! values from each row and need to populate typed Arrow arrays.

use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::ArrayRef;

use crate::parser::{CellType, ParseError, ParseResult};
use crate::parsers::jsonl;

pub enum TypedBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Timestamp(TimestampMicrosecondBuilder),
}

impl TypedBuilder {
    pub fn for_type(t: CellType) -> Self {
        match t {
            CellType::Int64 => TypedBuilder::Int64(Int64Builder::new()),
            CellType::Float64 => TypedBuilder::Float64(Float64Builder::new()),
            CellType::String => TypedBuilder::String(StringBuilder::new()),
            CellType::Timestamp => {
                TypedBuilder::Timestamp(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
            }
        }
    }

    /// Append a value parsed from a `&str` cell. Empty input or parse
    /// failure yields a NULL — the document-level error policy lives in
    /// the parent parser (skip / fail / warn).
    pub fn append_str(&mut self, raw: Option<&str>) -> ParseResult<()> {
        match self {
            TypedBuilder::Int64(b) => match raw.and_then(|s| s.parse::<i64>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            TypedBuilder::Float64(b) => match raw.and_then(|s| s.parse::<f64>().ok()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            TypedBuilder::String(b) => match raw {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            },
            TypedBuilder::Timestamp(b) => match raw.and_then(|s| jsonl::chrono_lite::parse_rfc3339_micros(s).ok()) {
                Some(micros) => b.append_value(micros),
                None => b.append_null(),
            },
        }
        Ok(())
    }

    pub fn finish(self) -> ArrayRef {
        match self {
            TypedBuilder::Int64(mut b) => Arc::new(b.finish()),
            TypedBuilder::Float64(mut b) => Arc::new(b.finish()),
            TypedBuilder::String(mut b) => Arc::new(b.finish()),
            TypedBuilder::Timestamp(mut b) => Arc::new(b.finish()),
        }
    }
}

#[allow(dead_code)]
pub fn ensure_arity<T>(items: Vec<T>, expected: usize) -> ParseResult<Vec<T>> {
    if items.len() != expected {
        return Err(ParseError::from(format!(
            "expected {expected} items, got {}",
            items.len()
        )));
    }
    Ok(items)
}
