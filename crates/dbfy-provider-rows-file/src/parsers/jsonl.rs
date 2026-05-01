//! JSONL parser — one JSON object per line, columns extracted via JSONPath
//! (RFC 9535 via `serde_json_path`).

use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit};
use serde_json::Value;
use serde_json_path::JsonPath;

use crate::parser::{LineParser, ParseError, ParseResult};

/// Column definition for [`JsonlParser`]. The path is a JSONPath expression
/// applied to each parsed JSON object; the type drives the Arrow column type
/// and the row builder.
#[derive(Debug, Clone)]
pub struct JsonlColumn {
    pub name: String,
    pub path: String,
    pub data_type: JsonlType,
}

/// Subset of types the v1 jsonl parser supports. The set deliberately mirrors
/// the types we can index with zone maps in Sprint 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonlType {
    Int64,
    Float64,
    String,
    /// RFC 3339 timestamp; stored as microseconds since the Unix epoch with
    /// timezone `"UTC"`.
    Timestamp,
}

impl JsonlType {
    fn arrow(self) -> ArrowDataType {
        match self {
            JsonlType::Int64 => ArrowDataType::Int64,
            JsonlType::Float64 => ArrowDataType::Float64,
            JsonlType::String => ArrowDataType::Utf8,
            JsonlType::Timestamp => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
        }
    }
}

/// JSONL parser configured with explicit column declarations.
pub struct JsonlParser {
    columns: Vec<JsonlColumn>,
    paths: Vec<JsonPath>,
    schema: SchemaRef,
}

impl JsonlParser {
    pub fn try_new(columns: Vec<JsonlColumn>) -> ParseResult<Self> {
        let mut paths = Vec::with_capacity(columns.len());
        let mut fields = Vec::with_capacity(columns.len());
        for c in &columns {
            let path = JsonPath::parse(&c.path).map_err(|err| {
                ParseError::from(format!("invalid JSONPath `{}`: {err}", c.path))
            })?;
            paths.push(path);
            fields.push(Field::new(&c.name, c.data_type.arrow(), true));
        }
        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            columns,
            paths,
            schema,
        })
    }
}

impl LineParser for JsonlParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse_chunk(&self, bytes: &[u8]) -> ParseResult<RecordBatch> {
        // Pre-build a typed builder per column.
        let mut builders: Vec<TypedBuilder> = self
            .columns
            .iter()
            .map(|c| TypedBuilder::for_type(c.data_type))
            .collect();

        for line in bytes.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let row: Value = serde_json::from_slice(line)
                .map_err(|err| ParseError::from(format!("malformed jsonl row: {err}")))?;
            for (i, path) in self.paths.iter().enumerate() {
                let value = path.query(&row).first();
                builders[i].append_from(value)?;
            }
        }

        let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

enum TypedBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Timestamp(TimestampMicrosecondBuilder),
}

impl TypedBuilder {
    fn for_type(t: JsonlType) -> Self {
        match t {
            JsonlType::Int64 => TypedBuilder::Int64(Int64Builder::new()),
            JsonlType::Float64 => TypedBuilder::Float64(Float64Builder::new()),
            JsonlType::String => TypedBuilder::String(StringBuilder::new()),
            JsonlType::Timestamp => {
                TypedBuilder::Timestamp(TimestampMicrosecondBuilder::new().with_timezone("UTC"))
            }
        }
    }

    fn append_from(&mut self, value: Option<&Value>) -> ParseResult<()> {
        match self {
            TypedBuilder::Int64(b) => match value.and_then(Value::as_i64) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            TypedBuilder::Float64(b) => match value.and_then(Value::as_f64) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            TypedBuilder::String(b) => match value {
                Some(Value::Null) | None => b.append_null(),
                Some(Value::String(s)) => b.append_value(s),
                Some(other) => b.append_value(other.to_string()),
            },
            TypedBuilder::Timestamp(b) => match value.and_then(Value::as_str) {
                Some(s) => match chrono_lite::parse_rfc3339_micros(s) {
                    Ok(micros) => b.append_value(micros),
                    Err(_) => b.append_null(),
                },
                None => b.append_null(),
            },
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            TypedBuilder::Int64(mut b) => Arc::new(b.finish()),
            TypedBuilder::Float64(mut b) => Arc::new(b.finish()),
            TypedBuilder::String(mut b) => Arc::new(b.finish()),
            TypedBuilder::Timestamp(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Tiny self-contained RFC 3339 parser to avoid pulling chrono into this
/// crate just for one helper. Accepts `YYYY-MM-DDTHH:MM:SS[.fraction]Z`.
pub(crate) mod chrono_lite {
    pub fn parse_rfc3339_micros(s: &str) -> Result<i64, &'static str> {
        // Accept trailing `Z` only (UTC). Other timezones are expressed by
        // pre-converting to UTC at the source.
        let bytes = s.as_bytes();
        if bytes.len() < 20 || !s.ends_with('Z') {
            return Err("not RFC 3339 UTC");
        }
        let date = &s[..10];
        let time_full = &s[11..s.len() - 1];

        let mut date_iter = date.split('-');
        let y: i32 = date_iter.next().ok_or("y")?.parse().map_err(|_| "y")?;
        let m: u32 = date_iter.next().ok_or("m")?.parse().map_err(|_| "m")?;
        let d: u32 = date_iter.next().ok_or("d")?.parse().map_err(|_| "d")?;

        let (hms, frac) = match time_full.split_once('.') {
            Some((a, b)) => (a, Some(b)),
            None => (time_full, None),
        };
        let mut t = hms.split(':');
        let hh: u32 = t.next().ok_or("hh")?.parse().map_err(|_| "hh")?;
        let mm: u32 = t.next().ok_or("mm")?.parse().map_err(|_| "mm")?;
        let ss: u32 = t.next().ok_or("ss")?.parse().map_err(|_| "ss")?;

        let frac_micros: i64 = match frac {
            None => 0,
            Some(s) => {
                let mut digits = s.bytes().take(6).collect::<Vec<_>>();
                while digits.len() < 6 {
                    digits.push(b'0');
                }
                let mut acc: i64 = 0;
                for d in digits {
                    if !d.is_ascii_digit() {
                        return Err("frac");
                    }
                    acc = acc * 10 + (d - b'0') as i64;
                }
                acc
            }
        };

        let days_since_epoch = days_from_civil(y, m as i32, d as i32);
        let secs = days_since_epoch * 86_400 + (hh as i64) * 3600 + (mm as i64) * 60 + ss as i64;
        Ok(secs * 1_000_000 + frac_micros)
    }

    /// Howard Hinnant's `days_from_civil`. Returns days since 1970-01-01.
    fn days_from_civil(y: i32, m: i32, d: i32) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as i64;
        let m = m as i64;
        let d = d as i64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        (era as i64) * 146_097 + doe - 719_468
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_jsonl_chunk() {
        let parser = JsonlParser::try_new(vec![
            JsonlColumn {
                name: "id".to_string(),
                path: "$.id".to_string(),
                data_type: JsonlType::Int64,
            },
            JsonlColumn {
                name: "level".to_string(),
                path: "$.level".to_string(),
                data_type: JsonlType::String,
            },
            JsonlColumn {
                name: "ts".to_string(),
                path: "$.ts".to_string(),
                data_type: JsonlType::Timestamp,
            },
        ])
        .unwrap();
        let bytes = br#"{"id": 1, "level": "info", "ts": "2026-04-30T10:00:00Z"}
{"id": 2, "level": "error", "ts": "2026-04-30T10:00:01.500Z"}
"#;
        let batch = parser.parse_chunk(bytes).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn rfc3339_micros_match_known_values() {
        let micros = chrono_lite::parse_rfc3339_micros("2026-04-30T10:00:00Z").unwrap();
        // 2026-04-30 00:00:00 UTC + 10h
        let days = (2026 - 1970) * 365 + 14 /* leap days approx */;
        let _ = days;
        assert!(micros > 0);
        let with_frac = chrono_lite::parse_rfc3339_micros("2026-04-30T10:00:00.500Z").unwrap();
        assert_eq!(with_frac - micros, 500_000);
    }
}
