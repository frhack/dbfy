//! logfmt parser — `key=value key="quoted value" foo=bar` per line, the
//! default of Go's `log/slog` text handler and many ops tools (etcd, k8s
//! componentstatuses, etc.).
//!
//! Implementation notes:
//!   * Keys are bareword chars (any non-whitespace, non-`=`).
//!   * Values are either bareword (terminated by whitespace) or
//!     double-quoted with `\"` and `\\` escapes.
//!   * Unknown keys on the line are ignored.
//!   * Missing keys for a declared column produce NULL.
//!
//! No external dep; the official `logfmt` crate is unmaintained and this
//! parser is small enough to keep in-tree (~80 LoC).

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};

use crate::parser::{CellType, LineParser, ParseError, ParseResult};
use crate::parsers::cells::TypedBuilder;

#[derive(Debug, Clone)]
pub struct LogfmtColumn {
    pub name: String,
    pub key: String,
    pub data_type: CellType,
}

pub struct LogfmtParser {
    columns: Vec<LogfmtColumn>,
    schema: SchemaRef,
}

impl LogfmtParser {
    pub fn try_new(columns: Vec<LogfmtColumn>) -> ParseResult<Self> {
        if columns.is_empty() {
            return Err(ParseError::from(
                "logfmt parser requires at least one column",
            ));
        }
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.arrow(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        Ok(Self { columns, schema })
    }
}

impl LineParser for LogfmtParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse_chunk(&self, bytes: &[u8]) -> ParseResult<RecordBatch> {
        let text = std::str::from_utf8(bytes)
            .map_err(|err| ParseError::from(format!("logfmt: invalid utf-8: {err}")))?;
        let mut builders: Vec<TypedBuilder> = self
            .columns
            .iter()
            .map(|c| TypedBuilder::for_type(c.data_type))
            .collect();
        let mut row_buf: Vec<(String, String)> = Vec::with_capacity(8);

        for line in text.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            row_buf.clear();
            scan_logfmt_line(line, &mut row_buf);
            for (i, col) in self.columns.iter().enumerate() {
                let value = row_buf
                    .iter()
                    .find(|(k, _)| k == &col.key)
                    .map(|(_, v)| v.as_str());
                builders[i].append_str(value)?;
            }
        }

        let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

fn scan_logfmt_line(line: &str, out: &mut Vec<(String, String)>) {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // skip whitespace
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        // key
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            // bareword w/o `=`: skip
            continue;
        }
        let key = std::str::from_utf8(&bytes[key_start..i])
            .unwrap_or("")
            .to_string();
        i += 1; // consume '='

        // value: quoted or bareword
        if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let val_start = i;
            let mut val = String::new();
            while i < bytes.len() {
                match bytes[i] {
                    b'\\' if i + 1 < bytes.len() => {
                        let next = bytes[i + 1];
                        if next == b'"' || next == b'\\' {
                            val.push(next as char);
                            i += 2;
                        } else {
                            val.push('\\');
                            i += 1;
                        }
                    }
                    b'"' => {
                        i += 1;
                        break;
                    }
                    other => {
                        val.push(other as char);
                        i += 1;
                    }
                }
            }
            let _ = val_start;
            out.push((key, val));
        } else {
            let val_start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            let value = std::str::from_utf8(&bytes[val_start..i])
                .unwrap_or("")
                .to_string();
            out.push((key, value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    #[test]
    fn parses_basic_kv() {
        let parser = LogfmtParser::try_new(vec![
            LogfmtColumn {
                name: "level".into(),
                key: "level".into(),
                data_type: CellType::String,
            },
            LogfmtColumn {
                name: "msg".into(),
                key: "msg".into(),
                data_type: CellType::String,
            },
            LogfmtColumn {
                name: "user".into(),
                key: "user_id".into(),
                data_type: CellType::Int64,
            },
        ])
        .unwrap();
        let bytes = br#"level=info msg="hello world" user_id=42 extra=ignored
level=error msg="oops \"quoted\"" user_id=7
"#;
        let batch = parser.parse_chunk(bytes).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let level = batch
            .column_by_name("level")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let msg = batch
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let user = batch
            .column_by_name("user")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(level.value(0), "info");
        assert_eq!(level.value(1), "error");
        assert_eq!(msg.value(0), "hello world");
        assert_eq!(msg.value(1), "oops \"quoted\"");
        assert_eq!(user.value(0), 42);
        assert_eq!(user.value(1), 7);
    }
}
