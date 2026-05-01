//! Regex parser — one regex with named groups applied per line.
//!
//! Covers Apache CLF, W3C ELF, syslog (until the dedicated parser ships),
//! and any custom k=v / fixed-shape format. Each schema column points at
//! a named capture group.
//!
//! ```ignore
//! let parser = RegexParser::try_new(
//!     r#"^(?P<ip>\S+) - - \[(?P<ts>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) [^"]+" (?P<status>\d+) (?P<bytes>\d+)$"#,
//!     vec![
//!         RegexColumn { name: "ip".into(),     group: "ip".into(),     data_type: CellType::String },
//!         RegexColumn { name: "ts".into(),     group: "ts".into(),     data_type: CellType::String },
//!         RegexColumn { name: "method".into(), group: "method".into(), data_type: CellType::String },
//!         RegexColumn { name: "status".into(), group: "status".into(), data_type: CellType::Int64 },
//!     ],
//! )?;
//! ```

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use regex::Regex;

use crate::parser::{CellType, LineParser, ParseError, ParseResult};
use crate::parsers::cells::TypedBuilder;

#[derive(Debug, Clone)]
pub struct RegexColumn {
    pub name: String,
    /// Name of the capture group inside the regex (without the `(?P<…>)` syntax).
    pub group: String,
    pub data_type: CellType,
}

#[derive(Debug)]
pub struct RegexParser {
    re: Regex,
    columns: Vec<RegexColumn>,
    schema: SchemaRef,
}

impl RegexParser {
    pub fn try_new(pattern: &str, columns: Vec<RegexColumn>) -> ParseResult<Self> {
        let re = Regex::new(pattern)
            .map_err(|err| ParseError::from(format!("invalid regex `{pattern}`: {err}")))?;
        // Verify each declared group is present in the regex to fail fast.
        let group_names: Vec<&str> = re.capture_names().flatten().collect();
        for c in &columns {
            if !group_names.contains(&c.group.as_str()) {
                return Err(ParseError::from(format!(
                    "regex column `{}` references group `{}` which the pattern does not capture",
                    c.name, c.group,
                )));
            }
        }
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.arrow(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            re,
            columns,
            schema,
        })
    }
}

impl LineParser for RegexParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse_chunk(&self, bytes: &[u8]) -> ParseResult<RecordBatch> {
        let text = std::str::from_utf8(bytes)
            .map_err(|err| ParseError::from(format!("regex parser: invalid utf-8: {err}")))?;
        let mut builders: Vec<TypedBuilder> = self
            .columns
            .iter()
            .map(|c| TypedBuilder::for_type(c.data_type))
            .collect();
        for line in text.split('\n') {
            if line.is_empty() {
                continue;
            }
            match self.re.captures(line) {
                Some(caps) => {
                    for (i, col) in self.columns.iter().enumerate() {
                        let raw = caps.name(&col.group).map(|m| m.as_str());
                        builders[i].append_str(raw)?;
                    }
                }
                None => {
                    // Malformed row — append nulls across, let downstream
                    // residual filters drop these.
                    for b in builders.iter_mut() {
                        b.append_str(None)?;
                    }
                }
            }
        }
        let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    #[test]
    fn parses_apache_clf() {
        let parser = RegexParser::try_new(
            r#"^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+) [^"]+" (?P<status>\d+) (?P<bytes>\d+)"#,
            vec![
                RegexColumn { name: "ip".into(),     group: "ip".into(),     data_type: CellType::String },
                RegexColumn { name: "method".into(), group: "method".into(), data_type: CellType::String },
                RegexColumn { name: "status".into(), group: "status".into(), data_type: CellType::Int64 },
            ],
        )
        .unwrap();
        let bytes =
            b"127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326\n";
        let batch = parser.parse_chunk(bytes).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let ip = batch
            .column_by_name("ip")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let status = batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ip.value(0), "127.0.0.1");
        assert_eq!(status.value(0), 200);
    }

    #[test]
    fn invalid_regex_rejected_at_construction() {
        let err = RegexParser::try_new("[unclosed", vec![]).unwrap_err();
        assert!(err.to_string().contains("invalid regex"));
    }

    #[test]
    fn missing_group_rejected_at_construction() {
        let err = RegexParser::try_new(
            r"^(?P<a>\d+)$",
            vec![RegexColumn {
                name: "b".into(),
                group: "b".into(),
                data_type: CellType::Int64,
            }],
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not capture"));
    }
}
