//! CSV parser (RFC 4180-ish, no multi-line quoted fields in v1).
//!
//! User declares the columns they care about; CSV file may have more or
//! fewer. Selection is by header name (when `has_header: true`) or by
//! 0-based index.
//!
//! **v1 limitation**: the chunk boundary is plain `\n`, so quoted fields
//! containing literal newlines split across chunks. The vast majority of
//! CSV files don't use multi-line quoted fields; documenting and pinning
//! to a Sprint 4 follow-up.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};

use crate::parser::{CellType, LineParser, ParseError, ParseResult};
use crate::parsers::cells::TypedBuilder;

#[derive(Debug, Clone)]
pub enum CsvSource {
    /// Pick the column by its header name. Requires `has_header = true`.
    Name(String),
    /// Pick the column by 0-based position.
    Index(usize),
}

#[derive(Debug, Clone)]
pub struct CsvColumn {
    pub name: String,
    pub source: CsvSource,
    pub data_type: CellType,
}

#[derive(Debug)]
pub struct CsvParser {
    columns: Vec<CsvColumn>,
    /// Resolved 0-based positions of each declared column. Populated either
    /// from `CsvSource::Index` directly or by matching `Name(_)` against
    /// the user-supplied header.
    positions: Vec<usize>,
    delimiter: u8,
    has_header: bool,
    schema: SchemaRef,
}

impl CsvParser {
    /// Build a parser. When `has_header` is `true`, you must pass the
    /// `header` (the first record of the file) so that `CsvSource::Name`
    /// can be resolved at construction time. Pass `None` for a
    /// `has_header: false` file or when all columns use `CsvSource::Index`.
    pub fn try_new(
        columns: Vec<CsvColumn>,
        delimiter: u8,
        has_header: bool,
        header: Option<&[String]>,
    ) -> ParseResult<Self> {
        let positions = columns
            .iter()
            .map(|c| match &c.source {
                CsvSource::Index(i) => Ok(*i),
                CsvSource::Name(name) => {
                    let header = header.ok_or_else(|| {
                        ParseError::from(format!(
                            "csv column `{}` references header name `{name}` but no header was supplied",
                            c.name
                        ))
                    })?;
                    header
                        .iter()
                        .position(|h| h == name)
                        .ok_or_else(|| {
                            ParseError::from(format!(
                                "csv column `{}` header `{name}` not found in file (have {header:?})",
                                c.name
                            ))
                        })
                }
            })
            .collect::<ParseResult<Vec<_>>>()?;
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.arrow(), true))
            .collect();
        Ok(Self {
            columns,
            positions,
            delimiter,
            has_header,
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

impl LineParser for CsvParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse_chunk(&self, bytes: &[u8]) -> ParseResult<RecordBatch> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(false) // header is consumed at the file level, not per chunk
            .flexible(true)
            .from_reader(bytes);

        let mut builders: Vec<TypedBuilder> = self
            .columns
            .iter()
            .map(|c| TypedBuilder::for_type(c.data_type))
            .collect();

        let mut first_row = true;
        for result in rdr.records() {
            let record = result.map_err(|err| {
                ParseError::from(format!("csv: malformed record: {err}"))
            })?;
            if first_row && self.has_header {
                first_row = false;
                continue;
            }
            first_row = false;
            for (i, &pos) in self.positions.iter().enumerate() {
                let value = record.get(pos);
                builders[i].append_str(value.filter(|s| !s.is_empty()))?;
            }
        }

        let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// Read the first line of a CSV file and split it on `delimiter`. Useful
/// when constructing a parser with `has_header: true` and `Name(_)`
/// column sources.
pub fn read_header(path: &std::path::Path, delimiter: u8) -> ParseResult<Vec<String>> {
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let line = line.trim_end_matches(|c: char| c == '\n' || c == '\r');
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .from_reader(line.as_bytes());
    let record = rdr
        .records()
        .next()
        .ok_or_else(|| ParseError::from("csv header: empty file"))?
        .map_err(|err| ParseError::from(format!("csv header: {err}")))?;
    Ok(record.iter().map(String::from).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};

    #[test]
    fn parses_csv_with_header_by_name() {
        let header = vec!["id".to_string(), "name".to_string(), "score".to_string()];
        let parser = CsvParser::try_new(
            vec![
                CsvColumn {
                    name: "user_id".into(),
                    source: CsvSource::Name("id".into()),
                    data_type: CellType::Int64,
                },
                CsvColumn {
                    name: "label".into(),
                    source: CsvSource::Name("name".into()),
                    data_type: CellType::String,
                },
                CsvColumn {
                    name: "value".into(),
                    source: CsvSource::Name("score".into()),
                    data_type: CellType::Float64,
                },
            ],
            b',',
            true,
            Some(&header),
        )
        .unwrap();
        let bytes = b"id,name,score\n1,alpha,3.14\n2,beta,2.71\n";
        let batch = parser.parse_chunk(bytes).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let id = batch
            .column_by_name("user_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let label = batch
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(id.value(0), 1);
        assert_eq!(label.value(1), "beta");
        assert!((value.value(0) - 3.14).abs() < 1e-9);
    }

    #[test]
    fn parses_csv_no_header_by_index() {
        let parser = CsvParser::try_new(
            vec![
                CsvColumn {
                    name: "first".into(),
                    source: CsvSource::Index(0),
                    data_type: CellType::String,
                },
                CsvColumn {
                    name: "third".into(),
                    source: CsvSource::Index(2),
                    data_type: CellType::Int64,
                },
            ],
            b',',
            false,
            None,
        )
        .unwrap();
        let bytes = b"alpha,SKIP,42\nbeta,SKIP,99\n";
        let batch = parser.parse_chunk(bytes).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let third = batch
            .column_by_name("third")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(third.value(0), 42);
        assert_eq!(third.value(1), 99);
    }

    #[test]
    fn missing_header_name_is_caught_at_construction() {
        let header = vec!["a".into(), "b".into()];
        let err = CsvParser::try_new(
            vec![CsvColumn {
                name: "c".into(),
                source: CsvSource::Name("missing".into()),
                data_type: CellType::String,
            }],
            b',',
            true,
            Some(&header),
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found in file"));
    }
}
