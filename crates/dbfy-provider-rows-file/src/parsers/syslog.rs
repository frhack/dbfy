//! syslog RFC 5424 parser.
//!
//! Format spec: <PRI>VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP
//! PROCID SP MSGID SP STRUCTURED-DATA SP MSG
//!
//! Example:
//!   <165>1 2003-10-11T22:14:15.003Z myhost evntslog - ID47 [exampleSDID@32473 iut="3"] BOMAn application event
//!
//! v1 limitation: structured-data is exposed as a raw string (the full
//! `[…]…[…]` block). Field-level extraction inside SD-DATA is deferred.
//! NIL values (`-`) become NULL.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};

use crate::parser::{CellType, LineParser, ParseError, ParseResult};
use crate::parsers::cells::TypedBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyslogField {
    /// 0..=191 PRI value, integer.
    Priority,
    /// Facility = PRI / 8.
    Facility,
    /// Severity = PRI % 8.
    Severity,
    /// Syslog protocol version (always 1 for RFC 5424).
    Version,
    /// RFC 3339 timestamp, NIL → NULL.
    Timestamp,
    Hostname,
    AppName,
    ProcId,
    MsgId,
    /// Raw structured-data block (everything between brackets, or NIL).
    StructuredData,
    /// The message body — everything after the structured-data field.
    Message,
}

#[derive(Debug, Clone)]
pub struct SyslogColumn {
    pub name: String,
    pub field: SyslogField,
    pub data_type: CellType,
}

#[derive(Debug)]
pub struct SyslogParser {
    columns: Vec<SyslogColumn>,
    schema: SchemaRef,
}

impl SyslogParser {
    pub fn try_new(columns: Vec<SyslogColumn>) -> ParseResult<Self> {
        if columns.is_empty() {
            return Err(ParseError::from(
                "syslog parser requires at least one column",
            ));
        }
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.arrow(), true))
            .collect();
        Ok(Self {
            columns,
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

impl LineParser for SyslogParser {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn parse_chunk(&self, bytes: &[u8]) -> ParseResult<RecordBatch> {
        let text = std::str::from_utf8(bytes)
            .map_err(|err| ParseError::from(format!("syslog: invalid utf-8: {err}")))?;
        let mut builders: Vec<TypedBuilder> = self
            .columns
            .iter()
            .map(|c| TypedBuilder::for_type(c.data_type))
            .collect();

        for line in text.split('\n') {
            if line.is_empty() {
                continue;
            }
            let parts = parse_5424(line);
            for (i, col) in self.columns.iter().enumerate() {
                let raw = match col.field {
                    SyslogField::Priority => parts.priority.map(|p| p.to_string()),
                    SyslogField::Facility => parts.priority.map(|p| (p / 8).to_string()),
                    SyslogField::Severity => parts.priority.map(|p| (p % 8).to_string()),
                    SyslogField::Version => parts.version.map(|v| v.to_string()),
                    SyslogField::Timestamp => parts.timestamp.map(String::from),
                    SyslogField::Hostname => parts.hostname.map(String::from),
                    SyslogField::AppName => parts.app_name.map(String::from),
                    SyslogField::ProcId => parts.proc_id.map(String::from),
                    SyslogField::MsgId => parts.msg_id.map(String::from),
                    SyslogField::StructuredData => parts.structured_data.map(String::from),
                    SyslogField::Message => parts.message.map(String::from),
                };
                builders[i].append_str(raw.as_deref())?;
            }
        }

        let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

#[derive(Debug, Default)]
struct Rfc5424Parts<'a> {
    priority: Option<u32>,
    version: Option<u32>,
    timestamp: Option<&'a str>,
    hostname: Option<&'a str>,
    app_name: Option<&'a str>,
    proc_id: Option<&'a str>,
    msg_id: Option<&'a str>,
    structured_data: Option<&'a str>,
    message: Option<&'a str>,
}

fn parse_5424(line: &str) -> Rfc5424Parts<'_> {
    let mut out = Rfc5424Parts::default();
    let bytes = line.as_bytes();
    let mut cursor = 0;

    // <PRI>VERSION (no spaces inside)
    if cursor < bytes.len() && bytes[cursor] == b'<' {
        if let Some(end) = line[cursor..].find('>') {
            let pri_str = &line[cursor + 1..cursor + end];
            if let Ok(pri) = pri_str.parse::<u32>() {
                out.priority = Some(pri);
            }
            cursor += end + 1;
            // Read VERSION (digits) until space.
            let ver_start = cursor;
            while cursor < bytes.len() && bytes[cursor] != b' ' {
                cursor += 1;
            }
            if let Ok(v) = line[ver_start..cursor].parse::<u32>() {
                out.version = Some(v);
            }
        }
    }
    // The five space-separated NIL-or-string fields:
    // TIMESTAMP HOSTNAME APP-NAME PROCID MSGID
    let take_token = |c: &mut usize| -> Option<&str> {
        while *c < bytes.len() && bytes[*c] == b' ' {
            *c += 1;
        }
        let start = *c;
        while *c < bytes.len() && bytes[*c] != b' ' {
            *c += 1;
        }
        if start == *c {
            None
        } else {
            let s = &line[start..*c];
            if s == "-" { None } else { Some(s) }
        }
    };
    out.timestamp = take_token(&mut cursor);
    out.hostname = take_token(&mut cursor);
    out.app_name = take_token(&mut cursor);
    out.proc_id = take_token(&mut cursor);
    out.msg_id = take_token(&mut cursor);

    // STRUCTURED-DATA: NIL `-` or one-or-more bracketed groups, no nesting.
    while cursor < bytes.len() && bytes[cursor] == b' ' {
        cursor += 1;
    }
    if cursor < bytes.len() {
        let sd_start = cursor;
        if bytes[cursor] == b'-' {
            cursor += 1;
            // SD is NIL.
        } else if bytes[cursor] == b'[' {
            // Consume one or more `[...]` groups, allowing escapes within.
            while cursor < bytes.len() && bytes[cursor] == b'[' {
                cursor += 1;
                let mut escaped = false;
                while cursor < bytes.len() {
                    let b = bytes[cursor];
                    if escaped {
                        escaped = false;
                    } else if b == b'\\' {
                        escaped = true;
                    } else if b == b']' {
                        cursor += 1;
                        break;
                    }
                    cursor += 1;
                }
            }
            out.structured_data = Some(&line[sd_start..cursor]);
        }
    }

    // MSG: everything after a single space following SD.
    if cursor < bytes.len() && bytes[cursor] == b' ' {
        cursor += 1;
    }
    if cursor < bytes.len() {
        // Trim BOM (UTF-8 BOM = EF BB BF) if present.
        let mut msg_bytes = &bytes[cursor..];
        if msg_bytes.len() >= 3 && &msg_bytes[..3] == b"\xEF\xBB\xBF" {
            msg_bytes = &msg_bytes[3..];
        }
        let msg = std::str::from_utf8(msg_bytes).unwrap_or("");
        if !msg.is_empty() {
            out.message = Some(msg);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, StringArray};

    #[test]
    fn parses_canonical_5424() {
        let parser = SyslogParser::try_new(vec![
            SyslogColumn { name: "pri".into(),    field: SyslogField::Priority,  data_type: CellType::Int64 },
            SyslogColumn { name: "host".into(),   field: SyslogField::Hostname,  data_type: CellType::String },
            SyslogColumn { name: "app".into(),    field: SyslogField::AppName,   data_type: CellType::String },
            SyslogColumn { name: "msgid".into(),  field: SyslogField::MsgId,     data_type: CellType::String },
            SyslogColumn { name: "sd".into(),     field: SyslogField::StructuredData, data_type: CellType::String },
            SyslogColumn { name: "msg".into(),    field: SyslogField::Message,   data_type: CellType::String },
        ])
        .unwrap();
        // Build the line as bytes so we can include the real UTF-8 BOM
        // (EF BB BF) before "An event" rather than the literal text "BOM".
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(
            br#"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="App"] "#,
        );
        bytes.extend_from_slice(b"\xEF\xBB\xBF");
        bytes.extend_from_slice(b"An event\n");
        let batch = parser.parse_chunk(&bytes).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let pri = batch
            .column_by_name("pri")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let host = batch
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let msgid = batch
            .column_by_name("msgid")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sd = batch
            .column_by_name("sd")
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
        assert_eq!(pri.value(0), 165);
        assert_eq!(host.value(0), "mymachine.example.com");
        assert_eq!(msgid.value(0), "ID47");
        assert!(sd.value(0).starts_with("[exampleSDID@32473"));
        assert_eq!(msg.value(0), "An event"); // BOM stripped
    }

    #[test]
    fn nil_dashes_become_null() {
        let parser = SyslogParser::try_new(vec![
            SyslogColumn { name: "host".into(), field: SyslogField::Hostname,  data_type: CellType::String },
            SyslogColumn { name: "app".into(),  field: SyslogField::AppName,   data_type: CellType::String },
            SyslogColumn { name: "proc".into(), field: SyslogField::ProcId,    data_type: CellType::String },
        ])
        .unwrap();
        let bytes = b"<13>1 - - - - - - just a message\n";
        let batch = parser.parse_chunk(bytes).unwrap();
        let host = batch
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(host.is_null(0));
    }

    #[test]
    fn facility_severity_split_from_priority() {
        let parser = SyslogParser::try_new(vec![
            SyslogColumn { name: "fac".into(), field: SyslogField::Facility, data_type: CellType::Int64 },
            SyslogColumn { name: "sev".into(), field: SyslogField::Severity, data_type: CellType::Int64 },
        ])
        .unwrap();
        // PRI=165 -> facility=20, severity=5
        let bytes = b"<165>1 - - - - - - msg\n";
        let batch = parser.parse_chunk(bytes).unwrap();
        let fac = batch
            .column_by_name("fac")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sev = batch
            .column_by_name("sev")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(fac.value(0), 20);
        assert_eq!(sev.value(0), 5);
    }
}
