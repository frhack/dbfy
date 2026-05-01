//! Schema inference for line-delimited record files.
//!
//! Reads a sample of the file and emits a YAML stanza ready to paste into
//! a `sources.<name>.tables.<table>` block of a dbfy config. Supports
//! jsonl / csv / logfmt / syslog. The user can paste the output and run
//! `dbfy query --config …`; no further hand-tweaking is needed for the
//! happy path.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde_json::Value;

const DEFAULT_SAMPLE: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Jsonl,
    Csv,
    Logfmt,
    Syslog,
}

impl Format {
    pub fn from_str(s: &str) -> Result<Self> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "jsonl" | "ndjson" => Format::Jsonl,
            "csv" | "tsv" => Format::Csv,
            "logfmt" => Format::Logfmt,
            "syslog" => Format::Syslog,
            other => return Err(anyhow!("unsupported format `{other}`")),
        })
    }

    fn from_extension(path: &Path) -> Option<Self> {
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        match ext.as_str() {
            "jsonl" | "ndjson" => Some(Format::Jsonl),
            "csv" | "tsv" => Some(Format::Csv),
            "log" | "logfmt" => Some(Format::Logfmt),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InferredType {
    Int64,
    Float64,
    Timestamp,
    String,
}

impl InferredType {
    fn yaml(&self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::Float64 => "float64",
            Self::Timestamp => "timestamp",
            Self::String => "string",
        }
    }

    fn from_value(v: &Value) -> Self {
        match v {
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    Self::Int64
                } else {
                    Self::Float64
                }
            }
            Value::String(s) if looks_like_rfc3339(s) => Self::Timestamp,
            _ => Self::String,
        }
    }

    fn from_str_value(s: &str) -> Self {
        if s.is_empty() {
            return Self::String;
        }
        if s.parse::<i64>().is_ok() {
            return Self::Int64;
        }
        if s.parse::<f64>().is_ok() {
            return Self::Float64;
        }
        if looks_like_rfc3339(s) {
            return Self::Timestamp;
        }
        Self::String
    }

    /// Combine two observations of the same column. Defaults toward the
    /// more permissive type.
    fn merge(self, other: Self) -> Self {
        use InferredType::*;
        if self == other {
            return self;
        }
        match (self, other) {
            (Int64, Float64) | (Float64, Int64) => Float64,
            (String, _) | (_, String) => String,
            (Timestamp, _) | (_, Timestamp) => String,
            _ => unreachable!("same-type pairs handled by early return"),
        }
    }
}

fn looks_like_rfc3339(s: &str) -> bool {
    // Cheap RFC 3339 check: `YYYY-MM-DDTHH:MM:SS` (with optional fraction
    // and `Z` or `±HH:MM` suffix). Good enough as a heuristic — actual
    // parsing happens in the parser at index time.
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return false;
    }
    bytes[4] == b'-'
        && bytes[7] == b'-'
        && (bytes[10] == b'T' || bytes[10] == b' ')
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[..4].iter().all(|c| c.is_ascii_digit())
}

#[derive(Debug, Clone)]
pub struct DetectOpts {
    pub format: Option<Format>,
    pub sample: usize,
    pub source_name: String,
    pub table_name: String,
}

impl Default for DetectOpts {
    fn default() -> Self {
        Self {
            format: None,
            sample: DEFAULT_SAMPLE,
            source_name: "files".to_string(),
            table_name: "events".to_string(),
        }
    }
}

pub fn detect(path: PathBuf, opts: DetectOpts) -> Result<String> {
    let format = opts
        .format
        .or_else(|| Format::from_extension(&path))
        .ok_or_else(|| {
            anyhow!(
                "could not infer format from extension `{}`; pass --format jsonl|csv|logfmt|syslog",
                path.display()
            )
        })?;

    let body = match format {
        Format::Jsonl => detect_jsonl(&path, opts.sample)?,
        Format::Csv => detect_csv(&path, opts.sample)?,
        Format::Logfmt => detect_logfmt(&path, opts.sample)?,
        Format::Syslog => detect_syslog_stanza(),
    };

    Ok(render_yaml(&path, &opts, body))
}

struct ParserBody {
    parser_yaml: String,
    suggested_indexed: Vec<String>,
}

fn detect_jsonl(path: &Path, sample: usize) -> Result<ParserBody> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(f);

    // (key insertion order, observed type, observed any null)
    let mut order: Vec<String> = Vec::new();
    let mut types: BTreeMap<String, Option<InferredType>> = BTreeMap::new();
    let mut count = 0usize;

    for line in reader.lines() {
        if count >= sample {
            break;
        }
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        count += 1;
        let value: Value =
            serde_json::from_str(&line).with_context(|| format!("invalid JSON on line {}", count))?;
        let Value::Object(map) = value else {
            return Err(anyhow!(
                "line {count}: expected JSON object at top level, got {:?}",
                kind_of(&value)
            ));
        };
        for (k, v) in map {
            if !types.contains_key(&k) {
                order.push(k.clone());
            }
            if v.is_null() {
                continue;
            }
            let observed = InferredType::from_value(&v);
            let entry = types.entry(k).or_insert(None);
            *entry = Some(match *entry {
                None => observed,
                Some(prev) => prev.merge(observed),
            });
        }
    }
    if count == 0 {
        return Err(anyhow!("file is empty: {}", path.display()));
    }

    let mut yaml = String::from("        parser:\n          format: jsonl\n          columns:\n");
    let mut suggested_indexed = Vec::new();
    for key in &order {
        let t = types.get(key).copied().flatten().unwrap_or(InferredType::String);
        let path_expr = format!("$.{key}");
        yaml.push_str(&format!(
            "            - {{ name: {key}, path: \"{path_expr}\", type: {} }}\n",
            t.yaml()
        ));
        // Heuristic: zone-map for ordered numerics, bloom for short
        // string identifiers.
        match t {
            InferredType::Int64 | InferredType::Float64 | InferredType::Timestamp => {
                suggested_indexed.push(format!("- {{ name: {key}, kind: zone_map }}"));
            }
            InferredType::String if key.ends_with("_id") || key == "id" => {
                suggested_indexed.push(format!("- {{ name: {key}, kind: bloom }}"));
            }
            _ => {}
        }
    }

    Ok(ParserBody {
        parser_yaml: yaml,
        suggested_indexed,
    })
}

fn detect_csv(path: &Path, sample: usize) -> Result<ParserBody> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(f);
    let header: Vec<String> = rdr
        .headers()?
        .iter()
        .map(|s| s.to_string())
        .collect();
    if header.is_empty() {
        return Err(anyhow!("csv has no header row: {}", path.display()));
    }

    let mut types: Vec<Option<InferredType>> = vec![None; header.len()];
    let mut count = 0usize;
    for record in rdr.records() {
        if count >= sample {
            break;
        }
        let record = record?;
        for (i, raw) in record.iter().enumerate() {
            if i >= types.len() {
                continue;
            }
            if raw.is_empty() {
                continue;
            }
            let observed = InferredType::from_str_value(raw);
            types[i] = Some(match types[i] {
                None => observed,
                Some(prev) => prev.merge(observed),
            });
        }
        count += 1;
    }

    let mut yaml = String::from("        parser:\n          format: csv\n          has_header: true\n          columns:\n");
    let mut suggested_indexed = Vec::new();
    for (name, t) in header.iter().zip(types.iter()) {
        let t = t.unwrap_or(InferredType::String);
        yaml.push_str(&format!(
            "            - {{ name: {name}, source: {{ name: {name} }}, type: {} }}\n",
            t.yaml()
        ));
        match t {
            InferredType::Int64 | InferredType::Float64 | InferredType::Timestamp => {
                suggested_indexed.push(format!("- {{ name: {name}, kind: zone_map }}"));
            }
            InferredType::String if name.ends_with("_id") || name == "id" => {
                suggested_indexed.push(format!("- {{ name: {name}, kind: bloom }}"));
            }
            _ => {}
        }
    }
    Ok(ParserBody {
        parser_yaml: yaml,
        suggested_indexed,
    })
}

fn detect_logfmt(path: &Path, sample: usize) -> Result<ParserBody> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(f);

    let mut order: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = Default::default();
    let mut count = 0usize;
    for line in reader.lines() {
        if count >= sample {
            break;
        }
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        count += 1;
        for (k, _) in iter_logfmt(&line) {
            if seen.insert(k.clone()) {
                order.push(k);
            }
        }
    }
    if order.is_empty() {
        return Err(anyhow!("no logfmt key=value pairs found in sample"));
    }

    let mut yaml = String::from("        parser:\n          format: logfmt\n          columns:\n");
    for k in &order {
        yaml.push_str(&format!(
            "            - {{ name: {k}, key: {k}, type: string }}\n"
        ));
    }
    Ok(ParserBody {
        parser_yaml: yaml,
        suggested_indexed: Vec::new(),
    })
}

/// Tiny logfmt iterator: walks `key=value` (or `key="quoted value"`) pairs.
/// Mirrors the parser in dbfy-provider-rows-file but outputs `(key, value)`
/// pairs by reference. Quotes and `\\` / `\"` escapes are stripped.
fn iter_logfmt(line: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if key_start == i || i >= bytes.len() || bytes[i] != b'=' {
            // No `=` → skip token
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        let key = std::str::from_utf8(&bytes[key_start..i])
            .unwrap_or("")
            .to_string();
        i += 1; // consume `=`
        let value = if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let mut buf = String::new();
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    buf.push(bytes[i + 1] as char);
                    i += 2;
                } else {
                    buf.push(bytes[i] as char);
                    i += 1;
                }
            }
            if i < bytes.len() {
                i += 1;
            }
            buf
        } else {
            let v_start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            std::str::from_utf8(&bytes[v_start..i])
                .unwrap_or("")
                .to_string()
        };
        out.push((key, value));
    }
    out
}

fn detect_syslog_stanza() -> ParserBody {
    // RFC 5424 schema is fixed. Emit the standard column set.
    let yaml = "        parser:\n          format: syslog\n          columns:\n            \
        - { name: priority,  field: priority,    type: int64 }\n            \
        - { name: facility,  field: facility,    type: int64 }\n            \
        - { name: severity,  field: severity,    type: int64 }\n            \
        - { name: ts,        field: timestamp,   type: timestamp }\n            \
        - { name: host,      field: hostname,    type: string }\n            \
        - { name: app,       field: app_name,    type: string }\n            \
        - { name: proc_id,   field: proc_id,     type: string }\n            \
        - { name: msg_id,    field: msg_id,      type: string }\n            \
        - { name: message,   field: message,     type: string }\n";
    ParserBody {
        parser_yaml: yaml.to_string(),
        suggested_indexed: vec![
            "- { name: ts, kind: zone_map }".to_string(),
            "- { name: severity, kind: zone_map }".to_string(),
            "- { name: host, kind: bloom }".to_string(),
        ],
    }
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn render_yaml(path: &Path, opts: &DetectOpts, body: ParserBody) -> String {
    let abs = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf());
    let mut out = String::new();
    out.push_str("# Generated by `dbfy detect`. Tweak names, types, and indexed_columns as needed.\n");
    out.push_str("version: 1\n");
    out.push_str("sources:\n");
    out.push_str(&format!("  {}:\n", opts.source_name));
    out.push_str("    type: rows_file\n");
    out.push_str("    tables:\n");
    out.push_str(&format!("      {}:\n", opts.table_name));
    out.push_str(&format!("        path: \"{}\"\n", abs.display()));
    out.push_str(&body.parser_yaml);
    if !body.suggested_indexed.is_empty() {
        out.push_str("        indexed_columns:\n");
        for line in &body.suggested_indexed {
            out.push_str(&format!("          {line}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn jsonl_inference_promotes_mixed_numerics_to_float() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("a.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, r#"{{"x": 1, "y": "hi", "z": "2024-01-02T03:04:05Z"}}"#).unwrap();
        writeln!(f, r#"{{"x": 1.5, "y": "ho", "z": "2024-01-02T03:04:05Z"}}"#).unwrap();
        let body = detect_jsonl(&path, 100).unwrap();
        assert!(
            body.parser_yaml.contains("name: x, path: \"$.x\", type: float64"),
            "yaml was: {}",
            body.parser_yaml
        );
        assert!(body.parser_yaml.contains("type: string"));
        assert!(body.parser_yaml.contains("type: timestamp"));
    }

    #[test]
    fn csv_inference_uses_header_and_picks_int_for_pure_int_column() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("a.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "id,name,score").unwrap();
        writeln!(f, "1,Mario,9.5").unwrap();
        writeln!(f, "2,Anna,8.1").unwrap();
        let body = detect_csv(&path, 100).unwrap();
        assert!(body.parser_yaml.contains("name: id, source: { name: id }, type: int64"));
        assert!(body.parser_yaml.contains("name: score, source: { name: score }, type: float64"));
        assert!(body.parser_yaml.contains("name: name, source: { name: name }, type: string"));
    }

    #[test]
    fn logfmt_inference_collects_keys_in_first_seen_order() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("a.log");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "level=info msg=\"hi there\" req_id=42").unwrap();
        writeln!(f, "level=warn msg=\"bye\" tenant=acme").unwrap();
        let body = detect_logfmt(&path, 100).unwrap();
        let lvl_pos = body.parser_yaml.find("name: level").unwrap();
        let msg_pos = body.parser_yaml.find("name: msg").unwrap();
        let tenant_pos = body.parser_yaml.find("name: tenant").unwrap();
        assert!(lvl_pos < msg_pos && msg_pos < tenant_pos);
    }

    #[test]
    fn full_yaml_round_trip_parses_in_dbfy_config() {
        // The detected YAML must round-trip through Config::from_yaml_str.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("e.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, r#"{{"id": 1, "msg": "boot"}}"#).unwrap();
        writeln!(f, r#"{{"id": 2, "msg": "ready"}}"#).unwrap();

        let yaml = detect(
            path,
            DetectOpts {
                source_name: "logs".into(),
                table_name: "boot".into(),
                ..DetectOpts::default()
            },
        )
        .unwrap();

        let config = dbfy_config::Config::from_yaml_str(&yaml)
            .expect("detect output must be a valid dbfy config");
        assert!(config.sources.contains_key("logs"));
    }
}
