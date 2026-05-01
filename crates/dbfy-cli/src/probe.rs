//! REST endpoint introspection.
//!
//! Hits the URL once, finds the most likely root JSONPath that yields an
//! array of records, infers types from the first item, and emits a YAML
//! `sources.<name>.tables.<table>` stanza ready to paste into a config.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use serde_json_path::JsonPath;

#[derive(Debug, Clone)]
pub struct ProbeOpts {
    pub source_name: String,
    pub table_name: String,
    pub endpoint_path: Option<String>,
    /// Force a specific root JSONPath instead of the auto-detection candidates.
    pub root: Option<String>,
    /// Bearer token environment variable to send as `Authorization: Bearer …`.
    pub auth_bearer_env: Option<String>,
    /// HTTP timeout in seconds.
    pub timeout_seconds: u64,
}

impl Default for ProbeOpts {
    fn default() -> Self {
        Self {
            source_name: "api".to_string(),
            table_name: "items".to_string(),
            endpoint_path: None,
            root: None,
            auth_bearer_env: None,
            timeout_seconds: 30,
        }
    }
}

const ROOT_CANDIDATES: &[&str] = &[
    // Try array-shaped roots first; only fall back to `$` (treat the
    // whole body as one row, or an unwrapped top-level array) when none
    // of the well-known wrapper keys match.
    "$.data[*]",
    "$.results[*]",
    "$.items[*]",
    "$.records[*]",
    "$.rows[*]",
    "$.value[*]",   // OData
    "$.payload[*]",
    "$",
];

pub async fn probe(url: String, opts: ProbeOpts) -> Result<String> {
    let parsed = reqwest::Url::parse(&url)
        .with_context(|| format!("invalid URL `{url}`"))?;

    let base_url = format!(
        "{}://{}{}",
        parsed.scheme(),
        parsed.host_str().unwrap_or(""),
        parsed.port().map(|p| format!(":{p}")).unwrap_or_default(),
    );
    let endpoint_path = opts
        .endpoint_path
        .clone()
        .unwrap_or_else(|| {
            let p = parsed.path().to_string();
            if p.is_empty() { "/".to_string() } else { p }
        });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(opts.timeout_seconds))
        .build()?;
    let mut req = client.get(parsed.clone());
    if let Some(env_var) = &opts.auth_bearer_env {
        let token = std::env::var(env_var).with_context(|| {
            format!("environment variable `{env_var}` is not set")
        })?;
        req = req.bearer_auth(token);
    }
    let response = req.send().await.context("HTTP request failed")?;
    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("server returned HTTP {status}"));
    }
    let body: Value = response
        .json()
        .await
        .context("response was not valid JSON")?;

    let (root_expr, sample) = pick_root(&body, opts.root.as_deref())?;
    let columns = infer_columns(&sample);

    Ok(render_yaml(
        &opts,
        &base_url,
        &endpoint_path,
        &root_expr,
        &columns,
    ))
}

#[derive(Debug, Clone)]
struct InferredColumn {
    name: String,
    path: String,
    yaml_type: &'static str,
}

fn pick_root(body: &Value, override_root: Option<&str>) -> Result<(String, Value)> {
    let candidates: Vec<&str> = if let Some(r) = override_root {
        vec![r]
    } else {
        ROOT_CANDIDATES.to_vec()
    };
    for cand in candidates {
        let path = match JsonPath::parse(cand) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let nodes = path.query(body).all();
        if nodes.is_empty() {
            continue;
        }
        match cand {
            // `$` query returns one node — the root itself. Accept either
            // a top-level array (use first object element) or a top-level
            // object (treat whole body as one row).
            "$" => {
                let root = nodes[0];
                if let Some(arr) = root.as_array() {
                    if let Some(first) = arr.iter().find(|v| v.is_object()) {
                        return Ok(("$".to_string(), first.clone()));
                    }
                } else if root.is_object() {
                    return Ok(("$".to_string(), root.clone()));
                }
            }
            // `…[*]` queries return one node per array element. If we got
            // ≥1 object node, that array is our row source.
            _ => {
                if let Some(first) = nodes.iter().find(|v| v.is_object()) {
                    return Ok((cand.to_string(), (*first).clone()));
                }
            }
        }
    }
    Err(anyhow!(
        "could not auto-detect a row array; try --root \"$.your.path[*]\". Body sample: {}",
        truncate(&serde_json::to_string(body).unwrap_or_default(), 200)
    ))
}

fn infer_columns(sample: &Value) -> Vec<InferredColumn> {
    let Value::Object(map) = sample else {
        // Single primitive at root — emit one synthetic `value` column.
        return vec![InferredColumn {
            name: "value".to_string(),
            path: "$".to_string(),
            yaml_type: yaml_type_of(sample),
        }];
    };
    let mut out = Vec::with_capacity(map.len());
    for (k, v) in map {
        if v.is_object() || v.is_array() {
            // Nested — emit a json column (string in YAML, raw payload).
            out.push(InferredColumn {
                name: sanitize(k),
                path: format!("$.{k}"),
                yaml_type: "string",
            });
            continue;
        }
        out.push(InferredColumn {
            name: sanitize(k),
            path: format!("$.{k}"),
            yaml_type: yaml_type_of(v),
        });
    }
    out
}

fn yaml_type_of(v: &Value) -> &'static str {
    match v {
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "int64"
            } else {
                "float64"
            }
        }
        Value::Bool(_) => "boolean",
        Value::String(s) if looks_like_rfc3339(s) => "timestamp",
        _ => "string",
    }
}

fn looks_like_rfc3339(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 19 {
        return false;
    }
    b[4] == b'-'
        && b[7] == b'-'
        && (b[10] == b'T' || b[10] == b' ')
        && b[13] == b':'
        && b[16] == b':'
        && b[..4].iter().all(|c| c.is_ascii_digit())
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

fn render_yaml(
    opts: &ProbeOpts,
    base_url: &str,
    endpoint_path: &str,
    root_expr: &str,
    columns: &[InferredColumn],
) -> String {
    let mut s = String::new();
    s.push_str("# Generated by `dbfy probe`. Tweak names, types, and pushdown as needed.\n");
    s.push_str("version: 1\n");
    s.push_str("sources:\n");
    s.push_str(&format!("  {}:\n", opts.source_name));
    s.push_str("    type: rest\n");
    s.push_str(&format!("    base_url: \"{base_url}\"\n"));
    if let Some(env) = &opts.auth_bearer_env {
        s.push_str("    auth:\n");
        s.push_str("      type: bearer\n");
        s.push_str(&format!("      token_env: {env}\n"));
    }
    s.push_str("    tables:\n");
    s.push_str(&format!("      {}:\n", opts.table_name));
    s.push_str("        endpoint:\n");
    s.push_str("          method: GET\n");
    s.push_str(&format!("          path: \"{endpoint_path}\"\n"));
    s.push_str(&format!("        root: \"{root_expr}\"\n"));
    s.push_str("        columns:\n");
    for c in columns {
        s.push_str(&format!(
            "          {}:\n            path: \"{}\"\n            type: {}\n",
            c.name, c.path, c.yaml_type
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pick_root_finds_data_array() {
        let body = json!({
            "data": [
                { "id": 1, "name": "Mario" },
                { "id": 2, "name": "Anna" }
            ],
            "meta": { "total": 2 }
        });
        let (root, sample) = pick_root(&body, None).unwrap();
        assert_eq!(root, "$.data[*]");
        assert_eq!(sample, json!({ "id": 1, "name": "Mario" }));
    }

    #[test]
    fn pick_root_falls_back_to_results() {
        let body = json!({ "results": [{ "x": 1 }, { "x": 2 }] });
        let (root, _) = pick_root(&body, None).unwrap();
        assert_eq!(root, "$.results[*]");
    }

    #[test]
    fn pick_root_handles_top_level_array() {
        let body = json!([{ "k": "v" }, { "k": "z" }]);
        let (root, sample) = pick_root(&body, None).unwrap();
        assert_eq!(root, "$");
        assert_eq!(sample, json!({ "k": "v" }));
    }

    #[test]
    fn pick_root_explicit_override_wins() {
        let body = json!({ "data": [{ "x": 1 }], "results": [{ "y": 2 }] });
        let (root, sample) = pick_root(&body, Some("$.results[*]")).unwrap();
        assert_eq!(root, "$.results[*]");
        assert_eq!(sample, json!({ "y": 2 }));
    }

    #[test]
    fn infer_columns_classifies_types() {
        let sample = json!({
            "id": 7,
            "score": 9.5,
            "name": "Mario",
            "active": true,
            "ts": "2026-01-02T03:04:05Z",
            "tags": ["a", "b"],
        });
        let cols = infer_columns(&sample);
        let by_name: std::collections::HashMap<_, _> =
            cols.iter().map(|c| (c.name.as_str(), c.yaml_type)).collect();
        assert_eq!(by_name["id"], "int64");
        assert_eq!(by_name["score"], "float64");
        assert_eq!(by_name["name"], "string");
        assert_eq!(by_name["active"], "boolean");
        assert_eq!(by_name["ts"], "timestamp");
        assert_eq!(by_name["tags"], "string"); // nested → fallback
    }

    #[tokio::test]
    async fn probe_round_trip_against_wiremock() {
        // We don't pull `wiremock` here (it's heavy and reserved for the
        // provider-rest test surface). A simple in-process server using
        // `tokio::net::TcpListener` keeps the dep surface small.
        use std::io::Write;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/things");

        // Spawn a one-shot HTTP server that returns a fixed JSON body.
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await.unwrap();
            let body = br#"{"data":[{"id":1,"name":"Mario","active":true}]}"#;
            let mut resp = Vec::new();
            write!(
                resp,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .unwrap();
            resp.extend_from_slice(body);
            sock.write_all(&resp).await.unwrap();
        });

        let yaml = probe(
            url,
            ProbeOpts {
                source_name: "api".into(),
                table_name: "things".into(),
                ..ProbeOpts::default()
            },
        )
        .await
        .expect("probe");

        // Round-trip: the emitted YAML must parse as a valid dbfy config.
        let config = dbfy_config::Config::from_yaml_str(&yaml).expect("parse");
        assert!(config.sources.contains_key("api"));
        // And the inferred root + columns are sane.
        assert!(yaml.contains("root: \"$.data[*]\""));
        assert!(yaml.contains("type: int64"));
        assert!(yaml.contains("type: boolean"));
    }
}
