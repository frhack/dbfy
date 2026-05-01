//! Interactive `dbfy init` wizard. Asks two or three questions, then
//! delegates to `detect` (for files) or `probe` (for REST endpoints) to
//! produce a complete YAML config. The output goes to stdout so the user
//! can redirect with `> config.yaml`.
//!
//! The wizard is deliberately small — its job is to compose the existing
//! introspection commands, not to recreate them.

use std::path::PathBuf;

use anyhow::Result;
use dialoguer::{Input, Select, theme::ColorfulTheme};

use crate::detect::{self, DetectOpts, Format};
use crate::probe::{self, ProbeOpts};

#[derive(Debug, Clone)]
pub struct InitOpts {
    /// Pre-fill the source name (skips the prompt when set).
    pub source_name: Option<String>,
    /// Pre-fill the table name (skips the prompt when set).
    pub table_name: Option<String>,
    /// Skip the kind prompt when set.
    pub kind: Option<SourceKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Rest,
    RowsFile,
}

impl Default for InitOpts {
    fn default() -> Self {
        Self {
            source_name: None,
            table_name: None,
            kind: None,
        }
    }
}

pub async fn init(opts: InitOpts) -> Result<String> {
    let theme = ColorfulTheme::default();

    let kind = match opts.kind {
        Some(k) => k,
        None => {
            let idx = Select::with_theme(&theme)
                .with_prompt("Source kind")
                .items(&[
                    "REST endpoint (HTTP/JSON)",
                    "rows-file (jsonl/csv/syslog/logfmt)",
                ])
                .default(0)
                .interact()?;
            if idx == 0 {
                SourceKind::Rest
            } else {
                SourceKind::RowsFile
            }
        }
    };

    let default_source = match kind {
        SourceKind::Rest => "api",
        SourceKind::RowsFile => "files",
    };
    let source_name = match opts.source_name {
        Some(s) => s,
        None => Input::with_theme(&theme)
            .with_prompt("Source name")
            .default(default_source.to_string())
            .interact_text()?,
    };

    let default_table = match kind {
        SourceKind::Rest => "items",
        SourceKind::RowsFile => "events",
    };
    let table_name = match opts.table_name {
        Some(t) => t,
        None => Input::with_theme(&theme)
            .with_prompt("Table name")
            .default(default_table.to_string())
            .interact_text()?,
    };

    match kind {
        SourceKind::RowsFile => {
            let path: String = Input::with_theme(&theme)
                .with_prompt("File path")
                .interact_text()?;
            let path_buf = PathBuf::from(path);
            let format_idx = Select::with_theme(&theme)
                .with_prompt("Format (auto picks from extension)")
                .items(&["auto", "jsonl", "csv", "logfmt", "syslog"])
                .default(0)
                .interact()?;
            let format = match format_idx {
                0 => None,
                1 => Some(Format::Jsonl),
                2 => Some(Format::Csv),
                3 => Some(Format::Logfmt),
                _ => Some(Format::Syslog),
            };
            detect::detect(
                path_buf,
                DetectOpts {
                    format,
                    sample: 200,
                    source_name,
                    table_name,
                },
            )
        }
        SourceKind::Rest => {
            let url: String = Input::with_theme(&theme)
                .with_prompt("Endpoint URL (e.g. https://api.example.com/items)")
                .interact_text()?;
            let auth_idx = Select::with_theme(&theme)
                .with_prompt("Auth")
                .items(&["none", "bearer (env var)"])
                .default(0)
                .interact()?;
            let auth_bearer_env = if auth_idx == 1 {
                let env: String = Input::with_theme(&theme)
                    .with_prompt("Env var holding the bearer token")
                    .default("API_TOKEN".to_string())
                    .interact_text()?;
                Some(env)
            } else {
                None
            };
            probe::probe(
                url,
                ProbeOpts {
                    source_name,
                    table_name,
                    endpoint_path: None,
                    root: None,
                    auth_bearer_env,
                    timeout_seconds: 30,
                },
            )
            .await
        }
    }
}
