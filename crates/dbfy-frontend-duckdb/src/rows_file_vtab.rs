//! `dbfy_rows_file()` DuckDB table function.
//!
//! Streams a typed view of a JSONL / CSV / logfmt / regex / syslog file
//! (or a glob of them) into DuckDB, backed by the indexed `RowsFileTable`
//! / `RowsFileGlob` providers — so range and equality predicates push
//! down to zone maps + bloom filters when the user declares
//! `indexed_columns`.
//!
//! ```sql
//! LOAD 'dbfy.duckdb_extension';
//!
//! SELECT id, level, msg
//!   FROM dbfy_rows_file(
//!     '/var/log/app/events.jsonl',
//!     config := '
//! parser:
//!   format: jsonl
//!   columns:
//!     - { name: id,    path: "$.id",    type: int64 }
//!     - { name: level, path: "$.level", type: string }
//!     - { name: msg,   path: "$.msg",   type: string }
//! indexed_columns:
//!   - { name: id,    kind: zone_map }
//!   - { name: level, kind: bloom }
//! '
//!   )
//!   WHERE id BETWEEN 1000 AND 1100;
//! ```
//!
//! The first positional argument is the file path or glob pattern. The
//! `config` named argument is a YAML/JSON document with `parser`,
//! optional `indexed_columns`, and optional `chunk_rows` — the same
//! schema used inside `RowsFileTableConfig` minus `path/glob` (those
//! come from the positional arg).

use std::error::Error as StdError;
use std::sync::Mutex;

use arrow_array::RecordBatch;
use dbfy_config::{
    IndexedColumnConfig, ParserConfig, RowsFileTableConfig,
};
use dbfy_provider::ProgrammaticTableProvider;
use dbfy_provider_rows_file::{build_handle, RowsFileHandle};
use duckdb::Connection;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use futures::StreamExt;
use serde::Deserialize;
use tokio::runtime::Runtime;

use crate::arrow_to_duckdb::{cell_type_to_duckdb, write_arrow_column};

/// Slim YAML schema for the inline `config` argument.
///
/// Mirrors `RowsFileTableConfig` from `dbfy-config` but drops `path` /
/// `glob` (those come in via the positional URL argument). Composing the
/// two halves at bind time gives us a full `RowsFileTableConfig` that we
/// pass to `dbfy_provider_rows_file::build_handle`.
#[derive(Debug, Deserialize)]
struct ExtensionConfig {
    parser: ParserConfig,
    #[serde(default)]
    indexed_columns: Vec<IndexedColumnConfig>,
    #[serde(default)]
    chunk_rows: Option<usize>,
}

pub struct RowsFileBindData {
    target: String,
    config: ExtensionConfig,
    /// The order in which we declared columns to DuckDB (matching the
    /// parser's column order). `init` translates DuckDB's column-index
    /// projection back into names through this vector.
    columns_order: Vec<String>,
}

pub struct RowsFileInitData {
    batches: Vec<RecordBatch>,
    projection_names: Vec<String>,
    cursor: Mutex<usize>,
}

pub struct RowsFileVTab;

impl VTab for RowsFileVTab {
    type BindData = RowsFileBindData;
    type InitData = RowsFileInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn StdError>> {
        let target = bind.get_parameter(0).to_string();
        let config_yaml = bind
            .get_named_parameter("config")
            .map(|v| v.to_string())
            .ok_or_else(|| {
                Box::<dyn StdError>::from(
                    "dbfy_rows_file requires a `config` named argument with parser+columns",
                )
            })?;

        let config: ExtensionConfig = serde_yaml::from_str(&config_yaml)
            .map_err(|err| Box::<dyn StdError>::from(format!("invalid `config`: {err}")))?;

        // Declare each column to DuckDB. We reach into the `ParserConfig`
        // to extract the (name, type) pairs declared in the parser.
        let mut columns_order = Vec::new();
        for (name, t) in declared_columns(&config.parser) {
            bind.add_result_column(&name, cell_type_to_duckdb(t));
            columns_order.push(name);
        }
        if columns_order.is_empty() {
            return Err("dbfy_rows_file: parser must declare at least one column".into());
        }

        Ok(RowsFileBindData {
            target,
            config,
            columns_order,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn StdError>> {
        let bind: &RowsFileBindData = unsafe { &*info.get_bind_data::<RowsFileBindData>() };

        let column_indices = info.get_column_indices();
        let projection_names: Vec<String> = column_indices
            .iter()
            .filter_map(|&i| bind.columns_order.get(i as usize).cloned())
            .collect();

        // Compose `RowsFileTableConfig`: positional arg → path or glob,
        // inline YAML → parser + indexed_columns + chunk_rows.
        let table_config = compose_table_config(&bind.target, &bind.config);

        let handle = build_handle(&table_config)
            .map_err(|err| Box::<dyn StdError>::from(err.to_string()))?;

        let runtime = Runtime::new()?;
        let projection_for_provider = projection_names.clone();
        let batches: Vec<RecordBatch> = runtime.block_on(async move {
            let response = scan_handle(&handle, projection_for_provider)
                .await
                .map_err(|err| Box::<dyn StdError>::from(err.to_string()))?;
            let mut all = Vec::new();
            let mut s = response;
            while let Some(batch) = s.next().await {
                all.push(
                    batch.map_err(|err| Box::<dyn StdError>::from(err.to_string()))?,
                );
            }
            Ok::<_, Box<dyn StdError>>(all)
        })?;

        Ok(RowsFileInitData {
            batches,
            projection_names,
            cursor: Mutex::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn StdError>> {
        let init = func.get_init_data();
        let mut cursor = init.cursor.lock().expect("cursor mutex");
        if *cursor >= init.batches.len() {
            output.set_len(0);
            return Ok(());
        }
        let batch = &init.batches[*cursor];
        *cursor += 1;
        let n_rows = batch.num_rows();
        for (col_idx, col_name) in init.projection_names.iter().enumerate() {
            let arrow_col = batch
                .column_by_name(col_name)
                .or_else(|| batch.columns().get(col_idx))
                .ok_or_else(|| format!("missing column `{col_name}` in rows-file batch"))?;
            write_arrow_column(arrow_col, output, col_idx, n_rows)?;
        }
        output.set_len(n_rows);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![(
            "config".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )])
    }

    fn supports_pushdown() -> bool {
        true
    }
}

fn declared_columns(parser: &ParserConfig) -> Vec<(String, dbfy_config::CellTypeConfig)> {
    match parser {
        ParserConfig::Jsonl { columns } => columns
            .iter()
            .map(|c| (c.name.clone(), c.r#type))
            .collect(),
        ParserConfig::Csv { columns, .. } => columns
            .iter()
            .map(|c| (c.name.clone(), c.r#type))
            .collect(),
        ParserConfig::Logfmt { columns } => columns
            .iter()
            .map(|c| (c.name.clone(), c.r#type))
            .collect(),
        ParserConfig::Regex { columns, .. } => columns
            .iter()
            .map(|c| (c.name.clone(), c.r#type))
            .collect(),
        ParserConfig::Syslog { columns } => columns
            .iter()
            .map(|c| (c.name.clone(), c.r#type))
            .collect(),
    }
}

fn compose_table_config(target: &str, ext: &ExtensionConfig) -> RowsFileTableConfig {
    // Treat the positional arg as a glob if it contains glob metacharacters,
    // else as a single file path.
    let is_glob = target.chars().any(|c| matches!(c, '*' | '?' | '['));
    RowsFileTableConfig {
        path: if is_glob { None } else { Some(target.to_string()) },
        glob: if is_glob { Some(target.to_string()) } else { None },
        parser: ext.parser.clone(),
        indexed_columns: ext.indexed_columns.clone(),
        chunk_rows: ext.chunk_rows,
    }
}

async fn scan_handle(
    handle: &RowsFileHandle,
    projection: Vec<String>,
) -> dbfy_provider::ProviderResult<
    futures::stream::BoxStream<'static, dbfy_provider::ProviderResult<RecordBatch>>,
> {
    let request = dbfy_provider::ScanRequest {
        projection: if projection.is_empty() {
            None
        } else {
            Some(projection)
        },
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        query_id: "duckdb-vtab".to_string(),
    };
    let response = match handle {
        RowsFileHandle::Single(t) => t.scan(request).await?,
        RowsFileHandle::Glob(g) => g.scan(request).await?,
    };
    Ok(response.stream)
}

/// Register `dbfy_rows_file()` on a DuckDB connection.
pub fn register(conn: &Connection) -> duckdb::Result<()> {
    conn.register_table_function::<RowsFileVTab>("dbfy_rows_file")
}
