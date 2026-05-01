//! `dbfy_rest()` DuckDB table function — v1 with typed columns.
//!
//! Two usage modes:
//!
//! ## Untyped exploration (one positional argument)
//!
//! ```sql
//! SELECT value FROM dbfy_rest('https://api.example.com/users');
//! ```
//!
//! Single `VARCHAR` column `value` containing the raw JSON object of each
//! row at `$.data[*]`. Useful for poking at an unknown endpoint.
//!
//! ## Typed via inline config (named `config` argument)
//!
//! ```sql
//! SELECT id, name, status FROM dbfy_rest(
//!     'https://api.example.com/users',
//!     config := '
//! root: $.data[*]
//! columns:
//!   id:     {path: "$.id",     type: int64}
//!   name:   {path: "$.name",   type: string}
//!   status: {path: "$.status", type: string}
//! '
//! );
//! ```
//!
//! The `config` value is a YAML/JSON string deserialised into the same
//! [`dbfy_config::ColumnConfig`] / [`dbfy_config::PaginationConfig`] /
//! [`dbfy_config::PushdownConfig`] / [`dbfy_config::AuthConfig`] types
//! used by the DataFusion frontend. Columns are declared to DuckDB with
//! their proper logical types; pagination, retry and auth flow through
//! the shared `dbfy-provider-rest` machinery.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::Mutex;

use arrow_array::RecordBatch;
use dbfy_config::{
    AuthConfig, ColumnConfig, DataType, EndpointConfig, HttpMethod, PaginationConfig,
    PushdownConfig, RestSourceConfig, RestTableConfig,
};
use dbfy_provider_rest::RestTable;
use duckdb::Connection;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use futures::StreamExt;
use serde::Deserialize;
use tokio::runtime::Runtime;
use url::Url;

use crate::arrow_to_duckdb::{arrow_data_type_to_duckdb, write_arrow_column};

#[derive(Debug, Deserialize)]
struct ExtensionConfig {
    #[serde(default = "default_root")]
    root: String,
    columns: BTreeMap<String, ColumnConfig>,
    #[serde(default)]
    pagination: Option<PaginationConfig>,
    #[serde(default)]
    pushdown: Option<PushdownConfig>,
    #[serde(default)]
    auth: Option<AuthConfig>,
}

fn default_root() -> String {
    "$.data[*]".to_string()
}

fn untyped_default_config() -> ExtensionConfig {
    ExtensionConfig {
        root: default_root(),
        columns: BTreeMap::from([(
            "value".to_string(),
            ColumnConfig {
                path: "$".to_string(),
                r#type: DataType::Json,
            },
        )]),
        pagination: None,
        pushdown: None,
        auth: None,
    }
}

pub struct RestBindData {
    base_url: String,
    path: String,
    config: ExtensionConfig,
    columns_order: Vec<String>,
}

pub struct RestInitData {
    batches: Vec<RecordBatch>,
    /// Column names actually projected (in DuckDB-output order). Used by
    /// `func` to look up the right Arrow column for each output slot.
    /// Empty means "all columns" (no projection pushdown applied).
    projection_names: Vec<String>,
    cursor: Mutex<usize>,
}

pub struct RestVTab;

impl VTab for RestVTab {
    type BindData = RestBindData;
    type InitData = RestInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn StdError>> {
        let url_param = bind.get_parameter(0).to_string();
        let config_yaml = bind
            .get_named_parameter("config")
            .map(|v| v.to_string());

        let config = match config_yaml {
            Some(yaml) if !yaml.trim().is_empty() => serde_yaml::from_str::<ExtensionConfig>(&yaml)
                .map_err(|err| Box::<dyn StdError>::from(format!("invalid `config`: {err}")))?,
            _ => untyped_default_config(),
        };

        if config.columns.is_empty() {
            return Err("dbfy_rest config must define at least one column".into());
        }

        let parsed = Url::parse(&url_param)?;
        let scheme = parsed.scheme();
        let host = parsed
            .host_str()
            .ok_or_else(|| "dbfy_rest URL must include a host".to_string())?;
        let port = parsed
            .port()
            .map(|p| format!(":{p}"))
            .unwrap_or_default();
        let base_url = format!("{scheme}://{host}{port}");
        let path = parsed.path().to_string();

        let mut columns_order = Vec::with_capacity(config.columns.len());
        for (col_name, col_cfg) in &config.columns {
            let logical_type = arrow_data_type_to_duckdb(&col_cfg.r#type);
            bind.add_result_column(col_name, logical_type);
            columns_order.push(col_name.clone());
        }

        Ok(RestBindData {
            base_url,
            path,
            config,
            columns_order,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn StdError>> {
        let bind: &RestBindData = unsafe { &*info.get_bind_data::<RestBindData>() };

        // Projection pushdown: DuckDB tells us via `get_column_indices`
        // which columns the consumer actually needs. Translate indices
        // back to column names (using the order we declared in `bind`)
        // and feed them to `RestTable::execute_stream` so the provider
        // only parses the JSON paths the user asked for.
        let column_indices = info.get_column_indices();
        let projection_names: Vec<String> = column_indices
            .iter()
            .filter_map(|&i| bind.columns_order.get(i as usize).cloned())
            .collect();

        let table_config = RestTableConfig {
            endpoint: EndpointConfig {
                method: HttpMethod::Get,
                path: bind.path.clone(),
            },
            root: bind.config.root.clone(),
            primary_key: None,
            include_raw_json: false,
            raw_column: "_raw".to_string(),
            columns: bind.config.columns.clone(),
            pagination: bind.config.pagination.clone(),
            pushdown: bind.config.pushdown.clone(),
        };

        let source_config = RestSourceConfig {
            base_url: bind.base_url.clone(),
            auth: bind.config.auth.clone(),
            runtime: None,
            tables: BTreeMap::new(),
        };

        let table = RestTable::new("dbfy_rest", &source_config, "vtab", table_config);

        let runtime = Runtime::new()?;
        let projection_for_provider: Vec<String> = projection_names.clone();
        let batches: Vec<RecordBatch> = runtime.block_on(async move {
            let stream = table
                .execute_stream(&projection_for_provider, &[], None)
                .map_err(|err| Box::<dyn StdError>::from(err.to_string()))?;
            let mut all = Vec::new();
            let mut chunks = stream.stream;
            while let Some(batch_result) = chunks.next().await {
                all.push(
                    batch_result.map_err(|err| Box::<dyn StdError>::from(err.to_string()))?,
                );
            }
            Ok::<_, Box<dyn StdError>>(all)
        })?;

        Ok(RestInitData {
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

        // The batch already contains only the projected columns (in the
        // order DuckDB asked for), so we map output slot N to batch
        // column N — but look it up by name to be defensive against any
        // reordering the provider might do.
        for (col_idx, col_name) in init.projection_names.iter().enumerate() {
            let arrow_col = batch
                .column_by_name(col_name)
                .or_else(|| batch.columns().get(col_idx))
                .ok_or_else(|| format!("missing column `{col_name}` in REST batch"))?;
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

    /// Enable DuckDB projection pushdown: the engine will tell us which
    /// columns the consumer needs and we forward that to the REST
    /// provider so the JSON-side work is bounded by the projection.
    fn supports_pushdown() -> bool {
        true
    }
}

/// Register the `dbfy_rest()` table function on a DuckDB connection.
pub fn register(conn: &Connection) -> duckdb::Result<()> {
    conn.register_table_function::<RestVTab>("dbfy_rest")
}
