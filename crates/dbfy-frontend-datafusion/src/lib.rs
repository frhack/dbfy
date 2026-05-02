use std::any::Any;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::{Statistics, TableReference};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    Expr, LogicalPlan, Operator, TableProviderFilterPushDown, TableScan,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use dbfy_config::{Config, RowsFileTableConfig, SourceConfig};
use dbfy_provider::{
    DynProvider, FilterOperator, ProviderCapabilities, ProviderError, ScalarValue, ScanRequest,
    SimpleFilter,
};
use dbfy_provider_rest::{RestProviderError, RestTable, SimpleFilter as RestSimpleFilter};
pub use dbfy_provider_rows_file::RowsFileHandle;
use futures::StreamExt;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Config(#[from] dbfy_config::ConfigError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Rest(#[from] RestProviderError),
    #[error("rows-file source `{source_name}.{table}` configuration is invalid: {message}")]
    RowsFileConfig {
        source_name: String,
        table: String,
        message: String,
    },
    #[error("table `{0}` is not registered")]
    UnknownTable(String),
    #[error("catalog `{0}` is not available in the current DataFusion session")]
    MissingCatalog(String),
    #[error("provider `{table}` returned a schema that does not match the table definition")]
    ProviderSchemaMismatch { table: String },
    #[error("programmatic filter pushdown is not implemented for expression `{0:?}`")]
    UnsupportedFilterExpression(Expr),
    #[error("explain currently supports only table scans and simple unary plans: {0}")]
    UnsupportedExplain(String),
    #[error("query execution is not implemented yet: {0}")]
    NotYetImplemented(&'static str),
}

static QUERY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
pub struct Engine {
    config: Option<Config>,
    rest_tables: BTreeMap<String, RestTable>,
    programmatic_tables: BTreeMap<String, DynProvider>,
    /// Sources that need async setup at session-build time
    /// (parquet schema discovery, excel/postgres materialisation,
    /// graphql query execution). Stored verbatim and replayed in
    /// `build_session_context`.
    deferred_sources: Vec<DeferredSource>,
}

/// Sources whose registration into a `SessionContext` happens at
/// `build_session_context()` time because it's async (or because we
/// materialise into a `MemTable` from a one-shot fetch).
#[derive(Debug, Clone)]
enum DeferredSource {
    Parquet {
        qualified: String,
        path: String,
    },
    Excel {
        qualified: String,
        cfg: dbfy_config::ExcelTableConfig,
    },
    Graphql {
        qualified: String,
        endpoint: String,
        auth: Option<dbfy_config::AuthConfig>,
        table: dbfy_config::GraphqlTableConfig,
    },
    Postgres {
        qualified: String,
        connection: String,
        relation: String,
    },
}

impl Engine {
    pub fn from_config(config: Config) -> Result<Self> {
        let mut engine = Self {
            config: Some(config.clone()),
            ..Self::default()
        };

        for (source_name, source) in &config.sources {
            match source {
                SourceConfig::Rest(rest) => {
                    for (table_name, table_config) in &rest.tables {
                        engine.rest_tables.insert(
                            qualify_table_name(source_name, table_name),
                            RestTable::new(source_name, rest, table_name, table_config.clone()),
                        );
                    }
                }
                SourceConfig::RowsFile(rf) => {
                    for (table_name, table_config) in &rf.tables {
                        let provider =
                            build_rows_file_provider(source_name, table_name, table_config)?;
                        engine
                            .programmatic_tables
                            .insert(qualify_table_name(source_name, table_name), provider);
                    }
                }
                SourceConfig::Parquet(p) => {
                    for (table_name, table_cfg) in &p.tables {
                        engine.deferred_sources.push(DeferredSource::Parquet {
                            qualified: qualify_table_name(source_name, table_name),
                            path: table_cfg.path.clone(),
                        });
                    }
                }
                dbfy_config::SourceConfig::Excel(e) => {
                    for (table_name, table_cfg) in &e.tables {
                        engine.deferred_sources.push(DeferredSource::Excel {
                            qualified: qualify_table_name(source_name, table_name),
                            cfg: table_cfg.clone(),
                        });
                    }
                }
                SourceConfig::Graphql(g) => {
                    for (table_name, table_cfg) in &g.tables {
                        engine.deferred_sources.push(DeferredSource::Graphql {
                            qualified: qualify_table_name(source_name, table_name),
                            endpoint: g.endpoint.clone(),
                            auth: g.auth.clone(),
                            table: table_cfg.clone(),
                        });
                    }
                }
                dbfy_config::SourceConfig::Postgres(pg) => {
                    for (table_name, table_cfg) in &pg.tables {
                        engine.deferred_sources.push(DeferredSource::Postgres {
                            qualified: qualify_table_name(source_name, table_name),
                            connection: pg.connection.clone(),
                            relation: table_cfg.relation.clone(),
                        });
                    }
                }
            }
        }

        Ok(engine)
    }

    pub fn from_config_file(path: impl AsRef<Path>) -> Result<Self> {
        let config = Config::from_path(path)?;
        Self::from_config(config)
    }

    pub fn register_provider(
        &mut self,
        table_name: impl Into<String>,
        provider: DynProvider,
    ) -> Result<()> {
        self.programmatic_tables.insert(table_name.into(), provider);
        Ok(())
    }

    pub fn config(&self) -> Option<&Config> {
        self.config.as_ref()
    }

    pub fn registered_tables(&self) -> Vec<String> {
        let mut tables = self.rest_tables.keys().cloned().collect::<Vec<_>>();
        tables.extend(self.programmatic_tables.keys().cloned());
        tables.sort();
        tables
    }

    pub fn inspect_rest_table(&self, table_name: &str) -> Result<&RestTable> {
        self.rest_tables
            .get(table_name)
            .ok_or_else(|| EngineError::UnknownTable(table_name.to_string()))
    }

    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let ctx = self.build_session_context().await?;
        let dataframe = ctx.sql(sql).await?;
        let batches = dataframe.collect().await?;
        Ok(batches)
    }

    pub async fn explain(&self, sql: &str) -> Result<String> {
        let ctx = self.build_session_context().await?;
        let dataframe = ctx.sql(sql).await?;
        let plan = dataframe.into_optimized_plan()?;
        render_explain(self, sql, &plan)
    }

    async fn build_session_context(&self) -> Result<SessionContext> {
        let ctx = SessionContext::new();

        for (table_name, table) in &self.rest_tables {
            let provider = Arc::new(RestDataFusionTableProvider::new(table.clone()));
            register_table(&ctx, table_name, provider)?;
        }

        for (table_name, provider) in &self.programmatic_tables {
            let provider = Arc::new(ProgrammaticDataFusionTableProvider::new(
                table_name.clone(),
                provider.clone(),
            ));
            register_table(&ctx, table_name, provider)?;
        }

        for source in &self.deferred_sources {
            match source {
                DeferredSource::Parquet { qualified, path } => {
                    register_parquet(&ctx, qualified, path).await?;
                }
                DeferredSource::Excel { qualified, cfg } => {
                    let provider = ExcelTableProvider::new(cfg.clone())?;
                    register_table(&ctx, qualified, Arc::new(provider))?;
                }
                DeferredSource::Graphql {
                    qualified,
                    endpoint,
                    auth,
                    table,
                } => {
                    let provider =
                        GraphqlTableProvider::new(endpoint.clone(), auth.clone(), table.clone())?;
                    register_table(&ctx, qualified, Arc::new(provider))?;
                }
                DeferredSource::Postgres {
                    qualified,
                    connection,
                    relation,
                } => {
                    let provider =
                        PostgresTableProvider::discover(connection.clone(), relation.clone())
                            .await?;
                    register_table(&ctx, qualified, Arc::new(provider))?;
                }
            }
        }

        Ok(ctx)
    }
}

fn qualify_table_name(source_name: &str, table_name: &str) -> String {
    format!("{source_name}.{table_name}")
}

/// Materialise a typed `RowsFileTable` / `RowsFileGlob` from a YAML config.
/// Thin wrapper around [`dbfy_provider_rows_file::build_handle`] that maps
/// rows-file's `ProviderError` into the engine's typed error.
pub fn build_rows_file_handle(
    source_name: &str,
    table_name: &str,
    cfg: &RowsFileTableConfig,
) -> Result<RowsFileHandle> {
    dbfy_provider_rows_file::build_handle(cfg).map_err(|err| EngineError::RowsFileConfig {
        source_name: source_name.to_string(),
        table: table_name.to_string(),
        message: err.to_string(),
    })
}

fn build_rows_file_provider(
    source_name: &str,
    table_name: &str,
    cfg: &RowsFileTableConfig,
) -> Result<DynProvider> {
    Ok(build_rows_file_handle(source_name, table_name, cfg)?.into_dyn())
}

fn register_table(
    ctx: &SessionContext,
    table_name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<()> {
    match TableReference::parse_str(table_name) {
        TableReference::Bare { table } => {
            ctx.register_table(TableReference::bare(table), provider)?;
        }
        TableReference::Partial { schema, table } => {
            let default_catalog = ctx
                .copied_config()
                .options()
                .catalog
                .default_catalog
                .clone();
            ensure_schema(ctx, &default_catalog, &schema)?;
            ctx.register_table(TableReference::partial(schema, table), provider)?;
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            ensure_schema(ctx, &catalog, &schema)?;
            ctx.register_table(TableReference::full(catalog, schema, table), provider)?;
        }
    }

    Ok(())
}

fn ensure_schema(ctx: &SessionContext, catalog_name: &str, schema_name: &str) -> Result<()> {
    let Some(catalog) = ctx.catalog(catalog_name) else {
        if catalog_name == ctx.copied_config().options().catalog.default_catalog {
            let catalog = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;
            ctx.register_catalog(catalog_name.to_string(), catalog);
        } else {
            let catalog = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;
            ctx.register_catalog(catalog_name.to_string(), catalog);
        }
        return ensure_schema(ctx, catalog_name, schema_name);
    };

    if catalog.schema(schema_name).is_none() {
        catalog.register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))?;
    }

    Ok(())
}

// ----------------------------------------------------------------
// Helpers for the four async / batch-materialised sources.
// ----------------------------------------------------------------

/// Register a Parquet file (or directory / glob of files) under the
/// qualified name. Uses DataFusion's native `register_parquet` which
/// installs a `ListingTable` — predicate, projection and row-group
/// pushdown into the parquet reader work natively (no
/// materialise-then-filter). Schemas are auto-discovered.
async fn register_parquet(ctx: &SessionContext, qualified: &str, path: &str) -> Result<()> {
    use datafusion::prelude::ParquetReadOptions;

    // `register_parquet` doesn't auto-create schemas the way our
    // manual `register_table` does, so mirror the qualifier handling
    // here for `source.table` / `catalog.source.table` references.
    let table_ref = TableReference::parse_str(qualified);
    match &table_ref {
        TableReference::Partial { schema, .. } => {
            let default_catalog = ctx
                .copied_config()
                .options()
                .catalog
                .default_catalog
                .clone();
            ensure_schema(ctx, &default_catalog, schema)?;
        }
        TableReference::Full {
            catalog, schema, ..
        } => {
            ensure_schema(ctx, catalog, schema)?;
        }
        TableReference::Bare { .. } => {}
    }

    ctx.register_parquet(table_ref, path, ParquetReadOptions::default())
        .await
        .map_err(DataFusionError::from)?;
    Ok(())
}

// ----------------------------------------------------------------
// Pushdown: a tiny shared helper that turns a DataFusion `Expr`
// representing `column OP literal` (or `column IS [NOT] NULL`) into
// a structured `(column, op, literal)` triple, plus a flag for
// `IS NULL` / `IS NOT NULL`. Returns None for anything we don't know
// how to push down — the caller then reports `Unsupported` to
// DataFusion so the engine evaluates the predicate above the scan.
// ----------------------------------------------------------------

#[derive(Debug, Clone)]
enum PushedOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    IsNull,
    IsNotNull,
    In, // value is a comma-joined list of the IN-list literals
}

#[derive(Debug, Clone)]
struct PushedFilter {
    column: String,
    op: PushedOp,
    /// Always populated except for `IS [NOT] NULL`. For `IN`, the
    /// individual list elements are joined later by each backend.
    literal: Option<datafusion::scalar::ScalarValue>,
    /// IN-list values (empty for non-IN).
    in_values: Vec<datafusion::scalar::ScalarValue>,
}

fn try_extract_pushed_filter(expr: &Expr) -> Option<PushedFilter> {
    match expr {
        Expr::BinaryExpr(bin) => {
            let (col, lit, flipped) = match (&*bin.left, &*bin.right) {
                (Expr::Column(c), Expr::Literal(s, _)) => (c.name.clone(), s.clone(), false),
                (Expr::Literal(s, _), Expr::Column(c)) => (c.name.clone(), s.clone(), true),
                _ => return None,
            };
            let op = match (bin.op, flipped) {
                (Operator::Eq, _) => PushedOp::Eq,
                (Operator::NotEq, _) => PushedOp::NotEq,
                (Operator::Lt, false) | (Operator::Gt, true) => PushedOp::Lt,
                (Operator::LtEq, false) | (Operator::GtEq, true) => PushedOp::LtEq,
                (Operator::Gt, false) | (Operator::Lt, true) => PushedOp::Gt,
                (Operator::GtEq, false) | (Operator::LtEq, true) => PushedOp::GtEq,
                _ => return None,
            };
            Some(PushedFilter {
                column: col,
                op,
                literal: Some(lit),
                in_values: vec![],
            })
        }
        Expr::IsNull(inner) => {
            if let Expr::Column(c) = &**inner {
                Some(PushedFilter {
                    column: c.name.clone(),
                    op: PushedOp::IsNull,
                    literal: None,
                    in_values: vec![],
                })
            } else {
                None
            }
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Column(c) = &**inner {
                Some(PushedFilter {
                    column: c.name.clone(),
                    op: PushedOp::IsNotNull,
                    literal: None,
                    in_values: vec![],
                })
            } else {
                None
            }
        }
        Expr::InList(in_list) => {
            if in_list.negated {
                return None;
            }
            let col = match &*in_list.expr {
                Expr::Column(c) => c.name.clone(),
                _ => return None,
            };
            let mut vals = Vec::with_capacity(in_list.list.len());
            for v in &in_list.list {
                match v {
                    Expr::Literal(s, _) => vals.push(s.clone()),
                    _ => return None,
                }
            }
            if vals.is_empty() {
                return None;
            }
            Some(PushedFilter {
                column: col,
                op: PushedOp::In,
                literal: None,
                in_values: vals,
            })
        }
        _ => None,
    }
}

fn scalar_to_sql_literal(s: &datafusion::scalar::ScalarValue) -> Option<String> {
    use datafusion::scalar::ScalarValue as S;
    match s {
        S::Boolean(Some(v)) => Some(v.to_string()),
        S::Int8(Some(v)) => Some(v.to_string()),
        S::Int16(Some(v)) => Some(v.to_string()),
        S::Int32(Some(v)) => Some(v.to_string()),
        S::Int64(Some(v)) => Some(v.to_string()),
        S::UInt8(Some(v)) => Some(v.to_string()),
        S::UInt16(Some(v)) => Some(v.to_string()),
        S::UInt32(Some(v)) => Some(v.to_string()),
        S::UInt64(Some(v)) => Some(v.to_string()),
        S::Float32(Some(v)) => Some(v.to_string()),
        S::Float64(Some(v)) => Some(v.to_string()),
        S::Utf8(Some(s)) | S::LargeUtf8(Some(s)) => Some(format!("'{}'", s.replace('\'', "''"))),
        _ => None,
    }
}

fn scalar_to_plain_string(s: &datafusion::scalar::ScalarValue) -> Option<String> {
    use datafusion::scalar::ScalarValue as S;
    match s {
        S::Boolean(Some(v)) => Some(v.to_string()),
        S::Int8(Some(v)) => Some(v.to_string()),
        S::Int16(Some(v)) => Some(v.to_string()),
        S::Int32(Some(v)) => Some(v.to_string()),
        S::Int64(Some(v)) => Some(v.to_string()),
        S::Float32(Some(v)) => Some(v.to_string()),
        S::Float64(Some(v)) => Some(v.to_string()),
        S::Utf8(Some(s)) | S::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

// ----------------------------------------------------------------
// PostgresTableProvider — pushes filter / projection / limit into a
// `SELECT ... FROM <relation> WHERE ... LIMIT N` over the wire
// protocol so the Postgres planner can use indexes.
// ----------------------------------------------------------------

#[derive(Debug)]
struct PostgresTableProvider {
    connection: String,
    relation: String,
    schema: SchemaRef,
}

impl PostgresTableProvider {
    async fn discover(connection: String, relation: String) -> Result<Self> {
        use tokio_postgres::NoTls;

        let (client, conn) = tokio_postgres::connect(&connection, NoTls)
            .await
            .map_err(|err| {
                EngineError::NotYetImplemented(Box::leak(
                    format!("postgres: connect failed: {err}").into_boxed_str(),
                ))
            })?;
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // Run a zero-row prepared SELECT to extract column metadata.
        let stmt = client
            .prepare(&format!("SELECT * FROM {relation} LIMIT 0"))
            .await
            .map_err(|err| {
                EngineError::NotYetImplemented(Box::leak(
                    format!("postgres: prepare for `{relation}` failed: {err}").into_boxed_str(),
                ))
            })?;

        let fields = pg_columns_to_arrow_fields(stmt.columns());
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        Ok(Self {
            connection,
            relation,
            schema,
        })
    }

    /// Translate a single `Expr` into a Postgres `WHERE` fragment. Used
    /// both for `supports_filters_pushdown` (yes/no) and for actually
    /// building the SQL in `scan`.
    fn translate_filter(filter: &Expr) -> Option<String> {
        let p = try_extract_pushed_filter(filter)?;
        let col = format!("\"{}\"", p.column.replace('"', "\"\""));
        match p.op {
            PushedOp::IsNull => Some(format!("{col} IS NULL")),
            PushedOp::IsNotNull => Some(format!("{col} IS NOT NULL")),
            PushedOp::In => {
                let parts: Vec<String> = p
                    .in_values
                    .iter()
                    .filter_map(scalar_to_sql_literal)
                    .collect();
                if parts.len() != p.in_values.len() {
                    return None;
                }
                Some(format!("{col} IN ({})", parts.join(", ")))
            }
            op => {
                let lit = scalar_to_sql_literal(p.literal.as_ref()?)?;
                let op_str = match op {
                    PushedOp::Eq => "=",
                    PushedOp::NotEq => "!=",
                    PushedOp::Lt => "<",
                    PushedOp::LtEq => "<=",
                    PushedOp::Gt => ">",
                    PushedOp::GtEq => ">=",
                    _ => unreachable!(),
                };
                Some(format!("{col} {op_str} {lit}"))
            }
        }
    }

    fn build_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> String {
        let cols = match projection {
            None => "*".to_string(),
            Some(indices) if indices.is_empty() => "1".to_string(),
            Some(indices) => indices
                .iter()
                .map(|i| format!("\"{}\"", self.schema.field(*i).name().replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", "),
        };
        let mut sql = format!("SELECT {cols} FROM {}", self.relation);
        let where_clauses: Vec<String> =
            filters.iter().filter_map(Self::translate_filter).collect();
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }
}

#[async_trait]
impl TableProvider for PostgresTableProvider {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|f| {
                if Self::translate_filter(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        use tokio_postgres::NoTls;

        let target_schema = projected_schema(self.schema.clone(), projection)?;
        let sql = self.build_sql(projection, filters, limit);

        let (client, conn) = tokio_postgres::connect(&self.connection, NoTls)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let rows = client
            .query(sql.as_str(), &[])
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let batch = pg_rows_to_batch_with_schema(
            &rows,
            &target_schema,
            projection.is_some_and(|p| p.is_empty()),
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let exec: Arc<dyn ExecutionPlan> =
            datafusion::catalog::memory::MemorySourceConfig::try_new_exec(
                &[vec![batch]],
                target_schema,
                None,
            )?;
        Ok(exec)
    }
}

fn pg_columns_to_arrow_fields(columns: &[tokio_postgres::Column]) -> Vec<arrow_schema::Field> {
    use arrow_schema::{DataType as ArrowDataType, Field};
    use tokio_postgres::types::Type as PgType;

    columns
        .iter()
        .map(|col| {
            let dt = match *col.type_() {
                PgType::BOOL => ArrowDataType::Boolean,
                PgType::INT2 | PgType::INT4 | PgType::INT8 => ArrowDataType::Int64,
                PgType::FLOAT4 | PgType::FLOAT8 => ArrowDataType::Float64,
                _ => ArrowDataType::Utf8,
            };
            Field::new(col.name(), dt, true)
        })
        .collect()
}

fn pg_rows_to_batch_with_schema(
    rows: &[tokio_postgres::Row],
    target_schema: &SchemaRef,
    is_count_star: bool,
) -> Result<RecordBatch> {
    use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use tokio_postgres::types::Type as PgType;

    if is_count_star {
        // count(*) projection — empty schema, only num_rows matters.
        let opts = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(target_schema.clone(), vec![], &opts).map_err(
            |err| {
                EngineError::NotYetImplemented(Box::leak(
                    format!("postgres count(*): {err}").into_boxed_str(),
                ))
            },
        );
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    for (out_idx, out_field) in target_schema.fields().iter().enumerate() {
        match out_field.data_type() {
            arrow_schema::DataType::Boolean => {
                let vals: Vec<Option<bool>> =
                    rows.iter().map(|r| r.try_get(out_idx).ok()).collect();
                arrays.push(Arc::new(BooleanArray::from(vals)));
            }
            arrow_schema::DataType::Int64 => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| {
                        // Try i64 first, then fallback to i32/i16 widening.
                        r.try_get::<_, i64>(out_idx)
                            .ok()
                            .or_else(|| r.try_get::<_, i32>(out_idx).ok().map(i64::from))
                            .or_else(|| r.try_get::<_, i16>(out_idx).ok().map(i64::from))
                    })
                    .collect();
                arrays.push(Arc::new(Int64Array::from(vals)));
            }
            arrow_schema::DataType::Float64 => {
                let vals: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| {
                        r.try_get::<_, f64>(out_idx)
                            .ok()
                            .or_else(|| r.try_get::<_, f32>(out_idx).ok().map(f64::from))
                    })
                    .collect();
                arrays.push(Arc::new(Float64Array::from(vals)));
            }
            _ => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| r.try_get::<_, String>(out_idx).ok())
                    .collect();
                let strs: Vec<Option<&str>> = vals.iter().map(|s| s.as_deref()).collect();
                arrays.push(Arc::new(StringArray::from(strs)));
            }
        }
        let _ = PgType::TEXT; // keep PgType import alive across cfg branches
    }
    RecordBatch::try_new(target_schema.clone(), arrays).map_err(|err| {
        EngineError::NotYetImplemented(Box::leak(
            format!("postgres: build batch: {err}").into_boxed_str(),
        ))
    })
}

// ----------------------------------------------------------------
// GraphqlTableProvider — POST + variables pushdown via a YAML
// `pushdown.variables.<column>` mapping that names which GraphQL
// variable receives the predicate value.
// ----------------------------------------------------------------

#[derive(Debug)]
struct GraphqlTableProvider {
    endpoint: String,
    auth: Option<dbfy_config::AuthConfig>,
    table: dbfy_config::GraphqlTableConfig,
    schema: SchemaRef,
}

impl GraphqlTableProvider {
    fn new(
        endpoint: String,
        auth: Option<dbfy_config::AuthConfig>,
        table: dbfy_config::GraphqlTableConfig,
    ) -> Result<Self> {
        let fields: Vec<arrow_schema::Field> = table
            .columns
            .iter()
            .map(|(name, cfg)| {
                let dt = match cfg.r#type {
                    dbfy_config::DataType::Boolean => arrow_schema::DataType::Boolean,
                    dbfy_config::DataType::Int64 => arrow_schema::DataType::Int64,
                    dbfy_config::DataType::Float64 => arrow_schema::DataType::Float64,
                    _ => arrow_schema::DataType::Utf8,
                };
                arrow_schema::Field::new(name.as_str(), dt, true)
            })
            .collect();
        Ok(Self {
            endpoint,
            auth,
            table,
            schema: Arc::new(arrow_schema::Schema::new(fields)),
        })
    }

    /// A column is pushable when the YAML's `pushdown.variables` block
    /// names a GraphQL variable for it. Without that mapping we can't
    /// translate a `WHERE` predicate into a query variable.
    fn translates_to_variable(
        &self,
        filter: &Expr,
    ) -> Option<(String, datafusion::scalar::ScalarValue)> {
        let p = try_extract_pushed_filter(filter)?;
        if !matches!(p.op, PushedOp::Eq) {
            return None;
        }
        let var = self.table.pushdown.as_ref()?.variables.get(&p.column)?;
        Some((var.clone(), p.literal?))
    }
}

#[async_trait]
impl TableProvider for GraphqlTableProvider {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|f| {
                if self.translates_to_variable(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        use serde_json::json;

        let target_schema = projected_schema(self.schema.clone(), projection)?;

        let mut variables = serde_json::Map::new();
        for f in filters {
            if let Some((var, value)) = self.translates_to_variable(f) {
                let json_val = match scalar_to_plain_string(&value) {
                    Some(s) => match value {
                        datafusion::scalar::ScalarValue::Boolean(Some(b)) => {
                            serde_json::Value::Bool(b)
                        }
                        datafusion::scalar::ScalarValue::Int64(Some(n)) => {
                            serde_json::Value::Number(n.into())
                        }
                        datafusion::scalar::ScalarValue::Float64(Some(n)) => {
                            serde_json::Number::from_f64(n)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        }
                        _ => serde_json::Value::String(s),
                    },
                    None => continue,
                };
                variables.insert(var, json_val);
            }
        }

        let body = json!({
            "query": self.table.query,
            "variables": variables,
        });
        let client = reqwest::Client::builder()
            .user_agent(concat!(
                "dbfy/",
                env!("CARGO_PKG_VERSION"),
                " (+https://github.com/frhack/gh:dbfy)"
            ))
            .build()
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let mut req = client.post(&self.endpoint).json(&body);
        if let Some(auth) = &self.auth {
            if let dbfy_config::AuthConfig::Bearer { token_env } = auth {
                let token = std::env::var(token_env).map_err(|_| {
                    DataFusionError::External(Box::<dyn std::error::Error + Send + Sync>::from(
                        format!("graphql: env var `{token_env}` not set"),
                    ))
                })?;
                req = req.bearer_auth(token);
            }
        }
        let resp = req
            .send()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let root_path = serde_json_path::JsonPath::parse(&self.table.root)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let nodes = root_path.query(&body);
        let mut rows: Vec<&serde_json::Value> = nodes.iter().copied().collect();
        if let Some(n) = limit {
            rows.truncate(n);
        }

        let batch = build_json_batch_with_schema(&rows, &target_schema, &self.table.columns)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let exec: Arc<dyn ExecutionPlan> =
            datafusion::catalog::memory::MemorySourceConfig::try_new_exec(
                &[vec![batch]],
                target_schema,
                None,
            )?;
        Ok(exec)
    }
}

fn build_json_batch_with_schema(
    rows: &[&serde_json::Value],
    target_schema: &SchemaRef,
    columns: &BTreeMap<String, dbfy_config::ColumnConfig>,
) -> Result<RecordBatch> {
    use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let cfg = columns.get(field.name()).ok_or_else(|| {
            EngineError::NotYetImplemented(Box::leak(
                format!("graphql: missing column config for `{}`", field.name()).into_boxed_str(),
            ))
        })?;
        let path = serde_json_path::JsonPath::parse(&cfg.path).map_err(|err| {
            EngineError::NotYetImplemented(Box::leak(
                format!("graphql: invalid path `{}`: {err}", cfg.path).into_boxed_str(),
            ))
        })?;
        match field.data_type() {
            arrow_schema::DataType::Boolean => {
                let vals: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| path.query(r).first().and_then(|v| v.as_bool()))
                    .collect();
                arrays.push(Arc::new(BooleanArray::from(vals)));
            }
            arrow_schema::DataType::Int64 => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| path.query(r).first().and_then(|v| v.as_i64()))
                    .collect();
                arrays.push(Arc::new(Int64Array::from(vals)));
            }
            arrow_schema::DataType::Float64 => {
                let vals: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| path.query(r).first().and_then(|v| v.as_f64()))
                    .collect();
                arrays.push(Arc::new(Float64Array::from(vals)));
            }
            _ => {
                let vals: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| {
                        path.query(r).first().map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                    })
                    .collect();
                let strs: Vec<Option<&str>> = vals.iter().map(|s| s.as_deref()).collect();
                arrays.push(Arc::new(StringArray::from(strs)));
            }
        }
    }
    RecordBatch::try_new(target_schema.clone(), arrays).map_err(|err| {
        EngineError::NotYetImplemented(Box::leak(
            format!("graphql: build batch: {err}").into_boxed_str(),
        ))
    })
}

// ----------------------------------------------------------------
// ExcelTableProvider — opens the workbook, discovers schema from the
// header row, then per-scan iterates rows and applies pushed
// predicates while reading. The provider claims `Exact` for any
// `column OP literal` against a string column (everything is string
// in v1) so DataFusion drops the redundant filter.
// ----------------------------------------------------------------

#[derive(Debug)]
struct ExcelTableProvider {
    cfg: dbfy_config::ExcelTableConfig,
    schema: SchemaRef,
    /// header indices: column name → cell index inside the source row
    column_indices: Vec<usize>,
}

impl ExcelTableProvider {
    fn new(cfg: dbfy_config::ExcelTableConfig) -> Result<Self> {
        use calamine::{Data, Reader, open_workbook_auto};

        let mut wb = open_workbook_auto(&cfg.path).map_err(|err| {
            EngineError::NotYetImplemented(Box::leak(
                format!("excel: open `{}`: {err}", cfg.path).into_boxed_str(),
            ))
        })?;
        let sheet_name = match &cfg.sheet {
            Some(s) => s.clone(),
            None => wb
                .sheet_names()
                .first()
                .cloned()
                .ok_or(EngineError::NotYetImplemented(
                    "excel: workbook has no sheets",
                ))?,
        };
        let range = wb.worksheet_range(&sheet_name).map_err(|err| {
            EngineError::NotYetImplemented(Box::leak(
                format!("excel: sheet `{sheet_name}`: {err}").into_boxed_str(),
            ))
        })?;
        let mut row_iter = range.rows();
        let header_names: Vec<String> = if cfg.has_header {
            row_iter
                .next()
                .map(|row| {
                    row.iter()
                        .enumerate()
                        .map(|(i, cell)| match cell {
                            Data::String(s) if !s.is_empty() => s.clone(),
                            Data::Empty => format!("col_{i}"),
                            other => other.to_string(),
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            let n = row_iter
                .next()
                .map(|r| r.len())
                .ok_or(EngineError::NotYetImplemented("excel: empty sheet"))?;
            (0..n).map(|i| format!("col_{i}")).collect()
        };
        if header_names.is_empty() {
            return Err(EngineError::NotYetImplemented("excel: no columns detected"));
        }
        let fields: Vec<arrow_schema::Field> = header_names
            .iter()
            .map(|n| arrow_schema::Field::new(n.as_str(), arrow_schema::DataType::Utf8, true))
            .collect();
        let column_indices = (0..header_names.len()).collect();
        Ok(Self {
            cfg,
            schema: Arc::new(arrow_schema::Schema::new(fields)),
            column_indices,
        })
    }

    fn translates_to_predicate(&self, filter: &Expr) -> Option<PushedFilter> {
        let p = try_extract_pushed_filter(filter)?;
        // Verify the column exists in our schema.
        self.schema.index_of(&p.column).ok()?;
        Some(p)
    }

    fn row_matches(&self, row_str: &[Option<String>], predicates: &[PushedFilter]) -> bool {
        predicates.iter().all(|p| {
            let idx = match self.schema.index_of(&p.column) {
                Ok(i) => i,
                Err(_) => return false,
            };
            let cell = row_str.get(idx).and_then(|c| c.as_deref());
            match &p.op {
                PushedOp::IsNull => cell.is_none(),
                PushedOp::IsNotNull => cell.is_some(),
                PushedOp::In => p
                    .in_values
                    .iter()
                    .filter_map(scalar_to_plain_string)
                    .any(|v| cell == Some(v.as_str())),
                op => {
                    let lit = match p.literal.as_ref().and_then(scalar_to_plain_string) {
                        Some(s) => s,
                        None => return false,
                    };
                    match op {
                        PushedOp::Eq => cell == Some(lit.as_str()),
                        PushedOp::NotEq => cell != Some(lit.as_str()),
                        PushedOp::Lt => cell.is_some_and(|c| c < lit.as_str()),
                        PushedOp::LtEq => cell.is_some_and(|c| c <= lit.as_str()),
                        PushedOp::Gt => cell.is_some_and(|c| c > lit.as_str()),
                        PushedOp::GtEq => cell.is_some_and(|c| c >= lit.as_str()),
                        _ => false,
                    }
                }
            }
        })
    }
}

#[async_trait]
impl TableProvider for ExcelTableProvider {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|f| {
                if self.translates_to_predicate(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        use arrow_array::{ArrayRef, StringArray};
        use calamine::{Data, Reader, open_workbook_auto};

        let target_schema = projected_schema(self.schema.clone(), projection)?;
        let predicates: Vec<PushedFilter> = filters
            .iter()
            .filter_map(|f| self.translates_to_predicate(f))
            .collect();

        let mut wb = open_workbook_auto(&self.cfg.path)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let sheet_name = match &self.cfg.sheet {
            Some(s) => s.clone(),
            None => wb.sheet_names().first().cloned().ok_or_else(|| {
                DataFusionError::External(Box::<dyn std::error::Error + Send + Sync>::from(
                    "excel: no sheets",
                ))
            })?,
        };
        let range = wb
            .worksheet_range(&sheet_name)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let n_cols = self.column_indices.len();
        let mut columns: Vec<Vec<Option<String>>> = vec![Vec::new(); n_cols];
        let mut rows_emitted = 0usize;
        let row_iter = range.rows();
        let mut iter = row_iter.into_iter();
        if self.cfg.has_header {
            iter.next();
        }
        for row in iter {
            if let Some(n) = limit {
                if rows_emitted >= n {
                    break;
                }
            }
            let mut row_str: Vec<Option<String>> = Vec::with_capacity(n_cols);
            for i in 0..n_cols {
                let cell = row.get(i);
                let s = cell.and_then(|c| match c {
                    Data::Empty => None,
                    Data::String(s) => Some(s.clone()),
                    Data::Int(n) => Some(n.to_string()),
                    Data::Float(n) => Some(n.to_string()),
                    Data::Bool(b) => Some(b.to_string()),
                    Data::DateTime(dt) => Some(dt.to_string()),
                    Data::Error(e) => Some(format!("#ERROR:{e:?}")),
                    Data::DurationIso(s) | Data::DateTimeIso(s) => Some(s.clone()),
                });
                row_str.push(s);
            }
            if !self.row_matches(&row_str, &predicates) {
                continue;
            }
            for (i, val) in row_str.into_iter().enumerate() {
                columns[i].push(val);
            }
            rows_emitted += 1;
        }

        // Build the projected batch directly so we don't pay column
        // copying for unused columns.
        let projected_indices: Vec<usize> = match projection {
            None => (0..n_cols).collect(),
            Some(p) => p.clone(),
        };
        let arrays: Vec<ArrayRef> = projected_indices
            .iter()
            .map(|i| {
                let strs: Vec<Option<&str>> = columns[*i].iter().map(|s| s.as_deref()).collect();
                Arc::new(StringArray::from(strs)) as ArrayRef
            })
            .collect();
        let batch = if projected_indices.is_empty() {
            let opts = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows_emitted));
            RecordBatch::try_new_with_options(target_schema.clone(), vec![], &opts)
        } else {
            RecordBatch::try_new(target_schema.clone(), arrays)
        }
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let exec: Arc<dyn ExecutionPlan> =
            datafusion::catalog::memory::MemorySourceConfig::try_new_exec(
                &[vec![batch]],
                target_schema,
                None,
            )?;
        Ok(exec)
    }
}

#[derive(Debug)]
struct RestDataFusionTableProvider {
    table: RestTable,
}

impl RestDataFusionTableProvider {
    fn new(table: RestTable) -> Self {
        Self { table }
    }
}

#[async_trait]
impl TableProvider for RestDataFusionTableProvider {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.config().arrow_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let projection = Vec::new();
        Ok(filters
            .iter()
            .map(|filter| match expr_to_rest_filter(filter) {
                Ok(rest_filter) => {
                    let plan = self.table.plan_request(&projection, &[rest_filter], None);
                    if plan.pushed_filters.is_empty() {
                        TableProviderFilterPushDown::Unsupported
                    } else {
                        TableProviderFilterPushDown::Exact
                    }
                }
                Err(_) => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let full_schema = self.schema();
        let projection_indices = projection.cloned();
        let output_schema = projected_schema(full_schema.clone(), projection.as_deref())?;

        // The REST provider always returns full-schema batches (it
        // doesn't know about column-level projection in the JSON
        // layer); the projection happens here, at the DataFusion
        // boundary, by post-projecting each batch via
        // `RecordBatch::project`. For `SELECT count(*)` DataFusion
        // sends `Some(empty)`, which becomes a `Schema::empty()` +
        // empty-column projection — Arrow handles this correctly,
        // emitting row-only batches with `num_rows` preserved.
        let projection_names = projection_to_names(full_schema, projection)?.unwrap_or_default();
        let rest_filters = filters
            .iter()
            .map(expr_to_rest_filter)
            .collect::<Result<Vec<_>>>()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let plan = RestStreamExecutionPlan::try_new(
            self.table.clone(),
            projection_names,
            projection_indices,
            output_schema,
            rest_filters,
            limit,
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Arc::new(plan))
    }
}

struct ProgrammaticDataFusionTableProvider {
    table_name: String,
    provider: DynProvider,
}

impl std::fmt::Debug for ProgrammaticDataFusionTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgrammaticDataFusionTableProvider")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl ProgrammaticDataFusionTableProvider {
    fn new(table_name: String, provider: DynProvider) -> Self {
        Self {
            table_name,
            provider,
        }
    }

    fn base_schema(&self) -> SchemaRef {
        self.provider.schema()
    }
}

#[async_trait]
impl TableProvider for ProgrammaticDataFusionTableProvider {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> std::result::Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let capabilities = self.provider.capabilities();
        Ok(filters
            .iter()
            .map(|filter| match expr_to_simple_filter(filter) {
                Ok(simple_filter) if capabilities.filter_pushdown.supports(&simple_filter) => {
                    TableProviderFilterPushDown::Inexact
                }
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let projection_names = projection_to_names(self.base_schema(), projection)?;
        let pushed_filters = pushdown_filters(self.provider.capabilities(), filters);

        let mut request = ScanRequest::new(next_query_id());
        request.projection = projection_names;
        request.filters = pushed_filters;
        request.limit = limit;

        Ok(Arc::new(
            ProgrammaticStreamExecutionPlan::new(
                self.table_name.clone(),
                self.provider.clone(),
                request,
                projection.cloned(),
                limit,
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?,
        ))
    }
}

struct ProgrammaticStreamExecutionPlan {
    table_name: String,
    provider: DynProvider,
    request: ScanRequest,
    base_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    limit: Option<usize>,
}

impl std::fmt::Debug for ProgrammaticStreamExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgrammaticStreamExecutionPlan")
            .field("table_name", &self.table_name)
            .field("request", &self.request)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl ProgrammaticStreamExecutionPlan {
    fn new(
        table_name: String,
        provider: DynProvider,
        request: ScanRequest,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let base_schema = provider.schema();
        let projected_schema = projected_schema(base_schema.clone(), projection.as_ref())?;
        let properties = Arc::new(Self::compute_properties(projected_schema.clone()));

        Ok(Self {
            table_name,
            provider,
            request,
            base_schema,
            projection,
            projected_schema,
            properties,
            limit,
        })
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for ProgrammaticStreamExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProgrammaticStreamExec: table={}", self.table_name)
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for ProgrammaticStreamExecutionPlan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ProgrammaticStreamExecutionPlan does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid partition {partition} for programmatic stream execution"
            )));
        }

        let schema = self.projected_schema.clone();
        let provider = self.provider.clone();
        let request = self.request.clone();
        let base_schema = self.base_schema.clone();
        let projection = self.projection.clone();
        let table_name = self.table_name.clone();
        let limit = self.limit;

        let mut builder = RecordBatchReceiverStream::builder(schema, 2);
        let tx = builder.tx();
        builder.spawn(async move {
            let response = provider.scan(request).await.map_err(provider_error)?;
            let mut stream = response.stream;
            let mut remaining = limit.unwrap_or(usize::MAX);

            while remaining > 0 {
                let Some(item) = stream.next().await else {
                    break;
                };

                let batch = item.map_err(provider_error)?;
                let batch = normalize_programmatic_batch(
                    &table_name,
                    base_schema.clone(),
                    projection.as_ref(),
                    batch,
                )
                .map_err(|error| DataFusionError::External(Box::new(error)))?;

                let Some(batch) = apply_stream_limit(batch, &mut remaining) else {
                    break;
                };

                if tx.send(Ok(batch)).await.is_err() {
                    break;
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> std::result::Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.projected_schema))
    }
}

fn provider_error(error: ProviderError) -> DataFusionError {
    DataFusionError::External(Box::new(EngineError::Provider(error)))
}

fn rest_provider_error(error: RestProviderError) -> DataFusionError {
    DataFusionError::External(Box::new(EngineError::Rest(error)))
}

struct RestStreamExecutionPlan {
    table: RestTable,
    projection_names: Vec<String>,
    /// `Some(indices)` when DataFusion requested a column subset (could
    /// be empty for `SELECT count(*)`); `None` when no projection was
    /// pushed down. Drives `RecordBatch::project` at emission so the
    /// physical batches match the logical schema DataFusion expects.
    projection_indices: Option<Vec<usize>>,
    filters: Vec<RestSimpleFilter>,
    limit: Option<usize>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for RestStreamExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestStreamExecutionPlan")
            .field("path", &self.table.config().endpoint.path)
            .field("projection_names", &self.projection_names)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl RestStreamExecutionPlan {
    fn try_new(
        table: RestTable,
        projection_names: Vec<String>,
        projection_indices: Option<Vec<usize>>,
        schema: SchemaRef,
        filters: Vec<RestSimpleFilter>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table,
            projection_names,
            projection_indices,
            filters,
            limit,
            schema,
            properties,
        })
    }
}

impl DisplayAs for RestStreamExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RestStreamExec: path={}",
                    self.table.config().endpoint.path
                )
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for RestStreamExecutionPlan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "RestStreamExecutionPlan does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid partition {partition} for rest stream execution"
            )));
        }

        let rest_stream = self
            .table
            .execute_stream(&self.projection_names, &self.filters, self.limit)
            .map_err(rest_provider_error)?;

        let schema = self.schema.clone();
        let limit = self.limit;
        let projection_indices = self.projection_indices.clone();

        let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 2);
        let tx = builder.tx();
        builder.spawn(async move {
            let mut stream = rest_stream.stream;
            let mut remaining = limit.unwrap_or(usize::MAX);

            while remaining > 0 {
                let Some(item) = stream.next().await else {
                    break;
                };
                let batch = item.map_err(rest_provider_error)?;
                let Some(batch) = apply_stream_limit(batch, &mut remaining) else {
                    break;
                };
                // The REST provider already returns batches matching
                // `projection_names`, so the only case we still need
                // to transform here is `Some(empty)` (count(*)) — the
                // provider returns full-schema rows then; DataFusion
                // expects an empty-schema row-only batch.
                let emit = match projection_indices.as_deref() {
                    Some([]) => empty_row_batch(&schema, batch.num_rows())?,
                    _ => batch,
                };
                if tx.send(Ok(emit)).await.is_err() {
                    break;
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> std::result::Result<Statistics, DataFusionError> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

fn next_query_id() -> String {
    QUERY_ID.fetch_add(1, Ordering::Relaxed).to_string()
}

fn expr_to_rest_filter(expr: &Expr) -> Result<RestSimpleFilter> {
    let SimpleFilter {
        column,
        operator,
        value,
    } = expr_to_simple_filter(expr)?;
    Ok(RestSimpleFilter {
        column,
        operator: operator.as_str().to_string(),
        value: scalar_to_rest_value(&value)?,
    })
}

fn scalar_to_rest_value(value: &ScalarValue) -> Result<String> {
    match value {
        ScalarValue::Boolean(value) => Ok(value.to_string()),
        ScalarValue::Int64(value) => Ok(value.to_string()),
        ScalarValue::Float64(value) => Ok(value.to_string()),
        ScalarValue::Utf8(value) => Ok(value.clone()),
        ScalarValue::Utf8List(values) => Ok(values.join(",")),
    }
}

/// Build a row-only `RecordBatch` (0 columns, given `num_rows`)
/// matching an empty `target_schema`. DataFusion's `count(*)` path
/// asks for this shape: it ignores column data and just sums row
/// counts. `RecordBatch::try_new` rejects empty-array batches without
/// an explicit `with_row_count`, so we use `try_new_with_options`.
fn empty_row_batch(
    target_schema: &SchemaRef,
    num_rows: usize,
) -> std::result::Result<RecordBatch, DataFusionError> {
    let opts = arrow_array::RecordBatchOptions::new().with_row_count(Some(num_rows));
    RecordBatch::try_new_with_options(target_schema.clone(), vec![], &opts)
        .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

fn projection_to_names(
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> std::result::Result<Option<Vec<String>>, DataFusionError> {
    Ok(projection.map(|projection| {
        projection
            .iter()
            .map(|index| schema.field(*index).name().clone())
            .collect()
    }))
}

fn projected_schema(
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> std::result::Result<SchemaRef, DataFusionError> {
    match projection {
        Some(projection) => {
            let fields = projection
                .iter()
                .map(|index| schema.field(*index).clone())
                .collect::<Vec<_>>();
            Ok(Arc::new(Schema::new(fields)))
        }
        None => Ok(schema),
    }
}

fn pushdown_filters(capabilities: ProviderCapabilities, filters: &[Expr]) -> Vec<SimpleFilter> {
    filters
        .iter()
        .filter_map(|expr| match expr_to_simple_filter(expr) {
            Ok(filter) if capabilities.filter_pushdown.supports(&filter) => Some(filter),
            _ => None,
        })
        .collect()
}

fn expr_to_simple_filter(expr: &Expr) -> Result<SimpleFilter> {
    if let Some(in_list) = try_or_chain_to_in_list(expr) {
        return Ok(in_list);
    }

    match expr {
        Expr::BinaryExpr(binary) => {
            let left_name = expr_column_name(&binary.left);
            let right_name = expr_column_name(&binary.right);
            let left_literal = expr_scalar_value(&binary.left);
            let right_literal = expr_scalar_value(&binary.right);

            match (left_name, right_literal) {
                (Some(column), Some(value)) => Ok(SimpleFilter {
                    column,
                    operator: binary_operator_to_filter(&binary.op)?,
                    value,
                }),
                _ => match (right_name, left_literal) {
                    (Some(column), Some(value)) => Ok(SimpleFilter {
                        column,
                        operator: reverse_binary_operator_to_filter(&binary.op)?,
                        value,
                    }),
                    _ => Err(EngineError::UnsupportedFilterExpression(expr.clone())),
                },
            }
        }
        Expr::InList(in_list) => {
            let Some(column) = expr_column_name(&in_list.expr) else {
                return Err(EngineError::UnsupportedFilterExpression(expr.clone()));
            };

            if in_list.negated {
                return Err(EngineError::UnsupportedFilterExpression(expr.clone()));
            }

            let mut values = Vec::with_capacity(in_list.list.len());
            for candidate in &in_list.list {
                let ScalarValue::Utf8(value) = expr_scalar_value(candidate)
                    .ok_or_else(|| EngineError::UnsupportedFilterExpression(expr.clone()))?
                else {
                    return Err(EngineError::UnsupportedFilterExpression(expr.clone()));
                };
                values.push(value);
            }

            Ok(SimpleFilter {
                column,
                operator: FilterOperator::In,
                value: ScalarValue::Utf8List(values),
            })
        }
        _ => Err(EngineError::UnsupportedFilterExpression(expr.clone())),
    }
}

/// Recognise `Or(Eq(col, v1), Eq(col, v2), ...)` over a single column with
/// string literal values and reconstruct it as a single `InList` filter.
///
/// DataFusion 53 routinely rewrites short `IN` lists into OR-of-equality
/// at planning time; without this normalisation, providers that declare
/// `IN`-pushdown never see those rewrites as `InList`.
fn try_or_chain_to_in_list(expr: &Expr) -> Option<SimpleFilter> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {}
        _ => return None,
    }
    let mut values = Vec::new();
    let column = collect_or_eq_chain(expr, &mut values)?;
    if values.len() < 2 {
        return None;
    }
    Some(SimpleFilter {
        column,
        operator: FilterOperator::In,
        value: ScalarValue::Utf8List(values),
    })
}

fn collect_or_eq_chain(expr: &Expr, values: &mut Vec<String>) -> Option<String> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = collect_or_eq_chain(&binary.left, values)?;
            let right = collect_or_eq_chain(&binary.right, values)?;
            if left == right { Some(left) } else { None }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (column, value) = match (
                expr_column_name(&binary.left),
                expr_scalar_value(&binary.right),
            ) {
                (Some(c), Some(v)) => (c, v),
                _ => match (
                    expr_column_name(&binary.right),
                    expr_scalar_value(&binary.left),
                ) {
                    (Some(c), Some(v)) => (c, v),
                    _ => return None,
                },
            };
            let ScalarValue::Utf8(text) = value else {
                return None;
            };
            values.push(text);
            Some(column)
        }
        _ => None,
    }
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn expr_scalar_value(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => match value {
            datafusion::scalar::ScalarValue::Boolean(Some(value)) => {
                Some(ScalarValue::Boolean(*value))
            }
            datafusion::scalar::ScalarValue::Int64(Some(value)) => Some(ScalarValue::Int64(*value)),
            datafusion::scalar::ScalarValue::Float64(Some(value)) => {
                Some(ScalarValue::Float64(*value))
            }
            datafusion::scalar::ScalarValue::Utf8(Some(value))
            | datafusion::scalar::ScalarValue::LargeUtf8(Some(value)) => {
                Some(ScalarValue::Utf8(value.clone()))
            }
            _ => None,
        },
        _ => None,
    }
}

fn binary_operator_to_filter(operator: &Operator) -> Result<FilterOperator> {
    match operator {
        Operator::Eq => Ok(FilterOperator::Eq),
        Operator::Gt => Ok(FilterOperator::Gt),
        Operator::GtEq => Ok(FilterOperator::Gte),
        Operator::Lt => Ok(FilterOperator::Lt),
        Operator::LtEq => Ok(FilterOperator::Lte),
        _ => Err(EngineError::NotYetImplemented("operator not supported")),
    }
}

fn reverse_binary_operator_to_filter(operator: &Operator) -> Result<FilterOperator> {
    match operator {
        Operator::Eq => Ok(FilterOperator::Eq),
        Operator::Gt => Ok(FilterOperator::Lt),
        Operator::GtEq => Ok(FilterOperator::Lte),
        Operator::Lt => Ok(FilterOperator::Gt),
        Operator::LtEq => Ok(FilterOperator::Gte),
        _ => Err(EngineError::NotYetImplemented("operator not supported")),
    }
}

fn render_explain(engine: &Engine, sql: &str, plan: &LogicalPlan) -> Result<String> {
    let query_limit = collect_query_limit(plan);
    let mut lines = vec![
        format!("SQL: {sql}"),
        String::new(),
        "Optimized logical plan:".to_string(),
        format!("  {}", plan.display_indent()),
    ];

    let table_scans = collect_table_scans(plan);
    if table_scans.is_empty() {
        return Err(EngineError::UnsupportedExplain(
            "no table scan found in logical plan".to_string(),
        ));
    }

    let residual_filters = collect_residual_filters(plan);
    lines.push(String::new());
    lines.push("Tables:".to_string());

    for table_scan in table_scans {
        lines.extend(render_table_explain(engine, table_scan, query_limit)?);
    }

    lines.push(String::new());
    lines.push("Residual local filters:".to_string());
    if residual_filters.is_empty() {
        lines.push("  none".to_string());
    } else {
        for filter in residual_filters {
            lines.push(format!("  - {filter}"));
        }
    }

    Ok(lines.join("\n"))
}

fn render_table_explain(
    engine: &Engine,
    table_scan: &TableScan,
    query_limit: Option<usize>,
) -> Result<Vec<String>> {
    let table_name = table_scan.table_name.to_string();
    if let Some(table) = engine.rest_tables.get(&table_name) {
        let projection = projection_names_for_rest_scan(table, table_scan)?;
        let filters = table_scan
            .filters
            .iter()
            .map(expr_to_rest_filter)
            .collect::<Result<Vec<_>>>()?;
        let plan = table.plan_request(&projection, &filters, table_scan.fetch);

        let mut lines = vec![
            format!("- {table_name}"),
            "  provider: rest".to_string(),
            format!("  projection: {}", describe_string_list(&projection)),
            format!(
                "  pushed filters: {}",
                describe_string_list(
                    &plan
                        .pushed_filters
                        .iter()
                        .map(|filter| {
                            format!("{} {} {}", filter.column, filter.operator, filter.value)
                        })
                        .collect::<Vec<_>>(),
                )
            ),
            format!(
                "  pushed limit: {}",
                describe_optional_usize(table_scan.fetch)
            ),
            format!("  http plan: {}", plan.render_http_request()),
        ];

        if !plan.residual_filters.is_empty() {
            lines.push(format!(
                "  provider residual filters: {}",
                describe_string_list(
                    &plan
                        .residual_filters
                        .iter()
                        .map(|filter| {
                            format!("{} {} {}", filter.column, filter.operator, filter.value)
                        })
                        .collect::<Vec<_>>(),
                )
            ));
        }

        return Ok(lines);
    }

    if let Some(provider) = engine.programmatic_tables.get(&table_name) {
        let projection = projection_names_for_programmatic_scan(provider, table_scan)?;
        let accepted_filters = table_scan
            .filters
            .iter()
            .map(expr_to_simple_filter)
            .collect::<Result<Vec<_>>>()?;
        let capabilities = provider.capabilities();

        return Ok(vec![
            format!("- {table_name}"),
            "  provider: programmatic".to_string(),
            format!(
                "  capabilities: {}",
                describe_programmatic_capabilities(&capabilities)
            ),
            format!(
                "  accepted projection: {}",
                describe_string_list(&projection)
            ),
            format!(
                "  accepted filters: {}",
                describe_string_list(
                    &accepted_filters
                        .iter()
                        .map(|filter| format!(
                            "{} {} {}",
                            filter.column,
                            filter.operator.as_str(),
                            describe_scalar_value(&filter.value)
                        ))
                        .collect::<Vec<_>>(),
                )
            ),
            format!("  query limit: {}", describe_optional_usize(query_limit)),
            format!(
                "  accepted limit: {}",
                describe_optional_usize(table_scan.fetch)
            ),
        ]);
    }

    Err(EngineError::UnknownTable(table_name))
}

fn collect_table_scans(plan: &LogicalPlan) -> Vec<&TableScan> {
    let mut scans = Vec::new();
    collect_table_scans_inner(plan, &mut scans);
    scans
}

fn collect_table_scans_inner<'a>(plan: &'a LogicalPlan, scans: &mut Vec<&'a TableScan>) {
    match plan {
        LogicalPlan::TableScan(table_scan) => scans.push(table_scan),
        _ => {
            for input in plan.inputs() {
                collect_table_scans_inner(input, scans);
            }
        }
    }
}

fn collect_residual_filters(plan: &LogicalPlan) -> Vec<String> {
    let mut filters = Vec::new();
    collect_residual_filters_inner(plan, &mut filters);
    filters
}

fn collect_query_limit(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::Limit(limit) => limit
            .fetch
            .as_deref()
            .and_then(limit_expr_to_usize)
            .or_else(|| {
                plan.inputs()
                    .iter()
                    .find_map(|input| collect_query_limit(input))
            }),
        _ => plan
            .inputs()
            .iter()
            .find_map(|input| collect_query_limit(input)),
    }
}

fn collect_residual_filters_inner(plan: &LogicalPlan, filters: &mut Vec<String>) {
    match plan {
        LogicalPlan::Filter(filter) => filters.push(filter.predicate.to_string()),
        _ => {
            for input in plan.inputs() {
                collect_residual_filters_inner(input, filters);
            }
        }
    }
}

fn projection_names_for_rest_scan(
    table: &RestTable,
    table_scan: &TableScan,
) -> Result<Vec<String>> {
    let schema = table.config().arrow_schema();
    projection_names_from_schema(schema, table_scan.projection.as_ref())
}

fn projection_names_for_programmatic_scan(
    provider: &DynProvider,
    table_scan: &TableScan,
) -> Result<Vec<String>> {
    projection_names_from_schema(provider.schema(), table_scan.projection.as_ref())
}

fn projection_names_from_schema(
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<Vec<String>> {
    match projection {
        Some(indices) => indices
            .iter()
            .map(|index| {
                schema
                    .fields()
                    .get(*index)
                    .map(|field| field.name().to_string())
                    .ok_or_else(|| {
                        EngineError::UnsupportedExplain(format!(
                            "projection index {index} out of bounds"
                        ))
                    })
            })
            .collect(),
        None => Ok(schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()),
    }
}

fn describe_string_list(items: &[String]) -> String {
    if items.is_empty() {
        "none".to_string()
    } else {
        items.join(", ")
    }
}

fn describe_optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn limit_expr_to_usize(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Literal(value, _) => match value {
            datafusion::scalar::ScalarValue::Int64(Some(value)) => usize::try_from(*value).ok(),
            datafusion::scalar::ScalarValue::UInt64(Some(value)) => usize::try_from(*value).ok(),
            datafusion::scalar::ScalarValue::Int32(Some(value)) => usize::try_from(*value).ok(),
            datafusion::scalar::ScalarValue::UInt32(Some(value)) => usize::try_from(*value).ok(),
            _ => None,
        },
        _ => None,
    }
}

fn describe_scalar_value(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::Float64(value) => value.to_string(),
        ScalarValue::Utf8(value) => value.clone(),
        ScalarValue::Utf8List(values) => values.join(","),
    }
}

fn describe_programmatic_capabilities(capabilities: &ProviderCapabilities) -> String {
    let mut parts = Vec::new();
    parts.push(format!("projection={}", capabilities.projection_pushdown));
    parts.push(format!("limit={}", capabilities.limit_pushdown));
    parts.push(format!(
        "filters=[{}]",
        describe_filter_capabilities(&capabilities.filter_pushdown)
    ));
    parts.join(", ")
}

fn describe_filter_capabilities(capabilities: &dbfy_provider::FilterCapabilities) -> String {
    let mut parts = Vec::new();
    if !capabilities.equals.is_empty() {
        parts.push(format!("=:{}", sorted_join(&capabilities.equals)));
    }
    if !capabilities.in_list.is_empty() {
        parts.push(format!("IN:{}", sorted_join(&capabilities.in_list)));
    }
    if !capabilities.greater_than.is_empty() {
        parts.push(format!(">:{}", sorted_join(&capabilities.greater_than)));
    }
    if !capabilities.greater_than_or_equal.is_empty() {
        parts.push(format!(
            ">=:{}",
            sorted_join(&capabilities.greater_than_or_equal)
        ));
    }
    if !capabilities.less_than.is_empty() {
        parts.push(format!("<:{}", sorted_join(&capabilities.less_than)));
    }
    if !capabilities.less_than_or_equal.is_empty() {
        parts.push(format!(
            "<=:{}",
            sorted_join(&capabilities.less_than_or_equal)
        ));
    }

    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join("; ")
    }
}

fn sorted_join(values: &std::collections::BTreeSet<String>) -> String {
    values.iter().cloned().collect::<Vec<_>>().join("|")
}

fn normalize_programmatic_batch(
    table_name: &str,
    base_schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    let Some(projection) = projection else {
        return Ok(batch);
    };

    // `SELECT count(*)` lands here as `Some(empty)`. Build a row-only
    // batch (0 columns, num_rows preserved) with `try_new_with_options`;
    // plain `try_new` would also accept it but `RecordBatch::try_new`
    // historically rejects empty-array batches without an explicit
    // row-count hint.
    if projection.is_empty() {
        let opts = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let schema = projected_schema(base_schema, Some(projection))?;
        return RecordBatch::try_new_with_options(schema, vec![], &opts).map_err(|_| {
            EngineError::ProviderSchemaMismatch {
                table: table_name.to_string(),
            }
        });
    }

    let projected_names = projection
        .iter()
        .map(|index| base_schema.field(*index).name().as_str())
        .collect::<Vec<_>>();
    let batch_schema = batch.schema();
    let batch_names = batch_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();

    if batch_names == projected_names {
        return Ok(batch);
    }

    let projected_schema = projected_schema(base_schema, Some(projection))?;
    let arrays = projection
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect::<Vec<_>>();

    RecordBatch::try_new(projected_schema, arrays).map_err(|_| {
        EngineError::ProviderSchemaMismatch {
            table: table_name.to_string(),
        }
    })
}

fn apply_stream_limit(batch: RecordBatch, remaining: &mut usize) -> Option<RecordBatch> {
    if *remaining == 0 {
        return None;
    }

    let rows = batch.num_rows();
    if rows <= *remaining {
        *remaining -= rows;
        Some(batch)
    } else {
        let limited = batch.slice(0, *remaining);
        *remaining = 0;
        Some(limited)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use dbfy_config::Config;
    use dbfy_provider::{ProgrammaticTableProvider, ProviderError, ProviderResult, ScanResponse};
    use futures::stream;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{BTreeMap, Engine, RowsFileHandle, ScanRequest};

    #[tokio::test]
    async fn or_chain_of_equalities_pushed_as_in_list() {
        use dbfy_provider::{FilterCapabilities, ProviderCapabilities, SimpleFilter};
        use std::sync::Mutex;

        struct CapturingProvider {
            schema: SchemaRef,
            captured: Arc<Mutex<Vec<SimpleFilter>>>,
        }

        #[async_trait]
        impl ProgrammaticTableProvider for CapturingProvider {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }

            fn capabilities(&self) -> ProviderCapabilities {
                let mut filters = FilterCapabilities::default();
                filters.in_list.insert("sensor".to_string());
                ProviderCapabilities {
                    filter_pushdown: filters,
                    ..ProviderCapabilities::default()
                }
            }

            async fn scan(&self, request: ScanRequest) -> ProviderResult<ScanResponse> {
                self.captured
                    .lock()
                    .unwrap()
                    .extend(request.filters.clone());
                let array = Arc::new(StringArray::from(vec!["alpha"])) as ArrayRef;
                let batch = RecordBatch::try_new(self.schema.clone(), vec![array]).unwrap();
                Ok(ScanResponse {
                    stream: Box::pin(stream::iter(vec![Ok(batch)])),
                    handled_filters: Vec::new(),
                    metadata: BTreeMap::new(),
                })
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "sensor",
            DataType::Utf8,
            false,
        )]));
        let captured = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(CapturingProvider {
            schema,
            captured: captured.clone(),
        });

        let mut engine = Engine::default();
        engine.register_provider("t", provider).expect("register");

        engine
            .query("SELECT sensor FROM t WHERE sensor IN ('alpha', 'beta')")
            .await
            .expect("query");

        let filters = captured.lock().unwrap().clone();
        assert_eq!(
            filters.len(),
            1,
            "expected one pushed filter, got {filters:?}"
        );
        let only = &filters[0];
        assert_eq!(only.column, "sensor");
        assert_eq!(only.operator.as_str(), "IN");
        match &only.value {
            dbfy_provider::ScalarValue::Utf8List(values) => {
                let mut sorted = values.clone();
                sorted.sort();
                assert_eq!(sorted, vec!["alpha".to_string(), "beta".to_string()]);
            }
            other => panic!("expected Utf8List, got {other:?}"),
        }
    }

    struct StaticCustomersProvider {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    struct ErrorAfterFirstBatchProvider {
        schema: SchemaRef,
        first_batch: RecordBatch,
    }

    #[async_trait]
    impl ProgrammaticTableProvider for StaticCustomersProvider {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        async fn scan(&self, _request: ScanRequest) -> ProviderResult<ScanResponse> {
            Ok(ScanResponse {
                stream: Box::pin(stream::iter(self.batches.clone().into_iter().map(Ok))),
                handled_filters: Vec::new(),
                metadata: BTreeMap::new(),
            })
        }
    }

    #[async_trait]
    impl ProgrammaticTableProvider for ErrorAfterFirstBatchProvider {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        async fn scan(&self, _request: ScanRequest) -> ProviderResult<ScanResponse> {
            Ok(ScanResponse {
                stream: Box::pin(stream::iter(vec![
                    Ok(self.first_batch.clone()),
                    Err(ProviderError::Generic {
                        message: "second batch should not be consumed".to_string(),
                    }),
                ])),
                handled_filters: Vec::new(),
                metadata: BTreeMap::new(),
            })
        }
    }

    #[test]
    fn lists_registered_rest_tables() {
        let raw = r#"
version: 1
sources:
  crm:
    type: rest
    base_url: https://api.example.com
    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
"#;

        let config = Config::from_yaml_str(raw).expect("config should parse");
        let engine = Engine::from_config(config).expect("engine should build");

        assert_eq!(
            engine.registered_tables(),
            vec!["crm.customers".to_string()]
        );
    }

    #[tokio::test]
    async fn executes_sql_against_programmatic_provider() {
        let mut engine = Engine::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Mario", "Anna"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["active", "inactive"])) as ArrayRef,
            ],
        )
        .expect("batch should build");

        engine
            .register_provider(
                "app.customers",
                Arc::new(StaticCustomersProvider {
                    schema,
                    batches: vec![batch],
                }),
            )
            .expect("provider should register");

        let batches = engine
            .query("SELECT id, name FROM app.customers WHERE status = 'active'")
            .await
            .expect("query should succeed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn limit_stops_programmatic_stream_before_later_error() {
        let mut engine = Engine::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let first_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Mario", "Anna"])) as ArrayRef,
            ],
        )
        .expect("batch should build");

        engine
            .register_provider(
                "app.customers",
                Arc::new(ErrorAfterFirstBatchProvider {
                    schema,
                    first_batch,
                }),
            )
            .expect("provider should register");

        let batches = engine
            .query("SELECT id FROM app.customers LIMIT 1")
            .await
            .expect("streaming limit should stop before later provider errors");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn explains_programmatic_query() {
        let mut engine = Engine::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Mario", "Anna"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["active", "inactive"])) as ArrayRef,
            ],
        )
        .expect("batch should build");
        let mut filter_capabilities = dbfy_provider::FilterCapabilities::default();
        filter_capabilities.equals.insert("status".to_string());

        engine
            .register_provider(
                "app.customers",
                Arc::new(
                    dbfy_provider_static::StaticRecordBatchProvider::new(schema, vec![batch])
                        .with_capabilities(dbfy_provider::ProviderCapabilities {
                            projection_pushdown: true,
                            filter_pushdown: filter_capabilities,
                            limit_pushdown: true,
                            order_by_pushdown: false,
                            aggregate_pushdown: false,
                        }),
                ),
            )
            .expect("provider should register");

        let explanation = engine
            .explain("SELECT id, name FROM app.customers WHERE status = 'active' LIMIT 1")
            .await
            .expect("explain should succeed");

        assert!(explanation.contains("provider: programmatic"));
        assert!(explanation.contains("accepted projection: id, name"));
        assert!(explanation.contains("accepted filters: status = active"));
        assert!(explanation.contains("query limit: 1"));
    }

    #[tokio::test]
    async fn executes_sql_against_rest_provider() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .and(query_param("status", "active"))
            .and(query_param("fields", "id,name"))
            .and(query_param("limit", "10"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 1, "name": "Mario", "status": "active" }
                ]
            })))
            .mount(&server)
            .await;

        let raw = format!(
            r#"
version: 1
sources:
  crm:
    type: rest
    base_url: {base_url}
    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
          status:
            path: "$.status"
            type: string
        pushdown:
          filters:
            status:
              param: status
              operators: ["="]
          limit:
            param: limit
          projection:
            param: fields
            style: comma_separated
"#,
            base_url = server.uri()
        );

        let config = Config::from_yaml_str(&raw).expect("config should parse");
        let engine = Engine::from_config(config).expect("engine should build");

        let batches = engine
            .query("SELECT id, name FROM crm.customers WHERE status = 'active' LIMIT 10")
            .await
            .expect("query should succeed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn select_count_star_against_rest_source() {
        // Regression for bug #2 caught by the showcase: DataFusion
        // sends `Some(empty)` projection for `count(*)`, which
        // collided with our REST provider returning the full schema
        // for empty input. The fix is in `RestStreamExecutionPlan`'s
        // emission path (project to empty schema, preserve num_rows).
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 1, "name": "Mario",   "status": "active"   },
                    { "id": 2, "name": "Anna",    "status": "active"   },
                    { "id": 3, "name": "Luca",    "status": "inactive" },
                    { "id": 4, "name": "Giulia",  "status": "active"   },
                ]
            })))
            .mount(&server)
            .await;

        let raw = format!(
            r#"
version: 1
sources:
  crm:
    type: rest
    base_url: {base_url}
    tables:
      customers:
        endpoint: {{ method: GET, path: /customers }}
        root: "$.data[*]"
        columns:
          id:     {{ path: "$.id",     type: int64  }}
          name:   {{ path: "$.name",   type: string }}
          status: {{ path: "$.status", type: string }}
"#,
            base_url = server.uri()
        );

        let config = Config::from_yaml_str(&raw).expect("config parses");
        let engine = Engine::from_config(config).expect("engine builds");

        // Bare `count(*)` with no WHERE — projection arrives as Some(empty).
        let batches = engine
            .query("SELECT count(*) FROM crm.customers")
            .await
            .expect("count(*) should succeed");
        let total: i64 = batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .map(|a| a.value(0))
            })
            .sum();
        assert_eq!(total, 4, "count(*) should return 4 rows");

        // Same path with a residual filter — exercises both
        // empty-projection and filter-pushdown together.
        let batches = engine
            .query("SELECT count(*) FROM crm.customers WHERE status = 'active'")
            .await
            .expect("count(*) WHERE should succeed");
        let total: i64 = batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .map(|a| a.value(0))
            })
            .sum();
        assert_eq!(total, 3, "WHERE status='active' matches 3 rows");
    }

    #[tokio::test]
    async fn explains_rest_query() {
        let server = MockServer::start().await;

        let raw = format!(
            r#"
version: 1
sources:
  crm:
    type: rest
    base_url: {base_url}
    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
          status:
            path: "$.status"
            type: string
        pushdown:
          filters:
            status:
              param: status
              operators: ["="]
          limit:
            param: limit
          projection:
            param: fields
            style: comma_separated
"#,
            base_url = server.uri()
        );

        let config = Config::from_yaml_str(&raw).expect("config should parse");
        let engine = Engine::from_config(config).expect("engine should build");

        let explanation = engine
            .explain("SELECT id, name FROM crm.customers WHERE status = 'active' LIMIT 10")
            .await
            .expect("explain should succeed");

        assert!(explanation.contains("provider: rest"));
        assert!(
            explanation.contains("http plan: GET /customers?fields=id,name&limit=10&status=active")
        );
        assert!(explanation.contains("pushed limit: 10"));
    }

    #[tokio::test]
    async fn executes_sql_against_cursor_paginated_rest_provider() {
        let server = MockServer::start().await;
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_for_responder = requests.clone();

        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(move |request: &wiremock::Request| {
                requests_for_responder.fetch_add(1, Ordering::SeqCst);
                let query = request.url.query().unwrap_or_default();
                if query.contains("cursor=next-1") {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "data": [
                            { "id": 2, "name": "Anna", "status": "active" }
                        ],
                        "next_cursor": null
                    }))
                } else {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "data": [
                            { "id": 1, "name": "Mario", "status": "active" }
                        ],
                        "next_cursor": "next-1"
                    }))
                }
            })
            .mount(&server)
            .await;

        let raw = format!(
            r#"
version: 1
sources:
  crm:
    type: rest
    base_url: {base_url}
    runtime:
      max_pages: 10
      retry:
        max_attempts: 2
        backoff_ms: 1
    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
          status:
            path: "$.status"
            type: string
        pagination:
          type: cursor
          cursor_param: cursor
          cursor_path: "$.next_cursor"
"#,
            base_url = server.uri()
        );

        let config = Config::from_yaml_str(&raw).expect("config should parse");
        let engine = Engine::from_config(config).expect("engine should build");

        let batches = engine
            .query("SELECT id, name FROM crm.customers LIMIT 2")
            .await
            .expect("query should succeed");

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 2);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn executes_sql_against_link_header_paginated_rest_provider() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        "Link",
                        format!("<{}/customers?page=2>; rel=\"next\"", server.uri()),
                    )
                    .set_body_json(json!({
                        "data": [
                            { "id": 1, "name": "Mario" }
                        ]
                    })),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 2, "name": "Anna" }
                ]
            })))
            .mount(&server)
            .await;

        let raw = format!(
            r#"
version: 1
sources:
  crm:
    type: rest
    base_url: {base_url}
    runtime:
      max_pages: 10
    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
        pagination:
          type: link_header
          rel: next
"#,
            base_url = server.uri()
        );

        let config = Config::from_yaml_str(&raw).expect("config should parse");
        let engine = Engine::from_config(config).expect("engine should build");

        let batches = engine
            .query("SELECT id, name FROM crm.customers LIMIT 2")
            .await
            .expect("query should succeed");

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn rows_file_jsonl_yaml_round_trip_runs_sql() {
        use std::io::Write;
        use tempfile::TempDir;

        // Write a 100-row JSONL fixture.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.jsonl");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            for id in 0..100 {
                let level = if id % 7 == 0 { "ERROR" } else { "INFO" };
                writeln!(
                    f,
                    r#"{{"id": {id}, "level": "{level}", "msg": "event {id}"}}"#
                )
                .unwrap();
            }
        }

        let raw = format!(
            r#"
version: 1
sources:
  app:
    type: rows_file
    tables:
      events:
        path: "{}"
        chunk_rows: 10
        parser:
          format: jsonl
          columns:
            - {{ name: id,    path: "$.id",    type: int64 }}
            - {{ name: level, path: "$.level", type: string }}
            - {{ name: msg,   path: "$.msg",   type: string }}
        indexed_columns:
          - {{ name: id,    kind: zone_map }}
          - {{ name: level, kind: bloom }}
"#,
            path.display()
        );

        let config = Config::from_yaml_str(&raw).expect("config parses");
        let engine = Engine::from_config(config).expect("engine builds");

        // Sanity: the table is registered under the qualified name.
        assert!(engine.registered_tables().iter().any(|t| t == "app.events"));

        let batches = engine
            .query("SELECT id FROM app.events WHERE level = 'ERROR' ORDER BY id")
            .await
            .expect("query runs");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Every 7th row → 0,7,14,...,98 = 15 rows.
        assert_eq!(total, 15);
    }

    #[test]
    fn rows_file_handle_refresh_lifecycle_tracks_decisions() {
        use std::io::Write;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("evt.jsonl");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            for id in 0..50 {
                writeln!(f, r#"{{"id": {id}}}"#).unwrap();
            }
        }

        let raw = format!(
            r#"
version: 1
sources:
  s:
    type: rows_file
    tables:
      t:
        path: "{}"
        chunk_rows: 50
        parser:
          format: jsonl
          columns:
            - {{ name: id, path: "$.id", type: int64 }}
"#,
            path.display()
        );
        let config = Config::from_yaml_str(&raw).unwrap();
        let cfg = match config.sources.get("s").unwrap() {
            dbfy_config::SourceConfig::RowsFile(rf) => rf.tables.get("t").unwrap().clone(),
            _ => panic!("rows_file expected"),
        };

        // First call against a fresh file → BuiltFresh, summary = 1 chunk.
        let handle = super::build_rows_file_handle("s", "t", &cfg).unwrap();
        let RowsFileHandle::Single(table) = handle else {
            panic!("expected single table");
        };
        let dec = table.refresh().unwrap();
        assert_eq!(dec, dbfy_provider_rows_file::RefreshDecision::BuiltFresh);
        let s1 = table.index_summary().unwrap();
        assert_eq!(s1.chunks, 1);
        assert_eq!(s1.total_rows, 50);

        // Second call without changes → Reused.
        let dec = table.refresh().unwrap();
        assert_eq!(dec, dbfy_provider_rows_file::RefreshDecision::Reused);

        // Append rows → Extended (incremental, prior chunks preserved).
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            for id in 50..100 {
                writeln!(f, r#"{{"id": {id}}}"#).unwrap();
            }
        }
        let dec = table.refresh().unwrap();
        assert_eq!(dec, dbfy_provider_rows_file::RefreshDecision::Extended);
        let s2 = table.index_summary().unwrap();
        assert_eq!(s2.total_rows, 100);
        assert!(s2.chunks >= 2, "extend should add at least one chunk");

        // Force rebuild → single chunk again (50-row chunk_rows + 100 rows = 2 chunks fresh).
        table.rebuild().unwrap();
        let dec = table.refresh().unwrap();
        assert_eq!(dec, dbfy_provider_rows_file::RefreshDecision::Reused);
    }

    #[tokio::test]
    async fn rows_file_jsonl_supports_range_pruning_via_zone_map() {
        use std::io::Write;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("range.jsonl");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            for id in 0..1000 {
                writeln!(f, r#"{{"id": {id}}}"#).unwrap();
            }
        }

        let raw = format!(
            r#"
version: 1
sources:
  s:
    type: rows_file
    tables:
      t:
        path: "{}"
        chunk_rows: 50
        parser:
          format: jsonl
          columns:
            - {{ name: id, path: "$.id", type: int64 }}
        indexed_columns:
          - {{ name: id, kind: zone_map }}
"#,
            path.display()
        );

        let config = Config::from_yaml_str(&raw).unwrap();
        let engine = Engine::from_config(config).unwrap();
        let batches = engine
            .query("SELECT id FROM s.t WHERE id >= 500 AND id < 510")
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
    }

    #[tokio::test]
    async fn parquet_source_keeps_pushdown() {
        // Regression: v1's first cut went through `read_parquet().collect()`
        // which materialised every row before applying filters. Calling
        // DataFusion's native `register_parquet` (ListingTable) instead
        // preserves predicate + projection + row-group pushdown.
        // We don't directly inspect what got pruned here; we just check
        // a `WHERE id = K` query against a 1000-row file actually finds
        // the row, which is the correctness side. The pushdown win is
        // visible in `EXPLAIN`.
        use datafusion::dataframe::DataFrameWriteOptions;
        use datafusion::execution::context::SessionContext as DfSessionContext;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let parquet_path = dir.path().join("big.parquet");
        let writer_ctx = DfSessionContext::new();
        let df = writer_ctx
            .sql("SELECT t.value AS id FROM generate_series(1, 1000) AS t")
            .await
            .expect("writer SQL");
        df.write_parquet(
            parquet_path.to_str().unwrap(),
            DataFrameWriteOptions::default(),
            None,
        )
        .await
        .expect("write parquet");

        let yaml = format!(
            r#"
version: 1
sources:
  data:
    type: parquet
    tables:
      items:
        path: "{}"
"#,
            parquet_path.display()
        );
        let config = Config::from_yaml_str(&yaml).expect("config");
        let engine = Engine::from_config(config).expect("engine");

        let batches = engine
            .query("SELECT id FROM data.items WHERE id = 777")
            .await
            .expect("filtered query");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "should find exactly id=777");

        // Range filter — confirms predicate pushdown into the parquet
        // reader doesn't break the result set. The actual proof that
        // pushdown is happening (vs materialise-then-filter) is the
        // ListingTable code path inside `register_parquet`; this test
        // pins the correctness side.
        let batches = engine
            .query("SELECT count(*) FROM data.items WHERE id BETWEEN 100 AND 199")
            .await
            .expect("range query");
        let total: i64 = batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .map(|a| a.value(0))
            })
            .sum();
        assert_eq!(total, 100, "BETWEEN 100 AND 199 should match 100 rows");
    }

    #[tokio::test]
    async fn parquet_source_round_trips() {
        // Write a small parquet file via DataFusion, then read it
        // back through dbfy's `parquet` source kind.
        use datafusion::dataframe::DataFrameWriteOptions;
        use datafusion::execution::context::SessionContext as DfSessionContext;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let parquet_path = dir.path().join("items.parquet");

        let writer_ctx = DfSessionContext::new();
        let df = writer_ctx
            .sql("SELECT * FROM (VALUES (1, 'mario'), (2, 'anna'), (3, 'luca')) AS t(id, name)")
            .await
            .expect("writer SQL");
        df.write_parquet(
            parquet_path.to_str().unwrap(),
            DataFrameWriteOptions::default(),
            None,
        )
        .await
        .expect("write parquet");

        let yaml = format!(
            r#"
version: 1
sources:
  data:
    type: parquet
    tables:
      items:
        path: "{}"
"#,
            parquet_path.display()
        );
        let config = Config::from_yaml_str(&yaml).expect("config parses");
        let engine = Engine::from_config(config).expect("engine builds");
        let batches = engine
            .query("SELECT count(*) FROM data.items")
            .await
            .expect("count over parquet");
        let total: i64 = batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .map(|a| a.value(0))
            })
            .sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn graphql_source_against_wiremock() {
        // Mock a GraphQL endpoint that returns a fixed JSON body and
        // verify dbfy POSTs the configured query and root-extracts
        // the response correctly.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "users": [
                        {"login": "mario", "stars": 100},
                        {"login": "anna",  "stars":  42},
                        {"login": "luca",  "stars":  73}
                    ]
                }
            })))
            .mount(&server)
            .await;

        let yaml = format!(
            r#"
version: 1
sources:
  gh:
    type: graphql
    endpoint: "{server}/graphql"
    tables:
      users:
        query: "{{ users {{ login stars }} }}"
        root: "$.data.users[*]"
        columns:
          login: {{ path: "$.login", type: string }}
          stars: {{ path: "$.stars", type: int64 }}
"#,
            server = server.uri(),
        );

        let config = Config::from_yaml_str(&yaml).expect("config parses");
        let engine = Engine::from_config(config).expect("engine builds");
        let batches = engine
            .query("SELECT login FROM gh.users WHERE stars > 50 ORDER BY login")
            .await
            .expect("graphql query");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "should match mario + luca");
    }

    #[test]
    fn parquet_config_round_trips_through_yaml() {
        let yaml = r#"
version: 1
sources:
  warehouse:
    type: parquet
    tables:
      orders:
        path: /data/orders/*.parquet
      products:
        path: /data/products.parquet
"#;
        let config = Config::from_yaml_str(yaml).expect("parses");
        match config.sources.get("warehouse").unwrap() {
            dbfy_config::SourceConfig::Parquet(p) => {
                assert_eq!(p.tables.len(), 2);
                assert!(p.tables.contains_key("orders"));
                assert!(p.tables.contains_key("products"));
            }
            _ => panic!("expected parquet source"),
        }
    }

    #[test]
    fn excel_config_round_trips_through_yaml() {
        let yaml = r#"
version: 1
sources:
  finance:
    type: excel
    tables:
      q1:
        path: /reports/q1.xlsx
        sheet: Revenue
        has_header: true
      q2:
        path: /reports/q2.xlsx
"#;
        let config = Config::from_yaml_str(yaml).expect("parses");
        match config.sources.get("finance").unwrap() {
            dbfy_config::SourceConfig::Excel(e) => {
                assert_eq!(e.tables.len(), 2);
                assert_eq!(e.tables["q1"].sheet.as_deref(), Some("Revenue"));
                assert!(e.tables["q1"].has_header);
                assert!(e.tables["q2"].sheet.is_none());
            }
            _ => panic!("expected excel source"),
        }
    }

    #[tokio::test]
    async fn graphql_pushdown_sends_variable_values_to_endpoint() {
        // Wiremock asserts the POST body contains the GraphQL
        // `variables.statusVar = "active"` payload — proves the WHERE
        // clause was translated into a variable, not applied above
        // the scan.
        use wiremock::matchers::body_partial_json;

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .and(body_partial_json(json!({
                "variables": { "statusVar": "active" }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "users": [
                        {"login": "mario", "status": "active"},
                        {"login": "anna",  "status": "active"}
                    ]
                }
            })))
            .mount(&server)
            .await;
        // Fallback that should NEVER match if pushdown works.
        Mock::given(method("POST"))
            .and(path("/graphql"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "users": [{"login": "FALLBACK", "status": "x"}] }
            })))
            .mount(&server)
            .await;

        let yaml = format!(
            r#"
version: 1
sources:
  gh:
    type: graphql
    endpoint: "{server}/graphql"
    tables:
      users:
        query: "query($statusVar: String) {{ users(status: $statusVar) {{ login status }} }}"
        root: "$.data.users[*]"
        columns:
          login:  {{ path: "$.login",  type: string }}
          status: {{ path: "$.status", type: string }}
        pushdown:
          variables:
            status: statusVar
"#,
            server = server.uri(),
        );
        let config = Config::from_yaml_str(&yaml).expect("config");
        let engine = Engine::from_config(config).expect("engine");
        let batches = engine
            .query("SELECT login FROM gh.users WHERE status = 'active'")
            .await
            .expect("graphql query");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 2,
            "should match mario + anna; FALLBACK means pushdown didn't fire"
        );
    }

    #[tokio::test]
    async fn excel_pushdown_filters_at_read_time() {
        use rust_xlsxwriter::Workbook;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let xlsx_path = dir.path().join("data.xlsx");
        let mut wb = Workbook::new();
        let sheet = wb.add_worksheet();
        sheet.write_string(0, 0, "id").unwrap();
        sheet.write_string(0, 1, "status").unwrap();
        sheet.write_string(0, 2, "name").unwrap();
        for (i, (status, name)) in [
            ("active", "mario"),
            ("inactive", "anna"),
            ("active", "luca"),
            ("active", "giulia"),
            ("inactive", "paolo"),
        ]
        .iter()
        .enumerate()
        {
            sheet
                .write_string((i + 1) as u32, 0, &(i + 1).to_string())
                .unwrap();
            sheet.write_string((i + 1) as u32, 1, *status).unwrap();
            sheet.write_string((i + 1) as u32, 2, *name).unwrap();
        }
        wb.save(&xlsx_path).unwrap();

        let yaml = format!(
            r#"
version: 1
sources:
  finance:
    type: excel
    tables:
      records:
        path: "{}"
        has_header: true
"#,
            xlsx_path.display()
        );
        let config = Config::from_yaml_str(&yaml).expect("config");
        let engine = Engine::from_config(config).expect("engine");
        let batches = engine
            .query("SELECT name FROM finance.records WHERE status = 'active' ORDER BY name")
            .await
            .expect("excel query");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "active rows: mario + luca + giulia");
    }

    #[test]
    fn postgres_filter_translation_emits_correct_sql() {
        // Unit test on the SQL builder: ensures `WHERE status =
        // 'active' AND id >= 100` becomes the right Postgres syntax
        // including identifier quoting and string escaping.
        use arrow_schema::{DataType as ArrowDataType, Field, Schema};
        use datafusion::logical_expr::{col, lit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("status", ArrowDataType::Utf8, true),
        ]));
        let provider = super::PostgresTableProvider {
            connection: "postgres://x".into(),
            relation: "public.users".into(),
            schema,
        };
        let f1 = col("status").eq(lit("active"));
        let f2 = col("id").gt_eq(lit(100i64));
        let sql = provider.build_sql(None, &[f1, f2], Some(50));
        // Order between filters depends on input order; we joined in
        // the same order via filter().filter_map().
        assert!(sql.starts_with("SELECT * FROM public.users WHERE"), "{sql}");
        assert!(sql.contains("\"status\" = 'active'"), "{sql}");
        assert!(sql.contains("\"id\" >= 100"), "{sql}");
        assert!(sql.ends_with("LIMIT 50"), "{sql}");
    }

    #[test]
    fn postgres_config_round_trips_through_yaml() {
        let yaml = r#"
version: 1
sources:
  prod_db:
    type: postgres
    connection: "postgres://reader:secret@host:5432/app"
    tables:
      users:
        relation: public.users
      orders:
        relation: orders
"#;
        let config = Config::from_yaml_str(yaml).expect("parses");
        match config.sources.get("prod_db").unwrap() {
            dbfy_config::SourceConfig::Postgres(pg) => {
                assert_eq!(pg.tables.len(), 2);
                assert_eq!(pg.tables["users"].relation, "public.users");
            }
            _ => panic!("expected postgres source"),
        }
    }
}
