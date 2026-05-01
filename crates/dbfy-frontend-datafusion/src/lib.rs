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
use futures::StreamExt;
use dbfy_config::{Config, RowsFileTableConfig, SourceConfig};
use dbfy_provider::{
    DynProvider, FilterOperator, ProviderCapabilities, ProviderError, ScalarValue, ScanRequest,
    SimpleFilter,
};
use dbfy_provider_rest::{RestProviderError, RestTable, SimpleFilter as RestSimpleFilter};
pub use dbfy_provider_rows_file::RowsFileHandle;
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
                        engine.programmatic_tables.insert(
                            qualify_table_name(source_name, table_name),
                            provider,
                        );
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
        let ctx = self.build_session_context()?;
        let dataframe = ctx.sql(sql).await?;
        let batches = dataframe.collect().await?;
        Ok(batches)
    }

    pub async fn explain(&self, sql: &str) -> Result<String> {
        let ctx = self.build_session_context()?;
        let dataframe = ctx.sql(sql).await?;
        let plan = dataframe.into_optimized_plan()?;
        render_explain(self, sql, &plan)
    }

    fn build_session_context(&self) -> Result<SessionContext> {
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
        let projection_names = projection_to_names(self.schema(), projection)?.unwrap_or_default();
        let rest_filters = filters
            .iter()
            .map(expr_to_rest_filter)
            .collect::<Result<Vec<_>>>()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let plan =
            RestStreamExecutionPlan::try_new(self.table.clone(), projection_names, rest_filters, limit)
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
        filters: Vec<RestSimpleFilter>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let schema = table.schema_for_projection(&projection_names)?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table,
            projection_names,
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

        let mut builder = RecordBatchReceiverStream::builder(schema, 2);
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

fn describe_filter_capabilities(
    capabilities: &dbfy_provider::FilterCapabilities,
) -> String {
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
    use futures::stream;
    use dbfy_config::Config;
    use dbfy_provider::{
        ProgrammaticTableProvider, ProviderError, ProviderResult, ScanResponse,
    };
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
                self.captured.lock().unwrap().extend(request.filters.clone());
                let array = Arc::new(StringArray::from(vec!["alpha"])) as ArrayRef;
                let batch =
                    RecordBatch::try_new(self.schema.clone(), vec![array]).unwrap();
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
        assert_eq!(filters.len(), 1, "expected one pushed filter, got {filters:?}");
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
                    dbfy_provider_static::StaticRecordBatchProvider::new(
                        schema,
                        vec![batch],
                    )
                    .with_capabilities(
                        dbfy_provider::ProviderCapabilities {
                            projection_pushdown: true,
                            filter_pushdown: filter_capabilities,
                            limit_pushdown: true,
                            order_by_pushdown: false,
                            aggregate_pushdown: false,
                        },
                    ),
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
        assert!(engine
            .registered_tables()
            .iter()
            .any(|t| t == "app.events"));

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
}
