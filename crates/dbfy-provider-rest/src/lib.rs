use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

mod cache;
pub use cache::{HttpCache, Metrics as CacheMetrics};

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use dbfy_config::{
    ApiKeyLocation, AuthConfig, DataType, LimitPushdownConfig, PaginationConfig,
    ProjectionPushdownConfig, ProjectionStyle, RestSourceConfig, RestTableConfig,
};
use futures::SinkExt;
use futures::stream::BoxStream;
use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
use reqwest::{Client, StatusCode, Url};
use serde_json::Value;
use serde_json_path::JsonPath;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, RestProviderError>;

#[derive(Debug, Error)]
pub enum RestProviderError {
    #[error("unknown column `{column}` for table `{table}`")]
    UnknownColumn { table: String, column: String },
    #[error("invalid base url for source `{source_name}`: {message}")]
    InvalidBaseUrl {
        source_name: String,
        message: String,
    },
    #[error("http error for `{source_name}.{table}`: {message}")]
    Http {
        source_name: String,
        table: String,
        message: String,
    },
    #[error("http timeout for `{source_name}.{table}`: {message}")]
    Timeout {
        source_name: String,
        table: String,
        message: String,
    },
    #[error("rate limit reached for `{source_name}.{table}`: {message}")]
    RateLimited {
        source_name: String,
        table: String,
        retry_after_seconds: Option<u64>,
        message: String,
    },
    #[error("http status error for `{source_name}.{table}` ({status_code}): {message}")]
    HttpStatus {
        source_name: String,
        table: String,
        status_code: u16,
        message: String,
    },
    #[error("json decode error for `{source_name}.{table}`: {message}")]
    JsonDecode {
        source_name: String,
        table: String,
        message: String,
    },
    #[error("unsupported root path `{root}` for `{source_name}.{table}`")]
    UnsupportedRootPath {
        source_name: String,
        table: String,
        root: String,
    },
    #[error("unsupported column path `{path}` for `{source_name}.{table}.{column}`")]
    UnsupportedColumnPath {
        source_name: String,
        table: String,
        column: String,
        path: String,
    },
    #[error("unsupported data type `{data_type:?}` for `{source_name}.{table}.{column}`")]
    UnsupportedDataType {
        source_name: String,
        table: String,
        column: String,
        data_type: DataType,
    },
    #[error(
        "authentication environment variable `{env_var}` is not set for source `{source_name}`"
    )]
    MissingEnvironmentVariable {
        source_name: String,
        env_var: String,
    },
    #[error("unsupported pagination for `{source_name}.{table}`")]
    UnsupportedPagination { source_name: String, table: String },
    #[error("unsupported cursor path `{path}` for `{source_name}.{table}`")]
    UnsupportedCursorPath {
        source_name: String,
        table: String,
        path: String,
    },
    #[error("invalid link header for `{source_name}.{table}`: {message}")]
    InvalidLinkHeader {
        source_name: String,
        table: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleFilter {
    pub column: String,
    pub operator: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestRequestPlan {
    pub source_name: String,
    pub table_name: String,
    pub path: String,
    pub query_params: BTreeMap<String, String>,
    pub pushed_filters: Vec<SimpleFilter>,
    pub residual_filters: Vec<SimpleFilter>,
}

impl RestRequestPlan {
    pub fn render_http_request(&self) -> String {
        if self.query_params.is_empty() {
            return format!("GET {}", self.path);
        }

        let query = self
            .query_params
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");

        format!("GET {}?{}", self.path, query)
    }
}

#[derive(Debug, Clone)]
pub struct RestExecutionResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

pub struct RestStream {
    pub schema: SchemaRef,
    pub stream: BoxStream<'static, Result<RecordBatch>>,
}

#[derive(Debug, Clone)]
pub(crate) struct HttpPageResponse {
    pub(crate) body: Value,
    pub(crate) headers: HeaderMap,
    pub(crate) url: Url,
}

#[derive(Debug, Clone)]
pub struct RestTable {
    source_name: String,
    table_name: String,
    base_url: String,
    auth: Option<AuthConfig>,
    runtime: Option<dbfy_config::RuntimeConfig>,
    config: RestTableConfig,
    cache: Option<Arc<HttpCache>>,
}

impl RestTable {
    pub fn new(
        source_name: impl Into<String>,
        source: &RestSourceConfig,
        table_name: impl Into<String>,
        config: RestTableConfig,
    ) -> Self {
        let cache = source
            .runtime
            .as_ref()
            .and_then(|r| r.cache.as_ref())
            .map(|cfg| Arc::new(HttpCache::from_config(cfg)));
        Self {
            source_name: source_name.into(),
            table_name: table_name.into(),
            base_url: source.base_url.clone(),
            auth: source.auth.clone(),
            runtime: source.runtime.clone(),
            config,
            cache,
        }
    }

    /// Replace the table's HTTP cache with an externally-managed instance.
    /// Useful when an engine wants to share one cache across many tables of
    /// the same source instead of using the per-table default.
    pub fn with_cache(mut self, cache: Arc<HttpCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Snapshot of the per-table cache counters, or `None` if no cache is
    /// attached. Mostly used by tests and operational dashboards.
    pub fn cache_metrics(&self) -> Option<CacheMetrics> {
        self.cache.as_ref().map(|c| c.metrics())
    }

    pub fn plan_request(
        &self,
        projection: &[String],
        filters: &[SimpleFilter],
        limit: Option<usize>,
    ) -> RestRequestPlan {
        let mut plan = RestRequestPlan {
            source_name: self.source_name.clone(),
            table_name: self.table_name.clone(),
            path: self.config.endpoint.path.clone(),
            query_params: BTreeMap::new(),
            pushed_filters: Vec::new(),
            residual_filters: Vec::new(),
        };

        if let Some(pushdown) = &self.config.pushdown {
            if let Some(projection_config) = &pushdown.projection {
                add_projection(&mut plan, projection_config, projection);
            }

            for filter in filters {
                if let Some(rule) = pushdown.filters.get(&filter.column) {
                    if let Some(param) = rule.resolve_param(&filter.operator) {
                        plan.query_params
                            .insert(param.to_string(), filter.value.clone());
                        plan.pushed_filters.push(filter.clone());
                        continue;
                    }
                }

                plan.residual_filters.push(filter.clone());
            }

            if let Some(limit_config) = &pushdown.limit {
                add_limit(&mut plan, limit_config, limit);
            }
        } else {
            plan.residual_filters.extend_from_slice(filters);
        }

        plan
    }

    pub async fn execute(
        &self,
        projection: &[String],
        filters: &[SimpleFilter],
        limit: Option<usize>,
    ) -> Result<RestExecutionResult> {
        let client = self.build_client()?;
        let mut plan = self.plan_request(projection, filters, limit);
        self.apply_auth_query_params(&mut plan)?;

        let rows = self.fetch_rows(&client, &plan, limit).await?;
        let effective_projection = self.effective_projection(projection);
        let schema = self.schema_for_projection(&effective_projection)?;
        let batch = self.rows_to_batch(&rows, &effective_projection, schema.clone())?;

        Ok(RestExecutionResult {
            schema,
            batches: vec![batch],
        })
    }

    pub fn execute_stream(
        &self,
        projection: &[String],
        filters: &[SimpleFilter],
        limit: Option<usize>,
    ) -> Result<RestStream> {
        let client = self.build_client()?;
        let mut plan = self.plan_request(projection, filters, limit);
        self.apply_auth_query_params(&mut plan)?;

        let effective_projection = self.effective_projection(projection);
        let schema = self.schema_for_projection(&effective_projection)?;

        let (mut tx, rx) = futures::channel::mpsc::channel::<Result<RecordBatch>>(2);
        let table = self.clone();
        let schema_for_task = schema.clone();

        tokio::spawn(async move {
            let outcome = table
                .stream_pages_into(
                    client,
                    plan,
                    effective_projection,
                    schema_for_task,
                    limit,
                    &mut tx,
                )
                .await;
            if let Err(error) = outcome {
                let _ = tx.send(Err(error)).await;
            }
        });

        Ok(RestStream {
            schema,
            stream: Box::pin(rx),
        })
    }

    async fn stream_pages_into(
        &self,
        client: Client,
        plan: RestRequestPlan,
        projection: Vec<String>,
        schema: SchemaRef,
        limit: Option<usize>,
        tx: &mut futures::channel::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let mut remaining = limit.unwrap_or(usize::MAX);

        match &self.config.pagination {
            None => {
                let body = self
                    .fetch_page(&client, &plan.path, &plan.query_params)
                    .await?;
                let rows = select_root_rows(
                    &body.body,
                    &self.config.root,
                    &self.source_name,
                    &self.table_name,
                )?;
                self.flush_rows(rows, &projection, schema, &mut remaining, tx)
                    .await?;
                Ok(())
            }
            Some(PaginationConfig::Page {
                page_param,
                size_param,
                start_page,
            }) => {
                let page_size = plan
                    .query_params
                    .get(size_param)
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or_else(|| limit.unwrap_or(1000).max(1));
                let mut page = start_page.unwrap_or(1);
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);

                for _ in 0..max_pages {
                    let mut params = plan.query_params.clone();
                    params.insert(page_param.clone(), page.to_string());
                    params
                        .entry(size_param.clone())
                        .or_insert_with(|| page_size.to_string());

                    let body = self.fetch_page(&client, &plan.path, &params).await?;
                    let rows = select_root_rows(
                        &body.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = rows.len();
                    let channel_ok = self
                        .flush_rows(rows, &projection, schema.clone(), &mut remaining, tx)
                        .await?;

                    if !channel_ok || remaining == 0 || row_count == 0 || row_count < page_size {
                        break;
                    }
                    page += 1;
                }
                Ok(())
            }
            Some(PaginationConfig::Offset {
                offset_param,
                limit_param,
            }) => {
                let page_size = plan
                    .query_params
                    .get(limit_param)
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or_else(|| limit.unwrap_or(1000).max(1));
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);
                let mut offset = 0usize;

                for _ in 0..max_pages {
                    let mut params = plan.query_params.clone();
                    params.insert(offset_param.clone(), offset.to_string());
                    params
                        .entry(limit_param.clone())
                        .or_insert_with(|| page_size.to_string());

                    let body = self.fetch_page(&client, &plan.path, &params).await?;
                    let rows = select_root_rows(
                        &body.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = rows.len();
                    let channel_ok = self
                        .flush_rows(rows, &projection, schema.clone(), &mut remaining, tx)
                        .await?;

                    if !channel_ok || remaining == 0 || row_count == 0 || row_count < page_size {
                        break;
                    }
                    offset += page_size;
                }
                Ok(())
            }
            Some(PaginationConfig::Cursor {
                cursor_param,
                cursor_path,
            }) => {
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);
                let mut next_cursor: Option<String> = None;

                for _ in 0..max_pages {
                    let mut params = plan.query_params.clone();
                    if let Some(cursor) = &next_cursor {
                        params.insert(cursor_param.clone(), cursor.clone());
                    }
                    let body = self.fetch_page(&client, &plan.path, &params).await?;
                    let rows = select_root_rows(
                        &body.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = rows.len();
                    let channel_ok = self
                        .flush_rows(rows, &projection, schema.clone(), &mut remaining, tx)
                        .await?;

                    if !channel_ok || remaining == 0 || row_count == 0 {
                        break;
                    }

                    next_cursor = extract_optional_string_path(
                        &body.body,
                        cursor_path,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    if next_cursor.is_none() {
                        break;
                    }
                }
                Ok(())
            }
            Some(PaginationConfig::LinkHeader { rel }) => {
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);
                let expected_rel = rel.as_deref().unwrap_or("next");
                let mut next_url: Option<Url> = None;

                for _ in 0..max_pages {
                    let page = match &next_url {
                        Some(url) => self.fetch_page_by_url(&client, url.clone()).await?,
                        None => {
                            self.fetch_page(&client, &plan.path, &plan.query_params)
                                .await?
                        }
                    };
                    let rows = select_root_rows(
                        &page.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = rows.len();
                    let channel_ok = self
                        .flush_rows(rows, &projection, schema.clone(), &mut remaining, tx)
                        .await?;

                    if !channel_ok || remaining == 0 || row_count == 0 {
                        break;
                    }

                    next_url = extract_next_link_url(
                        &page.headers,
                        &page.url,
                        expected_rel,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    if next_url.is_none() {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    async fn flush_rows(
        &self,
        mut rows: Vec<Value>,
        projection: &[String],
        schema: SchemaRef,
        remaining: &mut usize,
        tx: &mut futures::channel::mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<bool> {
        if rows.len() > *remaining {
            rows.truncate(*remaining);
        }
        let yielded = rows.len();
        if yielded == 0 {
            return Ok(true);
        }
        let batch = self.rows_to_batch(&rows, projection, schema)?;
        let channel_ok = tx.send(Ok(batch)).await.is_ok();
        *remaining = remaining.saturating_sub(yielded);
        Ok(channel_ok)
    }

    pub fn config(&self) -> &RestTableConfig {
        &self.config
    }

    pub fn schema_for_projection(&self, projection: &[String]) -> Result<SchemaRef> {
        let full_schema = self.config.arrow_schema();
        if projection.is_empty() {
            return Ok(full_schema);
        }

        let mut fields = Vec::with_capacity(projection.len());
        for name in projection {
            let index = full_schema
                .fields()
                .iter()
                .position(|field| field.name() == name)
                .ok_or_else(|| RestProviderError::UnknownColumn {
                    table: format!("{}.{}", self.source_name, self.table_name),
                    column: name.clone(),
                })?;
            fields.push(full_schema.field(index).clone());
        }

        Ok(std::sync::Arc::new(arrow_schema::Schema::new(fields)))
    }

    fn effective_projection(&self, projection: &[String]) -> Vec<String> {
        if projection.is_empty() {
            let mut names = self.config.columns.keys().cloned().collect::<Vec<_>>();
            if self.config.include_raw_json {
                names.push(self.config.raw_column.clone());
            }
            names
        } else {
            projection.to_vec()
        }
    }

    fn build_client(&self) -> Result<Client> {
        let mut builder = Client::builder();

        if let Some(runtime) = &self.runtime {
            if let Some(timeout_ms) = runtime.timeout_ms {
                builder = builder.timeout(Duration::from_millis(timeout_ms));
            }
        }

        builder.build().map_err(|error| RestProviderError::Http {
            source_name: self.source_name.clone(),
            table: self.table_name.clone(),
            message: error.to_string(),
        })
    }

    fn apply_auth_query_params(&self, plan: &mut RestRequestPlan) -> Result<()> {
        let Some(AuthConfig::ApiKey {
            r#in: ApiKeyLocation::Query,
            name,
            value_env,
        }) = &self.auth
        else {
            return Ok(());
        };

        let value = std::env::var(value_env).map_err(|_| {
            RestProviderError::MissingEnvironmentVariable {
                source_name: self.source_name.clone(),
                env_var: value_env.clone(),
            }
        })?;
        plan.query_params.insert(name.clone(), value);
        Ok(())
    }

    async fn fetch_rows(
        &self,
        client: &Client,
        plan: &RestRequestPlan,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        match &self.config.pagination {
            None => {
                let body = self
                    .fetch_page(client, &plan.path, &plan.query_params)
                    .await?;
                let mut rows = select_root_rows(
                    &body.body,
                    &self.config.root,
                    &self.source_name,
                    &self.table_name,
                )?;
                apply_limit(&mut rows, limit);
                Ok(rows)
            }
            Some(PaginationConfig::Page {
                page_param,
                size_param,
                start_page,
            }) => {
                let page_size = plan
                    .query_params
                    .get(size_param)
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or_else(|| limit.unwrap_or(1000).max(1));
                let mut page = start_page.unwrap_or(1);
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);
                let mut rows = Vec::new();

                for _ in 0..max_pages {
                    let mut params = plan.query_params.clone();
                    params.insert(page_param.clone(), page.to_string());
                    params
                        .entry(size_param.clone())
                        .or_insert_with(|| page_size.to_string());

                    let body = self.fetch_page(client, &plan.path, &params).await?;
                    let page_rows = select_root_rows(
                        &body.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = page_rows.len();
                    rows.extend(page_rows);
                    apply_limit(&mut rows, limit);

                    if row_count == 0 || row_count < page_size || limit_reached(rows.len(), limit) {
                        break;
                    }

                    page += 1;
                }

                Ok(rows)
            }
            Some(PaginationConfig::Offset {
                offset_param,
                limit_param,
            }) => {
                let page_size = plan
                    .query_params
                    .get(limit_param)
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or_else(|| limit.unwrap_or(1000).max(1));
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);
                let mut offset = 0usize;
                let mut rows = Vec::new();

                for _ in 0..max_pages {
                    let mut params = plan.query_params.clone();
                    params.insert(offset_param.clone(), offset.to_string());
                    params
                        .entry(limit_param.clone())
                        .or_insert_with(|| page_size.to_string());

                    let body = self.fetch_page(client, &plan.path, &params).await?;
                    let page_rows = select_root_rows(
                        &body.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = page_rows.len();
                    rows.extend(page_rows);
                    apply_limit(&mut rows, limit);

                    if row_count == 0 || row_count < page_size || limit_reached(rows.len(), limit) {
                        break;
                    }

                    offset += page_size;
                }

                Ok(rows)
            }
            Some(PaginationConfig::Cursor {
                cursor_param,
                cursor_path,
            }) => {
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);
                let mut rows = Vec::new();
                let mut next_cursor: Option<String> = None;

                for _ in 0..max_pages {
                    let mut params = plan.query_params.clone();
                    if let Some(cursor) = &next_cursor {
                        params.insert(cursor_param.clone(), cursor.clone());
                    }

                    let body = self.fetch_page(client, &plan.path, &params).await?;
                    let page_rows = select_root_rows(
                        &body.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = page_rows.len();
                    rows.extend(page_rows);
                    apply_limit(&mut rows, limit);

                    if row_count == 0 || limit_reached(rows.len(), limit) {
                        break;
                    }

                    next_cursor = extract_optional_string_path(
                        &body.body,
                        cursor_path,
                        &self.source_name,
                        &self.table_name,
                    )?;

                    if next_cursor.is_none() {
                        break;
                    }
                }

                Ok(rows)
            }
            Some(PaginationConfig::LinkHeader { rel }) => {
                let max_pages = self
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.max_pages)
                    .unwrap_or(1000);
                let expected_rel = rel.as_deref().unwrap_or("next");
                let mut rows = Vec::new();
                let mut next_url: Option<Url> = None;

                for _ in 0..max_pages {
                    let page = match &next_url {
                        Some(url) => self.fetch_page_by_url(client, url.clone()).await?,
                        None => {
                            self.fetch_page(client, &plan.path, &plan.query_params)
                                .await?
                        }
                    };
                    let page_rows = select_root_rows(
                        &page.body,
                        &self.config.root,
                        &self.source_name,
                        &self.table_name,
                    )?;
                    let row_count = page_rows.len();
                    rows.extend(page_rows);
                    apply_limit(&mut rows, limit);

                    if row_count == 0 || limit_reached(rows.len(), limit) {
                        break;
                    }

                    next_url = extract_next_link_url(
                        &page.headers,
                        &page.url,
                        expected_rel,
                        &self.source_name,
                        &self.table_name,
                    )?;

                    if next_url.is_none() {
                        break;
                    }
                }

                Ok(rows)
            }
        }
    }

    async fn fetch_page(
        &self,
        client: &Client,
        path: &str,
        query_params: &BTreeMap<String, String>,
    ) -> Result<HttpPageResponse> {
        let mut url =
            Url::parse(&self.base_url).map_err(|error| RestProviderError::InvalidBaseUrl {
                source_name: self.source_name.clone(),
                message: error.to_string(),
            })?;
        url = url.join(path.trim_start_matches('/')).map_err(|error| {
            RestProviderError::InvalidBaseUrl {
                source_name: self.source_name.clone(),
                message: error.to_string(),
            }
        })?;
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in query_params {
                pairs.append_pair(key, value);
            }
        }

        self.fetch_page_by_url(client, url).await
    }

    async fn fetch_page_by_url(&self, client: &Client, url: Url) -> Result<HttpPageResponse> {
        if let Some(cache) = self.cache.clone() {
            let table = self.clone();
            let client = client.clone();
            let url_for_fetch = url.clone();
            return cache
                .get_or_fetch(&url, move || async move {
                    table.fetch_page_uncached(&client, url_for_fetch).await
                })
                .await;
        }
        self.fetch_page_uncached(client, url).await
    }

    async fn fetch_page_uncached(&self, client: &Client, url: Url) -> Result<HttpPageResponse> {
        let retry = self
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.retry.clone());
        let max_attempts = retry.as_ref().map_or(1, |retry| retry.max_attempts.max(1));
        let base_backoff_ms = retry.as_ref().map_or(0, |retry| retry.backoff_ms);

        for attempt in 1..=max_attempts {
            let mut request = client.get(url.clone());
            request = self.apply_auth_headers(request)?;

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let response_url = response.url().clone();
                        let headers = response.headers().clone();
                        let body = response.json::<Value>().await.map_err(|error| {
                            RestProviderError::JsonDecode {
                                source_name: self.source_name.clone(),
                                table: self.table_name.clone(),
                                message: error.to_string(),
                            }
                        })?;
                        return Ok(HttpPageResponse {
                            body,
                            headers,
                            url: response_url,
                        });
                    }

                    if attempt < max_attempts && should_retry_status(status) {
                        sleep_before_retry(
                            base_backoff_ms,
                            attempt,
                            response.headers().get(RETRY_AFTER),
                        )
                        .await;
                        continue;
                    }

                    let retry_after =
                        parse_retry_after_seconds(response.headers().get(RETRY_AFTER));
                    return Err(classify_status_error(
                        self.source_name.clone(),
                        self.table_name.clone(),
                        status,
                        retry_after,
                        &url,
                    ));
                }
                Err(error) => {
                    if error.is_timeout() {
                        if attempt < max_attempts {
                            tokio::time::sleep(exponential_backoff(base_backoff_ms, attempt)).await;
                            continue;
                        }

                        return Err(RestProviderError::Timeout {
                            source_name: self.source_name.clone(),
                            table: self.table_name.clone(),
                            message: error.to_string(),
                        });
                    }

                    if attempt < max_attempts {
                        tokio::time::sleep(exponential_backoff(base_backoff_ms, attempt)).await;
                        continue;
                    }

                    return Err(RestProviderError::Http {
                        source_name: self.source_name.clone(),
                        table: self.table_name.clone(),
                        message: error.to_string(),
                    });
                }
            }
        }

        Err(RestProviderError::Http {
            source_name: self.source_name.clone(),
            table: self.table_name.clone(),
            message: "retry loop exhausted".to_string(),
        })
    }

    fn apply_auth_headers(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder> {
        let Some(auth) = &self.auth else {
            return Ok(request);
        };

        match auth {
            AuthConfig::None => Ok(request),
            AuthConfig::Bearer { token_env } => {
                let token = std::env::var(token_env).map_err(|_| {
                    RestProviderError::MissingEnvironmentVariable {
                        source_name: self.source_name.clone(),
                        env_var: token_env.clone(),
                    }
                })?;
                Ok(request.bearer_auth(token))
            }
            AuthConfig::Basic {
                username_env,
                password_env,
            } => {
                let username = std::env::var(username_env).map_err(|_| {
                    RestProviderError::MissingEnvironmentVariable {
                        source_name: self.source_name.clone(),
                        env_var: username_env.clone(),
                    }
                })?;
                let password = std::env::var(password_env).map_err(|_| {
                    RestProviderError::MissingEnvironmentVariable {
                        source_name: self.source_name.clone(),
                        env_var: password_env.clone(),
                    }
                })?;
                Ok(request.basic_auth(username, Some(password)))
            }
            AuthConfig::ApiKey {
                r#in: ApiKeyLocation::Header,
                name,
                value_env,
            } => {
                let value = std::env::var(value_env).map_err(|_| {
                    RestProviderError::MissingEnvironmentVariable {
                        source_name: self.source_name.clone(),
                        env_var: value_env.clone(),
                    }
                })?;
                Ok(request.header(name, value))
            }
            AuthConfig::ApiKey {
                r#in: ApiKeyLocation::Query,
                ..
            } => Ok(request),
            AuthConfig::CustomHeader { name, value_env } => {
                let value = std::env::var(value_env).map_err(|_| {
                    RestProviderError::MissingEnvironmentVariable {
                        source_name: self.source_name.clone(),
                        env_var: value_env.clone(),
                    }
                })?;
                Ok(request.header(name, value))
            }
        }
    }

    fn rows_to_batch(
        &self,
        rows: &[Value],
        projection: &[String],
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let mut arrays = Vec::with_capacity(projection.len());
        for column_name in projection {
            if self.config.include_raw_json && column_name == &self.config.raw_column {
                arrays.push(build_raw_json_array(rows) as ArrayRef);
                continue;
            }

            let column = self.config.columns.get(column_name).ok_or_else(|| {
                RestProviderError::UnknownColumn {
                    table: format!("{}.{}", self.source_name, self.table_name),
                    column: column_name.clone(),
                }
            })?;
            arrays.push(self.build_column_array(column_name, column, rows)?);
        }

        RecordBatch::try_new(schema, arrays).map_err(|error| RestProviderError::JsonDecode {
            source_name: self.source_name.clone(),
            table: self.table_name.clone(),
            message: error.to_string(),
        })
    }

    fn build_column_array(
        &self,
        column_name: &str,
        column: &dbfy_config::ColumnConfig,
        rows: &[Value],
    ) -> Result<ArrayRef> {
        match column.r#type {
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for row in rows {
                    match extract_path(row, &column.path) {
                        Some(Value::Bool(value)) => builder.append_value(*value),
                        Some(Value::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Ok(std::sync::Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for row in rows {
                    match extract_path(row, &column.path).and_then(Value::as_i64) {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
                Ok(std::sync::Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for row in rows {
                    match extract_path(row, &column.path).and_then(Value::as_f64) {
                        Some(value) => builder.append_value(value),
                        None => builder.append_null(),
                    }
                }
                Ok(std::sync::Arc::new(builder.finish()))
            }
            DataType::String | DataType::Json => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    match extract_path(row, &column.path) {
                        Some(Value::String(value)) => builder.append_value(value),
                        Some(value) if matches!(column.r#type, DataType::Json) => {
                            builder.append_value(value.to_string())
                        }
                        Some(value) => builder.append_value(value_to_string(value)),
                        None => builder.append_null(),
                    }
                }
                Ok(std::sync::Arc::new(builder.finish()))
            }
            DataType::Date => {
                let mut builder = Date32Builder::new();
                for row in rows {
                    match extract_path(row, &column.path).and_then(Value::as_str) {
                        Some(value) => match NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                            Ok(date) => builder.append_value(date.num_days_from_ce() - 719_163),
                            Err(_) => builder.append_null(),
                        },
                        None => builder.append_null(),
                    }
                }
                Ok(std::sync::Arc::new(builder.finish()))
            }
            DataType::Timestamp => {
                // The schema produced by `dbfy_config::DataType::Timestamp` is
                // `Timestamp(µs, "UTC")`, so the builder must carry the same
                // timezone — otherwise `RecordBatch::try_new` rejects the
                // batch with a schema mismatch.
                let mut builder = TimestampMicrosecondBuilder::new().with_timezone("UTC");
                for row in rows {
                    match extract_path(row, &column.path).and_then(Value::as_str) {
                        Some(value) => match DateTime::parse_from_rfc3339(value) {
                            Ok(timestamp) => builder
                                .append_value(timestamp.with_timezone(&Utc).timestamp_micros()),
                            Err(_) => builder.append_null(),
                        },
                        None => builder.append_null(),
                    }
                }
                Ok(std::sync::Arc::new(builder.finish()))
            }
            DataType::Decimal | DataType::List | DataType::Struct => {
                Err(RestProviderError::UnsupportedDataType {
                    source_name: self.source_name.clone(),
                    table: self.table_name.clone(),
                    column: column_name.to_string(),
                    data_type: column.r#type.clone(),
                })
            }
        }
    }
}

fn add_projection(
    plan: &mut RestRequestPlan,
    projection_config: &ProjectionPushdownConfig,
    projection: &[String],
) {
    if projection.is_empty() {
        return;
    }

    match projection_config.style {
        ProjectionStyle::CommaSeparated => {
            plan.query_params
                .insert(projection_config.param.clone(), projection.join(","));
        }
    }
}

fn add_limit(plan: &mut RestRequestPlan, limit_config: &LimitPushdownConfig, limit: Option<usize>) {
    let Some(limit) = limit else {
        return;
    };

    let effective_limit = limit_config.max.map_or(limit, |max| limit.min(max));
    plan.query_params
        .insert(limit_config.param.clone(), effective_limit.to_string());
}

fn apply_limit(rows: &mut Vec<Value>, limit: Option<usize>) {
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
}

fn limit_reached(current_rows: usize, limit: Option<usize>) -> bool {
    limit.is_some_and(|limit| current_rows >= limit)
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

async fn sleep_before_retry(
    base_backoff_ms: u64,
    attempt: usize,
    retry_after: Option<&reqwest::header::HeaderValue>,
) {
    if let Some(retry_after) = retry_after
        && let Ok(value) = retry_after.to_str()
        && let Ok(seconds) = value.parse::<u64>()
    {
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        return;
    }

    tokio::time::sleep(exponential_backoff(base_backoff_ms, attempt)).await;
}

fn exponential_backoff(base_backoff_ms: u64, attempt: usize) -> Duration {
    if base_backoff_ms == 0 {
        return Duration::from_millis(0);
    }

    let multiplier = 2u64.saturating_pow((attempt.saturating_sub(1)) as u32);
    Duration::from_millis(base_backoff_ms.saturating_mul(multiplier))
}

fn parse_retry_after_seconds(retry_after: Option<&HeaderValue>) -> Option<u64> {
    retry_after
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn classify_status_error(
    source_name: String,
    table: String,
    status: StatusCode,
    retry_after_seconds: Option<u64>,
    url: &Url,
) -> RestProviderError {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return RestProviderError::RateLimited {
            source_name,
            table,
            retry_after_seconds,
            message: format!("HTTP status {} for url ({url})", status),
        };
    }

    RestProviderError::HttpStatus {
        source_name,
        table,
        status_code: status.as_u16(),
        message: format!("HTTP status {} for url ({url})", status),
    }
}

fn select_root_rows(document: &Value, root: &str, source: &str, table: &str) -> Result<Vec<Value>> {
    let path = JsonPath::parse(root).map_err(|_| RestProviderError::UnsupportedRootPath {
        source_name: source.to_string(),
        table: table.to_string(),
        root: root.to_string(),
    })?;
    Ok(path.query(document).all().into_iter().cloned().collect())
}

fn extract_path<'a>(row: &'a Value, path: &str) -> Option<&'a Value> {
    JsonPath::parse(path).ok()?.query(row).first()
}

fn extract_optional_string_path(
    row: &Value,
    path: &str,
    source_name: &str,
    table_name: &str,
) -> Result<Option<String>> {
    let parsed = JsonPath::parse(path).map_err(|_| RestProviderError::UnsupportedCursorPath {
        source_name: source_name.to_string(),
        table: table_name.to_string(),
        path: path.to_string(),
    })?;

    match parsed.query(row).first() {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(RestProviderError::UnsupportedCursorPath {
            source_name: source_name.to_string(),
            table: table_name.to_string(),
            path: path.to_string(),
        }),
    }
}

fn extract_next_link_url(
    headers: &HeaderMap,
    current_url: &Url,
    expected_rel: &str,
    source_name: &str,
    table_name: &str,
) -> Result<Option<Url>> {
    let Some(link_header) = headers.get(reqwest::header::LINK) else {
        return Ok(None);
    };
    let link_value =
        link_header
            .to_str()
            .map_err(|error| RestProviderError::InvalidLinkHeader {
                source_name: source_name.to_string(),
                table: table_name.to_string(),
                message: error.to_string(),
            })?;

    for entry in link_value.split(',') {
        let entry = entry.trim();
        let Some(url_start) = entry.find('<') else {
            continue;
        };
        let Some(url_end) = entry[url_start + 1..].find('>') else {
            continue;
        };
        let href = &entry[url_start + 1..url_start + 1 + url_end];
        let params = entry[url_start + 1 + url_end + 1..].trim();
        if !params.contains(&format!("rel=\"{expected_rel}\""))
            && !params.contains(&format!("rel={expected_rel}"))
        {
            continue;
        }

        let url = current_url
            .join(href)
            .or_else(|_| Url::parse(href))
            .map_err(|error| RestProviderError::InvalidLinkHeader {
                source_name: source_name.to_string(),
                table: table_name.to_string(),
                message: error.to_string(),
            })?;
        return Ok(Some(url));
    }

    Ok(None)
}

fn build_raw_json_array(rows: &[Value]) -> std::sync::Arc<arrow_array::StringArray> {
    let mut builder = StringBuilder::new();
    for row in rows {
        builder.append_value(row.to_string());
    }
    std::sync::Arc::new(builder.finish())
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow_array::Array;
    use dbfy_config::{
        ColumnConfig, Config, DataType, EndpointConfig, FilterOperatorConfig, FilterPushdownConfig,
        HttpMethod, LimitPushdownConfig, PaginationConfig, ProjectionPushdownConfig,
        ProjectionStyle, PushdownConfig, RestSourceConfig, RestTableConfig, SourceConfig,
    };
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{RestProviderError, RestTable, SimpleFilter};

    #[test]
    fn plans_projection_filter_and_limit_pushdown() {
        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: "https://api.example.com".to_string(),
                auth: None,
                runtime: None,
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([(
                    "status".to_string(),
                    ColumnConfig {
                        path: "$.status".to_string(),
                        r#type: DataType::String,
                    },
                )]),
                pagination: None,
                pushdown: Some(PushdownConfig {
                    filters: BTreeMap::from([(
                        "status".to_string(),
                        FilterPushdownConfig {
                            param: Some("status".to_string()),
                            operators: FilterOperatorConfig::List(vec!["=".to_string()]),
                        },
                    )]),
                    limit: Some(LimitPushdownConfig {
                        param: "limit".to_string(),
                        max: Some(100),
                    }),
                    projection: Some(ProjectionPushdownConfig {
                        param: "fields".to_string(),
                        style: ProjectionStyle::CommaSeparated,
                    }),
                }),
            },
        );

        let plan = table.plan_request(
            &["id".to_string(), "status".to_string()],
            &[SimpleFilter {
                column: "status".to_string(),
                operator: "=".to_string(),
                value: "active".to_string(),
            }],
            Some(250),
        );

        assert_eq!(
            plan.render_http_request(),
            "GET /customers?fields=id,status&limit=100&status=active"
        );
        assert!(plan.residual_filters.is_empty());
    }

    #[tokio::test]
    async fn executes_page_paginated_requests_and_builds_batches() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .and(query_param("page", "1"))
            .and(query_param("limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 1, "name": "Mario", "status": "active" },
                    { "id": 2, "name": "Anna", "status": "active" }
                ]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .and(query_param("page", "2"))
            .and(query_param("limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 3, "name": "Luca", "status": "inactive" }
                ]
            })))
            .mount(&server)
            .await;

        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: Some(dbfy_config::RuntimeConfig {
                    timeout_ms: Some(5_000),
                    max_concurrency: None,
                    max_pages: Some(10),
                    retry: None,
                    cache: None,
                }),
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([
                    (
                        "id".to_string(),
                        ColumnConfig {
                            path: "$.id".to_string(),
                            r#type: DataType::Int64,
                        },
                    ),
                    (
                        "name".to_string(),
                        ColumnConfig {
                            path: "$.name".to_string(),
                            r#type: DataType::String,
                        },
                    ),
                    (
                        "status".to_string(),
                        ColumnConfig {
                            path: "$.status".to_string(),
                            r#type: DataType::String,
                        },
                    ),
                ]),
                pagination: Some(PaginationConfig::Page {
                    page_param: "page".to_string(),
                    size_param: "limit".to_string(),
                    start_page: Some(1),
                }),
                pushdown: Some(PushdownConfig {
                    filters: BTreeMap::new(),
                    limit: Some(LimitPushdownConfig {
                        param: "limit".to_string(),
                        max: Some(2),
                    }),
                    projection: None,
                }),
            },
        );

        let result = table
            .execute(
                &["id".to_string(), "name".to_string()],
                &[SimpleFilter {
                    column: "status".to_string(),
                    operator: "=".to_string(),
                    value: "active".to_string(),
                }],
                Some(3),
            )
            .await
            .expect("rest execution should succeed");

        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.batches[0].num_rows(), 3);
        assert_eq!(result.batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn executes_offset_paginated_requests() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .and(query_param("offset", "0"))
            .and(query_param("limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 1, "name": "Mario" },
                    { "id": 2, "name": "Anna" }
                ]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .and(query_param("offset", "2"))
            .and(query_param("limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 3, "name": "Luca" }
                ]
            })))
            .mount(&server)
            .await;

        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: Some(dbfy_config::RuntimeConfig {
                    timeout_ms: Some(5_000),
                    max_concurrency: None,
                    max_pages: Some(10),
                    retry: None,
                    cache: None,
                }),
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([
                    (
                        "id".to_string(),
                        ColumnConfig {
                            path: "$.id".to_string(),
                            r#type: DataType::Int64,
                        },
                    ),
                    (
                        "name".to_string(),
                        ColumnConfig {
                            path: "$.name".to_string(),
                            r#type: DataType::String,
                        },
                    ),
                ]),
                pagination: Some(PaginationConfig::Offset {
                    offset_param: "offset".to_string(),
                    limit_param: "limit".to_string(),
                }),
                pushdown: Some(PushdownConfig {
                    filters: BTreeMap::new(),
                    limit: Some(LimitPushdownConfig {
                        param: "limit".to_string(),
                        max: Some(2),
                    }),
                    projection: None,
                }),
            },
        );

        let result = table
            .execute(&["id".to_string(), "name".to_string()], &[], Some(3))
            .await
            .expect("offset pagination should succeed");

        assert_eq!(result.batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn executes_cursor_paginated_requests() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(|request: &wiremock::Request| {
                let query = request.url.query().unwrap_or_default();
                if query.contains("cursor=abc123") {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "data": [
                            { "id": 3, "name": "Luca" }
                        ],
                        "next_cursor": null
                    }))
                } else {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "data": [
                            { "id": 1, "name": "Mario" },
                            { "id": 2, "name": "Anna" }
                        ],
                        "next_cursor": "abc123"
                    }))
                }
            })
            .mount(&server)
            .await;

        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: Some(dbfy_config::RuntimeConfig {
                    timeout_ms: Some(5_000),
                    max_concurrency: None,
                    max_pages: Some(10),
                    retry: None,
                    cache: None,
                }),
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([
                    (
                        "id".to_string(),
                        ColumnConfig {
                            path: "$.id".to_string(),
                            r#type: DataType::Int64,
                        },
                    ),
                    (
                        "name".to_string(),
                        ColumnConfig {
                            path: "$.name".to_string(),
                            r#type: DataType::String,
                        },
                    ),
                ]),
                pagination: Some(PaginationConfig::Cursor {
                    cursor_param: "cursor".to_string(),
                    cursor_path: "$.next_cursor".to_string(),
                }),
                pushdown: None,
            },
        );

        let result = table
            .execute(&["id".to_string(), "name".to_string()], &[], Some(3))
            .await
            .expect("cursor pagination should succeed");

        assert_eq!(result.batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn executes_link_header_paginated_requests() {
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
                            { "id": 1, "name": "Mario" },
                            { "id": 2, "name": "Anna" }
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
                    { "id": 3, "name": "Luca" }
                ]
            })))
            .mount(&server)
            .await;

        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: Some(dbfy_config::RuntimeConfig {
                    timeout_ms: Some(5_000),
                    max_concurrency: None,
                    max_pages: Some(10),
                    retry: None,
                    cache: None,
                }),
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([
                    (
                        "id".to_string(),
                        ColumnConfig {
                            path: "$.id".to_string(),
                            r#type: DataType::Int64,
                        },
                    ),
                    (
                        "name".to_string(),
                        ColumnConfig {
                            path: "$.name".to_string(),
                            r#type: DataType::String,
                        },
                    ),
                ]),
                pagination: Some(PaginationConfig::LinkHeader {
                    rel: Some("next".to_string()),
                }),
                pushdown: None,
            },
        );

        let result = table
            .execute(&["id".to_string(), "name".to_string()], &[], Some(3))
            .await
            .expect("link header pagination should succeed");

        assert_eq!(result.batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn retries_transient_http_errors() {
        let server = MockServer::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_responder = attempts.clone();

        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(move |_request: &wiremock::Request| {
                let current = attempts_for_responder.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    ResponseTemplate::new(503)
                } else {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "data": [
                            { "id": 1, "name": "Mario" }
                        ]
                    }))
                }
            })
            .mount(&server)
            .await;

        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: Some(dbfy_config::RuntimeConfig {
                    timeout_ms: Some(5_000),
                    max_concurrency: None,
                    max_pages: Some(10),
                    retry: Some(dbfy_config::RetryConfig {
                        max_attempts: 2,
                        backoff_ms: 1,
                    }),
                    cache: None,
                }),
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([
                    (
                        "id".to_string(),
                        ColumnConfig {
                            path: "$.id".to_string(),
                            r#type: DataType::Int64,
                        },
                    ),
                    (
                        "name".to_string(),
                        ColumnConfig {
                            path: "$.name".to_string(),
                            r#type: DataType::String,
                        },
                    ),
                ]),
                pagination: None,
                pushdown: None,
            },
        );

        let result = table
            .execute(&["id".to_string(), "name".to_string()], &[], Some(1))
            .await
            .expect("retry should recover");

        assert_eq!(result.batches[0].num_rows(), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn returns_rate_limit_error_after_exhausting_retries() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "1")
                    .set_body_string("slow down"),
            )
            .mount(&server)
            .await;

        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: Some(dbfy_config::RuntimeConfig {
                    timeout_ms: Some(5_000),
                    max_concurrency: None,
                    max_pages: Some(10),
                    retry: Some(dbfy_config::RetryConfig {
                        max_attempts: 1,
                        backoff_ms: 1,
                    }),
                    cache: None,
                }),
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([(
                    "id".to_string(),
                    ColumnConfig {
                        path: "$.id".to_string(),
                        r#type: DataType::Int64,
                    },
                )]),
                pagination: None,
                pushdown: None,
            },
        );

        let error = table
            .execute(&["id".to_string()], &[], Some(1))
            .await
            .expect_err("request should fail");

        match error {
            RestProviderError::RateLimited {
                retry_after_seconds: Some(1),
                ..
            } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn returns_timeout_error() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/customers"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(50))
                    .set_body_json(json!({
                        "data": [{ "id": 1 }]
                    })),
            )
            .mount(&server)
            .await;

        let table = RestTable::new(
            "crm",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: Some(dbfy_config::RuntimeConfig {
                    timeout_ms: Some(5),
                    max_concurrency: None,
                    max_pages: Some(10),
                    retry: Some(dbfy_config::RetryConfig {
                        max_attempts: 1,
                        backoff_ms: 1,
                    }),
                    cache: None,
                }),
                tables: BTreeMap::new(),
            },
            "customers",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/customers".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([(
                    "id".to_string(),
                    ColumnConfig {
                        path: "$.id".to_string(),
                        r#type: DataType::Int64,
                    },
                )]),
                pagination: None,
                pushdown: None,
            },
        );

        let error = table
            .execute(&["id".to_string()], &[], Some(1))
            .await
            .expect_err("request should timeout");

        match error {
            RestProviderError::Timeout { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn supports_array_index_column_path() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/items"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 1, "tags": ["alpha", "beta"] },
                    { "id": 2, "tags": ["gamma", "delta"] }
                ]
            })))
            .mount(&server)
            .await;

        let table = RestTable::new(
            "store",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: None,
                tables: BTreeMap::new(),
            },
            "items",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/items".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([
                    (
                        "id".to_string(),
                        ColumnConfig {
                            path: "$.id".to_string(),
                            r#type: DataType::Int64,
                        },
                    ),
                    (
                        "first_tag".to_string(),
                        ColumnConfig {
                            path: "$.tags[0]".to_string(),
                            r#type: DataType::String,
                        },
                    ),
                ]),
                pagination: None,
                pushdown: None,
            },
        );

        let result = table
            .execute(&["id".to_string(), "first_tag".to_string()], &[], None)
            .await
            .expect("array-index column path should resolve");

        assert_eq!(result.batches[0].num_rows(), 2);
        let first_tag = result.batches[0]
            .column_by_name("first_tag")
            .expect("first_tag column")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("StringArray");
        assert_eq!(first_tag.value(0), "alpha");
        assert_eq!(first_tag.value(1), "gamma");
    }

    /// Regression: the schema declared by `dbfy_config::DataType::Timestamp`
    /// is `Timestamp(µs, "UTC")`. Before the Milestone 16 fix, the column
    /// builder produced `Timestamp(µs, None)` and `RecordBatch::try_new`
    /// rejected the batch with a schema mismatch. This was hit in the
    /// `dbfy-frontend-duckdb` `sensor_analytics.py` demo.
    #[tokio::test]
    async fn timestamp_column_builder_carries_utc_timezone() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    {"id": 1, "ts": "2026-04-30T10:30:00Z"},
                    {"id": 2, "ts": "2026-04-30T10:35:42.500Z"},
                    {"id": 3, "ts": null},
                ]
            })))
            .mount(&server)
            .await;

        let table = RestTable::new(
            "ev",
            &RestSourceConfig {
                base_url: server.uri(),
                auth: None,
                runtime: None,
                tables: BTreeMap::new(),
            },
            "events",
            RestTableConfig {
                endpoint: EndpointConfig {
                    method: HttpMethod::Get,
                    path: "/events".to_string(),
                },
                root: "$.data[*]".to_string(),
                primary_key: Some("id".to_string()),
                include_raw_json: false,
                raw_column: "_raw".to_string(),
                columns: BTreeMap::from([
                    (
                        "id".to_string(),
                        ColumnConfig {
                            path: "$.id".to_string(),
                            r#type: DataType::Int64,
                        },
                    ),
                    (
                        "ts".to_string(),
                        ColumnConfig {
                            path: "$.ts".to_string(),
                            r#type: DataType::Timestamp,
                        },
                    ),
                ]),
                pagination: None,
                pushdown: None,
            },
        );

        let result = table
            .execute(&[], &[], None)
            .await
            .expect("timestamp column should not raise schema mismatch");

        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 3);
        let ts_field = batch.schema().field_with_name("ts").unwrap().clone();
        assert_eq!(
            ts_field.data_type(),
            &arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            "schema must declare UTC timezone, otherwise downstream consumers \
             (DuckDB CREATE TABLE AS, DataFusion JOIN) reject the batch",
        );
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .expect("timestamp array");
        assert!(!ts.is_null(0) && !ts.is_null(1));
        assert!(ts.is_null(2), "null timestamp values must propagate");
    }

    #[test]
    fn config_example_builds_rest_table() {
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
        let source = match config.sources.get("crm").expect("source should exist") {
            SourceConfig::Rest(source) => source,
            other => panic!("expected REST source, got {other:?}"),
        };

        let table = RestTable::new(
            "crm",
            source,
            "customers",
            source.tables["customers"].clone(),
        );

        assert_eq!(table.config().endpoint.path, "/customers");
    }
}
