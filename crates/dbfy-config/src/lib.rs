use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow_schema::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use serde_json_path::JsonPath;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("invalid config: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub version: u32,
    pub sources: BTreeMap<String, SourceConfig>,
}

impl Config {
    pub fn from_yaml_str(input: &str) -> Result<Self> {
        let config: Self = serde_yaml::from_str(input)?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        Self::from_yaml_str(&raw)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(ConfigError::Validation(format!(
                "unsupported config version {}",
                self.version
            )));
        }

        if self.sources.is_empty() {
            return Err(ConfigError::Validation(
                "at least one source must be defined".to_string(),
            ));
        }

        for (source_name, source) in &self.sources {
            if source_name.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "source names must not be empty".to_string(),
                ));
            }
            source.validate(source_name)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Rest(RestSourceConfig),
    RowsFile(RowsFileSourceConfig),
}

impl SourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        match self {
            Self::Rest(rest) => rest.validate(source_name),
            Self::RowsFile(rf) => rf.validate(source_name),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestSourceConfig {
    pub base_url: String,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub runtime: Option<RuntimeConfig>,
    pub tables: BTreeMap<String, RestTableConfig>,
}

impl RestSourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        if self.base_url.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` must define a non-empty base_url"
            )));
        }

        if self.tables.is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` must define at least one table"
            )));
        }

        if let Some(auth) = &self.auth {
            auth.validate(source_name)?;
        }

        for (table_name, table) in &self.tables {
            if table_name.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "source `{source_name}` contains an empty table name"
                )));
            }
            table.validate(source_name, table_name)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    None,
    Basic {
        username_env: String,
        password_env: String,
    },
    Bearer {
        token_env: String,
    },
    ApiKey {
        r#in: ApiKeyLocation,
        name: String,
        value_env: String,
    },
    CustomHeader {
        name: String,
        value_env: String,
    },
}

impl AuthConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        match self {
            Self::Basic {
                username_env,
                password_env,
            } if username_env.trim().is_empty() || password_env.trim().is_empty() => {
                Err(ConfigError::Validation(format!(
                    "source `{source_name}` basic auth must reference non-empty environment variables"
                )))
            }
            Self::Bearer { token_env } if token_env.trim().is_empty() => {
                Err(ConfigError::Validation(format!(
                    "source `{source_name}` bearer auth must reference a non-empty token_env"
                )))
            }
            Self::ApiKey {
                name, value_env, ..
            } if name.trim().is_empty() || value_env.trim().is_empty() => {
                Err(ConfigError::Validation(format!(
                    "source `{source_name}` api_key auth must define name and value_env"
                )))
            }
            Self::CustomHeader { name, value_env }
                if name.trim().is_empty() || value_env.trim().is_empty() =>
            {
                Err(ConfigError::Validation(format!(
                    "source `{source_name}` custom_header auth must define name and value_env"
                )))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiKeyLocation {
    Header,
    Query,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_concurrency: Option<usize>,
    #[serde(default)]
    pub max_pages: Option<usize>,
    #[serde(default)]
    pub retry: Option<RetryConfig>,
    #[serde(default)]
    pub cache: Option<CacheConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub backoff_ms: u64,
}

/// In-memory HTTP response cache for the REST provider.
///
/// When set, identical GETs (same final URL after pushdown + pagination
/// expansion) within `ttl_seconds` reuse the prior response instead of
/// re-fetching. Concurrent identical requests are de-duplicated through
/// in-flight singleflight semantics: the second caller waits on the
/// first's response rather than issuing a parallel GET.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheConfig {
    /// How long a cached response stays fresh, in seconds. `0` disables
    /// reuse but still de-duplicates concurrent in-flight requests.
    pub ttl_seconds: u64,
    /// Hard upper bound on the number of distinct cached URLs. When the
    /// cache is full, the oldest insertion is evicted. Defaults to 1024
    /// when omitted.
    #[serde(default)]
    pub max_entries: Option<usize>,
}

/// Source backed by line-delimited record files indexed by `dbfy-provider-rows-file`.
///
/// The same source supports multiple tables (one per file or glob), and each
/// table picks its own parser (jsonl / csv / logfmt / regex / syslog).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowsFileSourceConfig {
    pub tables: BTreeMap<String, RowsFileTableConfig>,
}

impl RowsFileSourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        if self.tables.is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` must define at least one table"
            )));
        }
        for (table_name, table) in &self.tables {
            if table_name.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "source `{source_name}` contains an empty table name"
                )));
            }
            table.validate(source_name, table_name)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowsFileTableConfig {
    /// Single file. Mutually exclusive with `glob`.
    #[serde(default)]
    pub path: Option<String>,
    /// Glob pattern (e.g. `/var/log/app/*.log`). Mutually exclusive with `path`.
    #[serde(default)]
    pub glob: Option<String>,
    pub parser: ParserConfig,
    #[serde(default)]
    pub indexed_columns: Vec<IndexedColumnConfig>,
    /// Override the default chunk size used by the indexer (1024 rows).
    #[serde(default)]
    pub chunk_rows: Option<usize>,
}

impl RowsFileTableConfig {
    fn validate(&self, source_name: &str, table_name: &str) -> Result<()> {
        match (&self.path, &self.glob) {
            (None, None) => Err(ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` must define either `path` or `glob`"
            ))),
            (Some(_), Some(_)) => Err(ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` cannot define both `path` and `glob`"
            ))),
            _ => Ok(()),
        }?;
        self.parser.validate(source_name, table_name)?;
        for indexed in &self.indexed_columns {
            if indexed.name.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` has an empty indexed_columns entry"
                )));
            }
        }
        if let Some(n) = self.chunk_rows {
            if n == 0 {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` chunk_rows must be > 0"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum ParserConfig {
    Jsonl {
        columns: Vec<JsonlColumnConfig>,
    },
    Csv {
        columns: Vec<CsvColumnConfig>,
        #[serde(default)]
        delimiter: Option<char>,
        #[serde(default)]
        has_header: Option<bool>,
    },
    Logfmt {
        columns: Vec<LogfmtColumnConfig>,
    },
    Regex {
        pattern: String,
        columns: Vec<RegexColumnConfig>,
    },
    Syslog {
        columns: Vec<SyslogColumnConfig>,
    },
}

impl ParserConfig {
    fn validate(&self, source_name: &str, table_name: &str) -> Result<()> {
        let qual = format!("{source_name}.{table_name}");
        let check_named = |cols: &[(String, &str)], kind: &str| -> Result<()> {
            if cols.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "{kind} parser for `{qual}` requires at least one column"
                )));
            }
            for (name, _) in cols {
                if name.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "{kind} parser for `{qual}` has an empty column name"
                    )));
                }
            }
            Ok(())
        };
        match self {
            Self::Jsonl { columns } => {
                let v: Vec<(String, &str)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.path.as_str()))
                    .collect();
                check_named(&v, "jsonl")?;
                for c in columns {
                    JsonPath::parse(&c.path).map_err(|err| {
                        ConfigError::Validation(format!(
                            "table `{qual}` column `{}` has invalid JSONPath `{}`: {err}",
                            c.name, c.path
                        ))
                    })?;
                }
            }
            Self::Csv { columns, .. } => {
                let v: Vec<(String, &str)> = columns.iter().map(|c| (c.name.clone(), "")).collect();
                check_named(&v, "csv")?;
            }
            Self::Logfmt { columns } => {
                let v: Vec<(String, &str)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.key.as_str()))
                    .collect();
                check_named(&v, "logfmt")?;
            }
            Self::Regex { pattern, columns } => {
                if pattern.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "regex parser for `{qual}` requires a non-empty pattern"
                    )));
                }
                let v: Vec<(String, &str)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.group.as_str()))
                    .collect();
                check_named(&v, "regex")?;
            }
            Self::Syslog { columns } => {
                let v: Vec<(String, &str)> = columns.iter().map(|c| (c.name.clone(), "")).collect();
                check_named(&v, "syslog")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonlColumnConfig {
    pub name: String,
    pub path: String,
    pub r#type: CellTypeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CsvColumnConfig {
    pub name: String,
    pub source: CsvSourceConfig,
    pub r#type: CellTypeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CsvSourceConfig {
    /// Reference a CSV column by its header name.
    Name(String),
    /// Reference a CSV column by 0-based positional index.
    Index(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogfmtColumnConfig {
    pub name: String,
    pub key: String,
    pub r#type: CellTypeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegexColumnConfig {
    pub name: String,
    /// Named capture group from the parser-level `pattern`.
    pub group: String,
    pub r#type: CellTypeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyslogColumnConfig {
    pub name: String,
    pub field: SyslogFieldConfig,
    #[serde(default = "default_syslog_type")]
    pub r#type: CellTypeConfig,
}

fn default_syslog_type() -> CellTypeConfig {
    CellTypeConfig::String
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyslogFieldConfig {
    Priority,
    Facility,
    Severity,
    Version,
    Timestamp,
    Hostname,
    AppName,
    ProcId,
    MsgId,
    StructuredData,
    Message,
}

/// Cell types that the rows-file parsers can produce. A subset of `DataType`
/// kept separate because rows-file does not (yet) cover Date / Boolean / Json.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CellTypeConfig {
    Int64,
    Float64,
    String,
    Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedColumnConfig {
    pub name: String,
    pub kind: IndexKindConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexKindConfig {
    ZoneMap,
    Bloom,
    ZoneMapAndBloom,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestTableConfig {
    pub endpoint: EndpointConfig,
    pub root: String,
    #[serde(default)]
    pub primary_key: Option<String>,
    #[serde(default)]
    pub include_raw_json: bool,
    #[serde(default = "default_raw_column")]
    pub raw_column: String,
    pub columns: BTreeMap<String, ColumnConfig>,
    #[serde(default)]
    pub pagination: Option<PaginationConfig>,
    #[serde(default)]
    pub pushdown: Option<PushdownConfig>,
}

fn default_raw_column() -> String {
    "_raw".to_string()
}

impl RestTableConfig {
    fn validate(&self, source_name: &str, table_name: &str) -> Result<()> {
        if self.endpoint.path.trim().is_empty() || !self.endpoint.path.starts_with('/') {
            return Err(ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` must define an endpoint path starting with `/`"
            )));
        }

        if self.root.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` must define a non-empty root JSONPath"
            )));
        }
        JsonPath::parse(&self.root).map_err(|error| {
            ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` has invalid root JSONPath `{}`: {error}",
                self.root
            ))
        })?;

        if self.columns.is_empty() {
            return Err(ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` must define at least one column"
            )));
        }

        for (column_name, column) in &self.columns {
            if column_name.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` contains an empty column name"
                )));
            }
            column.validate(source_name, table_name, column_name)?;
        }

        if let Some(pushdown) = &self.pushdown {
            pushdown.validate(source_name, table_name)?;
        }

        if let Some(PaginationConfig::Cursor { cursor_path, .. }) = &self.pagination {
            if cursor_path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` cursor pagination must define a non-empty cursor_path"
                )));
            }
            JsonPath::parse(cursor_path).map_err(|error| {
                ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` cursor pagination has invalid cursor_path `{cursor_path}`: {error}"
                ))
            })?;
        }

        Ok(())
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        let mut fields = self
            .columns
            .iter()
            .map(|(name, column)| Field::new(name, column.r#type.to_arrow(), true))
            .collect::<Vec<_>>();

        if self.include_raw_json {
            fields.push(Field::new(&self.raw_column, ArrowDataType::Utf8, true));
        }

        Arc::new(Schema::new(fields))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndpointConfig {
    pub method: HttpMethod,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnConfig {
    pub path: String,
    pub r#type: DataType,
}

impl ColumnConfig {
    fn validate(&self, source_name: &str, table_name: &str, column_name: &str) -> Result<()> {
        if self.path.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "column `{source_name}.{table_name}.{column_name}` must define a non-empty JSONPath"
            )));
        }
        JsonPath::parse(&self.path).map_err(|error| {
            ConfigError::Validation(format!(
                "column `{source_name}.{table_name}.{column_name}` has invalid JSONPath `{}`: {error}",
                self.path
            ))
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Boolean,
    Int64,
    Float64,
    Decimal,
    String,
    Date,
    Timestamp,
    Json,
    List,
    Struct,
}

impl DataType {
    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            Self::Boolean => ArrowDataType::Boolean,
            Self::Int64 => ArrowDataType::Int64,
            Self::Float64 => ArrowDataType::Float64,
            Self::Decimal => ArrowDataType::Decimal128(38, 10),
            Self::String => ArrowDataType::Utf8,
            Self::Date => ArrowDataType::Date32,
            Self::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Json => ArrowDataType::Utf8,
            Self::List => {
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))
            }
            Self::Struct => ArrowDataType::Struct(Default::default()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PushdownConfig {
    #[serde(default)]
    pub filters: BTreeMap<String, FilterPushdownConfig>,
    #[serde(default)]
    pub limit: Option<LimitPushdownConfig>,
    #[serde(default)]
    pub projection: Option<ProjectionPushdownConfig>,
}

impl PushdownConfig {
    fn validate(&self, source_name: &str, table_name: &str) -> Result<()> {
        for (column_name, config) in &self.filters {
            config.validate(source_name, table_name, column_name)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FilterPushdownConfig {
    #[serde(default)]
    pub param: Option<String>,
    pub operators: FilterOperatorConfig,
}

impl FilterPushdownConfig {
    fn validate(&self, source_name: &str, table_name: &str, column_name: &str) -> Result<()> {
        if let Some(param) = &self.param {
            if param.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "filter pushdown `{source_name}.{table_name}.{column_name}` has an empty param"
                )));
            }
        }

        match &self.operators {
            FilterOperatorConfig::List(operators) => {
                if operators.is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "filter pushdown `{source_name}.{table_name}.{column_name}` must define at least one operator"
                    )));
                }
                if self.param.is_none() {
                    return Err(ConfigError::Validation(format!(
                        "filter pushdown `{source_name}.{table_name}.{column_name}` requires `param` when operators is a list"
                    )));
                }
            }
            FilterOperatorConfig::Map(operators) if operators.is_empty() => {
                return Err(ConfigError::Validation(format!(
                    "filter pushdown `{source_name}.{table_name}.{column_name}` must define at least one operator mapping"
                )));
            }
            _ => {}
        }

        Ok(())
    }

    pub fn resolve_param(&self, operator: &str) -> Option<&str> {
        match &self.operators {
            FilterOperatorConfig::List(operators) => operators
                .iter()
                .any(|candidate| candidate == operator)
                .then_some(self.param.as_deref())
                .flatten(),
            FilterOperatorConfig::Map(operators) => operators.get(operator).map(String::as_str),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum FilterOperatorConfig {
    List(Vec<String>),
    Map(BTreeMap<String, String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LimitPushdownConfig {
    pub param: String,
    #[serde(default)]
    pub max: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionPushdownConfig {
    pub param: String,
    pub style: ProjectionStyle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionStyle {
    CommaSeparated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationConfig {
    Page {
        page_param: String,
        size_param: String,
        #[serde(default)]
        start_page: Option<u64>,
    },
    Offset {
        offset_param: String,
        limit_param: String,
    },
    Cursor {
        cursor_param: String,
        cursor_path: String,
    },
    LinkHeader {
        #[serde(default)]
        rel: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::{Config, CsvSourceConfig, ParserConfig, SourceConfig};

    #[test]
    fn rejects_invalid_column_jsonpath() {
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
            path: "not-a-valid-path"
            type: int64
"#;
        let error = Config::from_yaml_str(raw).expect_err("config should fail");
        let message = error.to_string();
        assert!(
            message.contains("column `crm.customers.id` has invalid JSONPath"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_invalid_root_jsonpath() {
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
        root: "data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
"#;
        let error = Config::from_yaml_str(raw).expect_err("config should fail");
        let message = error.to_string();
        assert!(
            message.contains("invalid root JSONPath"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn parses_and_validates_rest_config() {
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
"#;

        let config = Config::from_yaml_str(raw).expect("config should parse");
        assert_eq!(config.sources.len(), 1);
    }

    #[test]
    fn parses_rows_file_jsonl_source() {
        let raw = r#"
version: 1
sources:
  logs:
    type: rows_file
    tables:
      access:
        path: /var/log/nginx/access.jsonl
        chunk_rows: 2048
        parser:
          format: jsonl
          columns:
            - { name: id,  path: "$.id",  type: int64 }
            - { name: ts,  path: "$.ts",  type: timestamp }
            - { name: msg, path: "$.msg", type: string }
        indexed_columns:
          - { name: id, kind: zone_map }
          - { name: msg, kind: bloom }
"#;
        let config = Config::from_yaml_str(raw).expect("config should parse");
        match config.sources.get("logs").unwrap() {
            SourceConfig::RowsFile(rf) => {
                let access = rf.tables.get("access").unwrap();
                assert_eq!(access.path.as_deref(), Some("/var/log/nginx/access.jsonl"));
                assert_eq!(access.chunk_rows, Some(2048));
                assert_eq!(access.indexed_columns.len(), 2);
                match &access.parser {
                    ParserConfig::Jsonl { columns } => assert_eq!(columns.len(), 3),
                    _ => panic!("expected jsonl parser"),
                }
            }
            _ => panic!("expected rows_file source"),
        }
    }

    #[test]
    fn rejects_rows_file_with_both_path_and_glob() {
        let raw = r#"
version: 1
sources:
  logs:
    type: rows_file
    tables:
      access:
        path: /a.log
        glob: "/var/log/*.log"
        parser:
          format: jsonl
          columns: [{ name: id, path: "$.id", type: int64 }]
"#;
        let err = Config::from_yaml_str(raw).expect_err("should fail");
        assert!(err.to_string().contains("cannot define both"));
    }

    #[test]
    fn rejects_jsonl_column_with_invalid_jsonpath() {
        let raw = r#"
version: 1
sources:
  logs:
    type: rows_file
    tables:
      access:
        path: /a.log
        parser:
          format: jsonl
          columns: [{ name: id, path: "not_a_path", type: int64 }]
"#;
        let err = Config::from_yaml_str(raw).expect_err("should fail");
        assert!(err.to_string().contains("invalid JSONPath"));
    }

    #[test]
    fn parses_rows_file_csv_with_glob() {
        let raw = r#"
version: 1
sources:
  exports:
    type: rows_file
    tables:
      sales:
        glob: "/data/sales-*.csv"
        parser:
          format: csv
          delimiter: ","
          has_header: true
          columns:
            - { name: id,    source: { name: order_id }, type: int64 }
            - { name: total, source: { index: 4 },       type: float64 }
"#;
        let config = Config::from_yaml_str(raw).expect("config should parse");
        match config.sources.get("exports").unwrap() {
            SourceConfig::RowsFile(rf) => {
                let sales = rf.tables.get("sales").unwrap();
                assert_eq!(sales.glob.as_deref(), Some("/data/sales-*.csv"));
                match &sales.parser {
                    ParserConfig::Csv {
                        columns,
                        delimiter,
                        has_header,
                    } => {
                        assert_eq!(columns.len(), 2);
                        assert_eq!(*delimiter, Some(','));
                        assert_eq!(*has_header, Some(true));
                        match &columns[0].source {
                            CsvSourceConfig::Name(n) => assert_eq!(n, "order_id"),
                            _ => panic!("expected name"),
                        }
                        match &columns[1].source {
                            CsvSourceConfig::Index(i) => assert_eq!(*i, 4),
                            _ => panic!("expected index"),
                        }
                    }
                    _ => panic!("expected csv parser"),
                }
            }
            _ => panic!("expected rows_file"),
        }
    }
}
