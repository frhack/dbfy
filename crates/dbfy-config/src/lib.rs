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
    Parquet(ParquetSourceConfig),
    Excel(ExcelSourceConfig),
    Graphql(GraphqlSourceConfig),
    Postgres(PostgresSourceConfig),
    Ldap(LdapSourceConfig),
}

impl SourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        match self {
            Self::Rest(rest) => rest.validate(source_name),
            Self::RowsFile(rf) => rf.validate(source_name),
            Self::Parquet(p) => p.validate(source_name),
            Self::Excel(e) => e.validate(source_name),
            Self::Graphql(g) => g.validate(source_name),
            Self::Postgres(pg) => pg.validate(source_name),
            Self::Ldap(ldap) => ldap.validate(source_name),
        }
    }
}

// ---------------------------------------------------------------
// Parquet source — local files / dirs / globs of `.parquet` files.
// Schema is auto-detected by DataFusion at first scan; the YAML only
// names the tables and points at their physical paths.
// ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParquetSourceConfig {
    pub tables: BTreeMap<String, ParquetTableConfig>,
}

impl ParquetSourceConfig {
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
            if table.path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` must define a non-empty path"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParquetTableConfig {
    /// File path, directory, or glob (`logs/*.parquet`).
    pub path: String,
}

// ---------------------------------------------------------------
// Excel source — `.xlsx` / `.xls` workbooks. Each declared table
// targets one sheet. Type inference happens at scan time from
// sampled rows, mirroring how `dbfy detect <file.csv>` works today.
// ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExcelSourceConfig {
    pub tables: BTreeMap<String, ExcelTableConfig>,
}

impl ExcelSourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        if self.tables.is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` must define at least one table"
            )));
        }
        for (table_name, table) in &self.tables {
            if table_name.trim().is_empty() || table.path.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` requires a non-empty path"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExcelTableConfig {
    pub path: String,
    /// Sheet name. If omitted, the first sheet is used.
    #[serde(default)]
    pub sheet: Option<String>,
    /// Whether the first row is a header (default `true`).
    #[serde(default = "default_excel_has_header")]
    pub has_header: bool,
}

fn default_excel_has_header() -> bool {
    true
}

// ---------------------------------------------------------------
// GraphQL source — single endpoint, one declared query per table.
// Uses POST + JSON body. Pagination + filter pushdown are deferred
// to a future sprint; v1 just sends the literal query and root-
// extracts the response via JSONPath.
// ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphqlSourceConfig {
    pub endpoint: String,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    pub tables: BTreeMap<String, GraphqlTableConfig>,
}

impl GraphqlSourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        if self.endpoint.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` must define a non-empty endpoint"
            )));
        }
        if self.tables.is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` must define at least one table"
            )));
        }
        for (table_name, table) in &self.tables {
            if table_name.trim().is_empty() || table.query.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` requires a non-empty query"
                )));
            }
            for column in table.columns.values() {
                JsonPath::parse(&column.path).map_err(|err| {
                    ConfigError::Validation(format!(
                        "table `{source_name}.{table_name}` column path `{}` invalid: {err}",
                        column.path
                    ))
                })?;
            }
            JsonPath::parse(&table.root).map_err(|err| {
                ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` root `{}` invalid: {err}",
                    table.root
                ))
            })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphqlTableConfig {
    /// Literal GraphQL query body. Variables come from the
    /// `pushdown.variables` mapping when DataFusion supplies a `WHERE
    /// column = literal` predicate that matches a declared mapping.
    pub query: String,
    /// JSONPath into the response body (e.g. `$.data.users[*]`).
    pub root: String,
    pub columns: BTreeMap<String, ColumnConfig>,
    /// Optional pushdown configuration. When a SQL filter `WHERE
    /// <column> = <literal>` arrives, the corresponding GraphQL
    /// variable from `variables` gets the literal at query time.
    #[serde(default)]
    pub pushdown: Option<GraphqlPushdownConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphqlPushdownConfig {
    /// Map from SQL column name → GraphQL variable name. The query
    /// must declare a matching `$<variable>` argument; dbfy injects
    /// the literal value at scan time.
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
}

// ---------------------------------------------------------------
// PostgreSQL source — wire-protocol connection + read-only SELECT
// against declared tables. Pushdown for filter / projection / limit
// will be added in a follow-up; v1 fetches and lets the engine
// filter above the scan.
// ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresSourceConfig {
    /// Postgres connection string (e.g. `postgres://user:pass@host:5432/db`).
    pub connection: String,
    pub tables: BTreeMap<String, PostgresTableConfig>,
}

impl PostgresSourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        if self.connection.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` requires a non-empty connection string"
            )));
        }
        if self.tables.is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` must define at least one table"
            )));
        }
        for (table_name, table) in &self.tables {
            if table_name.trim().is_empty() || table.relation.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` requires a non-empty relation"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresTableConfig {
    /// Schema-qualified table name (`public.users`) or unqualified
    /// (`users`). Used as `SELECT … FROM <relation>`.
    pub relation: String,
}

// ---------------------------------------------------------------
// LDAP source — directory server queried via the LDAP wire protocol.
// Each declared table is `(base_dn, scope, filter, attributes)`. The
// `pushdown.attributes` mapping translates SQL `WHERE col <op> lit`
// into native LDAP filter expressions, AND-merged into the table's
// base filter so the directory server does the work, not us.
// ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LdapSourceConfig {
    /// LDAP URL: `ldap://host:389` or `ldaps://host:636`.
    pub url: String,
    #[serde(default)]
    pub auth: Option<LdapAuthConfig>,
    pub tables: BTreeMap<String, LdapTableConfig>,
}

impl LdapSourceConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        if self.url.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` requires a non-empty url"
            )));
        }
        if !(self.url.starts_with("ldap://") || self.url.starts_with("ldaps://")) {
            return Err(ConfigError::Validation(format!(
                "source `{source_name}` url must start with ldap:// or ldaps://"
            )));
        }
        if let Some(auth) = &self.auth {
            auth.validate(source_name)?;
        }
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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LdapAuthConfig {
    /// Anonymous bind. No DN, no password.
    Anonymous,
    /// Simple bind with a DN + password from env.
    Simple {
        bind_dn: String,
        password_env: String,
    },
}

impl LdapAuthConfig {
    fn validate(&self, source_name: &str) -> Result<()> {
        match self {
            Self::Anonymous => Ok(()),
            Self::Simple {
                bind_dn,
                password_env,
            } => {
                if bind_dn.trim().is_empty() || password_env.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "source `{source_name}` simple auth requires non-empty bind_dn and password_env"
                    )));
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LdapTableConfig {
    /// Search root, e.g. `ou=people,dc=example,dc=com`.
    pub base_dn: String,
    /// Search scope. Defaults to `sub` (entire subtree).
    #[serde(default)]
    pub scope: Option<LdapScope>,
    /// Base LDAP filter, e.g. `(objectClass=inetOrgPerson)`. Defaults
    /// to `(objectClass=*)`. Pushed-down predicates are AND-merged
    /// into this filter at scan time.
    #[serde(default)]
    pub filter: Option<String>,
    /// Column → attribute mapping. Special LDAP-attribute name
    /// `__dn__` exposes the entry's distinguished name.
    pub attributes: BTreeMap<String, LdapAttributeConfig>,
    #[serde(default)]
    pub pushdown: Option<LdapPushdownConfig>,
}

impl LdapTableConfig {
    fn validate(&self, source_name: &str, table_name: &str) -> Result<()> {
        if self.base_dn.trim().is_empty() {
            return Err(ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` requires a non-empty base_dn"
            )));
        }
        if self.attributes.is_empty() {
            return Err(ConfigError::Validation(format!(
                "table `{source_name}.{table_name}` must declare at least one attribute"
            )));
        }
        for (col, cfg) in &self.attributes {
            if col.trim().is_empty() || cfg.ldap.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "table `{source_name}.{table_name}` attribute `{col}` requires non-empty `ldap`"
                )));
            }
        }
        if let Some(p) = &self.pushdown {
            for (col, ldap_attr) in &p.attributes {
                if col.trim().is_empty() || ldap_attr.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "table `{source_name}.{table_name}` pushdown attribute mapping has empty entry"
                    )));
                }
                if !self.attributes.contains_key(col) {
                    return Err(ConfigError::Validation(format!(
                        "table `{source_name}.{table_name}` pushdown references unknown column `{col}`"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LdapScope {
    /// Search the base entry only.
    Base,
    /// Search direct children of the base entry.
    One,
    /// Search the entire subtree rooted at the base entry.
    Sub,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LdapAttributeConfig {
    /// LDAP attribute name (`uid`, `mail`, `cn`, …) or `__dn__` for
    /// the entry's distinguished name.
    pub ldap: String,
    #[serde(default = "default_ldap_attr_type")]
    pub r#type: DataType,
}

fn default_ldap_attr_type() -> DataType {
    DataType::String
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LdapPushdownConfig {
    /// Map from SQL column name → LDAP attribute name. When a SQL
    /// `WHERE col <op> literal` predicate matches a column listed
    /// here, dbfy translates it into an LDAP filter fragment and
    /// AND-merges it into the table's base filter.
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
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
