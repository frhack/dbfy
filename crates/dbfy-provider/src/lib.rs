use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use thiserror::Error;

pub type ProviderResult<T> = std::result::Result<T, ProviderError>;
pub type RecordBatchStream = BoxStream<'static, ProviderResult<RecordBatch>>;

#[derive(Debug, Clone, PartialEq)]
pub struct ScanRequest {
    pub projection: Option<Vec<String>>,
    pub filters: Vec<SimpleFilter>,
    pub limit: Option<usize>,
    pub order_by: Vec<OrderExpr>,
    pub query_id: String,
}

impl ScanRequest {
    pub fn new(query_id: impl Into<String>) -> Self {
        Self {
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            query_id: query_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SimpleFilter {
    pub column: String,
    pub operator: FilterOperator,
    pub value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterOperator {
    Eq,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl FilterOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::In => "IN",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Utf8List(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderExpr {
    pub column: String,
    pub ascending: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderCapabilities {
    pub projection_pushdown: bool,
    pub filter_pushdown: FilterCapabilities,
    pub limit_pushdown: bool,
    pub order_by_pushdown: bool,
    pub aggregate_pushdown: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterCapabilities {
    pub equals: BTreeSet<String>,
    pub in_list: BTreeSet<String>,
    pub greater_than: BTreeSet<String>,
    pub greater_than_or_equal: BTreeSet<String>,
    pub less_than: BTreeSet<String>,
    pub less_than_or_equal: BTreeSet<String>,
}

impl FilterCapabilities {
    pub fn supports(&self, filter: &SimpleFilter) -> bool {
        let column = &filter.column;
        match filter.operator {
            FilterOperator::Eq => self.equals.contains(column),
            FilterOperator::In => self.in_list.contains(column),
            FilterOperator::Gt => self.greater_than.contains(column),
            FilterOperator::Gte => self.greater_than_or_equal.contains(column),
            FilterOperator::Lt => self.less_than.contains(column),
            FilterOperator::Lte => self.less_than_or_equal.contains(column),
        }
    }
}

pub struct ScanResponse {
    pub stream: RecordBatchStream,
    pub handled_filters: Vec<SimpleFilter>,
    pub metadata: BTreeMap<String, String>,
}

impl ScanResponse {
    pub fn empty() -> Self {
        Self {
            stream: Box::pin(stream::empty()),
            handled_filters: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }
}

#[async_trait]
pub trait ProgrammaticTableProvider: Send + Sync {
    fn schema(&self) -> SchemaRef;

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    async fn scan(&self, request: ScanRequest) -> ProviderResult<ScanResponse>;
}

pub type DynProvider = Arc<dyn ProgrammaticTableProvider>;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("provider error: {message}")]
    Generic { message: String },
    #[error("provider schema error: {message}")]
    Schema { message: String },
    #[error("provider unsupported pushdown: {message}")]
    UnsupportedPushdown { message: String },
    #[error("provider cancelled")]
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::{FilterCapabilities, FilterOperator, ScalarValue, SimpleFilter};

    #[test]
    fn matches_simple_filter_capabilities() {
        let mut capabilities = FilterCapabilities::default();
        capabilities.equals.insert("status".to_string());

        let filter = SimpleFilter {
            column: "status".to_string(),
            operator: FilterOperator::Eq,
            value: ScalarValue::Utf8("active".to_string()),
        };

        assert!(capabilities.supports(&filter));
    }
}
