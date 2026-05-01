use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use dbfy_frontend_datafusion::Engine;
use dbfy_provider::{
    FilterCapabilities, FilterOperator, ProgrammaticTableProvider, ProviderCapabilities,
    ProviderError, ProviderResult, ScalarValue, ScanRequest, ScanResponse, SimpleFilter,
};
use futures::stream;

const DEFAULT_BATCH_SIZE: usize = 1024;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone)]
struct CsvCustomersProvider {
    path: PathBuf,
    schema: SchemaRef,
    batch_size: usize,
    capabilities: ProviderCapabilities,
}

#[derive(Debug, Clone)]
struct CustomerRow {
    id: i64,
    name: String,
    status: String,
    country: String,
}

impl CsvCustomersProvider {
    fn new(path: PathBuf, batch_size: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("country", DataType::Utf8, false),
        ]));

        let mut filter_capabilities = FilterCapabilities::default();
        filter_capabilities.equals.insert("status".to_string());
        filter_capabilities.equals.insert("country".to_string());

        Self {
            path,
            schema,
            batch_size,
            capabilities: ProviderCapabilities {
                projection_pushdown: true,
                filter_pushdown: filter_capabilities,
                limit_pushdown: true,
                order_by_pushdown: false,
                aggregate_pushdown: false,
            },
        }
    }

    fn projected_columns(&self, request: &ScanRequest) -> ProviderResult<Vec<String>> {
        let columns = request.projection.clone().unwrap_or_else(|| {
            self.schema
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect()
        });

        for column in &columns {
            if self.schema.field_with_name(column).is_err() {
                return Err(ProviderError::Schema {
                    message: format!("unknown projected column `{column}`"),
                });
            }
        }

        Ok(columns)
    }
}

#[async_trait]
impl ProgrammaticTableProvider for CsvCustomersProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    async fn scan(&self, request: ScanRequest) -> ProviderResult<ScanResponse> {
        let projected_columns = self.projected_columns(&request)?;
        let mut reader = csv::Reader::from_path(&self.path).map_err(csv_error)?;
        let mut batches = Vec::new();
        let mut rows = Vec::with_capacity(self.batch_size);
        let mut remaining = request.limit.unwrap_or(usize::MAX);

        for record in reader.records() {
            let row = parse_row(record.map_err(csv_error)?)?;
            if !matches_filters(&row, &request.filters)? {
                continue;
            }

            rows.push(row);
            if remaining != usize::MAX {
                remaining = remaining.saturating_sub(1);
            }

            let limit_reached = remaining == 0;
            if rows.len() == self.batch_size || limit_reached {
                batches.push(build_batch(&projected_columns, &rows)?);
                rows.clear();
            }

            if limit_reached {
                break;
            }
        }

        if !rows.is_empty() {
            batches.push(build_batch(&projected_columns, &rows)?);
        }

        let mut metadata = BTreeMap::new();
        metadata.insert("path".to_string(), self.path.display().to_string());
        metadata.insert("batch_size".to_string(), self.batch_size.to_string());

        Ok(ScanResponse {
            stream: Box::pin(stream::iter(batches.into_iter().map(Ok))),
            handled_filters: request.filters.clone(),
            metadata,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let csv_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/data/customers.csv");
    let provider = CsvCustomersProvider::new(csv_path, DEFAULT_BATCH_SIZE);

    let mut engine = Engine::default();
    engine.register_provider("app.customers", Arc::new(provider))?;

    let sql = "SELECT id, name FROM app.customers WHERE status = 'active' LIMIT 2";
    let explain = engine.explain(sql).await?;
    println!("{explain}\n");

    let batches = engine.query(sql).await?;
    datafusion::arrow::util::pretty::print_batches(&batches)?;

    Ok(())
}

fn parse_row(record: csv::StringRecord) -> ProviderResult<CustomerRow> {
    Ok(CustomerRow {
        id: record
            .get(0)
            .ok_or_else(|| provider_generic("missing id"))?
            .parse::<i64>()
            .map_err(|error| ProviderError::Generic {
                message: format!("invalid id: {error}"),
            })?,
        name: record
            .get(1)
            .ok_or_else(|| provider_generic("missing name"))?
            .to_string(),
        status: record
            .get(2)
            .ok_or_else(|| provider_generic("missing status"))?
            .to_string(),
        country: record
            .get(3)
            .ok_or_else(|| provider_generic("missing country"))?
            .to_string(),
    })
}

fn matches_filters(row: &CustomerRow, filters: &[SimpleFilter]) -> ProviderResult<bool> {
    for filter in filters {
        let matches = match (&*filter.column, &filter.operator, &filter.value) {
            ("status", FilterOperator::Eq, ScalarValue::Utf8(value)) => row.status == *value,
            ("country", FilterOperator::Eq, ScalarValue::Utf8(value)) => row.country == *value,
            _ => {
                return Err(ProviderError::UnsupportedPushdown {
                    message: format!(
                        "example CSV provider supports only equality on status/country, got {filter:?}"
                    ),
                });
            }
        };

        if !matches {
            return Ok(false);
        }
    }

    Ok(true)
}

fn build_batch(projected_columns: &[String], rows: &[CustomerRow]) -> ProviderResult<RecordBatch> {
    let fields = projected_columns
        .iter()
        .map(|column| projected_field(column))
        .collect::<ProviderResult<Vec<_>>>()?;
    let arrays = projected_columns
        .iter()
        .map(|column| build_array(column, rows))
        .collect::<ProviderResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| {
        ProviderError::Generic {
            message: format!("failed to build record batch: {error}"),
        }
    })
}

fn projected_field(column: &str) -> ProviderResult<Field> {
    match column {
        "id" => Ok(Field::new("id", DataType::Int64, false)),
        "name" => Ok(Field::new("name", DataType::Utf8, false)),
        "status" => Ok(Field::new("status", DataType::Utf8, false)),
        "country" => Ok(Field::new("country", DataType::Utf8, false)),
        other => Err(ProviderError::Schema {
            message: format!("unknown column `{other}`"),
        }),
    }
}

fn build_array(column: &str, rows: &[CustomerRow]) -> ProviderResult<ArrayRef> {
    match column {
        "id" => {
            let mut builder = Int64Builder::new();
            for row in rows {
                builder.append_value(row.id);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        "name" => {
            let mut builder = StringBuilder::new();
            for row in rows {
                builder.append_value(&row.name);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        "status" => {
            let mut builder = StringBuilder::new();
            for row in rows {
                builder.append_value(&row.status);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        "country" => {
            let mut builder = StringBuilder::new();
            for row in rows {
                builder.append_value(&row.country);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => Err(ProviderError::Schema {
            message: format!("unknown column `{other}`"),
        }),
    }
}

fn csv_error(error: csv::Error) -> ProviderError {
    ProviderError::Generic {
        message: format!("csv error: {error}"),
    }
}

fn provider_generic(message: impl Into<String>) -> ProviderError {
    ProviderError::Generic {
        message: message.into(),
    }
}
