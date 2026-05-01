//! Reusable in-memory `ProgrammaticTableProvider` backed by a fixed list of
//! Arrow `RecordBatch` values. Useful for demos, fixtures, and small lookup
//! tables.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use dbfy_provider::{
    ProgrammaticTableProvider, ProviderCapabilities, ProviderResult, ScanRequest, ScanResponse,
};
use futures::stream;

#[derive(Debug, Clone)]
pub struct StaticRecordBatchProvider {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    capabilities: ProviderCapabilities,
}

impl StaticRecordBatchProvider {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches,
            capabilities: ProviderCapabilities::default(),
        }
    }

    pub fn with_capabilities(mut self, capabilities: ProviderCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
}

#[async_trait]
impl ProgrammaticTableProvider for StaticRecordBatchProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    async fn scan(&self, _request: ScanRequest) -> ProviderResult<ScanResponse> {
        Ok(ScanResponse {
            stream: Box::pin(stream::iter(self.batches.clone().into_iter().map(Ok))),
            handled_filters: Vec::new(),
            metadata: BTreeMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use dbfy_provider::ScanRequest;
    use futures::TryStreamExt;
    use futures::executor::block_on;

    use super::{ProgrammaticTableProvider, StaticRecordBatchProvider};

    #[test]
    fn yields_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("batch should build");
        let provider = StaticRecordBatchProvider::new(schema, vec![batch]);

        let batches = block_on(async move {
            let response = provider
                .scan(ScanRequest::new("test"))
                .await
                .expect("scan should succeed");
            response
                .stream
                .try_collect::<Vec<_>>()
                .await
                .expect("stream should collect")
        });

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }
}
