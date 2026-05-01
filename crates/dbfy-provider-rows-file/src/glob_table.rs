//! Glob-driven multi-file provider — each file gets its own `RowsFileTable`,
//! and a `scan()` consumes them in order. Common pattern for log directories
//! like `/var/log/app/*.log`.
//!
//! v1 keeps it simple: serial scan of each file, in lexicographic path
//! order. Each file maintains its own `.dbfy_idx` sidecar; pruning runs
//! per file, so the global skip ratio is the union of per-file decisions.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use dbfy_provider::{
    ProgrammaticTableProvider, ProviderCapabilities, ProviderError, ProviderResult, ScanRequest,
    ScanResponse,
};
use futures::stream::{self, StreamExt};

use crate::RowsFileTable;
use crate::indexer::IndexedColumn;
use crate::parser::DynParser;

/// Multi-file provider — same parser + indexed_columns applied to every
/// file matched by the glob. Useful for log directories where each rotated
/// file follows the same schema.
pub struct RowsFileGlob {
    tables: Vec<Arc<RowsFileTable>>,
    schema: SchemaRef,
    capabilities: ProviderCapabilities,
}

impl RowsFileGlob {
    /// Match `pattern` (a `glob`-style path expression like
    /// `/var/log/app/*.log`) against the filesystem and create one
    /// `RowsFileTable` per matching file. Files are sorted lexicographically
    /// by full path.
    pub fn try_new(
        pattern: &str,
        parser: DynParser,
        indexed_columns: Vec<IndexedColumn>,
    ) -> ProviderResult<Self> {
        let mut paths: Vec<PathBuf> = glob::glob(pattern)
            .map_err(|err| ProviderError::Generic {
                message: format!("invalid glob pattern `{pattern}`: {err}"),
            })?
            .filter_map(Result::ok)
            .filter(|p| p.is_file())
            .collect();
        paths.sort();
        if paths.is_empty() {
            return Err(ProviderError::Generic {
                message: format!("glob `{pattern}` matched no files"),
            });
        }
        let tables: Vec<Arc<RowsFileTable>> = paths
            .into_iter()
            .map(|p| {
                Arc::new(RowsFileTable::new(
                    p,
                    parser.clone(),
                    indexed_columns.clone(),
                ))
            })
            .collect();
        // Each table reports the same schema (driven by the parser); pick
        // the first as canonical.
        let head = tables.first().unwrap();
        let schema = head.schema();
        let capabilities = head.capabilities();
        Ok(Self {
            tables,
            schema,
            capabilities,
        })
    }

    /// Number of underlying files. Useful for tests/diagnostics.
    pub fn file_count(&self) -> usize {
        self.tables.len()
    }

    /// Refresh the sidecar of every underlying file. Returns one
    /// `(path, decision)` pair per file in the same order they were
    /// matched (lexicographic on path).
    pub fn refresh_each(&self) -> ProviderResult<Vec<(PathBuf, crate::RefreshDecision)>> {
        let mut out = Vec::with_capacity(self.tables.len());
        for t in &self.tables {
            let dec = t.refresh()?;
            out.push((t.file_path().to_path_buf(), dec));
        }
        Ok(out)
    }

    /// Aggregate index summary across all underlying files.
    pub fn index_summary(&self) -> ProviderResult<crate::IndexSummary> {
        let mut chunks = 0usize;
        let mut total_rows = 0u64;
        let mut file_size = 0u64;
        for t in &self.tables {
            let s = t.index_summary()?;
            chunks += s.chunks;
            total_rows += s.total_rows;
            file_size += s.file_size;
        }
        Ok(crate::IndexSummary {
            chunks,
            total_rows,
            file_size,
        })
    }

    /// Force a full rebuild on every underlying file. Returns the same
    /// `(path, Rebuilt)` shape as `refresh_each` for telemetry parity.
    pub fn rebuild_all(&self) -> ProviderResult<Vec<(PathBuf, crate::RefreshDecision)>> {
        let mut out = Vec::with_capacity(self.tables.len());
        for t in &self.tables {
            t.rebuild()?;
            out.push((t.file_path().to_path_buf(), crate::RefreshDecision::Rebuilt));
        }
        Ok(out)
    }
}

#[async_trait]
impl ProgrammaticTableProvider for RowsFileGlob {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities.clone()
    }

    async fn scan(&self, request: ScanRequest) -> ProviderResult<ScanResponse> {
        // Run each table's scan and concatenate the resulting streams.
        let mut all_streams = Vec::with_capacity(self.tables.len());
        for table in &self.tables {
            let response = table.scan(request.clone()).await?;
            all_streams.push(response.stream);
        }
        let merged = stream::iter(all_streams).flatten();
        Ok(ScanResponse {
            stream: Box::pin(merged),
            handled_filters: Vec::new(),
            metadata: std::collections::BTreeMap::new(),
        })
    }
}
