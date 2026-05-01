//! Indexed scan over line-delimited record files (jsonl, csv, logfmt,
//! regex, syslog) with zone-map pushdown.
//!
//! Sprint 1 ships the trait surface, the JSONL parser, zone maps over
//! int64/float64/string/timestamp, and the cache-invalidation policy.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod bloom;
pub mod from_config;
mod glob_table;
mod index;
mod indexer;
mod invalidation;
mod parser;
pub mod parsers;
mod pruner;
mod reader;

pub use from_config::{build_handle, RowsFileHandle};

pub use glob_table::RowsFileGlob;
// Re-exported for downstream use (CLI prints, diagnostics).

pub use bloom::Bloom;
pub use index::{idx_path, read as read_index, Index};
pub use invalidation::{decide as decide_invalidation, decide_with_path as decide_invalidation_with_path};
pub use parser::CellType;

pub use indexer::{IndexKind, IndexedColumn, DEFAULT_CHUNK_ROWS};
pub use invalidation::{Decision, FileSnapshot};
pub use parser::{DynParser, LineParser, ParseError, ParseResult};

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use dbfy_provider::{
    FilterCapabilities, ProgrammaticTableProvider, ProviderCapabilities, ProviderError,
    ProviderResult, ScanRequest, ScanResponse,
};
use futures::stream;


/// `ProgrammaticTableProvider` implementation: persistent indexed scan with
/// pushdown of equality and range filters on indexed columns.
pub struct RowsFileTable {
    file: PathBuf,
    parser: DynParser,
    indexed_columns: Vec<IndexedColumn>,
    chunk_rows: usize,
    /// Cached index. Loaded/built lazily on first scan and kept in memory
    /// for the lifetime of the provider.
    cached: Mutex<Option<Index>>,
}

impl RowsFileTable {
    pub fn new(file: PathBuf, parser: DynParser, indexed_columns: Vec<IndexedColumn>) -> Self {
        Self {
            file,
            parser,
            indexed_columns,
            chunk_rows: DEFAULT_CHUNK_ROWS,
            cached: Mutex::new(None),
        }
    }

    pub fn with_chunk_rows(mut self, n: usize) -> Self {
        self.chunk_rows = n.max(1);
        self
    }

    /// Force the index to be (re)built immediately. Useful after an explicit
    /// rewrite, in tests, or when invalidation heuristics fail.
    pub fn rebuild(&self) -> ProviderResult<()> {
        let idx = indexer::build(
            &self.file,
            self.parser.as_ref(),
            &self.indexed_columns,
            self.chunk_rows,
        )
        .map_err(parse_to_provider)?;
        let path = index::idx_path(&self.file);
        index::write(&path, &idx).map_err(io_to_provider)?;
        *self.cached.lock().unwrap() = Some(idx);
        Ok(())
    }

    /// Refresh the index against the current file state without forcing a
    /// full rebuild. Returns the decision that was actually taken
    /// (`BuiltFresh` on first run, `Reused` / `Extended` / `Rebuilt` on
    /// subsequent runs based on the invalidation policy).
    pub fn refresh(&self) -> ProviderResult<RefreshDecision> {
        self.ensure_index_inner()
    }

    /// Snapshot of the current sidecar: chunk count, total rows, file size.
    /// Forces a refresh first so the numbers reflect the latest valid index.
    pub fn index_summary(&self) -> ProviderResult<IndexSummary> {
        self.ensure_index_inner()?;
        let g = self.cached.lock().unwrap();
        let idx = g.as_ref().expect("index populated by ensure_index_inner");
        Ok(IndexSummary {
            chunks: idx.chunks.len(),
            total_rows: idx.chunks.iter().map(|c| c.row_count).sum(),
            file_size: idx.file_size,
        })
    }

    fn ensure_index(&self) -> ProviderResult<()> {
        self.ensure_index_inner().map(|_| ())
    }

    fn ensure_index_inner(&self) -> ProviderResult<RefreshDecision> {
        let mut guard = self.cached.lock().unwrap();
        let idx_path = index::idx_path(&self.file);
        let snapshot = FileSnapshot::capture(&self.file).map_err(io_to_provider)?;

        // Decide against either the in-memory cache (if any) or the on-disk
        // sidecar. We can't trust the in-memory cache across an external
        // file mutation; the snapshot drives every choice.
        let cached_for_decision: Option<index::Index> = guard.clone().or_else(|| index::read(&idx_path).ok());

        let (chosen, decision) = match cached_for_decision {
            Some(cached) => match invalidation::decide_with_path(&snapshot, &cached, Some(&self.file)) {
                Decision::Reuse => (cached, RefreshDecision::Reused),
                Decision::Extend => {
                    let extended = indexer::extend(
                        &self.file,
                        self.parser.as_ref(),
                        &self.indexed_columns,
                        self.chunk_rows,
                        cached,
                    )
                    .map_err(parse_to_provider)?;
                    index::write(&idx_path, &extended).map_err(io_to_provider)?;
                    (extended, RefreshDecision::Extended)
                }
                Decision::Rebuild => {
                    drop(cached);
                    let fresh = indexer::build(
                        &self.file,
                        self.parser.as_ref(),
                        &self.indexed_columns,
                        self.chunk_rows,
                    )
                    .map_err(parse_to_provider)?;
                    index::write(&idx_path, &fresh).map_err(io_to_provider)?;
                    (fresh, RefreshDecision::Rebuilt)
                }
            },
            None => {
                let fresh = indexer::build(
                    &self.file,
                    self.parser.as_ref(),
                    &self.indexed_columns,
                    self.chunk_rows,
                )
                .map_err(parse_to_provider)?;
                index::write(&idx_path, &fresh).map_err(io_to_provider)?;
                (fresh, RefreshDecision::BuiltFresh)
            }
        };

        *guard = Some(chosen);
        Ok(decision)
    }

    /// File path the table reads from.
    pub fn file_path(&self) -> &std::path::Path {
        &self.file
    }
}

/// Outcome of a `refresh()` call. Maps directly onto the cache-invalidation
/// branches so CLI tools can report which arm fired without re-deriving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshDecision {
    /// First-time build (no sidecar present).
    BuiltFresh,
    /// Sidecar valid, no work performed.
    Reused,
    /// Sidecar partially valid, only the appended tail was parsed.
    Extended,
    /// Sidecar invalidated; full rebuild from scratch.
    Rebuilt,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexSummary {
    pub chunks: usize,
    pub total_rows: u64,
    pub file_size: u64,
}

#[async_trait]
impl ProgrammaticTableProvider for RowsFileTable {
    fn schema(&self) -> SchemaRef {
        self.parser.schema()
    }

    fn capabilities(&self) -> ProviderCapabilities {
        let mut filters = FilterCapabilities::default();
        for c in &self.indexed_columns {
            if c.kind.has_zone_map() {
                filters.equals.insert(c.name.clone());
                filters.greater_than.insert(c.name.clone());
                filters.greater_than_or_equal.insert(c.name.clone());
                filters.less_than.insert(c.name.clone());
                filters.less_than_or_equal.insert(c.name.clone());
            }
            if c.kind.has_bloom() {
                filters.equals.insert(c.name.clone());
                filters.in_list.insert(c.name.clone());
            }
        }
        ProviderCapabilities {
            filter_pushdown: filters,
            ..Default::default()
        }
    }

    async fn scan(&self, request: ScanRequest) -> ProviderResult<ScanResponse> {
        self.ensure_index()?;
        let cached = self.cached.lock().unwrap().clone().unwrap();
        let schema = self.parser.schema();
        let surviving: Vec<index::Chunk> = pruner::prune(&cached, &schema, &request.filters)
            .into_iter()
            .cloned()
            .collect();

        let parser = self.parser.clone();
        let file = self.file.clone();
        let limit = request.limit;

        let stream = stream::unfold(
            ChunkIter {
                chunks: surviving.into_iter(),
                rows_emitted: 0,
            },
            move |mut state| {
                let parser = parser.clone();
                let file = file.clone();
                async move {
                    loop {
                        let chunk = state.chunks.next()?;
                        if let Some(lim) = limit {
                            if state.rows_emitted >= lim {
                                return None;
                            }
                        }
                        match reader::read_chunk(&file, &chunk, parser.as_ref()) {
                            Ok(batch) => {
                                state.rows_emitted += batch.num_rows();
                                return Some((Ok(batch), state));
                            }
                            Err(err) => {
                                return Some((
                                    Err(ProviderError::Generic {
                                        message: err.to_string(),
                                    }),
                                    state,
                                ));
                            }
                        }
                    }
                }
            },
        );

        Ok(ScanResponse {
            stream: Box::pin(stream),
            handled_filters: Vec::new(),
            metadata: std::collections::BTreeMap::new(),
        })
    }
}

struct ChunkIter {
    chunks: std::vec::IntoIter<index::Chunk>,
    rows_emitted: usize,
}

fn parse_to_provider(err: ParseError) -> ProviderError {
    ProviderError::Generic {
        message: err.to_string(),
    }
}

fn io_to_provider(err: std::io::Error) -> ProviderError {
    ProviderError::Generic {
        message: err.to_string(),
    }
}

// Make Arc<RowsFileTable> coerce-able into the workspace's DynProvider type.
// Tests register the table programmatically.
impl RowsFileTable {
    pub fn into_dyn(self: Arc<Self>) -> Arc<dyn ProgrammaticTableProvider> {
        self
    }

    /// Test/inspection helper: total chunks in the cached index and how many
    /// would survive a given filter set. Forces an index load if needed.
    pub fn debug_skip_stats(&self, filters: &[SimpleFilter]) -> ProviderResult<(usize, usize)> {
        self.ensure_index()?;
        let cached = self.cached.lock().unwrap();
        let idx = cached.as_ref().unwrap();
        let total = idx.chunks.len();
        let kept = pruner::prune(idx, &self.parser.schema(), filters).len();
        Ok((total, kept))
    }
}

use dbfy_provider::SimpleFilter;
