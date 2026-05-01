//! On-disk index format. v1 stores the index as a bincode blob next to the
//! source file (`<file>.dbfy_idx`). Migration to a Parquet sidecar is a
//! deferred follow-up; pinning the format version in the header lets us
//! invalidate cleanly on upgrade.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::bloom::Bloom;

/// Bumped every time the binary layout of [`Index`] changes incompatibly.
/// Older indexes are silently rebuilt. Sprint 1 = 1; Sprint 2 = 4 (added
/// `bloom` field, then made the zone-map range an `Option`, then added
/// `prefix_hash_window` so append-only detection compares the same byte
/// window even when the file grows past it).
pub const INDEXER_VERSION: u32 = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub indexer_version: u32,
    pub schema_hash: u64,
    pub file_size: u64,
    pub file_mtime_micros: i64,
    pub file_inode: u64,
    /// xxhash64 of the first `prefix_hash_window` bytes of the file.
    pub prefix_hash: u64,
    /// Number of bytes hashed for `prefix_hash`. Stored explicitly so the
    /// invalidator can re-hash the same window even after the file grows
    /// past it; without this the hash is implicitly size-dependent and
    /// every append looks like a content change.
    pub prefix_hash_window: u32,
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub byte_start: u64,
    pub byte_end: u64,
    pub row_start: u64,
    pub row_count: u64,
    pub stats: Vec<ColumnStats>,
}

/// Per-column stats. The `bloom` field carries equality / IN-list pruning;
/// the `zone` field carries range pruning (`<`, `<=`, `>`, `>=`, `=` lower
/// bound). Each is independently optional — a bloom-only column has
/// `zone = None` and the pruner ignores range checks for it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnStats {
    /// No index requested for this column.
    None,
    Int64 {
        zone: Option<ZoneRange<i64>>,
        all_null: bool,
        bloom: Option<Bloom>,
    },
    /// Float64 columns get zone maps only — bloom on f64 is not useful
    /// (NaN comparisons + precision pitfalls).
    Float64 {
        zone: Option<ZoneRange<f64>>,
        all_null: bool,
    },
    String {
        zone: Option<ZoneRange<String>>,
        all_null: bool,
        bloom: Option<Bloom>,
    },
    /// Microseconds since epoch. Same range filter semantics as Int64.
    TimestampMicros {
        zone: Option<ZoneRange<i64>>,
        all_null: bool,
        bloom: Option<Bloom>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneRange<T> {
    pub min: T,
    pub max: T,
}

pub fn idx_path(file: &Path) -> PathBuf {
    let mut p = file.to_path_buf().into_os_string();
    p.push(".dbfy_idx");
    PathBuf::from(p)
}

pub fn read(path: &Path) -> std::io::Result<Index> {
    let bytes = std::fs::read(path)?;
    bincode::deserialize(&bytes)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

pub fn write(path: &Path, index: &Index) -> std::io::Result<()> {
    let bytes = bincode::serialize(index)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    std::fs::write(path, bytes)
}
