//! Build a fresh `Index` from a file: split into chunks, parse each chunk,
//! compute zone-map stats per indexed column, persist as the sidecar `.dbfy_idx`.

use std::path::Path;

use arrow_array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::DataType as ArrowDataType;
use memchr::memchr_iter;

use crate::bloom::Bloom;
use crate::index::{Chunk, ColumnStats, Index, ZoneRange, INDEXER_VERSION};
use crate::invalidation::FileSnapshot;
use crate::parser::{LineParser, ParseError, ParseResult};

/// User-provided column index spec. Maps a column declared in the parser
/// schema to the kind of stats (zone map, none) we want stored.
#[derive(Debug, Clone)]
pub struct IndexedColumn {
    pub name: String,
    pub kind: IndexKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    None,
    /// Range filters (`<`, `<=`, `>`, `>=`, `=`) via per-chunk min/max.
    ZoneMap,
    /// Equality / IN-list filters via Bloom filter. Best for high-cardinality
    /// columns (trace_id, request_id, user_id).
    Bloom,
    /// Both zone-map and Bloom. Lets the pruner pick the cheapest path
    /// per filter shape.
    ZoneMapAndBloom,
}

impl IndexKind {
    pub fn has_zone_map(self) -> bool {
        matches!(self, IndexKind::ZoneMap | IndexKind::ZoneMapAndBloom)
    }
    pub fn has_bloom(self) -> bool {
        matches!(self, IndexKind::Bloom | IndexKind::ZoneMapAndBloom)
    }
}

/// Default bloom false-positive rate when the user does not override it.
/// 1 % is the sweet spot for skip rate vs index size on typical chunks.
pub const DEFAULT_BLOOM_FPR: f64 = 0.01;

/// Default chunk size in rows. Picked to keep stats granular without
/// inflating the index file beyond a few hundred bytes per chunk.
pub const DEFAULT_CHUNK_ROWS: usize = 1024;

pub fn build(
    file: &Path,
    parser: &dyn LineParser,
    indexed: &[IndexedColumn],
    chunk_rows: usize,
) -> ParseResult<Index> {
    build_from(file, parser, indexed, chunk_rows, None)
}

/// Append new chunks to an existing index without re-parsing earlier bytes.
/// Caller must have already verified that the file is append-only growth
/// (e.g. via [`crate::Decision::Extend`]).
pub fn extend(
    file: &Path,
    parser: &dyn LineParser,
    indexed: &[IndexedColumn],
    chunk_rows: usize,
    cached: Index,
) -> ParseResult<Index> {
    build_from(file, parser, indexed, chunk_rows, Some(cached))
}

fn build_from(
    file: &Path,
    parser: &dyn LineParser,
    indexed: &[IndexedColumn],
    chunk_rows: usize,
    base: Option<Index>,
) -> ParseResult<Index> {
    let bytes = std::fs::read(file)?;
    let snapshot = FileSnapshot::capture(file)?;
    let schema = parser.schema();
    let schema_hash = hash_schema(&schema);

    // Map each schema column to the requested index kind (None if user
    // didn't list it).
    let column_dispatch: Vec<IndexKind> = schema
        .fields()
        .iter()
        .map(|f| {
            indexed
                .iter()
                .find(|c| c.name == *f.name())
                .map(|c| c.kind)
                .unwrap_or(IndexKind::None)
        })
        .collect();

    // For an extend, start parsing from the byte offset just past the last
    // known chunk; row numbering picks up where we left off. For a fresh
    // build, start from 0.
    let (mut chunks, mut byte_start, mut row_start) = match &base {
        Some(b) if b.schema_hash == schema_hash && !b.chunks.is_empty() => {
            let last = b.chunks.last().unwrap();
            (
                b.chunks.clone(),
                last.byte_end as usize,
                last.row_start + last.row_count,
            )
        }
        _ => (Vec::new(), 0usize, 0u64),
    };

    while byte_start < bytes.len() {
        let chunk_end = find_chunk_end(parser, &bytes[byte_start..], chunk_rows)
            .map(|n| byte_start + n)
            .unwrap_or(bytes.len());
        if chunk_end == byte_start {
            break;
        }
        let chunk_bytes = &bytes[byte_start..chunk_end];
        let batch = parser.parse_chunk(chunk_bytes).map_err(|err| {
            ParseError::from(format!(
                "indexer: failed to parse chunk @ bytes [{byte_start}..{chunk_end}]: {err}"
            ))
        })?;
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            byte_start = chunk_end;
            continue;
        }

        let mut stats = Vec::with_capacity(batch.num_columns());
        for (col_idx, kind) in column_dispatch.iter().enumerate() {
            stats.push(stats_for_array(
                batch.column(col_idx).as_ref(),
                *kind,
                row_count as usize,
            )?);
        }

        chunks.push(Chunk {
            byte_start: byte_start as u64,
            byte_end: chunk_end as u64,
            row_start,
            row_count,
            stats,
        });
        row_start += row_count;
        byte_start = chunk_end;
    }

    Ok(Index {
        indexer_version: INDEXER_VERSION,
        schema_hash,
        file_size: snapshot.size,
        file_mtime_micros: snapshot.mtime_micros,
        file_inode: snapshot.inode,
        prefix_hash: snapshot.prefix_hash,
        prefix_hash_window: snapshot.prefix_hash_window,
        chunks,
    })
}

/// Find the byte offset just after the Nth `\n` in `bytes`, or `None` if no
/// `\n` is present at all. Used to size chunks at row granularity without
/// over-eager parsing.
fn find_chunk_end(parser: &dyn LineParser, bytes: &[u8], chunk_rows: usize) -> Option<usize> {
    let mut newlines = memchr_iter(b'\n', bytes);
    let nth = newlines.nth(chunk_rows.saturating_sub(1));
    match nth {
        Some(idx) => Some(idx + 1),
        None => parser.chunk_boundary(bytes),
    }
}

fn hash_schema(schema: &arrow_schema::Schema) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = twox_hash::xxh3::Hash64::default();
    for f in schema.fields() {
        f.name().hash(&mut hasher);
        format!("{:?}", f.data_type()).hash(&mut hasher);
    }
    hasher.finish()
}

fn stats_for_array(
    arr: &dyn Array,
    kind: IndexKind,
    row_count: usize,
) -> ParseResult<ColumnStats> {
    if kind == IndexKind::None {
        return Ok(ColumnStats::None);
    }
    let want_zone = kind.has_zone_map();
    let want_bloom = kind.has_bloom();

    match arr.data_type() {
        ArrowDataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            let mut all_null = true;
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            let mut bloom = if want_bloom {
                Some(Bloom::new(row_count.max(1), DEFAULT_BLOOM_FPR))
            } else {
                None
            };
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                all_null = false;
                let v = a.value(i);
                if want_zone {
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
                if let Some(b) = bloom.as_mut() {
                    b.add_i64(v);
                }
            }
            let zone = if want_zone && !all_null {
                Some(ZoneRange { min, max })
            } else {
                None
            };
            Ok(ColumnStats::Int64 {
                zone,
                all_null,
                bloom,
            })
        }
        ArrowDataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            let mut all_null = true;
            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                all_null = false;
                let v = a.value(i);
                if v.is_nan() {
                    continue;
                }
                if v < min {
                    min = v;
                }
                if v > max {
                    max = v;
                }
            }
            let zone = if want_zone && !all_null && min.is_finite() && max.is_finite() {
                Some(ZoneRange { min, max })
            } else {
                None
            };
            Ok(ColumnStats::Float64 { zone, all_null })
        }
        ArrowDataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            let mut all_null = true;
            let mut min: Option<String> = None;
            let mut max: Option<String> = None;
            let mut bloom = if want_bloom {
                Some(Bloom::new(row_count.max(1), DEFAULT_BLOOM_FPR))
            } else {
                None
            };
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                all_null = false;
                let v = a.value(i);
                if want_zone {
                    match &min {
                        Some(cur) if v < cur.as_str() => min = Some(v.to_string()),
                        None => min = Some(v.to_string()),
                        _ => {}
                    }
                    match &max {
                        Some(cur) if v > cur.as_str() => max = Some(v.to_string()),
                        None => max = Some(v.to_string()),
                        _ => {}
                    }
                }
                if let Some(b) = bloom.as_mut() {
                    b.add_str(v);
                }
            }
            let zone = if want_zone && !all_null {
                Some(ZoneRange {
                    min: min.unwrap_or_default(),
                    max: max.unwrap_or_default(),
                })
            } else {
                None
            };
            Ok(ColumnStats::String {
                zone,
                all_null,
                bloom,
            })
        }
        ArrowDataType::Timestamp(_, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| ParseError::from("stats: timestamp must be µs precision"))?;
            let mut all_null = true;
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            let mut bloom = if want_bloom {
                Some(Bloom::new(row_count.max(1), DEFAULT_BLOOM_FPR))
            } else {
                None
            };
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                all_null = false;
                let v = a.value(i);
                if want_zone {
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
                if let Some(b) = bloom.as_mut() {
                    b.add_i64(v);
                }
            }
            let zone = if want_zone && !all_null {
                Some(ZoneRange { min, max })
            } else {
                None
            };
            Ok(ColumnStats::TimestampMicros {
                zone,
                all_null,
                bloom,
            })
        }
        other => Err(ParseError::from(format!(
            "stats: unsupported arrow type {other}"
        ))),
    }
}
