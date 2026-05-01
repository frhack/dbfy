//! Predicate → byte ranges. Given the cached `Index` and the set of pushed
//! filters from `ScanRequest`, return the chunks whose stats *might*
//! contain a matching row. Conservative: any uncertainty defaults to
//! "keep the chunk".

use arrow_schema::Schema;
use dbfy_provider::{FilterOperator, ScalarValue, SimpleFilter};

use crate::index::{Chunk, ColumnStats, Index};

/// Returns references to the chunks of `index` that survive the filter set.
pub fn prune<'a>(index: &'a Index, schema: &Schema, filters: &[SimpleFilter]) -> Vec<&'a Chunk> {
    if filters.is_empty() {
        return index.chunks.iter().collect();
    }
    index
        .chunks
        .iter()
        .filter(|chunk| filters.iter().all(|f| chunk_passes(chunk, schema, f)))
        .collect()
}

fn chunk_passes(chunk: &Chunk, schema: &Schema, filter: &SimpleFilter) -> bool {
    let Some(col_idx) = schema
        .fields()
        .iter()
        .position(|f| *f.name() == filter.column)
    else {
        return true; // unknown column — be conservative.
    };
    let Some(stats) = chunk.stats.get(col_idx) else {
        return true;
    };

    match (stats, &filter.value, filter.operator.clone()) {
        (ColumnStats::None, _, _) => true,

        // Int64
        (
            ColumnStats::Int64 {
                zone,
                all_null,
                bloom,
            },
            ScalarValue::Int64(v),
            op,
        ) => {
            if *all_null {
                return false;
            }
            if op == FilterOperator::Eq {
                if let Some(b) = bloom {
                    if !b.may_contain_i64(*v) {
                        return false;
                    }
                }
            }
            match zone {
                Some(z) => stats_match_range(z.min, z.max, *v, op),
                None => true,
            }
        }

        // Float64
        (ColumnStats::Float64 { zone, all_null }, ScalarValue::Float64(v), op) => {
            if *all_null {
                return false;
            }
            match zone {
                Some(z) => stats_match_range_f64(z.min, z.max, *v, op),
                None => true,
            }
        }

        // String — Eq + In via bloom; range via zone.
        (
            ColumnStats::String {
                zone,
                all_null,
                bloom,
            },
            ScalarValue::Utf8(v),
            op,
        ) => {
            if *all_null {
                return false;
            }
            if op == FilterOperator::Eq {
                if let Some(b) = bloom {
                    if !b.may_contain_str(v.as_str()) {
                        return false;
                    }
                }
            }
            match zone {
                Some(z) => stats_match_range_str(z.min.as_str(), z.max.as_str(), v.as_str(), op),
                None => true,
            }
        }
        (
            ColumnStats::String {
                bloom: Some(b),
                all_null,
                ..
            },
            ScalarValue::Utf8List(values),
            FilterOperator::In,
        ) => {
            if *all_null {
                return false;
            }
            values.iter().any(|v| b.may_contain_str(v))
        }

        // Timestamp — same shape as Int64, scalar pushed as Int64 µs.
        (
            ColumnStats::TimestampMicros {
                zone,
                all_null,
                bloom,
            },
            ScalarValue::Int64(v),
            op,
        ) => {
            if *all_null {
                return false;
            }
            if op == FilterOperator::Eq {
                if let Some(b) = bloom {
                    if !b.may_contain_i64(*v) {
                        return false;
                    }
                }
            }
            match zone {
                Some(z) => stats_match_range(z.min, z.max, *v, op),
                None => true,
            }
        }

        // Type mismatch — conservative.
        _ => true,
    }
}

fn stats_match_range<T: Ord + Copy>(min: T, max: T, v: T, op: FilterOperator) -> bool {
    match op {
        FilterOperator::Eq => min <= v && v <= max,
        FilterOperator::Lt => min < v,
        FilterOperator::Lte => min <= v,
        FilterOperator::Gt => max > v,
        FilterOperator::Gte => max >= v,
        FilterOperator::In => true,
    }
}

fn stats_match_range_f64(min: f64, max: f64, v: f64, op: FilterOperator) -> bool {
    match op {
        FilterOperator::Eq => min <= v && v <= max,
        FilterOperator::Lt => min < v,
        FilterOperator::Lte => min <= v,
        FilterOperator::Gt => max > v,
        FilterOperator::Gte => max >= v,
        FilterOperator::In => true,
    }
}

fn stats_match_range_str(min: &str, max: &str, v: &str, op: FilterOperator) -> bool {
    match op {
        FilterOperator::Eq => min <= v && v <= max,
        FilterOperator::Lt => min < v,
        FilterOperator::Lte => min <= v,
        FilterOperator::Gt => max > v,
        FilterOperator::Gte => max >= v,
        FilterOperator::In => true,
    }
}
