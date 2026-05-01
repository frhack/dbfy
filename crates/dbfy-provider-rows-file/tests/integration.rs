//! End-to-end test: build a synthetic jsonl, register a `RowsFileTable`,
//! run a range query, assert correct rows + non-trivial chunk skip rate.

use std::io::Write;
use std::sync::Arc;

use dbfy_provider::{
    FilterOperator, ProgrammaticTableProvider, ScalarValue, ScanRequest, SimpleFilter,
};
use dbfy_provider_rows_file::parsers::jsonl::JsonlType;
use dbfy_provider_rows_file::parsers::{JsonlColumn, JsonlParser};
use dbfy_provider_rows_file::{IndexKind, IndexedColumn, RowsFileTable};
use futures::StreamExt;
use tempfile::TempDir;

fn make_jsonl(dir: &TempDir, total_rows: i64) -> std::path::PathBuf {
    let path = dir.path().join("events.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    for id in 0..total_rows {
        let level = match id % 5 {
            0 => "ERROR",
            1 => "WARN",
            _ => "INFO",
        };
        // Crank the timestamp by 1 second per row, anchored at 2026-04-30.
        // Easy to reason about ranges.
        let secs = 1_777_852_800 + id; // 2026-05-04T00:00:00Z roughly
        let ts = format_rfc3339(secs);
        writeln!(
            file,
            r#"{{"id": {id}, "level": "{level}", "ts": "{ts}", "msg": "row {id}"}}"#,
        )
        .unwrap();
    }
    file.flush().unwrap();
    path
}

fn format_rfc3339(secs: i64) -> String {
    // Minimal RFC 3339 formatter: convert epoch seconds → "YYYY-MM-DDTHH:MM:SSZ".
    let days = secs / 86_400;
    let mut secs_in_day = secs % 86_400;
    if secs_in_day < 0 {
        secs_in_day += 86_400;
    }
    let h = secs_in_day / 3600;
    let m = (secs_in_day % 3600) / 60;
    let s = secs_in_day % 60;
    let (y, mo, d) = days_to_civil(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

fn days_to_civil(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[tokio::test]
async fn jsonl_zone_map_filters_prune_chunks() {
    let dir = TempDir::new().unwrap();
    let path = make_jsonl(&dir, 10_000);

    let parser = Arc::new(
        JsonlParser::try_new(vec![
            JsonlColumn {
                name: "id".to_string(),
                path: "$.id".to_string(),
                data_type: JsonlType::Int64,
            },
            JsonlColumn {
                name: "level".to_string(),
                path: "$.level".to_string(),
                data_type: JsonlType::String,
            },
            JsonlColumn {
                name: "msg".to_string(),
                path: "$.msg".to_string(),
                data_type: JsonlType::String,
            },
        ])
        .unwrap(),
    );

    let table = Arc::new(
        RowsFileTable::new(
            path,
            parser,
            vec![
                IndexedColumn {
                    name: "id".to_string(),
                    kind: IndexKind::ZoneMap,
                },
                IndexedColumn {
                    name: "level".to_string(),
                    kind: IndexKind::ZoneMap,
                },
            ],
        )
        .with_chunk_rows(500),
    );

    // Query: rows in the [5000, 5100) id range.
    let request = ScanRequest {
        projection: None,
        filters: vec![
            SimpleFilter {
                column: "id".to_string(),
                operator: FilterOperator::Gte,
                value: ScalarValue::Int64(5000),
            },
            SimpleFilter {
                column: "id".to_string(),
                operator: FilterOperator::Lt,
                value: ScalarValue::Int64(5100),
            },
        ],
        limit: None,
        order_by: vec![],
        query_id: "test".to_string(),
    };

    let response = table.scan(request).await.expect("scan");
    let mut rows: Vec<(i64, String)> = Vec::new();
    let mut stream = response.stream;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let levels = batch
            .column_by_name("level")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            // Apply the residual filter ourselves: pruner kept whole chunks
            // where ANY row could match, but it's the consumer's job (or
            // DataFusion's) to enforce the predicate per row. Sprint 1 keeps
            // semantics correct by re-applying.
            let id = ids.value(i);
            if id >= 5000 && id < 5100 {
                rows.push((id, levels.value(i).to_string()));
            }
        }
    }

    assert_eq!(rows.len(), 100, "expected 100 rows in [5000, 5100)");
    assert_eq!(rows.first().unwrap().0, 5000);
    assert_eq!(rows.last().unwrap().0, 5099);
}

#[tokio::test]
async fn jsonl_zone_map_actually_skips_chunks() {
    let dir = TempDir::new().unwrap();
    let path = make_jsonl(&dir, 10_000);
    let parser = Arc::new(
        JsonlParser::try_new(vec![JsonlColumn {
            name: "id".to_string(),
            path: "$.id".to_string(),
            data_type: JsonlType::Int64,
        }])
        .unwrap(),
    );
    let table = Arc::new(
        RowsFileTable::new(
            path,
            parser,
            vec![IndexedColumn {
                name: "id".to_string(),
                kind: IndexKind::ZoneMap,
            }],
        )
        .with_chunk_rows(500),
    );

    // Range covering 1% of the data.
    let filters = vec![
        SimpleFilter {
            column: "id".to_string(),
            operator: FilterOperator::Gte,
            value: ScalarValue::Int64(5000),
        },
        SimpleFilter {
            column: "id".to_string(),
            operator: FilterOperator::Lt,
            value: ScalarValue::Int64(5100),
        },
    ];
    let (total, kept) = table.debug_skip_stats(&filters).unwrap();
    assert_eq!(
        total, 20,
        "expected 20 chunks for 10k rows / 500 chunk_rows"
    );
    // The matching range is entirely inside one chunk (5000..5500). Pruner
    // should keep exactly 1.
    assert_eq!(
        kept, 1,
        "expected to skip 19/20 chunks, kept {kept}/{total}"
    );
}

#[tokio::test]
async fn jsonl_no_filters_emits_all_rows() {
    let dir = TempDir::new().unwrap();
    let path = make_jsonl(&dir, 1_500);
    let parser = Arc::new(
        JsonlParser::try_new(vec![JsonlColumn {
            name: "id".to_string(),
            path: "$.id".to_string(),
            data_type: JsonlType::Int64,
        }])
        .unwrap(),
    );
    let table = Arc::new(RowsFileTable::new(path, parser, vec![]));
    let request = ScanRequest {
        projection: None,
        filters: vec![],
        limit: None,
        order_by: vec![],
        query_id: "all".to_string(),
    };
    let resp = table.scan(request).await.unwrap();
    let mut total = 0usize;
    let mut stream = resp.stream;
    while let Some(batch) = stream.next().await {
        total += batch.unwrap().num_rows();
    }
    assert_eq!(total, 1500);
}
