//! Bloom-driven skip rate. High-cardinality string column with declared
//! `Bloom` index → equality on a missing key skips most chunks.

use std::io::Write;
use std::sync::Arc;

use dbfy_provider::{FilterOperator, ScalarValue, SimpleFilter};
use dbfy_provider_rows_file::parsers::JsonlColumn;
use dbfy_provider_rows_file::parsers::jsonl::JsonlType;
use dbfy_provider_rows_file::parsers::JsonlParser;
use dbfy_provider_rows_file::{IndexKind, IndexedColumn, RowsFileTable};
use tempfile::TempDir;

fn make_jsonl_high_cardinality(dir: &TempDir, total: i64) -> std::path::PathBuf {
    let path = dir.path().join("traces.jsonl");
    let mut f = std::fs::File::create(&path).unwrap();
    for id in 0..total {
        // Each row has a unique trace_id; equality search hits one chunk only.
        writeln!(f, r#"{{"id": {id}, "trace_id": "trace-{id:08}"}}"#).unwrap();
    }
    f.flush().unwrap();
    path
}

#[tokio::test]
async fn bloom_skips_chunks_for_unknown_key() {
    let dir = TempDir::new().unwrap();
    let path = make_jsonl_high_cardinality(&dir, 10_000);
    let parser = Arc::new(
        JsonlParser::try_new(vec![
            JsonlColumn {
                name: "id".into(),
                path: "$.id".into(),
                data_type: JsonlType::Int64,
            },
            JsonlColumn {
                name: "trace_id".into(),
                path: "$.trace_id".into(),
                data_type: JsonlType::String,
            },
        ])
        .unwrap(),
    );
    let table = Arc::new(
        RowsFileTable::new(
            path,
            parser,
            vec![IndexedColumn {
                name: "trace_id".into(),
                kind: IndexKind::Bloom,
            }],
        )
        .with_chunk_rows(500),
    );

    // Search a key that DEFINITELY doesn't exist. Bloom should reject every
    // chunk (allowing for the configured FPR).
    let absent = vec![SimpleFilter {
        column: "trace_id".into(),
        operator: FilterOperator::Eq,
        value: ScalarValue::Utf8("trace-99999999".into()),
    }];
    let (total, kept) = table.debug_skip_stats(&absent).unwrap();
    assert_eq!(total, 20);
    // Up to 1% FPR + slack — almost zero chunks survive on a clean miss.
    assert!(
        kept <= 2,
        "expected ≤2 false-positive chunks, kept {kept}/{total}"
    );

    // Search a key that exists. Bloom must keep at least the chunk that
    // actually contains it.
    let present = vec![SimpleFilter {
        column: "trace_id".into(),
        operator: FilterOperator::Eq,
        value: ScalarValue::Utf8("trace-00005000".into()),
    }];
    let (_, kept_present) = table.debug_skip_stats(&present).unwrap();
    assert!(
        kept_present >= 1 && kept_present <= 3,
        "expected 1..=3 chunks for present key, kept {kept_present}/{total}"
    );
}

#[tokio::test]
async fn bloom_in_list_keeps_chunks_with_any_match() {
    let dir = TempDir::new().unwrap();
    let path = make_jsonl_high_cardinality(&dir, 5_000);
    let parser = Arc::new(
        JsonlParser::try_new(vec![JsonlColumn {
            name: "trace_id".into(),
            path: "$.trace_id".into(),
            data_type: JsonlType::String,
        }])
        .unwrap(),
    );
    let table = Arc::new(
        RowsFileTable::new(
            path,
            parser,
            vec![IndexedColumn {
                name: "trace_id".into(),
                kind: IndexKind::Bloom,
            }],
        )
        .with_chunk_rows(500),
    );
    // 3 keys, all present, in different chunks. Pruner should keep ~3 chunks.
    let in_list = vec![SimpleFilter {
        column: "trace_id".into(),
        operator: FilterOperator::In,
        value: ScalarValue::Utf8List(vec![
            "trace-00000010".into(),
            "trace-00002500".into(),
            "trace-00004500".into(),
        ]),
    }];
    let (total, kept) = table.debug_skip_stats(&in_list).unwrap();
    assert_eq!(total, 10);
    assert!(
        kept >= 3 && kept <= 5,
        "expected 3..=5 chunks for IN-list, kept {kept}/{total}"
    );
}
