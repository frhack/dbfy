//! Incremental EXTEND: verifies that appending to a tailed file does NOT
//! re-parse the original bytes — only the new tail. The contract: the new
//! index has all the old chunks (unchanged byte_starts) plus new ones for
//! the appended rows.

use std::io::Write;
use std::sync::Arc;

use dbfy_provider::{ProgrammaticTableProvider, ScanRequest};
use dbfy_provider_rows_file::parsers::JsonlColumn;
use dbfy_provider_rows_file::parsers::JsonlParser;
use dbfy_provider_rows_file::parsers::jsonl::JsonlType;
use dbfy_provider_rows_file::{IndexKind, IndexedColumn, RowsFileTable};
use futures::StreamExt;
use tempfile::TempDir;

fn write_rows(path: &std::path::Path, ids: std::ops::Range<i64>) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for id in ids {
        writeln!(f, r#"{{"id": {id}}}"#).unwrap();
    }
    f.flush().unwrap();
}

#[tokio::test]
async fn extend_preserves_old_chunks_and_appends_new() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("growing.jsonl");
    write_rows(&path, 0..1000);

    let parser = Arc::new(
        JsonlParser::try_new(vec![JsonlColumn {
            name: "id".into(),
            path: "$.id".into(),
            data_type: JsonlType::Int64,
        }])
        .unwrap(),
    );
    let table = Arc::new(
        RowsFileTable::new(
            path.clone(),
            parser,
            vec![IndexedColumn {
                name: "id".into(),
                kind: IndexKind::ZoneMap,
            }],
        )
        .with_chunk_rows(100),
    );

    // First scan builds the index.
    let req = ScanRequest {
        projection: None,
        filters: vec![],
        limit: None,
        order_by: vec![],
        query_id: "first".to_string(),
    };
    let resp = table.scan(req).await.unwrap();
    let mut total: usize = 0;
    let mut s = resp.stream;
    while let Some(batch) = s.next().await {
        total += batch.unwrap().num_rows();
    }
    assert_eq!(total, 1000);

    // Snapshot the index file size and chunk count BEFORE the append.
    let idx_path = dbfy_provider_rows_file::idx_path(&path);
    let cached_before = dbfy_provider_rows_file::read_index(&idx_path).unwrap();
    let chunks_before = cached_before.chunks.len();
    assert_eq!(chunks_before, 10, "expected 1000/100 = 10 chunks");
    let last_byte_before = cached_before.chunks.last().unwrap().byte_end;

    // Append 500 more rows. Bump mtime past the cached value so the
    // invalidator notices.
    std::thread::sleep(std::time::Duration::from_millis(20));
    write_rows(&path, 1000..1500);

    // Second scan — must take the EXTEND branch.
    let req2 = ScanRequest {
        projection: None,
        filters: vec![],
        limit: None,
        order_by: vec![],
        query_id: "second".to_string(),
    };
    let resp2 = table.scan(req2).await.unwrap();
    let mut total2: usize = 0;
    let mut s2 = resp2.stream;
    while let Some(batch) = s2.next().await {
        total2 += batch.unwrap().num_rows();
    }
    assert_eq!(total2, 1500);

    // The index must now have 15 chunks; the FIRST 10 must be byte-identical
    // to the cached snapshot (proving they weren't re-parsed).
    let cached_after = dbfy_provider_rows_file::read_index(&idx_path).unwrap();
    assert_eq!(cached_after.chunks.len(), 15);
    for i in 0..chunks_before {
        assert_eq!(
            cached_after.chunks[i].byte_start, cached_before.chunks[i].byte_start,
            "chunk {i} byte_start changed — extend re-parsed older bytes",
        );
        assert_eq!(
            cached_after.chunks[i].byte_end, cached_before.chunks[i].byte_end,
            "chunk {i} byte_end changed — extend re-parsed older bytes",
        );
    }
    assert_eq!(
        cached_after.chunks[chunks_before].byte_start, last_byte_before,
        "first new chunk should pick up exactly where the old index ended"
    );
}
