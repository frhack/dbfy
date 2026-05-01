//! While a query is running on `RowsFileTable`, another process appends to
//! the underlying file. The query must observe a consistent snapshot —
//! either it sees the new rows or it doesn't, but it never sees a partial
//! row.
//!
//! Sprint 1's `RowsFileTable` materialises the index once per scan and
//! reads byte ranges from the file, so the snapshot is implicitly the
//! state captured at index time. This test pins that contract: a writer
//! racing in the middle of a scan does not corrupt the result.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use dbfy_provider::{ProgrammaticTableProvider, ScanRequest};
use dbfy_provider_rows_file::parsers::JsonlColumn;
use dbfy_provider_rows_file::parsers::jsonl::JsonlType;
use dbfy_provider_rows_file::parsers::JsonlParser;
use dbfy_provider_rows_file::{IndexKind, IndexedColumn, RowsFileTable};
use futures::StreamExt;
use tempfile::TempDir;

#[tokio::test]
async fn concurrent_appender_does_not_break_running_query() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("live.jsonl");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        for id in 0..1000 {
            writeln!(f, r#"{{"id": {id}, "level": "INFO"}}"#).unwrap();
        }
        f.flush().unwrap();
    }

    let parser = Arc::new(
        JsonlParser::try_new(vec![
            JsonlColumn {
                name: "id".into(),
                path: "$.id".into(),
                data_type: JsonlType::Int64,
            },
            JsonlColumn {
                name: "level".into(),
                path: "$.level".into(),
                data_type: JsonlType::String,
            },
        ])
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

    // Pre-build the index — fixes the "snapshot" of rows the query will see.
    table.rebuild().unwrap();

    // Spawn a background appender that adds 1000 rows while the query runs.
    let writer_path = path.clone();
    let writer = std::thread::spawn(move || {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&writer_path)
            .unwrap();
        for id in 1000..2000 {
            writeln!(f, r#"{{"id": {id}, "level": "WARN"}}"#).unwrap();
            std::thread::sleep(Duration::from_micros(50));
        }
        f.flush().unwrap();
    });

    // Start the query NOW — should observe the 1000-row snapshot only.
    let request = ScanRequest {
        projection: None,
        filters: vec![],
        limit: None,
        order_by: vec![],
        query_id: "concurrent".to_string(),
    };
    let resp = table.scan(request).await.unwrap();
    let mut total = 0usize;
    let mut stream = resp.stream;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        // Every row's level should be INFO — WARN rows belong to bytes
        // appended after our snapshot.
        let levels = batch
            .column_by_name("level")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(
                levels.value(i),
                "INFO",
                "leaked a WARN row from after the snapshot"
            );
        }
        total += batch.num_rows();
    }
    writer.join().unwrap();
    assert_eq!(total, 1000, "should have read exactly the snapshot rows");
}
