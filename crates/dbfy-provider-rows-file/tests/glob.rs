//! Multi-file scan via `RowsFileGlob`. Three files in a directory, one row
//! per file → 3 rows total, sorted by path.

use std::io::Write;
use std::sync::Arc;

use dbfy_provider::{ProgrammaticTableProvider, ScanRequest};
use dbfy_provider_rows_file::parsers::JsonlColumn;
use dbfy_provider_rows_file::parsers::JsonlParser;
use dbfy_provider_rows_file::parsers::jsonl::JsonlType;
use dbfy_provider_rows_file::{IndexKind, IndexedColumn, RowsFileGlob};
use futures::StreamExt;
use tempfile::TempDir;

#[tokio::test]
async fn glob_pattern_unions_files() {
    let dir = TempDir::new().unwrap();
    for (i, name) in ["app-1.jsonl", "app-2.jsonl", "app-3.jsonl"]
        .iter()
        .enumerate()
    {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        for j in 0..100 {
            writeln!(f, r#"{{"id": {}}}"#, i * 1000 + j).unwrap();
        }
    }

    let parser = Arc::new(
        JsonlParser::try_new(vec![JsonlColumn {
            name: "id".into(),
            path: "$.id".into(),
            data_type: JsonlType::Int64,
        }])
        .unwrap(),
    );
    let pattern = format!("{}/app-*.jsonl", dir.path().display());
    let glob = RowsFileGlob::try_new(
        &pattern,
        parser,
        vec![IndexedColumn {
            name: "id".into(),
            kind: IndexKind::ZoneMap,
        }],
    )
    .unwrap();
    assert_eq!(glob.file_count(), 3);

    let req = ScanRequest {
        projection: None,
        filters: vec![],
        limit: None,
        order_by: vec![],
        query_id: "glob".into(),
    };
    let resp = glob.scan(req).await.unwrap();
    let mut total: usize = 0;
    let mut s = resp.stream;
    while let Some(batch) = s.next().await {
        total += batch.unwrap().num_rows();
    }
    assert_eq!(total, 300);
}
