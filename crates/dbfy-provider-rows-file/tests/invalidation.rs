//! Cache invalidation: 5 scenarios on the decision tree
//! (REUSE / EXTEND / REBUILD).

use std::io::Write;
use std::sync::Arc;

use dbfy_provider_rows_file::parsers::JsonlColumn;
use dbfy_provider_rows_file::parsers::JsonlParser;
use dbfy_provider_rows_file::parsers::jsonl::JsonlType;
use dbfy_provider_rows_file::{Decision, FileSnapshot, IndexKind, IndexedColumn, RowsFileTable};
use tempfile::TempDir;

fn write_jsonl(path: &std::path::Path, rows: &[(i64, &str)]) {
    let mut f = std::fs::File::create(path).unwrap();
    for (id, name) in rows {
        writeln!(f, r#"{{"id": {id}, "name": "{name}"}}"#).unwrap();
    }
    f.flush().unwrap();
}

fn append_jsonl(path: &std::path::Path, rows: &[(i64, &str)]) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    for (id, name) in rows {
        writeln!(f, r#"{{"id": {id}, "name": "{name}"}}"#).unwrap();
    }
    f.flush().unwrap();
}

fn provider(path: std::path::PathBuf) -> Arc<RowsFileTable> {
    let parser = Arc::new(
        JsonlParser::try_new(vec![JsonlColumn {
            name: "id".into(),
            path: "$.id".into(),
            data_type: JsonlType::Int64,
        }])
        .unwrap(),
    );
    Arc::new(
        RowsFileTable::new(
            path,
            parser,
            vec![IndexedColumn {
                name: "id".into(),
                kind: IndexKind::ZoneMap,
            }],
        )
        .with_chunk_rows(100),
    )
}

/// Helper: read cached index header + fresh snapshot, run the decision.
fn decide(path: &std::path::Path) -> Decision {
    let idx_path = dbfy_provider_rows_file::idx_path(path);
    let cached = dbfy_provider_rows_file::read_index(&idx_path).unwrap();
    let snapshot = FileSnapshot::capture(path).unwrap();
    dbfy_provider_rows_file::decide_invalidation_with_path(&snapshot, &cached, Some(path))
}

#[tokio::test]
async fn unchanged_file_reuses_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a.jsonl");
    write_jsonl(&path, &[(1, "a"), (2, "b"), (3, "c")]);

    let p = provider(path.clone());
    p.rebuild().unwrap();

    assert_eq!(decide(&path), Decision::Reuse);
}

#[tokio::test]
async fn append_keeps_prefix_emits_extend() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("b.jsonl");
    write_jsonl(&path, &[(1, "a"), (2, "b")]);
    let p = provider(path.clone());
    p.rebuild().unwrap();

    // Sleep enough to bump mtime past the cached value.
    std::thread::sleep(std::time::Duration::from_millis(10));
    append_jsonl(&path, &[(3, "c")]);
    assert_eq!(decide(&path), Decision::Extend);
}

#[tokio::test]
async fn truncation_emits_rebuild() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("c.jsonl");
    write_jsonl(&path, &[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]);
    let p = provider(path.clone());
    p.rebuild().unwrap();

    // Truncate to fewer bytes than the cached size.
    std::thread::sleep(std::time::Duration::from_millis(10));
    write_jsonl(&path, &[(1, "a")]);
    assert_eq!(decide(&path), Decision::Rebuild);
}

#[tokio::test]
async fn atomic_replace_via_mv_emits_rebuild() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.jsonl");
    write_jsonl(&path, &[(1, "a"), (2, "b")]);
    let p = provider(path.clone());
    p.rebuild().unwrap();

    // Mimic `cat new > tmp && mv tmp file`: write a totally new file,
    // then atomic-rename onto the original path. Inode changes.
    std::thread::sleep(std::time::Duration::from_millis(10));
    let tmp = dir.path().join("d.jsonl.tmp");
    write_jsonl(&tmp, &[(99, "z"), (100, "y")]);
    std::fs::rename(&tmp, &path).unwrap();
    assert_eq!(decide(&path), Decision::Rebuild);
}

#[tokio::test]
async fn mid_file_edit_emits_rebuild() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("e.jsonl");
    write_jsonl(&path, &[(1, "alpha"), (2, "beta"), (3, "gamma")]);
    let p = provider(path.clone());
    p.rebuild().unwrap();

    // Re-write the same file with completely different content of the same
    // OR larger size, KEEPING THE SAME INODE (we open with truncate, not mv).
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    writeln!(
        f,
        r#"{{"id": 99, "name": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"}}"#
    )
    .unwrap();
    writeln!(
        f,
        r#"{{"id": 88, "name": "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"}}"#
    )
    .unwrap();
    writeln!(
        f,
        r#"{{"id": 77, "name": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"}}"#
    )
    .unwrap();
    f.flush().unwrap();
    drop(f);

    assert_eq!(decide(&path), Decision::Rebuild);
}
