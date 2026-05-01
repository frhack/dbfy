//! End-to-end demo: synthesise a 50 k-row JSONL log file, build a
//! `RowsFileTable` with zone maps + bloom on the right columns, and run a
//! few realistic ops queries showing the **skip ratio** for each.
//!
//! The point of this demo is to make L3 indexing concrete: every query
//! prints how many chunks survived the prune, not just how many rows
//! came back. That's the difference between "index is wired" and "index
//! is doing useful work".
//!
//! Run with::
//!
//!     cargo run -p dbfy-provider-rows-file --example log_analytics --release

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

const TOTAL_ROWS: i64 = 50_000;
const CHUNK_ROWS: usize = 1_000;

fn build_fixture(path: &std::path::Path) {
    let mut f = std::fs::File::create(path).unwrap();
    let levels = ["INFO", "INFO", "INFO", "INFO", "WARN", "INFO", "ERROR"];
    let services = ["api", "billing", "search", "indexer"];
    let base_secs: i64 = 1_777_852_800; // ≈ 2026-05-04 UTC
    for id in 0..TOTAL_ROWS {
        let level = levels[id as usize % levels.len()];
        let service = services[id as usize % services.len()];
        let secs = base_secs + id;
        let ts = format_rfc3339(secs);
        let trace = format!("trace-{:08}", id);
        writeln!(
            f,
            r#"{{"id": {id}, "ts": "{ts}", "level": "{level}", "service": "{service}", "trace_id": "{trace}", "msg": "request {id}"}}"#,
        )
        .unwrap();
    }
    f.flush().unwrap();
}

fn format_rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let s_in_day = secs.rem_euclid(86_400);
    let h = s_in_day / 3600;
    let m = (s_in_day % 3600) / 60;
    let s = s_in_day % 60;
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = TempDir::new()?;
    let path = dir.path().join("app.jsonl");
    println!(
        "synthesising fixture: {} rows of structured logs",
        TOTAL_ROWS
    );
    let t0 = std::time::Instant::now();
    build_fixture(&path);
    let bytes = std::fs::metadata(&path)?.len();
    println!(
        "  fixture written in {:?}, {} bytes ({:.1} MiB)",
        t0.elapsed(),
        bytes,
        bytes as f64 / 1024.0 / 1024.0
    );

    let parser = Arc::new(JsonlParser::try_new(vec![
        JsonlColumn {
            name: "id".into(),
            path: "$.id".into(),
            data_type: JsonlType::Int64,
        },
        JsonlColumn {
            name: "ts".into(),
            path: "$.ts".into(),
            data_type: JsonlType::Timestamp,
        },
        JsonlColumn {
            name: "level".into(),
            path: "$.level".into(),
            data_type: JsonlType::String,
        },
        JsonlColumn {
            name: "service".into(),
            path: "$.service".into(),
            data_type: JsonlType::String,
        },
        JsonlColumn {
            name: "trace_id".into(),
            path: "$.trace_id".into(),
            data_type: JsonlType::String,
        },
        JsonlColumn {
            name: "msg".into(),
            path: "$.msg".into(),
            data_type: JsonlType::String,
        },
    ])?);

    let table = Arc::new(
        RowsFileTable::new(
            path,
            parser,
            vec![
                IndexedColumn {
                    name: "id".into(),
                    kind: IndexKind::ZoneMap,
                },
                IndexedColumn {
                    name: "ts".into(),
                    kind: IndexKind::ZoneMap,
                },
                IndexedColumn {
                    name: "level".into(),
                    kind: IndexKind::Bloom,
                },
                IndexedColumn {
                    name: "service".into(),
                    kind: IndexKind::Bloom,
                },
                IndexedColumn {
                    name: "trace_id".into(),
                    kind: IndexKind::Bloom,
                },
            ],
        )
        .with_chunk_rows(CHUNK_ROWS),
    );

    let t0 = std::time::Instant::now();
    table.rebuild()?;
    println!("  index built in {:?}", t0.elapsed());
    println!();

    // Three realistic ops queries:
    bench(
        &table,
        "1) range on id (1% of rows)",
        vec![
            SimpleFilter {
                column: "id".into(),
                operator: FilterOperator::Gte,
                value: ScalarValue::Int64(20_000),
            },
            SimpleFilter {
                column: "id".into(),
                operator: FilterOperator::Lt,
                value: ScalarValue::Int64(20_500),
            },
        ],
    )
    .await?;

    bench(
        &table,
        "2) bloom on missing trace_id",
        vec![SimpleFilter {
            column: "trace_id".into(),
            operator: FilterOperator::Eq,
            value: ScalarValue::Utf8("trace-99999999".into()),
        }],
    )
    .await?;

    bench(
        &table,
        "3) bloom on present trace_id",
        vec![SimpleFilter {
            column: "trace_id".into(),
            operator: FilterOperator::Eq,
            value: ScalarValue::Utf8("trace-00012345".into()),
        }],
    )
    .await?;

    bench(
        &table,
        "4) IN list on service (3 of 4 known)",
        vec![SimpleFilter {
            column: "service".into(),
            operator: FilterOperator::In,
            value: ScalarValue::Utf8List(vec!["api".into(), "billing".into(), "search".into()]),
        }],
    )
    .await?;

    bench(&table, "5) full scan (no filters)", vec![]).await?;

    Ok(())
}

async fn bench(
    table: &Arc<RowsFileTable>,
    label: &str,
    filters: Vec<SimpleFilter>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (total_chunks, kept_chunks) = table.debug_skip_stats(&filters)?;

    let req = ScanRequest {
        projection: None,
        filters: filters.clone(),
        limit: None,
        order_by: vec![],
        query_id: label.to_string(),
    };
    let t0 = std::time::Instant::now();
    let resp = table.scan(req).await?;
    let mut rows = 0usize;
    let mut s = resp.stream;
    while let Some(batch) = s.next().await {
        rows += batch?.num_rows();
    }
    let elapsed = t0.elapsed();

    let skipped_pct = if total_chunks == 0 {
        0.0
    } else {
        100.0 * (1.0 - kept_chunks as f64 / total_chunks as f64)
    };
    println!("{label}");
    println!("  chunks scanned: {kept_chunks}/{total_chunks} ({skipped_pct:.1}% skipped)");
    println!("  rows returned:  {rows}");
    println!("  elapsed:        {elapsed:?}");
    println!();
    Ok(())
}
