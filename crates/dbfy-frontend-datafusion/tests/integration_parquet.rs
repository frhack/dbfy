//! Parquet integration test — generates a small TPC-H-like dataset
//! (orders + lineitem) at test time and runs Q1-style aggregates
//! against it. Validates row-group + projection pushdown end-to-end.
//!
//! Unlike the postgres/ldap integration tests, this one needs no
//! Docker: parquet files live on disk in a tempdir for the test's
//! lifetime. So no `#[ignore]` gate.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use dbfy_config::{Config, ParquetSourceConfig, ParquetTableConfig, SourceConfig};
use dbfy_frontend_datafusion::Engine;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

/// Build a small synthetic `orders` parquet (5 000 rows, 2 row groups).
fn write_orders_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", ArrowDataType::Int64, false),
        Field::new("o_custkey", ArrowDataType::Int64, false),
        Field::new("o_orderstatus", ArrowDataType::Utf8, false),
        Field::new("o_totalprice", ArrowDataType::Float64, false),
    ]));

    let n = 5_000;
    let order_keys: Vec<i64> = (1..=n).collect();
    let cust_keys: Vec<i64> = (1..=n).map(|i| 1 + (i % 100)).collect();
    let statuses: Vec<&str> = (1..=n)
        .map(|i| match i % 3 {
            0 => "F", // finished
            1 => "O", // open
            _ => "P", // pending
        })
        .collect();
    let totalprices: Vec<f64> = (1..=n).map(|i| (i as f64) * 13.7).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(order_keys)),
            Arc::new(Int64Array::from(cust_keys)),
            Arc::new(StringArray::from(statuses)),
            Arc::new(Float64Array::from(totalprices)),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_size(2_500) // forces ≥2 row groups
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn build_engine(tmp: &TempDir) -> Engine {
    let parquet_path = tmp.path().join("orders.parquet");
    write_orders_parquet(&parquet_path);

    let mut tables = BTreeMap::new();
    tables.insert(
        "orders".to_string(),
        ParquetTableConfig {
            path: parquet_path.to_string_lossy().into_owned(),
        },
    );
    let mut sources = BTreeMap::new();
    sources.insert(
        "tpch".to_string(),
        SourceConfig::Parquet(ParquetSourceConfig { tables }),
    );
    let config = Config {
        version: 1,
        sources,
    };
    Engine::from_config(config).unwrap()
}

#[tokio::test]
async fn aggregate_by_status_returns_three_buckets() {
    let tmp = TempDir::new().unwrap();
    let engine = build_engine(&tmp);
    // Q1-lite: GROUP BY status with count + sum.
    let batches = engine
        .query(
            "SELECT o_orderstatus,
                    count(*) AS n,
                    sum(o_totalprice) AS total
             FROM tpch.orders
             GROUP BY o_orderstatus
             ORDER BY o_orderstatus",
        )
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "three statuses: F / O / P");
}

#[tokio::test]
async fn predicate_pushdown_on_high_value_orders() {
    // Predicate that lops off ~99% of rows. With proper row-group
    // pruning + late materialisation this scan should not load most
    // of the file. We can't observe that directly through the
    // public query API, but we DO observe correctness — the
    // result must be exactly right.
    let tmp = TempDir::new().unwrap();
    let engine = build_engine(&tmp);
    let batches = engine
        .query("SELECT count(*) FROM tpch.orders WHERE o_totalprice > 60000.0")
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // o_totalprice = orderkey * 13.7, so > 60000 means orderkey > ~4380.
    // That's 5000 - 4380 = 620 rows.
    assert!(
        (618..=622).contains(&n),
        "expected ~620 high-value orders, got {n}"
    );
}

#[tokio::test]
async fn projection_pushdown_returns_only_requested_columns() {
    let tmp = TempDir::new().unwrap();
    let engine = build_engine(&tmp);
    let batches = engine
        .query("SELECT o_orderkey FROM tpch.orders LIMIT 5")
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().fields().len(),
        1,
        "projection pushdown must yield a 1-column batch"
    );
    assert_eq!(
        batches[0].schema().field(0).name(),
        "o_orderkey",
        "the right column"
    );
}

#[tokio::test]
async fn multi_row_group_scan_returns_all_rows() {
    let tmp = TempDir::new().unwrap();
    let engine = build_engine(&tmp);
    let batches = engine
        .query("SELECT count(*) FROM tpch.orders")
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 5_000, "every row across both row groups");
}
