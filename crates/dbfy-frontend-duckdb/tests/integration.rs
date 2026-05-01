//! End-to-end test: spin up an in-process HTTP server with `wiremock`,
//! register `dbfy_rest()` on an in-memory DuckDB connection, run a SQL
//! query that joins the REST result with a literal table, assert the
//! merged rows.
//!
//! Run with::
//!
//!     cargo test -p dbfy-frontend-duckdb --features duckdb --jobs 1

#![cfg(feature = "duckdb")]

use duckdb::Connection;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test(flavor = "multi_thread")]
async fn dbfy_rest_round_trip() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/customers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "name": "Mario", "status": "active"},
                {"id": 2, "name": "Anna", "status": "inactive"},
                {"id": 3, "name": "Luca", "status": "active"},
            ]
        })))
        .mount(&server)
        .await;

    let url = format!("{}/customers", server.uri());

    // DuckDB integration is sync so we run it on a blocking thread, the
    // tokio runtime stays free for wiremock + reqwest.
    let url_clone = url.clone();
    let rows: Vec<(String,)> = tokio::task::spawn_blocking(move || {
        let conn = Connection::open_in_memory().expect("connection");
        dbfy_duckdb::register(&conn).expect("register");
        let sql = format!(
            "SELECT value FROM dbfy_rest('{url}') ORDER BY value",
            url = url_clone,
        );
        let mut stmt = conn.prepare(&sql).expect("prepare");
        let mut rows = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?,)))
            .expect("query_map")
            .collect::<Vec<_>>();
        rows.drain(..)
            .map(|r| r.expect("row"))
            .collect()
    })
    .await
    .expect("blocking");

    assert_eq!(rows.len(), 3, "expected 3 rows, got {rows:?}");
    // Each row is the JSON object as a string. We assert containment rather
    // than exact byte form because key ordering in serde_json output is
    // stable for objects but not portable across all versions.
    let joined: String = rows.iter().map(|(s,)| s.as_str()).collect::<Vec<_>>().join("\n");
    assert!(joined.contains("\"Mario\""), "missing Mario: {joined}");
    assert!(joined.contains("\"Anna\""), "missing Anna: {joined}");
    assert!(joined.contains("\"Luca\""), "missing Luca: {joined}");
}

#[tokio::test(flavor = "multi_thread")]
async fn dbfy_rest_typed_columns_with_filter_and_projection() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/customers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "name": "Mario", "status": "active",   "score": 9.5},
                {"id": 2, "name": "Anna",  "status": "inactive", "score": 7.1},
                {"id": 3, "name": "Luca",  "status": "active",   "score": 8.8},
            ]
        })))
        .mount(&server)
        .await;

    let url = format!("{}/customers", server.uri());

    let yaml_config = r#"
root: $.data[*]
columns:
  id:     {path: "$.id",     type: int64}
  name:   {path: "$.name",   type: string}
  status: {path: "$.status", type: string}
  score:  {path: "$.score",  type: float64}
"#;

    let url_clone = url.clone();
    let config_clone = yaml_config.to_string();
    let rows: Vec<(i64, String, f64)> = tokio::task::spawn_blocking(move || {
        let conn = Connection::open_in_memory().expect("connection");
        dbfy_duckdb::register(&conn).expect("register");

        // Pushdown of WHERE status = 'active' is not yet implemented in the
        // DuckDB frontend, but DuckDB applies it locally and the result is
        // identical: only `active` rows survive.
        let sql = format!(
            "SELECT id, name, score FROM dbfy_rest('{url}', config := ?) \
             WHERE status = 'active' ORDER BY id",
            url = url_clone,
        );
        let mut stmt = conn.prepare(&sql).expect("prepare");
        let rows = stmt
            .query_map([config_clone], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                ))
            })
            .expect("query_map")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        rows
    })
    .await
    .expect("blocking");

    assert_eq!(
        rows,
        vec![(1, "Mario".to_string(), 9.5), (3, "Luca".to_string(), 8.8)],
        "typed projection + filter mismatch",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dbfy_rest_projection_pushdown_emits_only_requested_columns() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "name": "alpha", "price": 9.99,  "in_stock": true},
                {"id": 2, "name": "beta",  "price": 14.50, "in_stock": false},
                {"id": 3, "name": "gamma", "price": 19.00, "in_stock": true},
            ]
        })))
        .mount(&server)
        .await;

    let url = format!("{}/items", server.uri());

    let yaml_config = r#"
root: $.data[*]
columns:
  id:       {path: "$.id",       type: int64}
  name:     {path: "$.name",     type: string}
  price:    {path: "$.price",    type: float64}
  in_stock: {path: "$.in_stock", type: boolean}
"#;

    // Case A: SELECT only `name` and `id` — projection pushdown picks
    // those two from the four declared columns.
    let url_a = url.clone();
    let cfg_a = yaml_config.to_string();
    let projected: Vec<(String, i64)> = tokio::task::spawn_blocking(move || {
        let conn = Connection::open_in_memory().expect("connection");
        dbfy_duckdb::register(&conn).expect("register");
        let sql = format!(
            "SELECT name, id FROM dbfy_rest('{url_a}', config := ?) ORDER BY id",
        );
        let mut stmt = conn.prepare(&sql).expect("prepare");
        stmt.query_map([cfg_a], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .expect("query_map")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect")
    })
    .await
    .expect("blocking");

    assert_eq!(
        projected,
        vec![
            ("alpha".to_string(), 1),
            ("beta".to_string(), 2),
            ("gamma".to_string(), 3),
        ],
        "projection pushdown order/values mismatch",
    );

    // Case B: SELECT * (all columns) — projection pushdown emits all four
    // columns in the declared order.
    let url_b = url.clone();
    let cfg_b = yaml_config.to_string();
    let full: Vec<(i64, String, f64, bool)> = tokio::task::spawn_blocking(move || {
        let conn = Connection::open_in_memory().expect("connection");
        dbfy_duckdb::register(&conn).expect("register");
        let sql = format!(
            "SELECT id, name, price, in_stock FROM dbfy_rest('{url_b}', config := ?) ORDER BY id",
        );
        let mut stmt = conn.prepare(&sql).expect("prepare");
        stmt.query_map([cfg_b], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .expect("query_map")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect")
    })
    .await
    .expect("blocking");

    assert_eq!(
        full,
        vec![
            (1, "alpha".to_string(), 9.99, true),
            (2, "beta".to_string(), 14.50, false),
            (3, "gamma".to_string(), 19.00, true),
        ],
        "full SELECT mismatch",
    );

    // Case C: count(*) with no column projection — provider may receive
    // an empty projection list; the row count must still be correct.
    let url_c = url.clone();
    let cfg_c = yaml_config.to_string();
    let count: i64 = tokio::task::spawn_blocking(move || {
        let conn = Connection::open_in_memory().expect("connection");
        dbfy_duckdb::register(&conn).expect("register");
        let sql = format!("SELECT count(*) FROM dbfy_rest('{url_c}', config := ?)");
        let mut stmt = conn.prepare(&sql).expect("prepare");
        stmt.query_row([cfg_c], |row| row.get::<_, i64>(0))
            .expect("query_row")
    })
    .await
    .expect("blocking");

    assert_eq!(count, 3, "count(*) row count mismatch");
}

#[tokio::test(flavor = "multi_thread")]
async fn dbfy_rows_file_indexed_jsonl_pushdown() {
    use std::io::Write;
    use tempfile::TempDir;

    // 200 jsonl rows, 50-row chunks → 4 chunks. A `BETWEEN 100 AND 119`
    // range filter should prune everything except the chunk that contains
    // those rows; the indexed_columns config gives the rows-file pruner
    // a zone map on `id`.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("events.jsonl");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        for id in 0..200 {
            writeln!(
                f,
                r#"{{"id": {id}, "level": "{}", "msg": "row {id}"}}"#,
                if id % 13 == 0 { "ERROR" } else { "INFO" }
            )
            .unwrap();
        }
    }
    let path_str = path.to_str().unwrap().to_string();

    let cfg = r#"
parser:
  format: jsonl
  columns:
    - { name: id,    path: "$.id",    type: int64 }
    - { name: level, path: "$.level", type: string }
    - { name: msg,   path: "$.msg",   type: string }
indexed_columns:
  - { name: id,    kind: zone_map }
  - { name: level, kind: bloom }
chunk_rows: 50
"#;

    let total = tokio::task::spawn_blocking({
        let path_str = path_str.clone();
        let cfg = cfg.to_string();
        move || {
            let conn = Connection::open_in_memory().expect("connection");
            dbfy_duckdb::register(&conn).expect("register");
            let sql = "SELECT count(*) FROM dbfy_rows_file(?, config := ?) WHERE id BETWEEN 100 AND 119";
            let mut stmt = conn.prepare(sql).expect("prepare");
            stmt.query_row([&path_str, &cfg], |row| row.get::<_, i64>(0))
                .expect("query_row")
        }
    })
    .await
    .expect("blocking");
    assert_eq!(total, 20, "BETWEEN 100 AND 119 should match 20 rows");

    // Bloom + ORDER BY across the whole file: ERROR rows are id ∈ {0, 13,
    // 26, …, 195} → 16 rows.
    let errors: Vec<(i64,)> = tokio::task::spawn_blocking({
        let path_str = path_str.clone();
        let cfg = cfg.to_string();
        move || {
            let conn = Connection::open_in_memory().expect("connection");
            dbfy_duckdb::register(&conn).expect("register");
            let sql = "SELECT id FROM dbfy_rows_file(?, config := ?) WHERE level = 'ERROR' ORDER BY id";
            let mut stmt = conn.prepare(sql).expect("prepare");
            let rows: Vec<(i64,)> = stmt
                .query_map([&path_str, &cfg], |row| Ok((row.get::<_, i64>(0)?,)))
                .expect("query")
                .filter_map(|r| r.ok())
                .collect();
            rows
        }
    })
    .await
    .expect("blocking");
    assert_eq!(errors.len(), 16);
    assert_eq!(errors.first().unwrap().0, 0);
    assert_eq!(errors.last().unwrap().0, 195);
}
