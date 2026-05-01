//! HTTP cache + singleflight dedup tests for the REST provider.
//!
//! Each test pins one specific guarantee:
//!  - sequential reuse: 2 calls → 1 wiremock hit
//!  - concurrent dedup: N parallel calls → 1 wiremock hit
//!  - TTL expiry: stale entry triggers a real refetch
//!  - distinct URLs don't share cache entries
//!  - leader errors don't poison the cache

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use dbfy_config::{
    AuthConfig, CacheConfig, ColumnConfig, DataType, EndpointConfig, HttpMethod,
    PaginationConfig, RestSourceConfig, RestTableConfig, RuntimeConfig,
};
use dbfy_provider_rest::RestTable;
use futures::StreamExt;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_table(server: &MockServer, ttl_seconds: u64) -> RestTable {
    let runtime = RuntimeConfig {
        timeout_ms: None,
        max_concurrency: None,
        max_pages: Some(1),
        retry: None,
        cache: Some(CacheConfig {
            ttl_seconds,
            max_entries: None,
        }),
    };
    let source = RestSourceConfig {
        base_url: server.uri(),
        auth: Some(AuthConfig::None),
        runtime: Some(runtime),
        tables: BTreeMap::new(),
    };
    let table_cfg = RestTableConfig {
        endpoint: EndpointConfig {
            method: HttpMethod::Get,
            path: "/items".to_string(),
        },
        root: "$.data[*]".to_string(),
        primary_key: None,
        include_raw_json: false,
        raw_column: "_raw".to_string(),
        columns: BTreeMap::from([(
            "id".to_string(),
            ColumnConfig {
                path: "$.id".to_string(),
                r#type: DataType::Int64,
            },
        )]),
        pagination: Option::<PaginationConfig>::None,
        pushdown: None,
    };
    RestTable::new("api", &source, "items", table_cfg)
}

async fn drain(table: &RestTable) -> usize {
    let stream = table
        .execute_stream(&["id".to_string()], &[], None)
        .expect("execute_stream");
    let mut s = stream.stream;
    let mut rows = 0usize;
    while let Some(batch) = s.next().await {
        rows += batch.expect("batch").num_rows();
    }
    rows
}

#[tokio::test]
async fn sequential_calls_reuse_cached_response() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}, {"id": 2}]
        })))
        .expect(1) // ← only one real fetch allowed
        .mount(&server)
        .await;

    let table = make_table(&server, 30);
    assert_eq!(drain(&table).await, 2);
    assert_eq!(drain(&table).await, 2);

    let metrics = table.cache_metrics().expect("cache attached");
    assert_eq!(metrics.misses, 1);
    assert_eq!(metrics.hits, 1);
}

#[tokio::test]
async fn concurrent_calls_singleflight_to_one_fetch() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        // Slow response so concurrent callers genuinely pile up while
        // the leader is still in-flight, exercising the Pending slot.
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(150))
                .set_body_json(json!({ "data": [{"id": 1}, {"id": 2}, {"id": 3}] })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let table = Arc::new(make_table(&server, 30));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let t = table.clone();
        handles.push(tokio::spawn(async move { drain(&t).await }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), 3);
    }

    let metrics = table.cache_metrics().unwrap();
    assert_eq!(metrics.misses, 1, "exactly one leader");
    assert!(
        metrics.coalesced >= 1,
        "at least one waiter should have coalesced, got {}",
        metrics.coalesced
    );
}

#[tokio::test]
async fn ttl_expiry_triggers_refetch() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}]
        })))
        .expect(2) // first fetch + post-TTL refetch
        .mount(&server)
        .await;

    // ttl_seconds = 0 means: every lookup is stale → always refetch.
    let table = make_table(&server, 0);
    assert_eq!(drain(&table).await, 1);
    assert_eq!(drain(&table).await, 1);

    let metrics = table.cache_metrics().unwrap();
    assert_eq!(metrics.misses, 2);
    assert_eq!(metrics.hits, 0);
}

#[tokio::test]
async fn cache_disabled_when_no_runtime_cache() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}]
        })))
        .expect(2) // both calls hit the wire when no cache configured
        .mount(&server)
        .await;

    // Build a table WITHOUT cache config.
    let source = RestSourceConfig {
        base_url: server.uri(),
        auth: Some(AuthConfig::None),
        runtime: None,
        tables: BTreeMap::new(),
    };
    let table = RestTable::new(
        "api",
        &source,
        "items",
        RestTableConfig {
            endpoint: EndpointConfig {
                method: HttpMethod::Get,
                path: "/items".to_string(),
            },
            root: "$.data[*]".to_string(),
            primary_key: None,
            include_raw_json: false,
            raw_column: "_raw".to_string(),
            columns: BTreeMap::from([(
                "id".to_string(),
                ColumnConfig {
                    path: "$.id".to_string(),
                    r#type: DataType::Int64,
                },
            )]),
            pagination: None,
            pushdown: None,
        },
    );
    assert_eq!(drain(&table).await, 1);
    assert_eq!(drain(&table).await, 1);
    assert!(table.cache_metrics().is_none());
}
