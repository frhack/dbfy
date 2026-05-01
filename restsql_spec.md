# SPEC — Embedded Federated SQL Engine for REST/JSON Datasources

## 1. Summary

This project is an open source, embeddable SQL federation engine written in Rust.

Its primary goal is to expose REST/JSON APIs and other datasources as SQL tables, allowing users to query them locally, without running a server.

The engine should support:

- REST/JSON APIs exposed as virtual SQL tables.
- Automatic schema/table inference.
- User-guided configuration for production-grade mappings.
- Programmatic table providers.
- SQL queries across multiple datasources.
- Pushdown of filters, limits, projections, and other operations when supported.
- Async execution for network-bound datasources.
- Embedding in Python, C, and Java.
- Apache Arrow as the internal data format.
- Apache DataFusion as the SQL planner and execution engine.

Target usage:

```sql
SELECT c.id, c.name, o.total
FROM crm.customers c
JOIN postgres.orders o ON o.customer_id = c.id
WHERE c.status = 'active'
LIMIT 100;
```

The query may involve:

```text
REST API + PostgreSQL + local files + object storage + programmatic providers
```

while still running fully embedded inside the host application.

---

## 2. Project Goals

### 2.1 Primary Goals

The system must:

1. Run embedded, without requiring a server process.
2. Be implemented in Rust.
3. Use async I/O for REST and network datasources.
4. Use Apache DataFusion for SQL parsing, planning, optimization, and execution.
5. Use Apache Arrow as the internal columnar format.
6. Allow REST/JSON endpoints to be represented as relational tables.
7. Support both:
   - automatic REST-to-table inference;
   - explicit user configuration.
8. Support programmatic table providers.
9. Support cross-datasource SQL queries.
10. Support partial pushdown into remote APIs, databases, and programmatic providers.
11. Provide bindings for:
   - Python;
   - C;
   - Java.
12. Be open source and clean-room implemented.

---

## 3. Non-Goals

The initial versions should not attempt to be:

1. A full replacement for CData, Trino, Dremio, or DuckDB.
2. A distributed query engine.
3. A server-first platform.
4. A general ETL platform.
5. A full API management gateway.
6. A complete BI semantic layer.
7. A perfect SQL-to-REST compiler for arbitrary APIs.
8. A write-back engine for arbitrary REST APIs in the MVP.
9. A replacement for specialized connectors where native database pushdown is already mature.
10. A sandboxed third-party plugin runtime in the MVP.

---

## 4. Core Use Cases

### 4.1 Query a REST API as a Table

Given:

```http
GET https://api.example.com/customers
```

Response:

```json
{
  "data": [
    {
      "id": 1,
      "name": "Mario Rossi",
      "status": "active",
      "country": "IT"
    }
  ]
}
```

The user should be able to query:

```sql
SELECT id, name
FROM crm.customers
WHERE status = 'active';
```

---

### 4.2 Query Across REST and Database Sources

Example:

```sql
SELECT c.id, c.name, SUM(o.total) AS revenue
FROM crm.customers c
JOIN sales.orders o ON o.customer_id = c.id
WHERE c.status = 'active'
GROUP BY c.id, c.name;
```

Where:

```text
crm.customers = REST API
sales.orders  = PostgreSQL
```

---

### 4.3 Query Across REST and Local Files

Example:

```sql
SELECT c.id, c.name, e.event_type
FROM crm.customers c
JOIN lake.events e ON e.customer_id = c.id
WHERE e.event_date >= DATE '2026-01-01';
```

Where:

```text
crm.customers = REST API
lake.events   = Parquet files
```

---

### 4.4 Query Across REST and Programmatic Providers

Example:

```sql
SELECT c.id, c.name, r.score
FROM app.customers c
JOIN crm.remote_scores r ON r.customer_id = c.id
WHERE c.status = 'active';
```

Where:

```text
app.customers     = programmatic provider
crm.remote_scores = REST provider
```

---

### 4.5 Automatic Discovery

The engine should be able to infer table mappings from:

- OpenAPI specifications;
- JSON Schema;
- sample JSON responses;
- runtime sampling;
- endpoint paths.

Example command:

```bash
restsql infer \
  --openapi ./openapi.yaml \
  --source-name crm \
  --out crm.generated.yaml
```

---

### 4.6 Guided Configuration

The user must be able to override all inferred behavior with explicit config.

Example:

```yaml
sources:
  crm:
    type: rest
    base_url: https://api.example.com
    auth:
      type: bearer
      token_env: CRM_TOKEN

    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        primary_key: id

        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
          status:
            path: "$.status"
            type: string
          country:
            path: "$.country"
            type: string

        pushdown:
          filters:
            status:
              param: status
              operators: ["=", "IN"]
            country:
              param: country
              operators: ["="]
          limit:
            param: limit
          projection:
            param: fields
            style: comma_separated

        pagination:
          type: cursor
          cursor_param: cursor
          cursor_path: "$.next_cursor"
```

---

## 5. High-Level Architecture

```text
Host Application
  ├─ Python / C / Java / Rust
  │
  └─ Embedded Engine
       ├─ SQL API
       ├─ DataFusion SessionContext
       ├─ Catalog Manager
       ├─ REST TableProvider
       ├─ Programmatic TableProvider
       ├─ Database TableProviders
       ├─ File/Object Store Providers
       ├─ Pushdown Planner
       ├─ Async Runtime
       ├─ Cache Layer
       └─ Arrow RecordBatch Streams
```

---

## 6. Main Components

### 6.1 Core Engine

Responsible for:

- initializing DataFusion;
- registering catalogs, schemas, and tables;
- loading datasource configs;
- registering programmatic providers;
- executing SQL queries;
- returning Arrow batches;
- exposing bindings to host languages.

Rust crate:

```text
dbfy-frontend-datafusion
```

---

### 6.2 Config Layer

Responsible for:

- parsing YAML/JSON config;
- validating mappings;
- resolving environment variables;
- validating authentication blocks;
- generating table mappings;
- checking schema compatibility.

Rust crate:

```text
dbfy-config
```

---

### 6.3 REST Provider

Responsible for:

- exposing REST endpoints as DataFusion `TableProvider`s;
- converting SQL projections and filters into HTTP requests;
- handling pagination;
- handling authentication;
- decoding JSON;
- flattening nested structures;
- streaming Arrow `RecordBatch` values.

Rust crate:

```text
dbfy-provider-rest
```

---

### 6.4 OpenAPI Inference

Responsible for:

- parsing OpenAPI specs;
- detecting endpoints that represent collections;
- inferring table names;
- inferring column names and types;
- detecting path parameters;
- detecting query parameters;
- detecting possible pushdown filters;
- generating editable config.

Rust crate:

```text
restsql-openapi
```

---

### 6.5 Federation Layer

Responsible for:

- representing datasource capabilities;
- deciding which predicates can be pushed down;
- distinguishing remote filters from residual local filters;
- supporting cross-source query planning;
- integrating with DataFusion optimizer rules where needed.

Rust crate:

```text
restsql-federation
```

---

### 6.6 Cache Layer

Responsible for optional local materialization of remote data.

Supported cache targets:

- in-memory Arrow batches;
- local Parquet files;
- object storage;
- SQLite or DuckDB-backed cache, optional.

Rust crate:

```text
dbfy-cache
```

---

### 6.7 Bindings

Planned crates:

```text
dbfy-python
dbfy-capi
restsql-java
```

Target integrations:

```text
Python → PyO3 / maturin
C      → C ABI + Arrow C Data Interface
Java   → JNI or JNA + Arrow Java integration
```

---

### 6.8 Programmatic Provider Layer

Responsible for allowing user code to expose virtual SQL tables.

The layer must support:

```text
row generators
Arrow batch generators
async batch streams
pushdown-aware providers
provider capability declarations
cross-source query participation
```

Rust crate:

```text
dbfy-provider
```

This crate should provide ergonomic APIs for Rust and shared abstractions for Python, C, and Java bindings.

---

## 7. Execution Model

### 7.1 Query Flow

```text
SQL query
  ↓
DataFusion parser
  ↓
Logical plan
  ↓
Optimizer
  ↓
TableProvider scan
  ↓
Pushdown planner
  ↓
ExecutionPlan
  ↓
Async datasource execution
  ↓
Arrow RecordBatch stream
  ↓
DataFusion local execution
  ↓
Result batches
```

---

### 7.2 REST Query Flow

```text
SQL query
  ↓
Projection/filter/limit extraction
  ↓
REST pushdown analysis
  ↓
HTTP request plan
  ↓
Async HTTP calls
  ↓
Pagination loop
  ↓
JSON decode
  ↓
Flattening / projection
  ↓
Arrow RecordBatch stream
```

---

### 7.3 Programmatic Provider Query Flow

```text
SQL query
  ↓
Projection/filter/limit extraction
  ↓
Programmatic provider capability analysis
  ↓
Provider scan request
  ↓
User code yields rows or batches
  ↓
Row-to-Arrow buffering, if needed
  ↓
Arrow RecordBatch stream
  ↓
DataFusion local execution
```

---

## 8. REST-to-Table Mapping

### 8.1 Table Mapping

A table represents one logical collection.

Example:

```yaml
tables:
  customers:
    endpoint:
      method: GET
      path: /customers
    root: "$.data[*]"
    primary_key: id
```

This maps:

```http
GET /customers
```

to:

```sql
crm.customers
```

---

### 8.2 Column Mapping

Columns map JSON paths to SQL/Arrow types.

```yaml
columns:
  id:
    path: "$.id"
    type: int64
  name:
    path: "$.name"
    type: string
  created_at:
    path: "$.created_at"
    type: timestamp
```

---

### 8.3 Nested Objects

Nested JSON objects may be flattened.

JSON:

```json
{
  "id": 1,
  "address": {
    "city": "Rome",
    "country": "IT"
  }
}
```

Table:

```sql
customers(
  id BIGINT,
  address_city TEXT,
  address_country TEXT
)
```

Or explicitly configured as:

```yaml
columns:
  city:
    path: "$.address.city"
    type: string
  country:
    path: "$.address.country"
    type: string
```

---

### 8.4 Arrays

Arrays may be represented as child tables.

JSON:

```json
{
  "id": 1,
  "tags": ["vip", "newsletter"]
}
```

Generated tables:

```sql
customers(id, ...)
customers_tags(customer_id, value)
```

---

### 8.5 Arrays of Objects

JSON:

```json
{
  "id": 1,
  "orders": [
    { "id": "A1", "total": 100 },
    { "id": "A2", "total": 200 }
  ]
}
```

Generated child table:

```sql
customers_orders(
  customer_id BIGINT,
  id TEXT,
  total DECIMAL
)
```

---

### 8.6 Raw JSON Column

Each REST table may optionally include the raw source object.

```yaml
include_raw_json: true
raw_column: _raw
```

Result:

```sql
customers(
  id BIGINT,
  name TEXT,
  status TEXT,
  _raw JSON
)
```

---

## 9. Inference Modes

### 9.1 Auto Mode

The engine infers mappings automatically.

Inputs may include:

```text
OpenAPI spec
JSON Schema
sample JSON
live endpoint sampling
```

Auto mode is optimized for:

- exploration;
- prototyping;
- fast onboarding.

It is not recommended as the sole production mode.

---

### 9.2 Guided Mode

The user provides explicit mapping.

Guided mode is optimized for:

- production;
- stable schemas;
- controlled pushdown;
- predictable performance;
- compatibility guarantees.

---

### 9.3 Hybrid Mode

Preferred mode.

Flow:

```text
1. System infers config.
2. System writes generated YAML.
3. User reviews and edits YAML.
4. Runtime uses YAML as source of truth.
```

Command:

```bash
restsql infer --openapi openapi.yaml --out crm.yaml
restsql validate crm.yaml
restsql query -c crm.yaml "SELECT * FROM crm.customers LIMIT 10"
```

---

## 10. Programmatic Table Providers

In addition to configured datasources such as REST APIs, databases, and files, the engine must support programmatic table providers.

A programmatic table provider is user-defined code that exposes a schema and produces rows or Arrow batches at query time.

This allows users to expose application-specific data as SQL tables without writing a full datasource connector.

Examples include:

```text
in-memory objects
custom SDK calls
internal application state
message queues
generated data
test fixtures
proprietary systems
temporary computed datasets
```

A programmatic table provider must be queryable like any other table and must participate in cross-datasource SQL queries.

Example:

```sql
SELECT c.id, c.name, r.score
FROM app.customers c
JOIN crm.remote_scores r ON r.customer_id = c.id
WHERE c.status = 'active';
```

Where:

```text
app.customers     = programmatic provider
crm.remote_scores = REST provider
```

---

### 10.1 Provider Levels

The engine should support three levels of programmatic providers.

---

### 10.2 Level 1 — Row Provider

A row provider is the simplest form.

The user provides code that yields records one by one.

Example Python API:

```python
@engine.table("app.customers", schema={
    "id": "int64",
    "name": "string",
    "status": "string",
})
def customers():
    yield {"id": 1, "name": "Mario", "status": "active"}
    yield {"id": 2, "name": "Anna", "status": "inactive"}
```

Query:

```sql
SELECT id, name
FROM app.customers
WHERE status = 'active';
```

In this mode, filters, projections, joins, and aggregations may be executed locally by DataFusion.

The row provider does not need to understand SQL pushdown.

Internally, row providers should be buffered into Arrow `RecordBatch` values for performance.

```text
user yields rows
  ↓
row buffer
  ↓
Arrow arrays
  ↓
RecordBatch
  ↓
DataFusion
```

The engine should expose a configurable batch size.

Example:

```python
engine.register_rows(
    "app.customers",
    schema=schema,
    provider=customers,
    batch_size=8192,
)
```

---

### 10.3 Level 2 — Arrow Batch Provider

A batch provider produces Arrow `RecordBatch` values directly.

This is the preferred mode for performance-sensitive integrations.

Example Python API:

```python
def customers_batches():
    yield pyarrow.record_batch(
        [
            pyarrow.array([1, 2]),
            pyarrow.array(["Mario", "Anna"]),
            pyarrow.array(["active", "inactive"]),
        ],
        names=["id", "name", "status"],
    )

engine.register_arrow_provider(
    name="app.customers",
    schema=schema,
    provider=customers_batches,
)
```

This avoids row-by-row conversion overhead and is suitable for larger datasets.

---

### 10.4 Level 3 — Pushdown-Aware Provider

A pushdown-aware provider receives query context from the engine.

The context may include:

```text
projection
filters
limit
sort order
runtime metadata
query id
cancellation signal
```

Example Python API:

```python
@engine.table("app.customers", schema={
    "id": "int64",
    "name": "string",
    "status": "string",
}, pushdown=["projection", "filter", "limit"])
def customers(ctx):
    yield from fetch_customers(
        fields=ctx.projection,
        status=ctx.filter_value("status"),
        limit=ctx.limit,
    )
```

Query:

```sql
SELECT id, name
FROM app.customers
WHERE status = 'active'
LIMIT 10;
```

Possible provider behavior:

```text
projection: id, name
filter: status = 'active'
limit: 10
```

The provider may use this information to call an external SDK, API, cache, or in-memory index more efficiently.

Correctness rule:

```text
If the provider declares that it handled a filter remotely,
the engine may skip applying that filter locally.

If the provider does not declare that it handled a filter,
DataFusion must apply the filter locally.
```

Pushdown must always be explicit and verifiable.

---

### 10.5 Rust Trait

The core Rust API should expose a trait similar to:

```rust
#[async_trait::async_trait]
pub trait ProgrammaticTableProvider: Send + Sync {
    fn schema(&self) -> arrow_schema::SchemaRef;

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    async fn scan(
        &self,
        request: ScanRequest,
    ) -> Result<RecordBatchStreamHandle>;
}
```

Where:

```rust
pub struct ScanRequest {
    pub projection: Option<Vec<String>>,
    pub filters: Vec<FilterExpr>,
    pub limit: Option<usize>,
    pub order_by: Vec<OrderExpr>,
    pub context: QueryContext,
}
```

And:

```rust
pub struct ProviderCapabilities {
    pub projection_pushdown: bool,
    pub filter_pushdown: FilterPushdown,
    pub limit_pushdown: bool,
    pub order_by_pushdown: bool,
    pub aggregate_pushdown: bool,
}
```

The provider should return an async stream of Arrow `RecordBatch` values.

---

### 10.6 Rust Convenience APIs

The engine should provide convenience APIs for common provider types.

Row provider:

```rust
engine.register_rows("app.customers", schema, || async_stream::try_stream! {
    yield row! {
        "id" => 1,
        "name" => "Mario",
        "status" => "active",
    };

    yield row! {
        "id" => 2,
        "name" => "Anna",
        "status" => "inactive",
    };
});
```

Batch provider:

```rust
engine.register_batches("app.customers", schema, || async_stream::try_stream! {
    yield batch;
});
```

Full provider:

```rust
engine.register_provider(
    "app.customers",
    Arc::new(MyCustomersProvider::new()),
);
```

---

### 10.7 Python API

Python should support decorators and registration methods.

Decorator-style row provider:

```python
@engine.table("app.customers", schema={
    "id": "int64",
    "name": "string",
    "status": "string",
})
def customers():
    yield {"id": 1, "name": "Mario", "status": "active"}
    yield {"id": 2, "name": "Anna", "status": "inactive"}
```

Async provider:

```python
@engine.table("app.events", schema={
    "id": "int64",
    "event_type": "string",
})
async def events():
    async for event in event_stream():
        yield {
            "id": event.id,
            "event_type": event.type,
        }
```

Pushdown-aware provider:

```python
@engine.table("app.customers", schema={
    "id": "int64",
    "name": "string",
    "status": "string",
}, pushdown=["projection", "filter", "limit"])
def customers(ctx):
    yield from fetch_customers(
        fields=ctx.projection,
        status=ctx.filter_value("status"),
        limit=ctx.limit,
    )
```

Arrow batch provider:

```python
engine.register_arrow_provider(
    name="app.customers",
    schema=schema,
    provider=customers_batches,
)
```

---

### 10.8 C API

The C API should support callback-based providers.

Example shape:

```c
typedef struct restsql_scan_request_t restsql_scan_request_t;
typedef struct restsql_batch_writer_t restsql_batch_writer_t;

typedef int (*restsql_provider_scan_fn)(
    const restsql_scan_request_t* request,
    restsql_batch_writer_t* writer,
    void* user_data
);
```

Registration:

```c
restsql_register_provider(
    engine,
    "app.customers",
    schema,
    scan_fn,
    user_data
);
```

The C provider API should support Arrow C Data Interface for zero-copy or low-copy batch exchange where possible.

---

### 10.9 Java API

Java should support both row-oriented and Arrow-oriented providers.

Row provider example:

```java
engine.registerProvider(
    "app.customers",
    Schema.of(
        Field.int64("id"),
        Field.utf8("name"),
        Field.utf8("status")
    ),
    scan -> Stream.of(
        Row.of(1, "Mario", "active"),
        Row.of(2, "Anna", "inactive")
    )
);
```

Arrow provider example:

```java
engine.registerArrowProvider(
    "app.customers",
    schema,
    scan -> List.of(recordBatch)
);
```

Pushdown-aware provider:

```java
engine.registerProvider(
    "app.customers",
    schema,
    scan -> customerService.fetch(
        scan.projection(),
        scan.filterValue("status"),
        scan.limit()
    )
);
```

---

### 10.10 Programmatic Provider Capabilities

Each provider may declare its capabilities.

Example:

```yaml
capabilities:
  projection: true
  limit: true
  filters:
    eq: true
    in: false
    gt: false
    lt: false
  order_by: false
  aggregate: false
```

For programmatic providers, this can also be declared in code.

Example Python:

```python
@engine.table(
    "app.customers",
    schema=schema,
    pushdown={
        "projection": True,
        "limit": True,
        "filters": {
            "=": ["status", "country"],
            "IN": ["status"]
        }
    }
)
def customers(ctx):
    ...
```

---

### 10.11 Programmatic Providers and Correctness

The engine must distinguish between:

```text
filters offered to provider
filters accepted by provider
filters actually handled by provider
residual filters to be applied locally
```

A provider must report which filters were handled.

Example:

```text
SQL filter:
  status = 'active' AND upper(name) LIKE 'M%'

Provider handled:
  status = 'active'

Residual local filter:
  upper(name) LIKE 'M%'
```

This ensures correctness even when only partial pushdown is possible.

---

### 10.12 Cancellation and Backpressure

Programmatic providers must support query cancellation and backpressure.

Requirements:

```text
providers should stop producing rows when the query is cancelled
batch streams should not eagerly materialize unbounded data
async providers should respect downstream demand
blocking providers should run outside the async runtime
```

---

### 10.13 Error Handling

Programmatic provider errors should be surfaced as typed execution errors.

Example categories:

```text
ProviderError
ProviderTimeoutError
ProviderSchemaError
ProviderCancelledError
ProviderPanicError
ProviderUnsupportedPushdownError
```

Errors should include:

```text
provider name
table name
query id
source language, where relevant
original error message
```

---

### 10.14 Security

Programmatic providers execute user/application code.

Security model:

```text
embedded mode assumes trusted application code
no sandboxing is required in MVP
future sandboxing may be considered for plugin use cases
```

If external plugins are supported later, the engine should define a separate plugin isolation model.

---

## 11. Pushdown Model

### 11.1 Pushdown Definition

Pushdown means translating part of a SQL query into the remote datasource’s native capabilities.

Example SQL:

```sql
SELECT id, name
FROM crm.customers
WHERE status = 'active'
LIMIT 100;
```

Possible REST request:

```http
GET /customers?status=active&limit=100&fields=id,name
```

---

### 11.2 Pushdown Types

The engine should support these pushdown categories:

```text
projection pushdown
filter pushdown
limit pushdown
offset pushdown
order-by pushdown
aggregate pushdown
join pushdown
```

MVP should support only:

```text
projection pushdown
simple filter pushdown
limit pushdown
pagination pushdown
programmatic provider projection/filter/limit awareness
```

---

### 11.3 Capability Matrix

Each datasource declares supported operations.

Example:

```yaml
capabilities:
  projection: true
  limit: true
  offset: true
  filters:
    eq: true
    in: true
    gt: true
    lt: true
    like: false
  order_by: false
  aggregate:
    count: false
    sum: false
  joins: false
```

---

### 11.4 REST Pushdown Config

```yaml
pushdown:
  filters:
    status:
      param: status
      operators: ["=", "IN"]

    created_at:
      operators:
        ">=": created_after
        "<": created_before

  limit:
    param: limit
    max: 500

  offset:
    param: offset

  projection:
    param: fields
    style: comma_separated
```

SQL:

```sql
SELECT id, name
FROM crm.customers
WHERE status = 'active'
  AND created_at >= TIMESTAMP '2026-01-01 00:00:00'
LIMIT 100;
```

HTTP:

```http
GET /customers?status=active&created_after=2026-01-01T00:00:00Z&limit=100&fields=id,name
```

---

### 11.5 Residual Filters

If a filter cannot be pushed down, it must be executed locally by DataFusion.

Example:

```sql
SELECT *
FROM crm.customers
WHERE upper(name) LIKE 'M%';
```

If REST API does not support `upper` or `LIKE`, then:

```text
remote: fetch rows
local: apply upper(name) LIKE 'M%'
```

The engine must always preserve correctness.

Pushdown is an optimization, not a semantic requirement.

---

## 12. Pagination

The REST provider must support multiple pagination styles.

### 12.1 Page-Based Pagination

```yaml
pagination:
  type: page
  page_param: page
  size_param: limit
  start_page: 1
```

Request:

```http
GET /customers?page=1&limit=100
GET /customers?page=2&limit=100
```

---

### 12.2 Offset-Based Pagination

```yaml
pagination:
  type: offset
  offset_param: offset
  limit_param: limit
```

Request:

```http
GET /customers?offset=0&limit=100
GET /customers?offset=100&limit=100
```

---

### 12.3 Cursor-Based Pagination

```yaml
pagination:
  type: cursor
  cursor_param: cursor
  cursor_path: "$.next_cursor"
```

Response:

```json
{
  "data": [],
  "next_cursor": "abc123"
}
```

Next request:

```http
GET /customers?cursor=abc123
```

---

### 12.4 Link Header Pagination

```yaml
pagination:
  type: link_header
  rel: next
```

Example header:

```http
Link: <https://api.example.com/customers?page=2>; rel="next"
```

---

## 13. Authentication

Supported authentication methods:

```text
none
basic
bearer token
api key header
api key query parameter
OAuth2 client credentials
custom header
```

Example:

```yaml
auth:
  type: bearer
  token_env: CRM_TOKEN
```

Example:

```yaml
auth:
  type: api_key
  in: header
  name: X-API-Key
  value_env: CRM_API_KEY
```

OAuth2 should not be in the MVP unless required.

---

## 14. Rate Limiting and Retry

The REST provider must support:

```text
retry with exponential backoff
respect for Retry-After header
per-source concurrency limits
request timeout
global timeout
max pages
max rows
circuit breaker
```

Example config:

```yaml
runtime:
  timeout_ms: 30000
  max_concurrency: 8
  max_pages: 1000
  retry:
    max_attempts: 3
    backoff_ms: 500
```

---

## 15. Async Runtime

The engine should use Tokio.

Principles:

1. HTTP I/O must be async.
2. Pagination should stream results incrementally.
3. Programmatic async providers should stream results incrementally.
4. JSON parsing and Arrow conversion should not block the async runtime heavily.
5. CPU-heavy work may use a separate blocking thread pool.
6. Backpressure must be respected through Arrow stream consumption.

Execution model:

```text
Tokio runtime:
  HTTP requests
  auth refresh
  rate-limit waits
  pagination
  async programmatic streams

Blocking pool:
  JSON parsing
  decompression
  complex flattening
  Arrow conversion
  blocking programmatic providers
```

---

## 16. Cross-Datasource Querying

The engine must support SQL across multiple registered datasources.

Example:

```sql
SELECT c.id, c.name, o.total
FROM crm.customers c
JOIN postgres.orders o ON o.customer_id = c.id
WHERE c.status = 'active';
```

Execution:

```text
REST provider:
  push WHERE c.status = 'active'

Postgres provider:
  push projection and possible filters

DataFusion:
  perform join locally
```

Programmatic providers must participate in cross-datasource queries.

Example:

```sql
SELECT c.id, c.name, o.total
FROM app.customers c
JOIN postgres.orders o ON o.customer_id = c.id;
```

---

## 17. Join Strategy

Initial version:

```text
All cross-source joins are executed locally by DataFusion.
```

Future versions may support:

```text
join pushdown within same datasource
semi-join reduction
dynamic filter pushdown
broadcast joins
cache-assisted joins
```

Example:

```sql
SELECT *
FROM pg.orders o
JOIN pg.customers c ON c.id = o.customer_id;
```

If both tables belong to the same PostgreSQL source, a future provider may push the full join to PostgreSQL.

But:

```sql
SELECT *
FROM crm.customers c
JOIN pg.orders o ON o.customer_id = c.id;
```

should normally execute the join locally.

---

## 18. N+1 Protection

REST endpoints with path parameters can create N+1 query patterns.

Example:

```http
GET /customers/{id}/orders
```

A join may accidentally cause:

```text
1 request to fetch customers
N requests to fetch orders per customer
```

The engine must include protections:

```yaml
n_plus_one:
  max_expanded_requests: 100
  require_explicit_enable: true
```

Possible behavior:

```text
fail query
warn user
use cache
batch requests if supported
```

---

## 19. Caching

REST APIs are often slow, rate-limited, or unstable.

The engine should provide optional cache modes.

### 19.1 Cache Modes

```text
none
memory
local_parquet
local_arrow
external_object_store
```

### 19.2 Cache Config

```yaml
cache:
  mode: local_parquet
  path: ./.restsql/cache
  ttl_seconds: 3600
```

### 19.3 Cache Semantics

Cache should support:

```text
TTL-based invalidation
manual refresh
query-aware cache keys
per-table cache policy
```

MVP may only support table-level cache.

---

## 20. Schema Drift

REST APIs may change response shape.

The engine should support:

```yaml
schema_inference:
  enabled: true
  allow_new_columns: false
  allow_type_widening: true
  include_raw_json: true
```

Recommended production behavior:

```text
do not silently add columns
do not silently change types
preserve raw JSON when configured
surface schema drift as warning or error
```

---

## 21. Data Types

Internal type system should use Arrow types.

Supported MVP types:

```text
boolean
int64
float64
decimal
utf8
date
timestamp
json/raw
list
struct
```

REST config types:

```yaml
type: int64
type: string
type: timestamp
type: boolean
type: float64
type: decimal
type: json
```

---

## 22. User-Facing APIs

### 22.1 Rust API

Example:

```rust
let engine = Engine::from_config_file("crm.yaml").await?;

let batches = engine
    .query("SELECT id, name FROM crm.customers LIMIT 10")
    .await?;
```

Registering a programmatic provider:

```rust
engine.register_provider(
    "app.customers",
    Arc::new(MyCustomersProvider::new()),
).await?;
```

---

### 22.2 Python API

Example:

```python
import dbfy

engine = dbfy.Engine.from_config("crm.yaml")

table = engine.query_arrow("""
    SELECT id, name
    FROM crm.customers
    WHERE status = 'active'
""")

df = table.to_pandas()
```

Registering a row provider:

```python
@engine.table("app.customers", schema={
    "id": "int64",
    "name": "string",
    "status": "string",
})
def customers():
    yield {"id": 1, "name": "Mario", "status": "active"}
```

---

### 22.3 C API

Example shape:

```c
restsql_engine_t* engine = restsql_engine_new_from_config("crm.yaml");

restsql_result_t* result = restsql_query(
    engine,
    "SELECT id, name FROM crm.customers LIMIT 10"
);

restsql_result_release(result);
restsql_engine_release(engine);
```

For tabular data exchange, the C API should support Arrow C Data Interface.

---

### 22.4 Java API

Example:

```java
Engine engine = Engine.fromConfig("crm.yaml");

ArrowReader reader = engine.query("""
    SELECT id, name
    FROM crm.customers
    LIMIT 10
""");
```

A JDBC facade may be considered later, but should not be required for MVP.

---

## 23. CLI

A CLI should exist for development and debugging.

Commands:

```bash
restsql infer
restsql validate
restsql query
restsql explain
restsql inspect
```

Examples:

```bash
restsql validate crm.yaml
```

```bash
restsql query -c crm.yaml \
  "SELECT id, name FROM crm.customers LIMIT 10"
```

```bash
restsql explain -c crm.yaml \
  "SELECT id FROM crm.customers WHERE status = 'active'"
```

The `explain` command should show:

```text
remote filters
local filters
remote projection
pagination
estimated requests
cache usage
provider pushdown
```

---

## 24. Explain Output

Example REST table:

```text
Table: crm.customers

Remote pushdown:
  projection: id, name
  filters:
    status = 'active'
  limit: 100

HTTP plan:
  GET /customers?status=active&limit=100&fields=id,name

Residual local filters:
  none

Pagination:
  cursor-based

Estimated requests:
  1+
```

Example programmatic provider:

```text
Table: app.customers

Provider pushdown offered:
  projection: id, name
  filters:
    status = 'active'
  limit: 100

Provider pushdown accepted:
  projection: id, name
  filters:
    status = 'active'

Residual local filters:
  none
```

---

## 25. Error Handling

Errors should be typed and structured.

Main error categories:

```text
ConfigError
AuthError
HttpError
RateLimitError
SchemaError
JsonDecodeError
PushdownError
ExecutionError
TimeoutError
UnsupportedQueryError
ProviderError
ProviderTimeoutError
ProviderSchemaError
ProviderCancelledError
ProviderPanicError
ProviderUnsupportedPushdownError
```

Error messages should include:

```text
source name
provider name, when relevant
table name
endpoint path, when relevant
HTTP status when relevant
request id when available
query id when available
suggested remediation when possible
```

---

## 26. Security

Security requirements:

1. Secrets must not be printed in logs.
2. Tokens should be read from environment variables or secret providers.
3. Config files should allow secret references, not only inline values.
4. Debug logs must redact headers such as:
   - Authorization;
   - X-API-Key;
   - Cookie.
5. TLS verification must be enabled by default.
6. Unsafe TLS mode must require explicit config.
7. Embedded programmatic providers are assumed to be trusted application code in the MVP.

Example:

```yaml
auth:
  type: bearer
  token_env: CRM_TOKEN
```

Avoid:

```yaml
auth:
  type: bearer
  token: hardcoded-secret
```

---

## 27. Logging and Observability

The engine should expose:

```text
request count
bytes downloaded
rows produced
pages fetched
pushdown ratio
cache hits/misses
remote latency
local execution time
provider execution time
provider rows produced
provider batches produced
```

Rust logging should use:

```text
tracing
```

Optional metrics interface:

```text
OpenTelemetry-compatible metrics
```

---

## 28. Testing Strategy

### 28.1 Unit Tests

Cover:

```text
config parsing
JSONPath extraction
type conversion
pushdown planning
pagination planning
auth header generation
schema inference
programmatic provider registration
row-to-Arrow conversion
provider capability handling
```

### 28.2 Integration Tests

Use mock HTTP servers.

Test:

```text
page pagination
cursor pagination
rate limits
HTTP errors
retry behavior
schema drift
nested JSON
arrays
programmatic provider errors
programmatic provider cancellation
```

### 28.3 Query Tests

Use SQL fixtures.

Example:

```sql
SELECT id, name
FROM crm.customers
WHERE status = 'active'
LIMIT 10;
```

Expected:

```text
correct HTTP request
correct pushed filter
correct returned rows
```

### 28.4 Cross-Source Tests

Use:

```text
REST mock source
programmatic provider source
local CSV/Parquet source
SQLite/Postgres test container, optional
```

---

## 29. Clean-Room Requirements

The project must not copy:

```text
CData code
CData internal behavior
CData proprietary docs
CData config formats
CData tests
CData error messages
reverse-engineered driver behavior
```

Allowed sources:

```text
public standards
public API documentation
Apache DataFusion documentation
Apache Arrow documentation
OpenAPI specification
SQL standards
independently written tests
```

Project positioning:

```text
Open source embedded SQL federation engine for REST/JSON APIs and heterogeneous datasources.
```

Avoid positioning:

```text
Open source CData clone.
```

---

## 30. Licensing

Recommended licenses:

```text
Apache-2.0
MIT
dual MIT/Apache-2.0
```

Given DataFusion and Arrow are Apache ecosystem projects, Apache-2.0 is a natural choice.

---

## 31. MVP Scope

### 31.1 MVP Must Have

1. Rust core.
2. DataFusion integration.
3. REST `TableProvider`.
4. Programmatic `TableProvider`.
5. YAML config.
6. Manual REST table mapping.
7. JSONPath column extraction.
8. Simple filter pushdown:
   - `=`;
   - `IN`;
   - `>`, `<`, `>=`, `<=` where configured.
9. Projection pushdown.
10. Limit pushdown.
11. Page or cursor pagination.
12. Async HTTP execution.
13. Arrow `RecordBatch` output.
14. Python binding.
15. Python row provider.
16. Python Arrow batch provider.
17. Rust row provider.
18. Rust Arrow batch provider.
19. CLI query execution.
20. Basic cross-source join with local CSV/Parquet.
21. Basic cross-source join involving a programmatic provider.

---

### 31.2 MVP Should Not Have

1. Full OpenAPI inference.
2. OAuth2 interactive flows.
3. Aggregate pushdown.
4. Join pushdown.
5. Write support.
6. ODBC driver.
7. Full JDBC driver.
8. Distributed execution.
9. Automatic N+1 optimization.
10. Advanced cost-based optimization.
11. C callback providers.
12. Java callback providers.
13. Sandboxed plugin execution.

---

## 32. Version Roadmap

### v0.1 — REST and Programmatic Provider MVP

Features:

```text
manual YAML config
REST GET endpoints
JSONPath extraction
projection pushdown
simple filter pushdown
limit pushdown
page/cursor pagination
CLI query
Python query_arrow API
Rust programmatic row provider
Rust programmatic batch provider
Python row provider
Python Arrow batch provider
programmatic provider participation in SQL queries
```

---

### v0.2 — Hybrid Discovery and Provider Pushdown

Features:

```text
OpenAPI import
sample JSON inference
generated config
config validation
schema drift detection
raw JSON column
async Python providers
pushdown-aware programmatic providers
provider explain output
provider cancellation
```

---

### v0.3 — Cross-Source Federation

Features:

```text
multiple datasources
REST + Parquet joins
REST + database joins
programmatic + REST joins
basic capability matrix
explain pushdown output
C callback provider API
Arrow C Data Interface support
Java provider prototype
```

---

### v0.4 — Cache and Performance

Features:

```text
local Parquet cache
TTL cache
rate limiting
retry policies
concurrency control
streaming JSON decode
provider-level metrics
```

---

### v0.5 — Multi-Language Embedding

Features:

```text
stable C ABI provider interface
stable Java provider interface
improved Python package
Arrow C Data Interface
stable public API
advanced pushdown reporting
```

---

### v1.0 — Production Baseline

Features:

```text
stable config format
stable Rust API
stable Python API
stable C ABI
documented provider interface
OpenAPI-driven generation
robust error handling
observability
security hardening
```

---

## 33. Open Questions

1. Should the initial public API expose DataFusion directly or wrap it fully?
2. Should the config format be YAML-only or YAML/JSON/TOML?
3. Should OpenAPI inference be part of core or a separate optional crate?
4. Should database providers be built in or delegated to existing DataFusion providers?
5. Should cache be table-level only in v1?
6. Should Java support use JNI, JNA, or Arrow Flight-style embedded APIs?
7. Should there eventually be a JDBC driver facade?
8. How much SQL compatibility should be guaranteed beyond DataFusion’s SQL dialect?
9. Should REST write operations be supported later as `INSERT`, `UPDATE`, `DELETE`?
10. Should GraphQL and OData become first-class providers?
11. Should programmatic providers be allowed to return lazy streams only, or also eager in-memory tables?
12. How should provider pushdown acceptance be represented in the DataFusion physical plan?
13. Should provider code be sandboxed in future versions?

---

## 34. Recommended Initial Repository Layout

```text
restsql/
  Cargo.toml
  README.md
  LICENSE
  crates/
    dbfy-frontend-datafusion/
    dbfy-config/
    dbfy-provider-rest/
    dbfy-provider/
    restsql-openapi/
    restsql-federation/
    dbfy-cache/
    dbfy-cli/
    dbfy-python/
    dbfy-capi/
    restsql-java/
  examples/
    crm-rest/
    programmatic-provider/
    rest-plus-parquet/
    rest-plus-postgres/
    rest-plus-programmatic/
  tests/
    fixtures/
      openapi/
      configs/
      responses/
      sql/
  docs/
    architecture.md
    config-reference.md
    rest-provider.md
    programmatic-provider.md
    pushdown.md
    bindings.md
```

---

## 35. Example End-to-End Config

```yaml
version: 1

sources:
  crm:
    type: rest
    base_url: https://api.example.com

    auth:
      type: bearer
      token_env: CRM_TOKEN

    runtime:
      timeout_ms: 30000
      max_concurrency: 4
      retry:
        max_attempts: 3
        backoff_ms: 500

    tables:
      customers:
        endpoint:
          method: GET
          path: /customers

        root: "$.data[*]"
        primary_key: id
        include_raw_json: true

        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
          status:
            path: "$.status"
            type: string
          country:
            path: "$.address.country"
            type: string
          created_at:
            path: "$.created_at"
            type: timestamp

        pagination:
          type: cursor
          cursor_param: cursor
          cursor_path: "$.next_cursor"

        pushdown:
          filters:
            status:
              param: status
              operators: ["=", "IN"]
            country:
              param: country
              operators: ["="]
            created_at:
              operators:
                ">=": created_after
                "<": created_before

          limit:
            param: limit
            max: 500

          projection:
            param: fields
            style: comma_separated
```

---

## 36. Example Query

```sql
SELECT id, name, country
FROM crm.customers
WHERE status = 'active'
  AND country = 'IT'
LIMIT 100;
```

Generated HTTP request:

```http
GET /customers?status=active&country=IT&limit=100&fields=id,name,country
```

Local execution:

```text
none, if all predicates are pushed down
```

If the query is:

```sql
SELECT id, name
FROM crm.customers
WHERE upper(name) LIKE 'M%';
```

Then:

```text
remote:
  fetch configured table data

local:
  apply upper(name) LIKE 'M%'
```

---

## 37. Example Programmatic Provider Query

Provider:

```python
@engine.table("app.customers", schema={
    "id": "int64",
    "name": "string",
    "status": "string",
})
def customers():
    yield {"id": 1, "name": "Mario", "status": "active"}
    yield {"id": 2, "name": "Anna", "status": "inactive"}
```

Query:

```sql
SELECT id, name
FROM app.customers
WHERE status = 'active';
```

Execution:

```text
provider yields rows
engine buffers rows into Arrow RecordBatch
DataFusion applies local filter
result returned as Arrow batches
```

---

## 38. Design Principle

The system must always prioritize correctness over pushdown.

Therefore:

```text
If an operation can be safely pushed down:
  push it down.

If an operation cannot be safely pushed down:
  execute it locally.

If correctness cannot be guaranteed:
  fail with an explicit error.
```

---

## 39. Final Direction

The recommended technical stack is:

```text
Language:
  Rust

Async runtime:
  Tokio

SQL engine:
  Apache DataFusion

Columnar format:
  Apache Arrow

REST client:
  reqwest or hyper

Config:
  YAML/JSON via serde

Python binding:
  PyO3 + maturin

C integration:
  C ABI + Arrow C Data Interface

Java integration:
  JNI/JNA + Arrow Java
```

The core product identity should be:

```text
An embeddable Rust SQL federation engine for REST/JSON, programmatic providers, and heterogeneous datasources.
```

Not:

```text
A CData clone.
```
