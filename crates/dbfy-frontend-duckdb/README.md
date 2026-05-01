# dbfy-frontend-duckdb

DuckDB extension frontend for `dbfy`. Reuses `dbfy-provider-rest` and
`dbfy-provider-rows-file` from the shared core; provides two table functions
DuckDB consumers can query directly:

```sql
-- HTTP/JSON sources:
SELECT * FROM dbfy_rest('https://api.example.com/users', config := '...');
-- line-delimited file sources (jsonl/csv/logfmt/regex/syslog):
SELECT * FROM dbfy_rows_file('/var/log/app/*.log',       config := '...');
```

Both support typed columns, projection pushdown, and (for rows-file) the
full L3 indexing story: zone maps + bloom filters + incremental EXTEND.

## Demo

The full annotated demo is in
[`examples/sensor_analytics.py`](examples/sensor_analytics.py): two REST
endpoints served by the Python stdlib HTTP server plus a synthetic JSONL
log of fleet-operations events, the dbfy extension loaded into vanilla
`pip install duckdb`, and six progressively richer SQL patterns. Run it
with:

```bash
cargo build -p dbfy-frontend-duckdb --features loadable_extension --release --jobs 1
strip target/release/libdbfy_duckdb.so
python3 crates/dbfy-frontend-duckdb/scripts/append_metadata.py \
    target/release/libdbfy_duckdb.so \
    --output target/release/dbfy.duckdb_extension --duckdb-capi-version v1.2.0

.venv/bin/python crates/dbfy-frontend-duckdb/examples/sensor_analytics.py
```

### (b) Cross-source JOIN — two REST endpoints in one query

```sql
SELECT
    s.zone,
    count(*)                     AS samples,
    round(avg(r.temperature), 2) AS avg_temp,
    max(r.humidity)              AS peak_humidity
FROM dbfy_rest(?, config := ?) r
JOIN dbfy_rest(?, config := ?) s USING (sensor_id)
GROUP BY s.zone
ORDER BY avg_temp DESC;
```

```
zone            samples   avg_temp   peak_hum
warehouse-b         120      23.06         75
warehouse-a         120      23.01         75
loading-dock        120      22.93         75
cold-room           120       19.1         75   ← cold room visibly cooler
```

A single SQL query does what a `requests + json + pandas + merge` pipeline
would express in ~30 imperative lines.

### (c) File federation — same SQL story for line-delimited sources

```sql
SELECT kind, count(*) AS n, round(avg(duration_ms), 1) AS avg_ms
FROM dbfy_rows_file(?, config := ?)
GROUP BY kind
ORDER BY n DESC;
```

```
kind                n    avg_ms
restart           500    2513.8
deploy            500    2567.4
alarm_acked       500    2436.7
calibration       500    2427.1
```

The `config` argument is the same YAML schema as the standalone CLI — you
declare a `parser` (jsonl/csv/logfmt/regex/syslog) and an optional
`indexed_columns` list. The L3 sidecar (`<file>.dbfy_idx`) is built on
first scan and reused on every subsequent query.

### (d) The killer query — REST × REST × FILE in a single SELECT

```sql
SELECT
    s.zone,
    count(*)                     AS hot_acked_events,
    round(avg(r.temperature), 2) AS avg_temp_at_event
FROM dbfy_rest(?, config := ?) r
JOIN dbfy_rest(?, config := ?) s USING (sensor_id)
JOIN dbfy_rows_file(?, config := ?) e
  ON e.sensor_id = r.sensor_id
 AND e.kind = 'alarm_acked'
 AND abs(epoch(e.ts) - epoch(r.timestamp)) <= 90
WHERE r.temperature > 22
GROUP BY s.zone
ORDER BY hot_acked_events DESC;
```

```
zone            hot_acked   avg_temp
loading-dock           16      24.18
warehouse-a            12      24.24
warehouse-b            12      23.88
```

Three sources — two REST endpoints and a JSONL file — joined in one
declarative SELECT. The pre-dbfy version of this query would have been
three scripts, two CSV intermediates, and a manual `pd.merge`.

### (e) Snapshot once, query at native speed

```sql
CREATE TABLE readings_local AS SELECT * FROM dbfy_rest(?,        config := ?);
CREATE TABLE sensors_local  AS SELECT * FROM dbfy_rest(?,        config := ?);
CREATE TABLE events_local   AS SELECT * FROM dbfy_rows_file(?,   config := ?);
```

Subsequent queries hit the local columnar store: in the demo, **20× the
same 3-way JOIN runs in 71.7 ms total (3.59 ms/query, zero HTTP, zero
file IO)**.

### (f) Window function + Parquet export

```sql
WITH delta AS (
    SELECT
        sensor_id, timestamp, temperature,
        temperature - avg(temperature) OVER (PARTITION BY sensor_id) AS deviation
    FROM readings_local
)
SELECT sensor_id, timestamp, temperature, round(deviation, 2) AS dev
FROM delta
WHERE abs(deviation) > 3
ORDER BY abs(deviation) DESC LIMIT 5;

COPY readings_local TO 'sensor_demo.parquet' (FORMAT 'parquet');
```

```
sensor_id    timestamp                temp    dev
sensor-028   2026-05-01 04:19:38     25.98   3.36
sensor-034   2026-05-01 03:54:38     16.22  -3.31
sensor-019   2026-05-01 04:39:38     20.15  -3.28
```

Window functions, aggregates, joins, and Parquet output all come from
DuckDB; dbfy only provides the table-shaped REST surface.

## Build modes

The crate is in the workspace by default but **does not pull DuckDB** unless
a feature is enabled. The two modes are mutually exclusive in practice:

| Command | Mode | Output |
|---|---|---|
| `cargo check -p dbfy-frontend-duckdb` | scaffold only | nothing — fast |
| `cargo check -p dbfy-frontend-duckdb --features duckdb` | embedded DuckDB (via `bundled`) | static link, ~30 min first compile, ~2.5 GB peak RAM |
| `cargo test  -p dbfy-frontend-duckdb --features duckdb --jobs 1` | embedded DuckDB | integration tests using `duckdb::Connection` directly |
| `cargo build -p dbfy-frontend-duckdb --features loadable_extension --release --jobs 1` | loadable extension | header-only build, ~10 min, **~6.5 MB cdylib** stripped |

**`duckdb` vs `loadable_extension` features**: cargo features are additive,
so we use the conditional `duckdb?/X` syntax to keep them disjoint. Without
the `?`, `duckdb/bundled` would silently activate when the user picked
`loadable_extension`, dragging a static DuckDB copy (~190 MB) into the
loadable cdylib — exactly the wrong outcome.

The `--jobs 1` cap reduces peak memory pressure during the bundled compile;
remove it on machines with plenty of RAM.

## Producing the loadable `.duckdb_extension`

DuckDB requires a 512-byte metadata + signature footer at the end of any file
loaded via `LOAD '...'`. The Cargo cdylib alone is not loadable; append the
footer with the bundled script:

```bash
cargo build -p dbfy-frontend-duckdb --features loadable_extension --release --jobs 1
strip target/release/libdbfy_duckdb.so

python3 crates/dbfy-frontend-duckdb/scripts/append_metadata.py \
    target/release/libdbfy_duckdb.so \
    --output target/release/dbfy.duckdb_extension \
    --duckdb-capi-version v1.2.0 \
    --platform linux_amd64
```

`v1.2.0` is the DuckDB C API version compatible with DuckDB 1.5.x hosts. Adjust
`--platform` for cross-builds (`osx_arm64`, `windows_amd64`, etc.).

The release+strip combination produces a `~6.5 MB` extension. The debug
profile build is `~190 MB` because of debug info, not because DuckDB is
linked in (`size` confirms `text` is ~6 MB).

The resulting file loads into any compatible DuckDB:

```python
import duckdb
con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'target/debug/dbfy.duckdb_extension'")
con.execute("SELECT count(*) FROM dbfy_rest('https://api.example.com/users')").fetchall()
```

Set `allow_unsigned_extensions=true` because the script appends an empty
signature; production community-extension distribution requires DuckDB-side
signing via the official `extension-ci-tools` repository.

## Usage from Rust

```rust
use duckdb::Connection;

let conn = Connection::open_in_memory()?;
dbfy_duckdb::register(&conn)?;

let mut stmt = conn.prepare(
    "SELECT value FROM dbfy_rest('https://api.example.com/users')",
)?;
for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
    println!("{}", row?);
}
```

## Current capabilities (v1)

- `dbfy_rest(url)` table function — quick exploration: GET the URL,
  extract array at `$.data[*]`, return one VARCHAR row per element with
  the raw JSON.
- `dbfy_rest(url, config := '<YAML or JSON>')` — typed columns,
  pagination, retry, JSONPath (RFC 9535), and auth flow through the
  shared `dbfy-provider-rest` machinery. The config schema is the same
  one used by the DataFusion frontend's YAML config:

  ```yaml
  root: $.data[*]
  columns:
    id:     {path: "$.id",     type: int64}
    name:   {path: "$.name",   type: string}
    score:  {path: "$.score",  type: float64}
  pagination:
    type: cursor
    cursor_param: cursor
    cursor_path: "$.next_cursor"
  ```

- Supported column types map to DuckDB logical types:
  `boolean → BOOLEAN`, `int64 → BIGINT`, `float64 → DOUBLE`,
  `string|json → VARCHAR`, `date → DATE`, `timestamp → TIMESTAMP`.
  `decimal`/`list`/`struct` fall back to `VARCHAR` (typed Arrow support
  pending in the core).
- Materialises the full result in `init()` before serving DuckDB chunks
  (one batch from `RestTable` becomes one chunk to DuckDB). Pagination
  and retry happen inside `dbfy-provider-rest` so the materialisation
  itself is streaming at the HTTP boundary.

## Pushdown

| Pushdown | Status |
|---|---|
| **Projection** | ✅ enabled — `SELECT name, id FROM dbfy_rest(...)` only parses those JSON paths via `dbfy-provider-rest`. DuckDB's `InitInfo::get_column_indices()` is read in `init` and translated to the provider's projection list. |
| **Filter** | ❌ blocked upstream — the public C ABI of `libduckdb-sys 1.10502` doesn't expose `TableFilterSet`. The C++ engine has it; once the Rust wrapper picks it up, our `dbfy_provider::SimpleFilter` plumbing already exists in `RestTable::plan_request`. |
| **Limit**  | ❌ not yet wired (deferred). |

## Planned (v2)

- `ATTACH 'config.yaml' AS api (TYPE dbfy)` — catalog provider that
  exposes every table declared in a `dbfy-config` YAML file as a DuckDB
  table reachable from SQL.
- Loadable `.duckdb_extension` build for distribution via DuckDB's
  community extension registry.
- Filter pushdown when `libduckdb-sys` exposes `TableFilterSet`.
- `Decimal/List/Struct` Arrow types beyond the current VARCHAR fallback.
