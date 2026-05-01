# dbfy

[![tests](https://github.com/frhack/dbfy/actions/workflows/test.yml/badge.svg)](https://github.com/frhack/dbfy/actions/workflows/test.yml)
[![release](https://img.shields.io/github/v/release/frhack/dbfy?include_prereleases)](https://github.com/frhack/dbfy/releases)
[![pypi](https://img.shields.io/pypi/v/dbfy.svg)](https://pypi.org/project/dbfy/)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Embedded SQL federation engine.** Bring REST APIs and line-delimited log
files (jsonl, csv, logfmt, syslog, regex) to the same SQL surface as your
Parquet, CSV, and in-memory data — no glue scripts, no `pd.merge`.

```sql
-- One SELECT joining two REST endpoints and a local JSONL log:
SELECT s.zone, count(*) AS hot_acked
  FROM dbfy_rest('https://api.example.com/readings', config := '...') r
  JOIN dbfy_rest('https://api.example.com/sensors',  config := '...') s USING (sensor_id)
  JOIN dbfy_rows_file('/var/log/fleet.jsonl',        config := '...') e
    ON e.sensor_id = r.sensor_id AND e.kind = 'alarm_acked'
 WHERE r.temperature > 22
 GROUP BY s.zone;
```

→ **[Quickstart](docs/quickstart.md)** — five minutes, zero to your first SQL query.

## Install

**Python**:
```bash
pip install dbfy
```

**CLI binary** (Linux x86_64, macOS x86_64/arm64) — download from [Releases](https://github.com/frhack/dbfy/releases) and add to `PATH`.

**DuckDB extension** — also on the Releases page (`dbfy-<target>.duckdb_extension`); load with:
```sql
LOAD 'path/to/dbfy.duckdb_extension';
```

**From source** (Rust 1.85+):
```bash
git clone https://github.com/frhack/dbfy && cd dbfy
cargo build --release -p dbfy-cli
```

## What it does

- **REST/HTTP** — GET endpoints with page / offset / cursor / link-header
  pagination, retry + `Retry-After`, four auth modes, RFC 9535 JSONPath,
  filter + projection + limit pushdown, in-memory TTL+singleflight HTTP
  cache.
- **Line-delimited files** — jsonl, csv, logfmt, regex (CLF/ELF), syslog
  (RFC 5424). L3 indexing via persistent `.dbfy_idx` sidecar: zone maps,
  bloom filters, prefix-hash invalidation, incremental EXTEND on
  append-only growth, glob multi-file expansion.
- **Two SQL frontends share one core**:
  - **Standalone CLI** (`dbfy validate / inspect / explain / query`) backed
    by a DataFusion engine, with onboarding helpers (`dbfy detect`,
    `dbfy probe`, `dbfy init`, `dbfy index`, `dbfy duckdb-attach`).
  - **DuckDB extension** — `LOAD 'dbfy.duckdb_extension'` exposes
    `dbfy_rest()` and `dbfy_rows_file()` table functions to any DuckDB
    host (Python, R, JS, JVM, CLI).
- **Language bindings** — Python (sync + `asyncio`, PyArrow zero-copy via
  the Arrow C Data Interface, type stubs), C (`cbindgen`), Java
  (`jni` + Arrow IPC).

## Layout

```text
crates/
  dbfy-config/                  # YAML schema + validation
  dbfy-provider/                # ProgrammaticTableProvider trait
  dbfy-provider-rest/           # HTTP/JSON source + cache
  dbfy-provider-rows-file/      # indexed file source
  dbfy-provider-static/         # in-memory RecordBatch provider
  dbfy-frontend-datafusion/     # standalone SQL engine
  dbfy-frontend-duckdb/         # `.duckdb_extension`
  dbfy-cli/ dbfy-py/ dbfy-c/ dbfy-jni/   # language surfaces
docs/
  quickstart.md
  implementation-plan.md
```

## Documentation

- **[Quickstart](docs/quickstart.md)** — install, build, first query (CLI + DuckDB).
- [Implementation plan](docs/implementation-plan.md) — milestone-by-milestone history.
- [DuckDB extension reference](crates/dbfy-frontend-duckdb/README.md) — table-function syntax, demo, capabilities matrix.
- [Python bindings](crates/dbfy-py/README.md) — `maturin develop` wheel + API.
- [restsql_spec.md](restsql_spec.md) — original product spec.

## License

Apache-2.0.
