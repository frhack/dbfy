# dbfy

[![tests](https://github.com/frhack/dbfy/actions/workflows/test.yml/badge.svg)](https://github.com/frhack/dbfy/actions/workflows/test.yml)
[![release](https://img.shields.io/github/v/release/frhack/dbfy?include_prereleases)](https://github.com/frhack/dbfy/releases)
[![pypi](https://img.shields.io/pypi/v/dbfy.svg)](https://pypi.org/project/dbfy/)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Everything\* is a SQL table.** APIs, logs, files, your CSV scratch dir —
joined declaratively in one SELECT.

<sub>\*If yours isn't yet, file a bug — that's the contract.</sub>

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
→ **[Showcase](examples/showcase/)** — end-to-end demo + integration test against the live GitHub API + 50k JSONL + 5×syslog.

## Install

**Python**:
```bash
pip install dbfy
```

**CLI binary** (Linux x86_64, macOS arm64) — download from [Releases](https://github.com/frhack/dbfy/releases) and add to `PATH`.

**DuckDB extension** — also on the Releases page (`dbfy-<target>.duckdb_extension`); load with:
```sql
LOAD 'path/to/dbfy.duckdb_extension';
```

**From source** (Rust 1.85+):
```bash
git clone https://github.com/frhack/dbfy && cd dbfy
cargo build --release -p dbfy-cli
```

## Supported languages and modes

### Modes

| Mode | Install | Use |
|---|---|---|
| **CLI binary** | [Releases](https://github.com/frhack/dbfy/releases) (Linux x86_64, macOS arm64) | `dbfy query --config x.yaml "SELECT …"` |
| **DuckDB extension** | [Releases](https://github.com/frhack/dbfy/releases) → `LOAD 'dbfy.duckdb_extension'` | `SELECT * FROM dbfy_rest('…')` / `dbfy_rows_file('…')` |
| **Python** | `pip install dbfy` | `import dbfy; engine = dbfy.Engine.from_yaml(…)` (sync + `asyncio`, PyArrow zero-copy) |
| **Rust library** | path / git dep on `dbfy-frontend-datafusion` | `use dbfy_frontend_datafusion::Engine;` |
| **C** | `libdbfy.{a,so}` + `dbfy.h` (cbindgen-generated) | engine lifecycle + Arrow C Data Interface |
| **Java / Kotlin** | `dbfy-jni` + `com.dbfy.Dbfy` | Arrow IPC bytes → `ArrowStreamReader` |

### Sources

| Kind | Status | Coverage |
|---|---|---|
| **REST / HTTP** | ✅ stable | page / offset / cursor / link-header pagination · 4 auth modes · retry + `Retry-After` · in-memory TTL + singleflight HTTP cache · filter pushdown into URL params |
| **Line-delimited files** | ✅ stable | JSONL · CSV · logfmt · syslog (RFC 5424) · regex (CLF / ELF) — with L3 indexing (zone maps + bloom + incremental EXTEND on append) and glob multi-file expansion |
| **In-memory programmatic** | ✅ stable | static Arrow `RecordBatch` provider · Python-defined custom providers via Arrow C Data Interface |
| **Coming next** | 🟡 wishlist — file a bug to vote | GraphQL · gRPC · Parquet remote · XML · LDAP · HTML / DOM scraping |

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
examples/
  showcase/                     # end-to-end demo + validator
```

## Documentation

- **[Quickstart](docs/quickstart.md)** — install, build, first query (CLI + DuckDB).
- **[Showcase](examples/showcase/)** — runnable end-to-end demo: GitHub API × 50k JSONL × 5×syslog joined in one SELECT, with timing + skip-ratio numbers.
- [Implementation plan](docs/implementation-plan.md) — milestone-by-milestone history.
- [DuckDB extension reference](crates/dbfy-frontend-duckdb/README.md) — table-function syntax, demo, capabilities matrix.
- [Python bindings](crates/dbfy-py/README.md) — `maturin develop` wheel + API.
- [restsql_spec.md](restsql_spec.md) — original product spec.

## License

Apache-2.0.
