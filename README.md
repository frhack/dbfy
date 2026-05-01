# dbfy

Embedded SQL federation engine for REST/JSON datasources, local files, and programmatic providers.

## Current status

This repository currently contains an executable MVP slice:

- Rust workspace aligned with the specification.
- YAML configuration model with fail-fast validation, including RFC 9535 JSONPath syntax checks for root, column, and cursor paths.
- REST table planning and executable `GET` runtime with page, offset, cursor, and link-header pagination.
- Programmatic provider traits and scan request/capability types.
- A reusable static programmatic provider for Arrow `RecordBatch` data.
- Core engine with DataFusion-backed SQL execution for programmatic and REST providers.
- Streaming execution for both programmatic and REST providers through native DataFusion batch streams; pages flow incrementally and `LIMIT` can interrupt pagination before later pages are fetched.
- Full RFC 9535 JSONPath support for root and column extraction (array indexing, slices, filters, recursive descent).
- Query `explain` output for optimized plans, pushdown visibility, and REST request previews.
- Minimal CLI with `validate`, `inspect`, `query`, and `explain`.
- Python bindings (PyO3 + maturin) with sync and `asyncio`-compatible APIs, distributed type stubs (PEP 561), and Python-defined programmatic providers; returns PyArrow `RecordBatch` objects with zero-copy via the Arrow C Data Interface.
- C bindings (cbindgen) exposing engine lifecycle, query, explain, and Arrow C Data Interface batch export through `libdbfy.{a,so}` and a generated `dbfy.h`.
- Java bindings scaffolding (`jni` crate + `com.dbfy.Dbfy`) that returns query results as Arrow IPC stream bytes for `ArrowStreamReader` (JDK required only for the Java-side compile, not the Rust build).

Language bindings and advanced REST features are planned in the next milestones. At the moment, REST execution covers `GET` tables with RFC 9535 JSONPath extraction, page/offset/cursor/link-header pagination, basic transient retry, and typed timeout/rate-limit/status errors; richer operational controls and broader Arrow type coverage (`Decimal/List/Struct`) remain pending. The product-facing slice is now broader: the CLI can explain query plans, programmatic providers can be backed by in-memory Arrow batches or custom chunked readers, and the core consumes both programmatic and REST batch streams incrementally instead of materializing them all up front.

## Commands

```bash
cargo run -p dbfy-cli -- validate dbfy_spec.md
```

The example above intentionally fails because `dbfy_spec.md` is not a config file; use it only to verify the CLI wiring.

```bash
cargo run -p dbfy-frontend-datafusion --example programmatic_csv
```

The example implements a small chunked CSV provider on top of the programmatic provider layer, reads `crates/dbfy-frontend-datafusion/examples/data/customers.csv` row by row, prints the optimized `explain` output, and then runs the SQL query.

## Layout

```text
crates/
  dbfy-c/                       # C bindings via cbindgen
  dbfy-cli/
  dbfy-config/
  dbfy-frontend-datafusion/     # standalone engine (DataFusion)
  dbfy-frontend-duckdb/         # DuckDB community extension (scaffold)
  dbfy-jni/                     # Java bindings via the jni crate
  dbfy-provider/                # Provider trait + shared types
  dbfy-provider-rest/           # REST provider impl
  dbfy-provider-static/         # In-memory RecordBatch provider
  dbfy-py/                      # Python bindings via PyO3 + maturin
docs/
  implementation-plan.md
```

## Python

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install maturin pyarrow
cd crates/dbfy-py && maturin develop
python tests/smoke.py
```

See `crates/dbfy-py/README.md` for the full Python API.

## DuckDB extension (preview)

The `dbfy-frontend-duckdb` crate exposes a `dbfy_rest()` table function
to embedded DuckDB. The build is gated behind a cargo feature so the
default workspace build stays fast:

```bash
cargo check -p dbfy-frontend-duckdb                    # default scaffold (fast)
cargo check -p dbfy-frontend-duckdb --features duckdb  # bundled DuckDB C++ (~10 min cold)
cargo test  -p dbfy-frontend-duckdb --features duckdb --jobs 1
```

```rust
let conn = duckdb::Connection::open_in_memory()?;
dbfy_duckdb::register(&conn)?;
let mut stmt = conn.prepare(
    "SELECT value FROM dbfy_rest('https://api.example.com/users')",
)?;
```

See `crates/dbfy-frontend-duckdb/README.md` for the current capabilities
and the planned v1 surface (typed columns, filter pushdown, ATTACH).
