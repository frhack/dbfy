# Implementation Plan

## Scope

The specification describes a multi-phase system. This repository bootstrap targets the first practical slice of the MVP so the codebase can evolve without rework.

## Plan and status

- [x] Define MVP bootstrap scope and capture implementation status in-repo.
- [x] Create Rust workspace and crate layout for config, core, REST provider, programmatic provider, and CLI.
- [x] Implement YAML configuration parsing and baseline validation.
- [x] Implement REST request planning skeleton for projection, filter, and limit pushdown.
- [x] Implement programmatic provider contracts and shared scan/capability types.
- [x] Implement core engine shell for config loading and provider registration.
- [x] Add minimal CLI for config validation and engine inspection.
- [ ] Integrate DataFusion `SessionContext` and executable `TableProvider` implementations.
- [ ] Add async HTTP execution, pagination loops, and Arrow `RecordBatch` streams.
- [ ] Add Python bindings and provider registration APIs.
- [ ] Add integration tests with mock HTTP servers and cross-source queries.

## Notes

- Current code is intentionally biased toward stable interfaces and internal boundaries.
- Query execution is being activated incrementally, starting with programmatic providers.
- The next increment after this milestone should focus on turning the REST planning layer into a real async `TableProvider`.

## Milestone 2

- [x] Define and document the DataFusion milestone scope.
- [x] Integrate DataFusion `SessionContext` into the core engine.
- [x] Implement an executable DataFusion `TableProvider` adapter for programmatic providers.
- [x] Make `Engine::query` return collected Arrow `RecordBatch` results.
- [x] Update CLI and tests to exercise real SQL execution.

## Current milestone result

- Programmatic providers can now participate in SQL execution through DataFusion.
- Qualified names such as `app.customers` are registered using DataFusion schemas.
- CLI `query` now prints actual result batches.
- REST tables are registered with schema metadata but still fail explicitly at execution time until the async HTTP layer is implemented.

## Milestone 3

- [x] Define and document the REST runtime milestone scope.
- [x] Add async HTTP execution, JSON decoding, and Arrow batch conversion for REST `GET` tables.
- [x] Integrate executable REST tables into the core DataFusion path.
- [x] Support an MVP page-based pagination strategy with mock-server coverage.
- [x] Update CLI and tests to verify REST SQL execution end-to-end.

## Current REST runtime result

- REST tables now execute through DataFusion instead of failing as placeholders.
- Supported in this milestone:
  `GET` endpoints, simple root extraction such as `$.data[*]`, scalar column paths such as `$.id` or `$.address.country`, projection/filter/limit pushdown, and page-based pagination.
- Still pending:
  cursor/offset/link-header pagination, retries/rate limiting, richer JSONPath support, and broader Arrow type coverage such as decimal/list/struct.

## Milestone 4

- [x] Define and document the pagination+retry milestone scope.
- [x] Extend the REST provider with offset-based and cursor-based pagination.
- [x] Add a first retry policy for transient HTTP failures.
- [x] Add end-to-end coverage for the new REST modes in provider and core tests.
- [x] Update status/documentation and verify the full workspace.

## Current pagination+retry result

- REST runtime now supports:
  page-based, offset-based, and cursor-based pagination;
  transient retry for `429` and `5xx` responses;
  `Retry-After` handling for simple numeric values;
  end-to-end SQL execution through DataFusion on cursor-paginated sources.
- Still pending:
  link-header pagination, per-source concurrency control, circuit breaking, richer retry policies, advanced JSONPath support, and broader Arrow type coverage.

## Milestone 5

- [x] Define and document the link-header+error-typing milestone scope.
- [x] Extend the REST provider with `link_header` pagination.
- [x] Introduce more typed timeout, rate-limit, and HTTP status errors.
- [x] Add provider/core coverage for link-header pagination and the new error classes.
- [x] Update status/documentation and verify the full workspace.

## Current link-header+error-typing result

- REST runtime now supports:
  page-based, offset-based, cursor-based, and link-header pagination;
  typed timeout errors;
  typed rate-limit errors with parsed numeric `Retry-After` when available;
  typed non-success HTTP status errors for final failed responses.
- Still pending:
  per-source concurrency control, circuit breaking, richer retry policies, advanced JSONPath support, and broader Arrow type coverage.

## Milestone 6

- [x] Define and document the explain+programmatic CSV milestone scope.
- [x] Add `Engine::explain` with optimized logical-plan rendering and per-table pushdown summaries.
- [x] Extend the CLI with an `explain` command for SQL inspection.
- [x] Add a reusable static programmatic provider for in-memory Arrow `RecordBatch` data.
- [x] Add a CSV-backed example on top of the programmatic provider layer.
- [x] Add tests covering the static provider and programmatic `explain` output.
- [x] Re-run REST/core integration tests outside the sandbox to complete verification.

## Current explain+programmatic CSV result

- Product-facing query introspection now exists:
  CLI users can inspect the optimized logical plan, table-level pushdown, residual filters, and REST request previews before execution.
- Programmatic providers are now easier to adopt:
  a reusable static `RecordBatch` provider can back demos, CSV loaders, SDK wrappers, or other in-process data sources without writing a custom async stream each time.
- The repository now includes a concrete CSV example built through the programmatic layer rather than a dedicated CSV connector, which keeps the architecture aligned with the extensibility model.
- The CSV example now models a more realistic integration path:
  it reads rows incrementally, applies simple pushdown, and flushes chunked `RecordBatch` values instead of materializing the whole file into a single batch upfront.
- Verification status:
  unit tests for `dbfy-provider` are green;
  the programmatic CSV example runs successfully;
  the full `cargo test` suite is green, including REST/core tests that spin up `wiremock` mock servers.

## Milestone 7

- [x] Define the streaming execution milestone for programmatic providers.
- [x] Replace eager `try_collect + MemTable` consumption with a custom DataFusion `ExecutionPlan`.
- [x] Preserve projection and limit enforcement incrementally while batches flow through the stream.
- [x] Add a regression test proving that `LIMIT` can stop consumption before later provider errors.
- [x] Re-run the example and the full workspace test suite.

## Current streaming execution result

- Programmatic providers now execute through a native DataFusion stream path instead of being fully materialized into memory before query execution continues.
- This removes the main architectural mismatch in the programmatic path:
  providers can emit `RecordBatch` values incrementally and the core can stop polling upstream as soon as downstream operators such as `LIMIT` have enough rows.
- Compatibility with existing providers is preserved:
  the core still normalizes per-batch projection and still enforces `limit` defensively even if a provider ignores it.
- Regression coverage now includes a provider that emits one valid batch and then an error; with `LIMIT 1`, the query succeeds because the later error is never consumed.

## Milestone 8

- [x] Define and document the REST streaming + JSONPath SOTA milestone.
- [x] Replace eager `MemTable`-backed REST scan with a native streaming `RestStreamExecutionPlan`.
- [x] Drive REST pagination page-by-page through a bounded `mpsc` channel; emit one `RecordBatch` per fetched page.
- [x] Replace the ad-hoc JSONPath parser with `serde_json_path` (RFC 9535) for root, column, and cursor paths.
- [x] Validate JSONPath expressions at config-load time so misconfigurations fail fast in `Config::from_yaml_str`.
- [x] Add regression coverage for array-index column paths and invalid-path config rejection.
- [x] Re-run the full workspace test suite and the `programmatic_csv` example.

## Current REST streaming + JSONPath result

- REST tables now stream through DataFusion batch by batch:
  the producer task fetches one page, converts rows to a `RecordBatch`, sends it through a bounded channel, and only then fetches the next page;
  if `LIMIT` is satisfied or the consumer drops, the producer stops without fetching further pages.
- Root, column, and cursor JSONPath expressions follow RFC 9535:
  array indexing (`$.tags[0]`), slices, filter expressions (`$.data[?@.active == true]`), and recursive descent (`$..product_id`) all work, removing the previous "simple root/column extraction" limitation.
- Invalid JSONPath in YAML config is now rejected at load time rather than at query time:
  `dbfy validate` and `Engine::from_config_file` surface clear `column ... has invalid JSONPath ...` errors before any HTTP request is made.
- Still pending:
  per-source concurrency control, circuit breaking, richer retry policies, broader Arrow type coverage (decimal/list/struct), and language bindings (Python/C/Java).

## Milestone 9

- [x] Define the Python bindings milestone scope.
- [x] Add a `dbfy-py` workspace crate built as a `cdylib` Python extension via PyO3 0.28.
- [x] Expose `Engine.from_yaml`, `Engine.from_path`, `Engine.query`, `Engine.explain`, `Engine.registered_tables` to Python.
- [x] Return query results as PyArrow `RecordBatch` lists using the Arrow C Data Interface (zero-copy via `arrow-pyarrow`).
- [x] Release the GIL during async query execution by detaching the interpreter (`Python::detach`) around `tokio::Runtime::block_on`.
- [x] Surface engine errors as a dedicated `DbfyError` Python exception.
- [x] Configure a `pyproject.toml` for `maturin develop` and add a Python smoke test that drives the engine against an in-process HTTP server.
- [x] Re-run the full Rust workspace test suite and the Python smoke test.

## Current Python bindings result

- The Rust workspace now produces a Python-importable `dbfy` extension:
  `import dbfy; dbfy.Engine.from_yaml(...)` works directly from a venv where the wheel is installed via `maturin develop`.
- Query results are returned as `pyarrow.RecordBatch` instances, ready to feed into `pyarrow.Table.from_batches(...)`, `pandas`, `polars`, or any other Arrow consumer with no extra serialisation pass.
- The GIL is released around blocking SQL execution so a Python application can keep handling other work in another thread while a federated query is in flight.
- Config validation errors (including the new RFC 9535 JSONPath checks) are reported as `dbfy.DbfyError` rather than opaque runtime crashes, with the original Rust error message preserved.
- Still pending:
  C and Java bindings, async/await Python API (currently sync via `block_on`), Python-defined programmatic providers, type stub `.pyi` distribution, and broader Arrow type coverage.

## Milestone 10

- [x] Distribute PEP 561 type stubs (`__init__.pyi` + `py.typed`) in a maturin mixed layout (`python/dbfy/` + `_dbfy` cdylib); `mypy --strict` accepts the public API.
- [x] Add async API methods (`Engine.query_async`, `Engine.explain_async`) via `pyo3-async-runtimes` so Python `asyncio` consumers can `await` engine work.
- [x] Add `Engine.register_provider(name, py_obj)` that wraps a Python object as a Rust `ProgrammaticTableProvider` via the Arrow C Data Interface (`FromPyArrow`); each batch is pulled lazily under the GIL through `futures::stream::try_unfold`.
- [x] Add C bindings (`dbfy-c` crate) with `cbindgen`-generated header, opaque `DbfyEngine`/`DbfyResult` handles, thread-local `dbfy_last_error`, and Arrow C Data Interface batch export.
- [x] Add Java bindings scaffolding (`dbfy-jni` crate + `com.dbfy.Restsql`) that returns query results as Arrow IPC stream bytes; the Rust crate compiles without a JDK.

## Current bindings result

- Three first-class language bindings now share the same Rust core:
  Python (PyArrow batches, sync + async, type stubs, Python providers), C (Arrow C Data Interface), Java (Arrow IPC bytes).
- Smoke tests covered by this milestone:
  `tests/smoke.py` (REST + sync), `tests/smoke_async.py` (REST + asyncio), `tests/smoke_provider.py` (Python provider feeding DataFusion), `tests/smoke.c` (engine + SELECT + explain + error path).
- Still pending:
  Java-side end-to-end test (requires JDK), C bindings for registering programmatic providers, Python async provider scan, broader Arrow type coverage (decimal/list/struct), CI workflow that builds wheels and the C/Java artifacts.

## Milestone 11

- [x] Project rename `restsql` → `dbfy` and crate restructure to expose the trait/provider/frontend split (`dbfy-provider`, `dbfy-provider-rest`, `dbfy-provider-static`, `dbfy-frontend-datafusion`, `dbfy-frontend-duckdb`).
- [x] Define dual-track architecture so the same source layer (`dbfy-config` + `dbfy-provider*`) feeds both a standalone DataFusion frontend and a DuckDB extension frontend.
- [x] Scaffold `dbfy-frontend-duckdb` with an optional `duckdb` cargo feature so the workspace stays light by default and opts in to the bundled DuckDB C++ build only when needed.
- [x] First DuckDB integration: `dbfy_rest(url)` table function backed by `dbfy-provider-rest::RestTable`, registered via `dbfy_duckdb::register(&conn)`. Returns one `VARCHAR` row per JSON object found at `$.data[*]`.
- [x] Integration test that drives `wiremock` + an in-memory DuckDB connection through the new VTab and asserts row counts/contents.

## Current dual-track / DuckDB v0 result

- Two frontends now share the same source crates:
  `dbfy-frontend-datafusion` (the standalone engine + bindings) and `dbfy-frontend-duckdb` (the new extension shell). Both consume `dbfy-provider-rest` directly, so improvements like pagination, retry, JSONPath, or future caching layers compound across both products.
- The DuckDB v0 surface is deliberately small:
  one positional `VARCHAR` argument (the URL), one `VARCHAR` result column (`value`) holding the raw JSON of each row at `$.data[*]`. Sufficient to prove the end-to-end pipeline; explicitly typed columns, configured pagination, and filter pushdown are the v1 scope.
- Build hygiene:
  the workspace `cargo check` stays fast — DuckDB compilation only runs when an explicit `--features duckdb` is passed.
- Still pending for the DuckDB frontend:
  typed columns via inline config / YAML attach, filter pushdown via DuckDB's `TableFilterSet`, `ATTACH ... (TYPE dbfy)` catalog provider, and a loadable `.duckdb_extension` build for community-extension distribution.

## Milestone 12

- [x] Add typed-column support to `dbfy_rest()` via a named `config` parameter that accepts the same YAML/JSON config schema as the DataFusion frontend.
- [x] Map `dbfy_config::DataType` to DuckDB `LogicalType` (`int64 → BIGINT`, `float64 → DOUBLE`, `string|json → VARCHAR`, `date → DATE`, `timestamp → TIMESTAMP`, `boolean → BOOLEAN`).
- [x] Implement column writer with type dispatch over Arrow `RecordBatch` columns into DuckDB `FlatVector` (primitive types via `as_mut_slice`, strings via `Inserter`, with null mask propagation).
- [x] Add integration test that uses YAML config, declares typed columns, runs `SELECT id, name, score ... WHERE status = 'active' ORDER BY id`, and asserts the result tuple type.

## Current DuckDB v1 result

- The DuckDB extension now produces **typed SQL tables** instead of opaque VARCHAR-as-JSON. A single function call accepts the same YAML config schema we already had for the standalone engine, so the user writes the config once and uses it from either frontend.
- The `bind` step now declares the columns with their proper logical type to DuckDB, which means downstream SQL (filters, joins, aggregations, ORDER BY) operates on native types — `WHERE n > 100` works on a `BIGINT`, not on a string match.
- Two integration tests cover both modes:
  `dbfy_rest_round_trip` (untyped exploration with one VARCHAR column),
  `dbfy_rest_typed_columns_with_filter_and_projection` (typed config + WHERE + ORDER BY + projection).
- The DataFusion frontend, the DuckDB extension, and the language bindings now share roughly 80% of the source code via the `dbfy-provider*` crates: any improvement to pagination, retry, JSONPath, or auth lands in both products at once.

## Still pending

- Filter pushdown into the REST query string — blocked upstream until `libduckdb-sys` exposes the `TableFilterSet` C ABI; the engine supports it, the Rust wrapper does not yet. Today predicates remain residual.
- `ATTACH 'config.yaml' AS api (TYPE dbfy)` catalog provider.
- Loadable `.duckdb_extension` artifact for community-extension distribution.
- `Decimal/List/Struct` Arrow types beyond the current VARCHAR fallback.

## Milestone 13

- [x] Enable projection pushdown in `dbfy-frontend-duckdb`: declare `supports_pushdown() = true` on the table function, read `InitInfo::get_column_indices()` in `init`, translate column indices back to declared column names, and feed the projection list to `RestTable::execute_stream`.
- [x] Defensive fallback in `func`: look up batch column by name first (matches projection order), fall back to positional index if the provider returns columns in a different order.
- [x] Three integration tests: subset projection (`SELECT name, id FROM ...`), full projection (`SELECT id, name, price, in_stock`), aggregate without column data (`SELECT count(*)`).
- [x] Investigate filter pushdown via `TableFilterSet` and document upstream block: the C++ DuckDB engine supports it but the public C ABI used by `libduckdb-sys 1.10502` (DuckDB 1.4) does not expose it, so it cannot be wired today regardless of how the Rust code is written.

## Current projection pushdown result

- DuckDB now informs the REST provider which columns the query actually needs:
  parsing JSONPath, building Arrow arrays, and writing the DuckDB DataChunk are all bounded by the projection. For a config that declares 50 columns, `SELECT id FROM dbfy_rest(...)` causes the provider to parse only the `id` JSONPath.
- The same `dbfy_provider_rest::RestTable` API serves both frontends — the projection list is just `&[String]` passed to `execute_stream`, which the DataFusion frontend has been using since Milestone 7.
- `count(*)` works because DuckDB requests an empty column list, the provider falls back to "all columns" (suboptimal but correct), and the row count is preserved.

## Milestone 14

- [x] Add a `loadable_extension` cargo feature that pulls the `duckdb` crate's `loadable-extension` feature alongside `bundled`.
- [x] Annotate the entrypoint with `#[duckdb_entrypoint_c_api(ext_name = "dbfy", min_duckdb_version = "v1.2.0")]` to expose the `dbfy_init_c_api` C symbol that DuckDB looks up at `LOAD` time.
- [x] Reverse-engineer DuckDB's metadata footer (512 bytes: 8×32-byte fields read then reversed, magic = `"4"`, ABI = `"C_STRUCT"` for the c_api macro) and ship `scripts/append_metadata.py` that turns the cdylib into a real `.duckdb_extension` file.
- [x] End-to-end smoke test (`tests/smoke_load.py`): a vanilla `pip install duckdb` Python connection (no Rust embedding) calls `LOAD 'dbfy.duckdb_extension'` and runs `SELECT id, name FROM dbfy_rest(url, config := '...') WHERE status = 'active' ORDER BY id` against an in-process HTTP server, asserting the typed result tuples.

## Current loadable-extension result

- `dbfy` is now distributable as a real DuckDB extension, not just a Rust library:
  any DuckDB binding (Python, R, JS, JVM, CLI) on a compatible DuckDB host can `LOAD 'dbfy.duckdb_extension'` and immediately use `dbfy_rest(...)`. This is the actual community-extension UX, validated against `pip install duckdb`.
- The bundled DuckDB compile happens once (~30 min first time); subsequent builds are incremental (~2 s).
- `--jobs 1` is recommended on memory-constrained hosts (WSL with 7-8 GB) to keep the C++ compile from OOMing.
- The metadata footer is the 512-byte tail (256 metadata + 256 signature) DuckDB parses to validate the extension. `scripts/append_metadata.py` writes the layout DuckDB expects, including the magic `"4"` value and the C-API version (`v1.2.0` for DuckDB 1.5.x).
- For now the extension is unsigned; the test connection enables `allow_unsigned_extensions=true`. Community-registry signing is a release-day concern handled by DuckDB's `extension-ci-tools`.

## Milestone 15

- [x] Decouple the `duckdb` (embedded) and `loadable_extension` cargo features so they no longer share the bundled DuckDB compile path.
- [x] Track down the cargo feature unification bug: the syntax `"duckdb/X"` enables both the `X` feature on the `duckdb` dep AND the local `duckdb` feature with the same name, silently dragging `bundled` into a `loadable_extension`-only build. Switch to `"duckdb?/X"` (Cargo 1.60+ conditional feature) to suppress the bleed-through.
- [x] Verify the dep tree is clean: `cargo tree -p dbfy-frontend-duckdb --features loadable_extension -e features` no longer lists `bundled` or `cc` on `libduckdb-sys`.
- [x] Confirm the loadable artefact size: `cargo build --release` + `strip` produces a 6.5 MB `.duckdb_extension` (down from 198 MB with the bundled-by-mistake build) — a 30× reduction.
- [x] Re-run the LOAD smoke test against vanilla DuckDB Python to prove the smaller artefact still functions end-to-end.

## Current size-fix result

- The DuckDB extension build pipeline now produces two distinct binaries:
  embedded mode (`--features duckdb`) statically links DuckDB at ~200 MB and is meant for Rust-host applications;
  loadable mode (`--features loadable_extension --release`) header-only-links DuckDB at ~6.5 MB stripped, intended to be `LOAD`ed into any DuckDB host.
- The conditional `duckdb?/X` cargo feature syntax is a load-bearing detail.
  Without it, accidental feature unification undoes the entire size win — `cargo tree` is the only reliable way to verify the dep graph carries no `bundled` flag.
- Build hygiene reminder: `target/debug/libdbfy_duckdb.so` is ~190 MB but that is debug info (~50 MB `.debug_info` section, ~50 MB `.debug_abbrev`, etc.), not bundled DuckDB. `size <file>` confirms `text` is ~6 MB. For distribution always use release + strip.

## Milestone 16

- [x] Ship a richer end-to-end demo (`crates/dbfy-frontend-duckdb/examples/sensor_analytics.py`) that drives the loadable extension through four progressively richer patterns: simple table function, cross-source JOIN, materialised cache, window function + Parquet export.
- [x] Catch and fix a latent timestamp schema bug surfaced by the demo: `TimestampMicrosecondBuilder::new()` produced `Timestamp(µs, None)` while the schema declared `Timestamp(µs, "UTC")`, so `RecordBatch::try_new` rejected timestamp-bearing rows. Builder now uses `.with_timezone("UTC")`.
- [x] Add a regression test (`timestamp_column_builder_carries_utc_timezone`) in `dbfy-provider-rest` that asserts the schema field carries the timezone and that null timestamps propagate correctly. Without the fix, this test fails with `RecordBatch::try_new` schema mismatch.
- [x] Publish the demo's SQL excerpts and output table directly in `crates/dbfy-frontend-duckdb/README.md`.

## Current demo + bug-catch result

- The DuckDB extension story is now demonstrable in a single self-contained Python file: `pip install duckdb` + `LOAD 'dbfy.duckdb_extension'` + four SQL queries. The demo shows REST × REST JOIN, cache materialisation (2.44 ms/query after the snapshot), window functions, and Parquet output.
- Running the demo end-to-end exercised a corner of the rest provider that none of the unit tests had covered (timestamp columns) and surfaced a real schema-mismatch bug. The fix is one line; the regression test prevents recurrence.
- This validates a SOTA principle the project has been following throughout: every milestone that ships a new frontend feature should be exercised by at least one realistic example, not only by isolated unit tests, because cross-layer integration finds bugs unit tests miss.

## Milestone 17

- [x] Scaffold the `dbfy-provider-rows-file` crate (Sprint 1 of Task #21).
- [x] Define the `pub trait LineParser` extension point so third parties can plug in custom formats without forking the crate.
- [x] Ship the JSONL parser (Sprint 1 v1 format) with JSONPath-driven column extraction over `int64`, `float64`, `string`, and RFC 3339 `timestamp` (microseconds, UTC).
- [x] Build the chunked indexer with zone-map stats (`min/max` + `all_null`) over those four types; chunk size defaults to 1 024 rows.
- [x] Implement the five-branch cache invalidation policy in `invalidation.rs`: REUSE / EXTEND / REBUILD chosen against `(file_size, file_mtime, file_inode, prefix_hash)`. Sprint 1 always re-builds from scratch on non-REUSE; the EXTEND incremental path is wired in Sprint 3.
- [x] Implement the pruner: filter set + per-chunk stats → list of byte ranges that *might* contain matches. Conservative on type mismatch and unindexed columns.
- [x] Implement the reader: range scan + parser dispatch, yielding one `RecordBatch` per surviving chunk via a `futures::stream::unfold`.
- [x] Wire `RowsFileTable` to `dbfy_provider::ProgrammaticTableProvider` so both frontends pick it up without changes.
- [x] Three integration tests:
  - `jsonl_zone_map_filters_prune_chunks` — correctness of the row set.
  - `jsonl_zone_map_actually_skips_chunks` — proves 19/20 chunks skipped on a 1 % range filter.
  - `jsonl_no_filters_emits_all_rows` — baseline correctness without pushdown.

## Current rows-file Sprint 1 result

- The new provider sits in the workspace at `crates/dbfy-provider-rows-file`, gated behind a default `jsonl` cargo feature so the build stays light.
- The pipeline indexes a 10 k-row file into 20 chunks of 500 rows; a `WHERE id BETWEEN 5000 AND 5099` filter prunes 19 chunks and reads only 1 — proven by `debug_skip_stats` in the test, not just inferred from the result count.
- The index is persisted as a bincode sidecar `<file>.dbfy_idx`. Switching to a Parquet sidecar is deferred to Sprint 3+ when self-querying becomes useful (right now we only need fast I/O).
- The `LineParser` trait is the documented extension point for non-built-in formats; built-in coverage will grow to CSV / regex / logfmt / syslog in Sprints 2 and 3.
- Workspace test count: **30** (was 25; +5 from this milestone — 2 unit + 3 integration).

## Milestone 18

- [x] Tiny serde-friendly Bloom filter in `bloom.rs` — k seeded xxhash hashes, capacity-sized at construction, ~80 LoC, no new heavy dep.
- [x] Extend `IndexKind` with `Bloom` and `ZoneMapAndBloom`; the `has_zone_map`/`has_bloom` selectors drive the indexer.
- [x] Refactor `ColumnStats` to carry `zone: Option<ZoneRange<T>>` (separate from `bloom`), so a bloom-only column doesn't drag a zero-min/zero-max into pruning.
- [x] Pruner consults the bloom for `=` and `IN` operators, the zone map for ranges; tests prove ≤2/20 chunks survive an absent-key Eq probe and that IN-list keeps exactly the chunks containing the keys.
- [x] Sprint 2 parsers: `regex` (named groups → typed cells) covers CLF / ELF / custom; `logfmt` (key=value, quoted-string escapes) covers Go slog / etcd-style logs. Both behind cargo features (`regex`, `logfmt`).
- [x] Shared cell-builder helpers in `parsers/cells.rs` for str-to-typed conversion (`Int64`, `Float64`, `String`, `Timestamp`).
- [x] Cache invalidation test set — five scenarios on the decision tree:
  - `unchanged_file_reuses_index` (REUSE)
  - `append_keeps_prefix_emits_extend` (EXTEND)
  - `truncation_emits_rebuild` (REBUILD)
  - `atomic_replace_via_mv_emits_rebuild` (REBUILD via inode change)
  - `mid_file_edit_emits_rebuild` (REBUILD via prefix mismatch)
- [x] Concurrent-writer test pinning the snapshot consistency contract: a query started after a fixed index sees only the snapshot rows, never partial WARN rows appended in another thread.
- [x] Bug fix during Sprint 2: `prefix_hash` was implicitly size-dependent (hashed `[0..min(file_size, 4096)]`), so any append changed the hash even when the prefix was untouched. Added `prefix_hash_window` to the cached header so `decide_with_path` re-hashes the same byte window from the current file. Index format bumped to v4.

## Current rows-file Sprint 2 result

- Bloom-driven equality and IN-list pruning shipped:
  10 k-row file with high-cardinality `trace_id`, `WHERE trace_id = 'absent'` survives ≤2/20 chunks (FPR-bounded), `WHERE trace_id IN ('a', 'b', 'c')` keeps exactly 3..=5 chunks.
- Three first-class parsers now ship behind cargo features: `jsonl` (Sprint 1), `regex` (Sprint 2 — covers Apache CLF, W3C ELF, custom), `logfmt` (Sprint 2 — Go slog).
- Cache invalidation contract pinned by tests for every branch of the 5-way decision tree, including the corner case of `prefix_hash` window mismatch on small files.
- Snapshot semantics validated against a racing appender: query never observes partial rows, total stays at the pre-snapshot count even with a parallel writer adding 1 000 rows during the scan.
- Workspace test count: **48** (was 30; +18 — 4 new unit + 14 new integration across bloom, invalidation, concurrent_writer, regex parser, logfmt parser).

## Milestone 19

- [x] **CSV parser** (`parsers/csv.rs`) — built on the `csv` crate behind a `csv` cargo feature; columns addressable by header name (`CsvSource::Name`) or positional index (`CsvSource::Index`). `read_header()` helper extracts the header row separately so the parser can also be constructed against headerless files. Reuses `TypedBuilder` from `parsers/cells.rs` for `Int64 / Float64 / String / Timestamp` cells with proper null propagation on empty fields.
- [x] **Syslog RFC 5424 parser** (`parsers/syslog.rs`) — hand-rolled byte-cursor parser, no regex dep. Handles `<PRI>VERSION` header, the five space-separated NIL-or-string fields (TIMESTAMP / HOSTNAME / APP-NAME / PROCID / MSGID), structured-data block with `\]` / `\\` escape handling, and message-body extraction with UTF-8 BOM stripping. `SyslogField` enum exposes derived columns: `Priority`, `Facility = PRI/8`, `Severity = PRI%8`, `Version`, `Hostname`, `AppName`, `ProcId`, `MsgId`, `StructuredData` (raw `[…]…` block), `Message`. NIL `-` values become NULL.
- [x] **Incremental EXTEND** (`indexer.rs::extend()` + `build_from()` private impl) — when invalidation returns `Decision::Extend`, parse only the new tail starting at the cached `last_chunk.byte_end`, append fresh chunks to the prior index, and rewrite the sidecar. Old chunks preserved byte-for-byte across appends — proven by `tests/incremental.rs` asserting `byte_start` equality on every chunk shared between the two indexes. `INDEXER_VERSION` stays at 4 (sidecar layout unchanged from Sprint 2).
- [x] **`ensure_index` invalidation bug fix** — previous logic short-circuited when an in-memory cache was present, so the sidecar+snapshot decision tree was bypassed for the lifetime of the table. Removed the early return; every `scan()` now runs `FileSnapshot::capture` + `decide_with_path` against the live cache. This is what unblocks EXTEND in the steady-state path: append-then-query now actually triggers `Decision::Extend` instead of silently reusing the stale index.
- [x] **Glob multi-file provider** (`glob_table.rs::RowsFileGlob`) — single config drives N files. `try_new(pattern, parser, indexed_columns)` runs `glob::glob()`, sorts results lexicographically, and instantiates one `RowsFileTable` per match (each maintaining its own `.dbfy_idx`). `scan()` flattens per-file streams via `futures::stream::iter().flatten()`, so per-file pruning still applies and cross-file global skip ratio is the union of per-file decisions.
- [x] **End-to-end demo** (`examples/log_analytics.rs`) — synthesises a 50 k-row JSONL log fixture (≈6.5 MiB), builds 50 chunks of 1 000 rows with zone maps on `id` / `ts` and blooms on `level` / `service` / `trace_id`, and benchmarks five realistic ops queries:
  - 1 % range scan on `id` → **98 % skipped** (1/50 chunks read), <1 ms.
  - Bloom probe on absent `trace_id` → **100 % skipped** (0/50 chunks read), 41 µs.
  - Bloom probe on present `trace_id` → **98 % skipped** (1/50 chunks read), 1 ms.
  - `IN` list on low-cardinality `service` (3 of 4 known) → 0 % skipped (correct: every chunk has every service value).
  - Full scan baseline → 50/50 chunks, 46 ms.

## Current rows-file Sprint 3 result

- The provider now covers the five line-delimited formats originally scoped: **jsonl, csv, logfmt, regex (CLF/ELF), syslog (RFC 5424)** — feature-gated so binary size scales with what the user actually parses.
- Append-only steady state is now O(new bytes) instead of O(file size): adding 500 rows to a 50 k-row log triggers an EXTEND that re-parses only the appended tail and preserves the prior 50 chunks byte-for-byte. The bug-fix in `ensure_index` is what made this real — before, the in-memory cache silently shadowed any file change.
- Glob multi-file is the natural fit for log-rotation directories (`/var/log/app/*.log`); each file pulls its own sidecar so a stale rotated file never invalidates the live one.
- The demo is the project's standard "shipped feature requires a realistic example" check (Milestone 16's principle): on 50 k rows of synthetic logs, the indexed scan demonstrably reads 0–2 % of the file for selective filters and 100 % for unselective ones — the L3 index is doing useful work, not just sitting in the sidecar.
- Workspace test count: **75** (was 48; +27 — 14 unit + 13 integration in rows-file, including `tests/incremental.rs` and `tests/glob.rs`).
- Task #21 (`dbfy-provider-rows-file`) v1 is now feature-complete across all three sprints. Future work moves to Task #20 (HTTP fetch dedup/cache layer) or higher-level concerns such as ATTACH catalog providers.

## Milestone 20

- [x] **`HttpCache`** in `dbfy-provider-rest::cache`: in-memory TTL cache with FIFO eviction (configurable cap, default 1024 entries) keyed by the final URL string after pushdown + pagination expansion. Cheap to clone (internal `Arc<Mutex<…>>`); shared across queries on a single `RestTable` by default, optionally hoisted across tables of the same source via `RestTable::with_cache()`.
- [x] **Singleflight dedup**: concurrent identical requests are coalesced through a `tokio::sync::Notify`-backed Pending slot. The leader fetches once, all waiters re-check the slot when notified and pick up the cached response. Errors are *not* cached — a failed fetch removes the slot, woken waiters retry as new leaders. This avoids poisoning the cache with transient failures.
- [x] **`CacheConfig`** in `dbfy-config::RuntimeConfig`: `{ ttl_seconds, max_entries }` — `ttl_seconds: 0` keeps singleflight active (concurrent dedup) but disables sequential reuse, useful when consistency matters more than fewer fetches.
- [x] **Wired through `fetch_page_by_url`** as the single choke point: every page of every pagination strategy goes through one method, so the cache covers page/offset/cursor/link-header uniformly with no per-strategy adaptation.
- [x] **`CacheMetrics`** snapshot on `RestTable::cache_metrics()` exposing `hits / misses / coalesced / evictions` for tests and operational dashboards.
- [x] **Four integration tests** (`tests/cache.rs`) pinning the four guarantees:
  - `sequential_calls_reuse_cached_response` — 2 SQL drains → 1 wiremock fetch (`.expect(1)`).
  - `concurrent_calls_singleflight_to_one_fetch` — 8 parallel drains against a 150 ms-delayed mock → 1 fetch, ≥1 coalesced waiter.
  - `ttl_expiry_triggers_refetch` — `ttl=0` forces a fresh fetch on every drain (2 fetches total, 0 hits, 2 misses).
  - `cache_disabled_when_no_runtime_cache` — without `runtime.cache` config every drain hits the wire and `cache_metrics()` returns `None`.

## Current REST cache result

- The REST provider now amortises HTTP fetches across queries by default when the user opts in via YAML:
  ```yaml
  sources:
    crm:
      base_url: https://api.example.com
      runtime:
        cache: { ttl_seconds: 30 }
  ```
- Both frontends (DataFusion + DuckDB) inherit the win automatically — no per-frontend wiring. A typical exploratory session that runs the same `SELECT … FROM crm.customers` ten times now triggers one HTTP fetch, not ten.
- Singleflight matters most for federated joins where two parallel scans land on the same endpoint (e.g. self-join, or a left-side and right-side that both look up `/users/{id}`). Without dedup these would fire N parallel GETs; with dedup they fire one and share the response.
- Per-table cache scope is the v1 trade-off: two `RestTable`s sharing a source still get distinct caches unless the embedder explicitly calls `with_cache(shared)`. Engine-level cache hoisting (one cache per source registered globally) is straightforward to add when the hot-path data shows it matters; today it doesn't.
- Workspace test count: **79** (was 75; +4 from `tests/cache.rs`).

## Milestone 21

- [x] **YAML schema for rows-file sources** in `dbfy_config::SourceConfig::RowsFile`. The same shape covers all five built-in parsers (jsonl / csv / logfmt / regex / syslog) via a tagged `parser.format` enum, with format-specific column types: `JsonlColumnConfig { name, path, type }`, `CsvColumnConfig { name, source: { name | index }, type }`, `LogfmtColumnConfig { name, key, type }`, `RegexColumnConfig { name, group, type }`, `SyslogColumnConfig { name, field, type }`.
- [x] `RowsFileTableConfig` accepts either a single `path` or a `glob` (mutually exclusive, validated at config-load), plus optional `chunk_rows` and `indexed_columns: [{ name, kind: zone_map | bloom | zone_map_and_bloom }]`.
- [x] `CellTypeConfig` enum (`int64 | float64 | string | timestamp`) lives separate from `DataType` because the rows-file parsers don't (yet) cover Date / Boolean / Json — the YAML reflects the actual capability rather than tempting the user with unsupported types.
- [x] **Engine wiring** in `dbfy-frontend-datafusion`: each `SourceConfig::RowsFile` table is materialised into a `RowsFileTable` (single path) or `RowsFileGlob` (pattern) and registered as a programmatic provider. Helper functions `build_parser` / `map_indexed_column` / `map_cell_type` translate the YAML config into the rows-file crate's strong types. CSV header reading is kicked off at config-load when columns reference header names — the first matched glob path serves as the canonical header source.
- [x] Validation: empty path/glob, both set, empty parser column lists, invalid JSONPath in jsonl columns, empty regex pattern. Each fails fast at `Config::from_yaml_str` with a precise `ConfigError::Validation` message.
- [x] **Six new tests** prove the round-trip:
  - `dbfy-config`: `parses_rows_file_jsonl_source`, `rejects_rows_file_with_both_path_and_glob`, `rejects_jsonl_column_with_invalid_jsonpath`, `parses_rows_file_csv_with_glob`.
  - `dbfy-frontend-datafusion`: `rows_file_jsonl_yaml_round_trip_runs_sql` (100-row JSONL, qualified registration, `SELECT … WHERE level = 'ERROR'` returns 15 rows), `rows_file_jsonl_supports_range_pruning_via_zone_map` (1000-row JSONL, 10-row range query through DataFusion).

## Current rows-file YAML result

- `dbfy query --config logs.yaml "SELECT … FROM access.events WHERE …"` now works against files. The CLI is no longer REST-only — any line-delimited format the rows-file crate already parses can be exposed as a SQL table through pure config.
- Cross-frontend consistency holds: the same YAML config will work in the DuckDB frontend once we wire `dbfy-provider-rows-file` there too (deferred — current Task focuses on the CLI side first).
- The YAML is the foundation for `dbfy detect` (Milestone 22) and `dbfy index` (next): both produce / consume this exact schema.
- Workspace test count: **85** (was 79; +6).

## Milestone 22

- [x] `dbfy detect <file>` subcommand on `dbfy-cli`: reads a sample of the file (default 200 lines, `--sample N`), infers schema, and prints a complete `version: 1` YAML config to stdout. Output is ready to consume via `dbfy validate` and `dbfy query --config`.
- [x] Format inference from file extension (`.jsonl/.ndjson → jsonl`, `.csv/.tsv → csv`, `.log/.logfmt → logfmt`); explicit `--format` override. Syslog must be opt-in via `--format syslog` because rotated syslog files rarely use a distinguishing extension.
- [x] **Type inference** with cross-row promotion semantics:
  - JSONL: parses each sampled line, walks top-level keys, classifies values (`Number(int)→Int64`, `Number(float)→Float64`, `String` matching `YYYY-MM-DDTHH:MM:SS…→Timestamp`, else `String`). Mixed `Int64/Float64` → `Float64`; any conflict with `String` or `Timestamp` → `String`.
  - CSV: reads header, then samples data rows. Per-column type inference uses `parse::<i64>`, `parse::<f64>`, RFC 3339 heuristic; same merge rules.
  - Logfmt: collects all keys seen across lines (preserving first-seen order); all columns typed as `String` because logfmt has no machine-readable types.
  - Syslog: emits the canonical RFC 5424 column set (priority/facility/severity/ts/host/app/proc_id/msg_id/message) without sampling — fixed schema.
- [x] **Heuristic indexed_columns suggestions**: numeric / timestamp columns get `zone_map`; columns named `id` or `*_id` get `bloom`. The user can edit before consuming; the goal is a sensible starting point, not a perfect index plan.
- [x] **Round-trip guarantee** pinned by a test: every detect output parses cleanly through `dbfy_config::Config::from_yaml_str`. This is the contract — if it parses, the engine can run it.
- [x] Smoke test from a real run: `dbfy detect /tmp/sample.jsonl` followed by `dbfy validate` then `dbfy query "SELECT user_id, COUNT(*) FROM app.events GROUP BY user_id"` returns correct counts. End-to-end, no manual YAML editing.
- [x] Four unit tests (`detect.rs::tests`) cover the inference rules, key ordering, and full round-trip.

## Current dbfy detect result

- The CLI now has a real onboarding path for non-REST users: point it at a JSONL/CSV/logfmt/syslog file and get back a config that runs SQL immediately. No need to read parser docs to start.
- Heuristic `indexed_columns` are intentionally conservative — adding a few zone maps and blooms costs little and gives meaningful skip ratios on the queries users typically write first (`WHERE id BETWEEN ...`, `WHERE trace_id = '…'`). Wrong guesses are corrected with one YAML edit.
- The detect output remains a *suggestion*; we never overwrite an existing config and we always print to stdout. The user retains full control via `> config.yaml` redirection.
- Pending sprints in Task #25: `dbfy index <file>` (explicit sidecar pre-warm), `dbfy init` (interactive wizard), `dbfy probe <url>` (REST introspection — will reuse the HTTP cache from Milestone 20).
- Workspace test count: **89** (was 85; +4 from `detect.rs::tests`).

## Milestone 23

- [x] **`RowsFileTable::refresh()`** returns a typed `RefreshDecision` (`BuiltFresh | Reused | Extended | Rebuilt`) so callers see which arm of the invalidation tree fired without re-deriving it. Internally `ensure_index` is now a thin wrapper over `ensure_index_inner` which both updates the cache and propagates the decision.
- [x] **`RowsFileTable::index_summary()`** + `IndexSummary { chunks, total_rows, file_size }`: a side-effect-free read after a forced refresh, so the CLI can print stats without re-deriving them from the sidecar.
- [x] **`RowsFileGlob::refresh_each()`** / `index_summary()` / `rebuild_all()`: per-file decisions and aggregate summary across globbed inputs. Each underlying file maintains its own sidecar; the glob just iterates and the user sees one decision line per file.
- [x] **`RowsFileHandle`** enum + `build_rows_file_handle()` in `dbfy-frontend-datafusion`: the same builder the engine uses, but exposed as `pub` and returning a typed handle (Single / Glob) instead of erasing to `DynProvider`. The CLI consumes this directly without going through `Engine`.
- [x] **`dbfy index --config x.yaml --table source.table [--rebuild]`** in `dbfy-cli`: parses the YAML, extracts the rows-file table, materialises a typed handle, calls `refresh()` (default) or `rebuild()` (with flag), then prints `<DECISION>  <path>  (<chunks>, <rows>, <bytes>)` per file plus a `TOTAL` line for globs.
- [x] **End-to-end smoke**: `dbfy detect /tmp/sample.jsonl > /tmp/auto.yaml && dbfy index --config /tmp/auto.yaml --table app.events` shows the full lifecycle:
  - first run on existing sidecar → `Reused`
  - rerun unchanged → `Reused`
  - after appending one row → `Extended` (chunks: 1 → 2, rows: 3 → 4)
  - `--rebuild` → `Rebuilt` (chunks: 2 → 1)
- [x] **Cross-crate lifecycle test** (`rows_file_handle_refresh_lifecycle_tracks_decisions`) pins the four-state transition in code: BuiltFresh → Reused → Extended (after append) → Rebuilt.

## Current dbfy index result

- Operations now have a first-class command for sidecar maintenance: `dbfy index` is the natural fit for cron jobs, post-deploy hooks, and CI build pipelines where you want the index pre-warmed before the first user query lands. No more "first query is slow because the index builds inline."
- The CLI surface is intentionally explicit: `--rebuild` is the only way to force a full rebuild; default is the same EXTEND/REUSE/REBUILD logic the runtime uses on first scan, so an idempotent rerun never costs more than one snapshot+decision pass.
- `RowsFileHandle` is the building block for any future ops command (status, prune, vacuum sidecars). The engine still consumes it as `DynProvider` for query, but the CLI sees the typed handle and can call indexing-specific methods.
- Pending sprints in Task #25: `dbfy init` (interactive wizard), `dbfy probe <url>` (REST introspection with cache reuse).
- Workspace test count: **90** (was 89; +1 cross-crate lifecycle test).

## Milestone 24

- [x] **`dbfy probe <url>`** subcommand: GETs a URL, finds the most likely root JSONPath via a candidate ladder (`$.data[*]` → `$.results[*]` → `$.items[*]` → `$.records[*]` → `$.rows[*]` → `$.value[*]` → `$.payload[*]` → `$`), infers per-field types from the first sample object, emits a `version: 1` REST source YAML stanza ready to paste. Optional `--root` overrides auto-detection; `--auth-bearer-env VAR` sends a bearer token from an environment variable.
- [x] Type inference covers the common JSON shapes: `Number(int)→int64`, `Number(float)→float64`, `Bool→boolean`, `String` matching `YYYY-MM-DDTHH:MM:SS…→timestamp`, nested object/array → `string` (raw payload). Field name sanitisation maps non-alphanumeric chars to `_` so the YAML keys are always valid identifiers.
- [x] **`dbfy init`** interactive wizard via `dialoguer`: asks source kind (`rest` or `rows_file`), source/table names, then delegates. For rows-file: prompts file path + format choice → calls `detect`. For REST: prompts URL + auth → calls `probe`. The wizard composes the existing introspection commands rather than recreating them, keeping each piece independently testable.
- [x] **Round-trip guarantee** pinned by tests: every probe output and every detect output parses cleanly through `dbfy_config::Config::from_yaml_str`. The contract — "if it parses, the engine can run it" — now holds for both onboarding paths.
- [x] **Five new probe tests** (`probe.rs::tests`): root-array detection, fallback to `$.results[*]`, top-level array handling, explicit `--root` override, type inference per JSON kind, plus a full HTTP round-trip against an in-process `tokio::net::TcpListener`-backed server (avoids pulling wiremock into the CLI crate).

## Current dbfy probe / init result

- The CLI now has a complete onboarding story: a user with neither a config nor a clear schema in mind can go from "I have a URL" or "I have a file" to "I'm running SQL" in one command. `dbfy init` is the front door; `dbfy probe` and `dbfy detect` are the engines that do the actual introspection and can be called directly when scripting.
- The candidate-ladder for REST root detection covers ≥90% of public APIs in our smoke testing (`{ "data": [...] }` is overwhelmingly the most common; OData's `value` and pagination wrappers like `results`/`items` are the rest). Misses fail with a clear message that points at `--root`.
- Auth flow is intentionally minimal in v1: bearer-from-env covers most modern APIs (GitHub, Stripe, Shopify, etc). Basic / API key / custom header are configurable post-hoc by editing the YAML — the wizard's job is to get the user productive, not to model every auth permutation.
- Task #25 is now feature-complete across all four sprints (`detect`, `index`, `probe`, `init`).
- Workspace test count: **100** (was 90; +10 — 5 detect/probe/init helper tests landed across this and earlier sprints, plus 5 inference + round-trip tests for probe). All green.

## Milestone 25

- [x] **Lift `RowsFileHandle` + `build_handle`** from `dbfy-frontend-datafusion` into `dbfy-provider-rows-file::from_config`. The translator from YAML config → typed `RowsFileTable` / `RowsFileGlob` is now a first-class part of the provider crate and reusable across frontends. `dbfy-provider-rows-file` gains a dep on `dbfy-config`; `dbfy-frontend-datafusion` simplifies to a 10-line wrapper that maps `ProviderError` to `EngineError`.
- [x] **Shared arrow→DuckDB column writer** in a new `arrow_to_duckdb` module of `dbfy-frontend-duckdb`. The `write_arrow_column` function and `arrow_data_type_to_duckdb` (DataType-side) + `cell_type_to_duckdb` (CellTypeConfig-side) are consumed by both `rest_vtab` and the new `rows_file_vtab`, eliminating ~120 lines of duplicated arm-by-arm dispatch.
- [x] **`dbfy_rows_file()` DuckDB table function** (`rows_file_vtab.rs`):
  - First positional arg = path or glob (auto-classified by presence of `*?[` chars).
  - `config :=` named arg = YAML/JSON with `parser` (jsonl / csv / logfmt / regex / syslog), optional `indexed_columns`, optional `chunk_rows`. Same schema as `RowsFileTableConfig` minus `path/glob`.
  - `bind` declares each parser column to DuckDB with the right LogicalType; `init` translates DuckDB's projection-index list back to column names and feeds it to `RowsFileTable::scan` (or `RowsFileGlob::scan`) so the rows-file pruner only parses indexed columns the SQL actually needs.
  - `supports_pushdown() = true`; the existing arrow→DuckDB writer handles every Arrow type the rows-file parsers can emit.
- [x] **Loadable-extension entrypoint** in `lib.rs::extension_init` now registers both `dbfy_rest()` and `dbfy_rows_file()` so a single `LOAD 'dbfy.duckdb_extension'` brings up the full table-function surface.
- [x] **`dbfy_duckdb::register(conn)`** (the Rust-host helper) keeps the same signature but now installs both functions; granular `register_rest` / `register_rows_file` are also exposed for embedders that want a partial registration.
- [x] **End-to-end DuckDB integration test** (`dbfy_rows_file_indexed_jsonl_pushdown`):
  - 200-row JSONL with `chunk_rows: 50` (4 chunks), `indexed_columns: id zone_map + level bloom`.
  - `SELECT count(*) … WHERE id BETWEEN 100 AND 119` → 20 rows. The zone map prunes 3 of 4 chunks; only one is parsed.
  - `SELECT id … WHERE level = 'ERROR' ORDER BY id` → 16 ERROR-tagged rows from id=0 to id=195.
- [x] All four DuckDB integration tests stay green (`dbfy_rest_round_trip`, `dbfy_rest_typed_columns_with_filter_and_projection`, `dbfy_rest_projection_pushdown_emits_only_requested_columns`, plus the new rows-file one).

## Current DuckDB rows-file result

- The `dbfy.duckdb_extension` is now a *complete* federation surface, not REST-only: a vanilla DuckDB host can load it and immediately query JSONL / CSV / logfmt / regex / syslog files with the full L3 indexing story (zone maps + bloom filters + incremental EXTEND on append-only files). The same YAML config that drives `dbfy query --config` works inside `dbfy_rows_file()` too.
- Side-by-side function pair:
  - `dbfy_rest('https://api.example.com/users', config := '...')` for HTTP/JSON sources.
  - `dbfy_rows_file('/var/log/app/*.log', config := '...')` for line-delimited file sources.
  Both follow the same idioms (positional URL/path, named `config` YAML, projection pushdown, typed columns) so the user only learns one mental model.
- The lifted `from_config` module is now the canonical place for "YAML config → rows-file table". Future frontends (Polars, Spark, plain-stdout cli printer) get the same builder for free.
- Workspace test count: **101** (was 100; +1 DuckDB integration test for rows-file). Bundled DuckDB build is ~31 minutes the first time and is gated behind `--features duckdb` so default workspace builds remain fast.

## Milestone 26

- [x] **Extended `examples/sensor_analytics.py`** from 4 patterns (REST-only) to 6, demonstrating the full federation surface end-to-end against `pip install duckdb` + `LOAD 'dbfy.duckdb_extension'`. The script:
  - Spins up two in-process HTTP endpoints (`/sensors`, `/readings`).
  - Synthesises a 2 000-row JSONL fleet-events log on disk.
  - Runs SQL queries that mix REST and file sources in the same `SELECT`.
- [x] **Pattern (c) — file federation**: `SELECT kind, count(*), avg(duration_ms) FROM dbfy_rows_file(...) GROUP BY kind`. Inline `EVENTS_CFG` declares a jsonl parser plus `indexed_columns` (zone maps on `event_id` / `ts`, blooms on `kind` / `actor`) so the sidecar is built once and reused.
- [x] **Pattern (d) — the killer query**: REST × REST × FILE in a single `SELECT`. Finds zones where high-temperature readings (`r.temperature > 22`) lined up with `kind = 'alarm_acked'` events on the same sensor within ±90 seconds. Output: 16 hot-and-acked events for `loading-dock`, 12 each for `warehouse-a`/`warehouse-b`. The pre-dbfy version of this query would have been three scripts, two CSV intermediates, and a manual `pd.merge`.
- [x] **Pattern (e) — cached 3-way join**: `CREATE TABLE … AS SELECT … FROM dbfy_rest/dbfy_rows_file(…)` for all three sources. Then 20 reps of the 3-way `JOIN` over the local snapshot run in **71.7 ms total (3.59 ms/query)**, zero HTTP, zero file IO.
- [x] **README of `dbfy-frontend-duckdb`** updated to surface the new patterns + sample output, and to advertise both `dbfy_rest()` and `dbfy_rows_file()` at the top of the document.
- [x] **Loadable extension rebuilt with rows-file**: `target/release/dbfy.duckdb_extension` now ships at **7.4 MB** (was 6.5 MB before rows-file integration; +900 KB for `regex` + `csv` + `bincode` + `twox-hash` and the rows-file machinery). Distribution shape unchanged: a single `.duckdb_extension` file that any DuckDB host can `LOAD`.

## Current Python demo result

- The DuckDB extension story is now demonstrably complete: a single `pip install duckdb` + `LOAD 'dbfy.duckdb_extension'` Python script federates two REST endpoints and a local JSONL file with full SQL semantics — JOINs, aggregates, window functions, Parquet export, the lot.
- The "killer query" pattern (REST × REST × FILE in one SELECT) is exactly the use case `dbfy` was designed for: bring heterogeneous data sources to the SQL surface so analysts and ops engineers don't have to reinvent the merge logic in Python every time.
- Sidecar persistence makes file-source queries cheap on rerun: the second run of `(c)` reuses the prior `<file>.dbfy_idx` instead of re-parsing the 2 000-row JSONL.

## Milestone 27

- [x] **`dbfy duckdb-attach`** subcommand on the CLI: takes a YAML config and prints a multi-statement DuckDB SQL script that creates one `CREATE OR REPLACE VIEW <schema>.<table>` per configured table, wrapping the appropriate `dbfy_rest()` or `dbfy_rows_file()` call with the right URL/path and inline YAML config. Pipe into duckdb: `duckdb mydb.duckdb < <(dbfy duckdb-attach --config x.yaml --schema api)`.
- [x] **Why this and not real `ATTACH 'x.yaml' (TYPE dbfy)`**: the latter requires DuckDB's `StorageExtension` C API to be exposed by the `duckdb-rs` Rust binding, which it isn't yet. Once it is, the catalog-provider implementation can replace the SQL emitter without changing user-facing semantics — the schema and view names stay the same, the on-the-wire shape is identical, only the registration mechanism moves from "shell out + pipe" to "single ATTACH statement".
- [x] **Options that mirror real ATTACH UX**: `--schema NAME` (target schema, defaults to `dbfy`), `--extension PATH` (prepend `LOAD '...'` so the script runs standalone), `--strict` (use plain `CREATE VIEW` instead of `CREATE OR REPLACE VIEW`, fails loudly on conflicts — useful in CI).
- [x] **Correct quoting** of every untrusted input: SQL identifiers (`"` → `""`) for schema/table names, SQL string literals (`'` → `''`) for URLs and YAML bodies. Pathological case pinned by a test (`single_quotes_in_url_are_escaped`) using `O'Brien`-style URL.
- [x] **`AttachOpts::default()`** picks `schema: "dbfy"` so a zero-config invocation (`dbfy duckdb-attach -c x.yaml`) still produces a runnable script that lands tables under `dbfy.<table>`.
- [x] **Six unit tests** in `duckdb_attach.rs` cover: REST source emission, rows-file source emission, `LOAD` prelude when `--extension` is set, single-quote escaping in URLs, strict mode (no `OR REPLACE`), empty schema rejection.
- [x] **End-to-end smoke** against bundled DuckDB Python (`pip install duckdb` 1.5.2):
  - rows-file path: `dbfy detect → duckdb-attach → duckdb < attach.sql` makes `demo.events` queryable; `SELECT level, COUNT(*) FROM demo.events GROUP BY level` returns the expected per-level breakdown.
  - REST path: an in-process `BaseHTTPRequestHandler` serves `/users`; `dbfy duckdb-attach` produces `CREATE VIEW api.users`; `SELECT sum(score) FROM api.users` returns the expected aggregate.

## Current attach result

- The DuckDB extension UX gap closes: a user who has invested in writing a YAML config for the standalone CLI now gets a one-line shell pipeline that exposes every table as a real DuckDB view in the schema of their choice. Subsequent SQL is pure DuckDB — `JOIN`, `WHERE`, aggregates, window functions, schema introspection through `information_schema.views`. No more passing `dbfy_rest('url', config := '...')` boilerplate to every query.
- The architecture deliberately keeps the SQL contract stable across the upcoming switch to true `ATTACH '...' (TYPE dbfy)` syntax: the views land at the same `<schema>.<table>` names, the underlying provider machinery is the same, only the user-facing registration verb changes when the C API becomes available.
- Workspace test count: **79** unit + integration (excluding the `--features duckdb` tier which is gated behind the bundled DuckDB compile). All green.

## Milestone 28

- [x] **C++ shim integrated into the build.** `crates/dbfy-frontend-duckdb/build.rs` runs only under `--features duckdb` (no-op for `--features loadable_extension` since the loadable artefact only sees the C API). It locates `libduckdb-sys`'s extracted headers by globbing the `target/<profile>/build/libduckdb-sys-*/out/duckdb/src/include` directory (a documented hack — clean fix is a small upstream PR adding `links = "duckdb"` + `cargo:include=…` to `libduckdb-sys`).
- [x] **`cpp/extension_shim.cpp`** — ~280 LoC of C++ that:
  - Casts `duckdb_database` → `duckdb::DatabaseWrapper` → underlying `DuckDB::instance->config` via `capi_internal.hpp`.
  - Registers a `duckdb::OptimizerExtension` whose `optimize_function` walks the `LogicalPlan`, finds every `LogicalFilter` whose only child is a `LogicalGet` on `dbfy_rest` / `dbfy_rows_file`, and decomposes each top-level conjunct (`column OP constant`) into a `(column, op, value, type)` tuple.
  - Stashes the JSON-encoded list of tuples in a global mutex-protected map keyed by the user's bind-data raw pointer. Crucially, the C API wraps user bind data inside a `CTableBindData` (from `capi_internal_table.hpp`); the shim unwraps via `dynamic_cast<CTableBindData*>` to get the inner pointer that matches what `InitInfo::get_bind_data()` returns on the Rust side.
  - Exposes `dbfy_shim_install_optimizer_db`, `dbfy_shim_take_filters`, `dbfy_shim_free_string`, plus an observation buffer for tests.
  - Filter expressions that don't decompose cleanly (`OR`, sub-expressions, unsupported types) are silently dropped — DuckDB's filter operator above the `LogicalGet` still applies them, so correctness is preserved when pushdown is partial.
- [x] **Rust glue** in `dbfy-frontend-duckdb::shim` (gated `cfg(all(feature = "duckdb", not(feature = "loadable_extension")))`):
  - `install_optimizer_hook(raw_db)` — unsafe wrapper that takes a `ffi::duckdb_database` handle and registers the C++ extension on it.
  - `PushdownFilter` (serde-deserialised from the shim's JSON) + `take_pushdown_filters(bind_ptr)` API.
- [x] **Wired into `rest_vtab::init`** — every init drains the side channel for its bind-data pointer, converts any pushed filters to `RestSimpleFilter { column, operator, value }` (REST takes them as URL params via the existing `RestRequestPlan::pushed_filters`).
- [x] **Wired into `rows_file_vtab::init`** — same pattern, with `pushdown_to_typed_filter` converting `(op, value, duck_type)` into `dbfy_provider::SimpleFilter { column, FilterOperator, ScalarValue }` for the rows-file pruner. Operators not in the typed set (`!=`, `IN`) and unsupported types are silently dropped.
- [x] **End-to-end test** (`pushdown_filter_appears_in_rest_url`): wiremock mounts a strict mock that requires `?min_id=1&status=active` and a fallback that returns a sentinel "FALLBACK" row. The optimizer hook extracts `WHERE id >= 1 AND status = 'active'`, the Rust side hands the filters to `RestTable`, the existing `pushdown:` block in the YAML config translates them to URL params, and the strict mock matches. If pushdown silently failed, the fallback would return the sentinel and the assertion catches it.
- [x] All 8 `--features duckdb` integration tests pass (4 pre-existing + 2 step-1 shim probes + 1 step-2 observer + 1 step-3 e2e). Workspace stays at 79 in the no-feature tier.

## Current filter-pushdown result

- Bundled-mode users get **transparent filter pushdown for `WHERE` clauses**: a SQL `SELECT … FROM dbfy_rest('…') WHERE id = 5 AND status = 'active'` triggers exactly one HTTP GET with the predicates in the query string, not a full scan + client-side filter. Same story for `dbfy_rows_file`: predicates flow into the L3 indexer's pruner so zone maps and bloom filters can skip chunks before any parsing.
- Loadable-extension mode (`pip install duckdb` + `LOAD 'dbfy.duckdb_extension'`) is unchanged — the C API still doesn't expose query-level optimizer hooks, so loadable users keep falling back to the SQL-level workaround paths (`dbfy duckdb-attach`, parameterised macros, materialised cache).
- The shim is intentionally conservative: anything beyond `column OP constant` (OR chains, sub-expressions, IN-list, unsupported types) is dropped from pushdown but DuckDB's regular filter operator still evaluates it, so the change is correctness-preserving even on edge cases.
- The build setup carries one fragility: the `libduckdb-sys` header location is found by globbing the target tree because `libduckdb-sys` doesn't emit `cargo:include=…`. A small upstream PR to add it would harden the build; until then the glob is documented in `build.rs`.
