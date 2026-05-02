# Changelog

All notable changes to dbfy. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning is [SemVer](https://semver.org/spec/v2.0.0.html).

## [0.4.1] — 2026-05-02

### Hardening release. No API changes.

#### Added — testing

- **Postgres integration tests** against a real `postgres:11-alpine`
  container via `testcontainers-modules` (5 cases — pushdown,
  count(*), JOIN, LIMIT, streaming). Gated `#[ignore]`; run via
  `.github/workflows/integration.yml`.
- **LDAP integration tests** against `bitnami/openldap:2.6` (5 cases
  — simple bind, eq pushdown, IN-list, `__dn__` synthetic column,
  ORDER BY + LIMIT).
- **Parquet TPC-H-lite integration test** (4 cases on a 5 000-row,
  2-row-group synthetic dataset).
- **CI workflow** `integration.yml` matrix on ubuntu-latest with
  pre-pulled images.

#### Added — showcases

Every showcase doubles as an integration test via assertion
validators in `run.sh`.

- **`examples/auth-audit/`** — LDAP × Postgres × syslog. Killer
  query: per-user `audit_count` vs `syslog_count` gap. Catches
  `mallory` orphan, `eve` login storm.
- **`examples/finance-recon/`** — Excel × Parquet × REST. Killer
  query: per-deal `warehouse_missing | payment_pending |
  amount_mismatch | reconciled` status.
- **`examples/saas-metrics/`** — GraphQL × Postgres × Excel. Killer
  query: ARPU per million events vs tier budget share. Catches
  `tyrell` shelf-ware, `initech` MRR drift.

#### Added — language hello-worlds

`examples/lang/{rust,python,csharp,java,kotlin,node,swift}/` — same
canonical query, idiomatic shape per binding.

#### Added — docs

- `docs/pushdown-matrix.md` — operator coverage per source.
- `CHANGELOG.md` (this file).

## [0.4.0] — 2026-05-02

### Added

- **`dbfy-jvm` Maven Central artifact** — Java JNI binding with
  `CompletableFuture<byte[]> queryAsyncArrowIpc(sql)`. Per-platform
  classifier jars (`natives-linux-x86_64`, `natives-osx-arm64`, …).
- **`dbfy-kotlin` Maven Central artifact** — Kotlin coroutine
  extensions over `dbfy-jvm`: `suspend fun query(sql)` +
  `Flow<ByteArray> queryStream(sql)`. Idiomatic exceptions (no
  `ExecutionException` unwrap).
- **`@frhack/dbfy` npm package** via napi-rs. Per-platform
  prebuilds (linux x64/arm64, macOS x64/arm64, windows x64).
  `await engine.query(sql)` returns `Promise<Buffer>` resolved on
  the V8 main thread.
- **Swift Package Manager bindings** — binaryTarget xcframework
  covering macOS 12+, iOS 15+, Mac Catalyst 15+. `actor Engine`
  with `try await engine.query(sql)` via
  `withCheckedThrowingContinuation`.
- **`bindings-smoke.yml`** CI workflow runs each binding's unit
  tests on every PR.
- **`maven-jvm.yml`**, **`npm.yml`**, **`swift.yml`** publish
  workflows triggered on tag push.

## [0.3.0] — 2026-05-02

### Added

- **LDAP source** with native filter pushdown. AND-merges SQL
  `WHERE` predicates into native LDAP filter syntax. Anonymous +
  simple bind. RFC 4515 value escaping. `__dn__` synthetic column.
- **Streaming Postgres + LDAP** — replaces the fetch-all-then-
  MemoryExec pattern with `RecordBatchReceiverStream` + custom
  ExecutionPlan. Cancellation propagates via channel-drop signal:
  the producer task exits and the underlying `Client` / `Ldap`
  handle drops, releasing wire connections.
- **C# `Engine.QueryAsync(sql, ct)`** via FFI callback →
  `TaskCompletionSource<Result>`. Continuations off the tokio
  worker thread (`RunContinuationsAsynchronously`).
- **Java `Dbfy.queryAsyncArrowIpc(sql)`** via JNI GlobalRef +
  `JavaVM::AttachCurrentThread`. Same FFI design as C#.
- **Excel `spawn_blocking`** — calamine I/O moved to the blocking
  thread pool so it doesn't sequester tokio workers.

### Source coverage matrix (8/8 with pushdown)

REST · rows-file · Parquet · Excel · GraphQL · PostgreSQL · LDAP ·
Programmatic.

## [0.2.0] — 2026-04-29

### Added

- Project rename `restsql` → `dbfy`. Crate split:
  `dbfy-provider*` / `dbfy-frontend-*` / `dbfy-{c,jni,py,cli}`.
- **Postgres source** with filter / projection / LIMIT pushdown
  into native wire SQL.
- **GraphQL source** with `pushdown.variables` mapping (`WHERE col
  = lit` → GraphQL variable).
- **Excel source** with predicate-during-row-stream pushdown.
- **Parquet source** via DataFusion `ListingTable` (row-group +
  projection pushdown native).
- **`dbfy-c` C ABI** with cbindgen-generated header.
- **`Dbfy` NuGet package** for .NET (sync `Engine.Query()`,
  Apache.Arrow zero-copy via the C Data Interface).
- **GitHub Actions release pipeline** — CLI binaries (Linux x86_64,
  macOS arm64), DuckDB extension, PyPI publish via trusted
  publishing.

## [0.1.0] — Earlier 2026

Initial release. REST + rows-file (JSONL / CSV / logfmt / syslog /
regex) sources. Single SQL surface via DataFusion engine.
Programmatic provider trait. Python bindings via PyO3 + maturin.
DuckDB extension via shim with `dbfy_rest()` / `dbfy_rows_file()`
table functions.
