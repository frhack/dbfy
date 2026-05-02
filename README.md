# dbfy

[![tests](https://github.com/typeeffect/dbfy/actions/workflows/test.yml/badge.svg)](https://github.com/typeeffect/dbfy/actions/workflows/test.yml)
[![release](https://img.shields.io/github/v/release/typeeffect/dbfy?include_prereleases)](https://github.com/typeeffect/dbfy/releases)
[![pypi](https://img.shields.io/pypi/v/dbfy.svg)](https://pypi.org/project/dbfy/)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Everything\* is a SQL table.** Each source — REST API, log file, CSV,
JSONL, syslog — gets its own typed table in your schema. Cross-source
JOINs work natively.

<sub>\*If yours isn't yet, file a bug — that's the contract.</sub>

```sql
-- Three sources, three tables, one query:
SELECT s.zone, count(*) AS hot_acked
  FROM dbfy_rest('https://api.example.com/readings', config := '...') r
  JOIN dbfy_rest('https://api.example.com/sensors',  config := '...') s USING (sensor_id)
  JOIN dbfy_rows_file('/var/log/fleet.jsonl',        config := '...') e
    ON e.sensor_id = r.sensor_id AND e.kind = 'alarm_acked'
 WHERE r.temperature > 22
 GROUP BY s.zone;
```

→ **[Quickstart](docs/quickstart.md)** — five minutes, zero to your first SQL query.
→ **[Showcases](examples/)** — four end-to-end demos that double as integration tests: [Service health](examples/showcase/) · [Auth audit](examples/auth-audit/) · [Finance recon](examples/finance-recon/) · [SaaS metrics](examples/saas-metrics/).
→ **[Hello-world per language](examples/lang/)** — same canonical query in 7 idiomatic shapes (Rust · Python · C# · Java · Kotlin · Node · Swift).

## Install

Pick your language:

```bash
# Python
pip install dbfy

# Node.js / TypeScript
npm install @typeeffect/dbfy

# .NET
dotnet add package Dbfy
```

```kotlin
// Maven / Gradle (Kotlin DSL)
dependencies {
    implementation("io.github.typeeffect:dbfy-kotlin:0.4.1")        // Kotlin coroutine API
    // -- or, for Java consumers --
    implementation("io.github.typeeffect:dbfy-jvm:0.4.1")
    runtimeOnly    ("io.github.typeeffect:dbfy-jvm:0.4.1:natives-linux-x86_64")
}
```

```swift
// Swift Package Manager
dependencies: [
    .package(url: "https://github.com/typeeffect/dbfy", from: "0.4.1"),
],
```

**CLI binary** (Linux x86_64, macOS arm64) — download from [Releases](https://github.com/typeeffect/dbfy/releases) and add to `PATH`.

**DuckDB extension** — same Releases page (`dbfy-<target>.duckdb_extension`):
```sql
LOAD 'path/to/dbfy.duckdb_extension';
```

**From source** (Rust 1.85+):
```bash
git clone https://github.com/typeeffect/dbfy && cd dbfy
cargo build --release -p dbfy-cli
```

## Supported languages and modes

### Modes

| Mode | Install | Use |
|---|---|---|
| **CLI binary** | [Releases](https://github.com/typeeffect/dbfy/releases) (Linux x86_64, macOS arm64) | `dbfy query --config x.yaml "SELECT …"` |
| **DuckDB extension** | [Releases](https://github.com/typeeffect/dbfy/releases) → `LOAD 'dbfy.duckdb_extension'` | `SELECT * FROM dbfy_rest('…')` / `dbfy_rows_file('…')` |
| **Python** | `pip install dbfy` | `import dbfy; engine = dbfy.Engine.from_yaml(…)` (sync + `asyncio`, PyArrow zero-copy) |
| **Rust library** | path / git dep on `dbfy-frontend-datafusion` | `use dbfy_frontend_datafusion::Engine;` |
| **C** | `libdbfy.{a,so}` + `dbfy.h` (cbindgen-generated) | engine lifecycle + Arrow C Data Interface |
| **Java** | Maven `io.github.typeeffect:dbfy-jvm:0.4.1` + classifier `natives-<rid>` | sync `byte[] queryArrowIpc(sql)` + async `CompletableFuture<byte[]> queryAsyncArrowIpc(sql)` (FFI callback → `complete`/`completeExceptionally`, JNI `AttachCurrentThread` from tokio worker); Arrow IPC bytes → `ArrowStreamReader` |
| **Kotlin** | Maven `io.github.typeeffect:dbfy-kotlin:0.4.1` (transitively pulls `dbfy-jvm`) | idiomatic `suspend fun query(sql)` + `Flow<ByteArray>` streaming, exceptions thrown directly (no `ExecutionException` unwrap) |
| **C# / .NET** | `dotnet add package Dbfy` | `using Dbfy; var engine = Engine.FromYaml(…)` — sync `Query()` + async `Task<Result> QueryAsync(sql, ct)` (FFI callback → `TaskCompletionSource`, no thread blocking, continuations off the tokio worker); Apache.Arrow `RecordBatch` zero-copy via the C Data Interface; net8.0+ |
| **Node.js** | npm `@typeeffect/dbfy@0.4.1` (prebuilt binaries for linux/macOS/windows × x64/arm64) | `import { Engine } from '@typeeffect/dbfy';` async `Promise<Buffer>` from `engine.query(sql)` (napi-rs ThreadsafeFunction → V8 main thread) + sync `querySync()` |
| **Swift / iOS / macOS** | SwiftPM `https://github.com/typeeffect/dbfy` from 0.4.1 (binaryTarget xcframework) | `import Dbfy; let engine = try Engine.fromYaml(yaml)` — async/await `try await engine.query(sql)` via `withCheckedThrowingContinuation`; macOS 12+, iOS 15+, Mac Catalyst 15+ |

### Sources

| Kind | Status | Coverage |
|---|---|---|
| **REST / HTTP** | ✅ stable | page / offset / cursor / link-header pagination · 4 auth modes · retry + `Retry-After` · in-memory TTL + singleflight HTTP cache · filter pushdown into URL params |
| **Line-delimited files** | ✅ stable | JSONL · CSV · logfmt · syslog (RFC 5424) · regex (CLF / ELF) — with L3 indexing (zone maps + bloom + incremental EXTEND on append) and glob multi-file expansion |
| **Parquet** | ✅ stable | local files + glob, schema auto-discovery, **predicate + projection + row-group pushdown** native (DataFusion ListingTable) |
| **Excel** (`.xlsx` / `.xls`) | ✅ stable | sheet selection, **predicate + projection + limit pushdown applied at row-iteration time** (skip-on-read, no full materialisation) |
| **GraphQL** | ✅ stable | POST + bearer auth, JSONPath root extraction, **variables pushdown** via `pushdown.variables` mapping (`WHERE col = lit` → GraphQL variable) |
| **PostgreSQL** | ✅ stable | read-only `SELECT` over the wire — **filter + projection + limit pushdown** translated into native `WHERE / SELECT / LIMIT` so the Postgres planner can use indexes |
| **LDAP / LDAPS** | ✅ stable | anonymous + simple bind, base / one / sub scope, **filter pushdown** that AND-merges SQL `WHERE` predicates into native LDAP filter syntax (`(&(uid=mario)(objectClass=person))`) with RFC 4515 value escaping; multi-valued attributes joined; synthetic `__dn__` column exposes entry DN |
| **In-memory programmatic** | ✅ stable | static Arrow `RecordBatch` provider · Python-defined custom providers via Arrow C Data Interface |
| **Coming next** | 🟡 wishlist — [vote with an issue](https://github.com/typeeffect/dbfy/issues?q=is%3Aissue+label%3Asource-request) | MySQL · gRPC · Parquet remote (S3/GCS) · Kafka · MongoDB · Loki / Prometheus · HTML / DOM scraping |

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
  dbfy-cli/ dbfy-py/ dbfy-c/ dbfy-jni/ dbfy-node/   # language native crates
bindings/
  csharp/                       # .NET (NuGet `Dbfy`)
  jvm/dbfy-jvm/                 # Java (Maven `io.github.typeeffect:dbfy-jvm`)
  jvm/dbfy-kotlin/              # Kotlin (Maven `io.github.typeeffect:dbfy-kotlin`)
  swift/                        # SwiftPM `https://github.com/typeeffect/dbfy`
docs/
  quickstart.md
  pushdown-matrix.md            # which operator pushes down per source
  implementation-plan.md
examples/
  showcase/                     # service-health demo (REST × JSONL × syslog)
  auth-audit/                   # LDAP × Postgres × syslog
  finance-recon/                # Excel × Parquet × REST
  saas-metrics/                 # GraphQL × Postgres × Excel
  lang/                         # one canonical hello-world per binding
```

## Documentation

- **[Quickstart](docs/quickstart.md)** — install, build, first query (CLI + DuckDB).
- **[Pushdown matrix](docs/pushdown-matrix.md)** — what each source pushes down (filter / projection / limit / aggregates) and how it appears on the wire.
- **[CHANGELOG](CHANGELOG.md)** — release notes 0.1 → today.
- [Showcases](examples/) — four runnable demos that double as integration tests:
  - [Service health](examples/showcase/) — GitHub API × 50k JSONL × 5×syslog
  - [Auth audit](examples/auth-audit/) — LDAP × Postgres × syslog (login storm + orphan account detection)
  - [Finance recon](examples/finance-recon/) — Excel × Parquet × REST (deals reconciliation)
  - [SaaS metrics](examples/saas-metrics/) — GraphQL × Postgres × Excel (ARPU vs usage cohort)
- [Hello-world per language](examples/lang/) — same canonical query in 7 idiomatic shapes.
- [Implementation plan](docs/implementation-plan.md) — milestone-by-milestone history.
- [DuckDB extension reference](crates/dbfy-frontend-duckdb/README.md) — table-function syntax, demo, capabilities matrix.
- [Python bindings](crates/dbfy-py/README.md) — `maturin develop` wheel + API.
- [Maven publishing setup](bindings/jvm/PUBLISHING.md) — Sonatype Central + GPG signing for the JVM artifacts.
- [restsql_spec.md](restsql_spec.md) — original product spec.

## License

Apache-2.0.
