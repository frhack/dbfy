# Pushdown matrix

What each source pushes down to its backend, and what stays in the
DataFusion engine. "Native" means the predicate / projection /
limit travels in the protocol the source speaks — over the wire to
Postgres, into the LDAP filter, into the GraphQL POST body, into
the URL params for REST, into the parquet file metadata for
row-group pruning, etc. Everything else is a post-scan operation
DataFusion runs on the buffered RecordBatch.

The point of pushing down is correctness *and* speed: it shrinks
the wire footprint and lets the backend's own indexes do the
matching work.

## Per source

| Source            | `=` `!=` | `<` `<=` `>` `>=` | `IS [NOT] NULL` | `IN (…)` | `LIMIT` | Projection | Aggregates |
|-------------------|----------|--------------------|------------------|----------|---------|------------|------------|
| **REST**          | ✅ via URL params (`pushdown.filters`) | configurable per-column param | ❌ | ❌ | ✅ via `pushdown.limit` | ✅ via `pushdown.projection.style: comma_separated` | ❌ |
| **rows-file** (jsonl/csv/syslog/regex/logfmt) | ✅ via L3 zone-map + bloom skip | ✅ zone-map | ❌ | ❌ | ✅ chunk-level early break | ✅ column subset only | ❌ |
| **Parquet**       | ✅ row-group pruning + late materialise | ✅ | ✅ | ✅ | ✅ | ✅ | partial (counts via metadata) |
| **Excel**         | ✅ predicate evaluated during calamine row stream | ✅ string-comparison semantics | ✅ | ✅ | ✅ early break | ✅ project at array build time | ❌ |
| **GraphQL**       | ✅ via `pushdown.variables` mapping (`WHERE col = lit` → `$variable`) | ❌ (single `=` only today) | ❌ | ❌ | ❌ (depends on the GraphQL query body) | n/a (the query body picks fields) | ❌ |
| **PostgreSQL**    | ✅ native `WHERE col = literal` over the wire | ✅ | ✅ | ✅ | ✅ | ✅ `SELECT col1, col2…` | ❌ today (server-side `count(*)` is a planned 0.5 win) |
| **LDAP / LDAPS**  | ✅ AND-merged into the table filter as `(attr=val)` / `(!(attr=val))` | ✅ via `(attr<=val)` / `(attr>=val)` (note: `<` and `>` synthesised as `(&(<=v)(!(=v)))`) | ✅ via `(!(attr=*))` / `(attr=*)` | ✅ as `(\|(attr=v1)(attr=v2)…)` | post-scan today | ✅ `attrs` requested in the search | ❌ |
| **Programmatic**  | depends on the provider's `ProviderCapabilities` | depends | depends | depends | depends | ✅ | ❌ |

## How "native" gets verified

Every source has at least one test that asserts the pushdown
arrives at the backend in the expected shape:

| Source     | Test                                                                                           |
|------------|------------------------------------------------------------------------------------------------|
| REST       | `select_count_star_against_rest_source` (wiremock asserts query string)                       |
| rows-file  | `dbfy-provider-rows-file` zone-map + bloom unit tests (skip ratio measured)                   |
| Parquet    | `tests/integration_parquet.rs` — TPC-H-lite, row-group pruning observed via correctness       |
| Excel      | `excel_pushdown_filters_at_read_time` — 5-row workbook, asserts 3 rows survive the predicate  |
| GraphQL    | `graphql_pushdown_sends_variable_values_to_endpoint` — `body_partial_json` matches the literal |
| Postgres   | `postgres_filter_translation_emits_correct_sql` (unit) + `tests/integration_postgres.rs` (real PG) |
| LDAP       | `ldap_filter_translation_*` (unit) + `tests/integration_ldap.rs` (real OpenLDAP)              |
| Programmatic | `dbfy-provider-static` round-trip tests                                                     |

## What's *not* pushed down (today)

- **Cross-source joins**: each side scans independently with its own
  pushdown; the JOIN is a DataFusion HashJoin / SortMergeJoin in the
  engine. This is correct but means a JOIN that filters one side
  heavily still over-reads the unfiltered side. The 0.5 milestone
  adds `dynamic_filter` semi-join propagation so a heavily-filtered
  Postgres scan can shrink the LDAP scan it joins to.
- **Aggregates**: `count(*)`, `sum`, `avg` always run in DataFusion.
  Postgres / Parquet metadata-level aggregate pushdown is on the
  roadmap.
- **Window functions**: never pushed down. Always evaluated by the
  engine.
- **String functions / casts in WHERE**: `WHERE upper(name) =
  'ALICE'` falls back to post-scan filtering on every source.

## Adding pushdown for a new operator

1. Extend `try_extract_pushed_filter` in
   `crates/dbfy-frontend-datafusion/src/lib.rs` to recognise the new
   `Expr` shape and emit a `PushedFilter` describing it.
2. For each source provider, decide whether to support it:
   - Postgres: extend `PostgresTableProvider::translate_filter` to
     emit the SQL.
   - LDAP: extend `LdapTableProvider::translate_filter` to emit the
     filter fragment.
   - Excel: extend `excel_row_matches` for in-process evaluation.
3. Add a unit test on the translator (string equality on the emitted
   fragment) and an integration test against the real backend.
4. Update this matrix.
