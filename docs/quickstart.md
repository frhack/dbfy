# dbfy — Quickstart

Five-minute path from zero to running federated SQL. Pick whichever
section matches your starting point — **a local log file**, **a REST
endpoint**, or **DuckDB**. The three paths share the same YAML config
schema, so a config you generate for one frontend works in the other.

## 1. Build

```bash
git clone https://github.com/frhack/dbfy && cd dbfy

# Standalone CLI (under target/release/dbfy-cli):
cargo build --release -p dbfy-cli
```

The DuckDB extension build is opt-in (it pulls bundled DuckDB the first
time, ~30 minutes; subsequent builds are incremental):

```bash
cargo build -p dbfy-frontend-duckdb --features loadable_extension --release --jobs 1
strip target/release/libdbfy_duckdb.so
python3 crates/dbfy-frontend-duckdb/scripts/append_metadata.py \
    target/release/libdbfy_duckdb.so \
    --output target/release/dbfy.duckdb_extension --duckdb-capi-version v1.2.0
```

## 2. Path A — query a JSONL log file

You have `app.jsonl` with one JSON object per line and want to run SQL
on it.

```bash
# 1. Auto-detect schema and emit a starter YAML config:
./target/release/dbfy-cli detect /var/log/app.jsonl > app.yaml

# 2. Verify the config is valid:
./target/release/dbfy-cli validate app.yaml

# 3. Run SQL:
./target/release/dbfy-cli query --config app.yaml \
    "SELECT level, count(*) FROM files.events GROUP BY level ORDER BY 2 DESC"
```

The first query builds a `.dbfy_idx` sidecar next to the file (zone
maps + bloom filters on whatever columns `detect` heuristically picked
as indexable). Subsequent queries reuse it; if the file grows on the
end, the next query incrementally indexes only the new tail.

Pre-warm the sidecar explicitly (e.g. from cron or a build pipeline):

```bash
./target/release/dbfy-cli index --config app.yaml --table files.events
```

## 3. Path B — query a REST endpoint

You have an HTTP/JSON endpoint and want to query it as a SQL table.

```bash
# 1. Probe the endpoint, infer schema:
./target/release/dbfy-cli probe https://api.example.com/users > api.yaml

# 2. Run SQL:
./target/release/dbfy-cli query --config api.yaml \
    "SELECT id, name FROM api.items LIMIT 10"
```

`probe` performs one GET, finds the row array via a candidate ladder
(`$.data[*]`, `$.results[*]`, `$.items[*]`, …), infers per-field types
(int / float / timestamp / string / boolean), and emits a YAML config
ready to consume.

For auth, pass the env var holding your bearer token:

```bash
./target/release/dbfy-cli probe https://api.github.com/user \
    --auth-bearer-env GITHUB_TOKEN > github.yaml
```

## 4. Path C — interactive wizard

If you don't yet know whether your source is a file or a REST endpoint:

```bash
./target/release/dbfy-cli init > config.yaml
```

The wizard prompts for source kind, name, and target, then delegates to
`detect` (file path) or `probe` (URL) under the hood.

## 5. Path D — DuckDB

Use the extension from any DuckDB host. The example below runs from the
DuckDB Python package, but the same SQL works in `duckdb` CLI, R, JVM,
JS, etc.

```bash
pip install duckdb
```

```python
import duckdb
con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'target/release/dbfy.duckdb_extension'")

# Untyped exploration: every JSON object at $.data[*] returned as one VARCHAR row.
print(con.execute("SELECT count(*) FROM dbfy_rest('https://api.example.com/users')").fetchone())

# Typed columns + projection pushdown via inline YAML config.
config = """
root: $.data[*]
columns:
  id:   { path: "$.id",   type: int64  }
  name: { path: "$.name", type: string }
"""
print(con.execute(
    "SELECT id, name FROM dbfy_rest(?, config := ?) LIMIT 5",
    [f"https://api.example.com/users", config],
).fetchall())

# Same story for files:
file_config = """
parser:
  format: jsonl
  columns:
    - { name: id,    path: "$.id",    type: int64 }
    - { name: level, path: "$.level", type: string }
indexed_columns:
  - { name: id,    kind: zone_map }
  - { name: level, kind: bloom    }
"""
print(con.execute(
    "SELECT level, count(*) FROM dbfy_rows_file(?, config := ?) GROUP BY level",
    ["/var/log/app.jsonl", file_config],
).fetchall())
```

## 6. Path E — DuckDB ATTACH-style: every config table as a SQL view

If you've already produced a `config.yaml` (via `detect`, `probe`, or
`init`), the CLI can emit a SQL script that registers every configured
table as a `CREATE OR REPLACE VIEW <schema>.<table>` over the right
table function. Subsequent queries are pure DuckDB — no need to repeat
URL/config in every SELECT.

```bash
./target/release/dbfy-cli duckdb-attach \
    --config config.yaml \
    --schema api \
    --extension target/release/dbfy.duckdb_extension > attach.sql

duckdb mydb.duckdb < attach.sql
```

```sql
-- After the script runs, every configured table is a normal DuckDB view:
SELECT * FROM api.users;
SELECT u.name, count(o.id)
  FROM api.users u
  JOIN api.orders o USING (user_id)
  GROUP BY u.name;
```

The full `ATTACH 'config.yaml' (TYPE dbfy)` syntax is a planned upgrade
once `duckdb-rs` exposes the `StorageExtension` C API; today's emitter
is the SQL-equivalent shortcut and the `<schema>.<table>` contract is
forward-compatible.

## 7. Federated query — REST × REST × file

The point of dbfy is putting heterogeneous sources in one SELECT. After
the above paths, you can do this in a single statement:

```sql
SELECT s.zone, count(*) AS hot_acked, round(avg(r.temperature), 2) AS avg_temp
  FROM api.readings r
  JOIN api.sensors  s USING (sensor_id)
  JOIN files.events e
    ON e.sensor_id = r.sensor_id
   AND e.kind = 'alarm_acked'
   AND abs(epoch(e.ts) - epoch(r.timestamp)) <= 90
 WHERE r.temperature > 22
 GROUP BY s.zone
 ORDER BY hot_acked DESC;
```

Pre-dbfy version of this query: three Python scripts, two CSV
intermediates, one `pd.merge` chain, and a regex over the log file.
Now: one SELECT.

## Where to next

- `crates/dbfy-frontend-duckdb/examples/sensor_analytics.py` — annotated
  end-to-end demo of all six patterns above (run it after building the
  loadable extension).
- `crates/dbfy-provider-rows-file/examples/log_analytics.rs` — Rust
  example showing the L3 index doing 98–100 % chunk-skip on selective
  filters across 50k synthetic log rows.
- [DuckDB extension reference](../crates/dbfy-frontend-duckdb/README.md) — full table-function surface, capabilities matrix.
- [Python bindings](../crates/dbfy-py/README.md) — `maturin develop` wheel + API + asyncio.
- [Implementation plan](implementation-plan.md) — what was built, why, and what's next.
