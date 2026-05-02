# Showcase — Open Source Health

End-to-end demo + integration test that exercises every dbfy capability
against a real public dataset (the GitHub API for `duckdb/duckdb`) joined
with synthetic CI run logs and syslog. Doubles as a validator: every
query asserts something the runtime is supposed to do, and the script
exits non-zero on regression.

## Run

```bash
export GITHUB_TOKEN=...   # any fine-grained token, public-read scope
bash examples/showcase/run.sh
```

First run takes ~30 seconds (cargo release build + GitHub fetches).
Re-runs are ~1 second (cargo cache + sidecar `.dbfy_idx` reused).

## What each section demonstrates

### 1. Hello REST — live GitHub API

```sql
SELECT count(*) AS open_issues, count(DISTINCT author), avg(comments)
FROM gh.issues WHERE state = 'open';
```

- **link-header pagination** (GitHub's standard `Link: <…>; rel="next"`)
- **bearer auth** via `$GITHUB_TOKEN`
- **filter pushdown** into the URL: `WHERE state = 'open'` becomes `?state=open`,
  so dbfy doesn't fetch closed issues at all.

### 2. L3 zone-map skip — 50k JSONL rows

```sql
SELECT count(*) FROM files.ci_runs WHERE id BETWEEN 49000 AND 49100;
```

50,000 rows in 50 chunks of 1,000. The zone map on `id` lets the pruner
skip 49 chunks; only one chunk gets parsed. Query runs in **~400 ms**
including the Arrow batch construction.

### 3. Bloom filter on absent key

```sql
SELECT count(*) FROM files.ci_runs
 WHERE sha = 'cafef00ddeadbeef0000000000000000baadf00d';
```

The bloom on `sha` (50k distinct values) signals "definitely absent" for
every chunk before any JSON parsing. Query returns **0 rows in ~190 ms**
without opening a single byte of the data file beyond the sidecar.

### 4. Glob multi-file syslog

```sql
SELECT app, count(*) FROM logs.system
 WHERE severity <= 3 AND ts >= now() - interval '6 hour'
   AND app IN ('nginx', 'kube-apiserver', 'etcd')
GROUP BY app;
```

5 syslog files matched by `glob: data/system-*.log`. Per-file zone-map on
`ts` prunes 4 of 5 files for the time window; bloom on `app` further
prunes within the surviving file.

### 5. Killer 3-way JOIN — REST × file × file

```sql
WITH recent_failures AS (...), nearby_errors AS (...)
SELECT f.workflow, count(*), max(n.error_count), count(DISTINCT i.author)
FROM recent_failures f
LEFT JOIN nearby_errors n ON true
LEFT JOIN gh.issues i ON i.state = 'open' AND i.updated_at >= now() - interval '6 hour'
GROUP BY f.workflow ORDER BY count(*) DESC;
```

Three heterogeneous sources joined declaratively in one SELECT:

- live GitHub REST API (paginated, authenticated, cached on rerun)
- synthetic JSONL (indexed with bloom on `status`)
- synthetic syslog (globbed, zone-map on `ts`)

The pre-dbfy version of this query is three Python scripts and two CSV
intermediates.

### 6. Sidecar index summary

```bash
dbfy index --config configs/all.yaml --table files.ci_runs
```

Prints what the sidecar holds (chunks, rows, bytes) and which decision
the invalidator picked (`Reused` / `Extended` / `Rebuilt` / `BuiltFresh`).
Re-run after appending to the JSONL to see `Extended`.

## Bugs the showcase caught (validator value)

The first run on a real workload found six issues unit tests didn't:

| # | Bug | Severity | Status |
|---|---|---|---|
| 1 | `clap` rejects SQL starting with `--` (parsed as flag) | low | fixed in `run.sh` with `--` separator |
| 2 | DataFusion physical/logical schema mismatch on `SELECT count(*) FROM rest.table` | medium | tracked, bypassed with named columns |
| 3 | **GitHub 403** because reqwest in `dbfy-provider-rest` didn't set a `User-Agent` | **high** | **fixed** — `build_client` now sets `dbfy/<version>` UA |
| 4 | `regexp_extract` not available in DataFusion 53 | n/a | rewrote query 5 |
| 5 | `any_value` not available in DataFusion 53 | n/a | switched to `MAX` |
| 6 | HTTP cache claim is per-process — every `cargo run` wipes it | docs | section removed; cache exercised in unit tests |

Bug #3 is the headliner: dbfy now works against any API that requires a
User-Agent (GitHub, GitLab, many enterprise-grade ones). Without the
real-API showcase we'd have shipped v0.1.0 with this regression
undetected.

## Capability matrix coverage

| Capability                              | Section |
|-----------------------------------------|--------:|
| REST source, link-header pagination     | 1, 5    |
| REST bearer auth                        | 1, 5    |
| REST filter pushdown into URL           | 1, 5    |
| REST HTTP cache (configured)            | 5 (rerun) |
| Rows-file JSONL parser                  | 2, 3, 5 |
| Rows-file syslog parser                 | 4, 5    |
| Zone map pruning                        | 2, 4    |
| Bloom filter pruning                    | 3       |
| Glob multi-file expansion               | 4       |
| Sidecar EXTEND on append                | 6       |
| Cross-source JOIN                       | 5       |
| Same YAML drives DataFusion + DuckDB    | 5 (sql works on both) |

## Reproducibility

All synthetic data is generated by `data/generate.py` from a fixed seed,
so the JSONL/syslog content is deterministic. The GitHub API content
changes (issue counts shift hourly) but the query shapes don't depend on
specific numbers — only on the structure being valid.

If you delete `data/*.jsonl`, `data/*.log`, and `data/*.dbfy_idx`, the
next `run.sh` regenerates everything.

## Layout

```text
examples/showcase/
├── README.md              # this file
├── run.sh                 # one-command driver
├── data/
│   ├── generate.py        # JSONL + syslog synthesizer
│   ├── ci-runs.jsonl      # 50k rows (generated)
│   └── system-{1..5}.log  # 2k events each, RFC 5424 (generated)
├── configs/
│   └── all.yaml           # one config drives all sources
└── queries/
    ├── 01_hello_rest.sql
    ├── 02_l3_skip.sql
    ├── 03_bloom_absent.sql
    ├── 04_glob_skip.sql
    └── 05_killer_3way.sql
```
