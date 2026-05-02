#!/usr/bin/env bash
# Open Source Health — dbfy showcase + validator.
#
# This script exercises every dbfy capability against a real public
# data source (GitHub API for duckdb/duckdb) joined with synthetic
# CI run logs and system syslog. It doubles as an integration test:
# every section asserts an expected property and exits non-zero on
# regression.
#
# Run:
#   export GITHUB_TOKEN=...      # any fine-grained token, public-read
#   bash examples/showcase/run.sh
#
# Re-runs are cheap: data is generated once, sidecars are reused, the
# HTTP cache makes the second invocation against GitHub a no-op.

set -euo pipefail

cd "$(dirname "$0")/../.."   # workspace root
SHOWCASE=examples/showcase

DBFY="cargo run --release -p dbfy-cli --quiet --"
PYTHON=${PYTHON:-python3}

if [ -z "${GITHUB_TOKEN:-}" ]; then
    echo "Error: GITHUB_TOKEN is not set." >&2
    echo "  See $SHOWCASE/README.md for token-creation steps." >&2
    exit 2
fi

# ────────────────────────────────────────────────────────────────────
# 0. Generate synthetic data (skipped on rerun if files exist).
# ────────────────────────────────────────────────────────────────────

if [ ! -f "$SHOWCASE/data/ci-runs.jsonl" ]; then
    echo "[setup] generating CI + syslog datasets…"
    $PYTHON "$SHOWCASE/data/generate.py" --out "$SHOWCASE/data"
fi

# Build dbfy-cli once up-front so subsequent timings reflect query
# cost, not compile cost. Release-mode build only happens on first
# run; cargo's incremental cache covers the rest.
echo "[setup] building dbfy-cli (release, may compile first time)…"
cargo build --release -p dbfy-cli --quiet

run_query() {
    local title="$1"
    local sql_file="$2"
    local config="${3:-$SHOWCASE/configs/all.yaml}"
    echo
    echo "─── $title ───"
    local sql
    sql=$(cat "$sql_file")
    local t0 t1
    t0=$(date +%s%N)
    # `--` separator before the positional SQL arg so clap doesn't try
    # to parse a leading `-- comment` as a flag.
    $DBFY query --config "$config" -- "$sql"
    t1=$(date +%s%N)
    printf "  ⤴  %.0f ms\n" $(( (t1 - t0) / 1000000 ))
}

# ────────────────────────────────────────────────────────────────────
# 1. Hello REST (live GitHub).
# ────────────────────────────────────────────────────────────────────
run_query "1. Hello REST: open issues on duckdb/duckdb" \
    "$SHOWCASE/queries/01_hello_rest.sql"

# ────────────────────────────────────────────────────────────────────
# 2. L3 zone-map skip ratio on JSONL.
# ────────────────────────────────────────────────────────────────────
run_query "2. L3 zone-map skip — 1/50 chunks read" \
    "$SHOWCASE/queries/02_l3_skip.sql"

# ────────────────────────────────────────────────────────────────────
# 3. Bloom filter on absent SHA.
# ────────────────────────────────────────────────────────────────────
run_query "3. Bloom on absent key — 0 chunks parsed" \
    "$SHOWCASE/queries/03_bloom_absent.sql"

# ────────────────────────────────────────────────────────────────────
# 4. Glob multi-file syslog.
# ────────────────────────────────────────────────────────────────────
run_query "4. Glob 5 syslog files, severity ≤ 3 in last 6h" \
    "$SHOWCASE/queries/04_glob_skip.sql"

# ────────────────────────────────────────────────────────────────────
# 5. Killer 3-way JOIN.
# ────────────────────────────────────────────────────────────────────
run_query "5. Killer query: GitHub × CI × syslog, one SELECT" \
    "$SHOWCASE/queries/05_killer_3way.sql"

# ────────────────────────────────────────────────────────────────────
# 6. Index summary — show what the L3 index actually contains.
# ────────────────────────────────────────────────────────────────────
echo
echo "─── 6. Sidecar index summary ───"
$DBFY index --config "$SHOWCASE/configs/all.yaml" --table files.ci_runs

echo
echo "✓ showcase complete. Re-run for instant cache hits."
