#!/usr/bin/env bash
# saas-metrics showcase — GraphQL × Postgres × Excel.

set -euo pipefail

cd "$(dirname "$0")/../.."
SHOWCASE=examples/saas-metrics

# 1. Start Postgres.
( cd "$SHOWCASE" && docker compose up -d --wait )

# 2. Generate the budget spreadsheet (only once).
[[ -f "$SHOWCASE/data/budget.xlsx" ]] || python3 "$SHOWCASE/data/budget.py"

# 3. Start the GraphQL stub in background.
python3 "$SHOWCASE/data/graphql-server.py" >/dev/null 2>&1 &
GQL_PID=$!
trap "kill $GQL_PID 2>/dev/null || true" EXIT
sleep 0.5

# 4. Build CLI.
cargo build -q --release -p dbfy-cli

run_query() {
    local title="$1"
    local sql_file="$2"
    echo
    echo "==> $title"
    echo "    ($sql_file)"
    echo
    target/release/dbfy query \
        --config "$SHOWCASE/configs/saas.yaml" \
        -- "$(cat "$sql_file")"
}

run_query "Q1 — usage summary"          "$SHOWCASE/queries/01_usage_summary.sql"
run_query "Q2 — revenue vs budget"      "$SHOWCASE/queries/02_revenue_vs_budget.sql"
run_query "Q3 — MRR drift"              "$SHOWCASE/queries/03_mrr_drift.sql"
run_query "Q4 — killer: ARPU vs usage"  "$SHOWCASE/queries/04_killer_arpu_vs_usage.sql"

echo
echo "==> validating expected findings"

# Initech (slug=initech) paid 12000 vs MRR 15000 → drift -3000.
echo "    expecting initech in mrr-drift"
target/release/dbfy query --config "$SHOWCASE/configs/saas.yaml" -- \
    "SELECT slug FROM billing.customers c JOIN billing.monthly_revenue r ON r.customer_id = c.id WHERE r.month = DATE '2026-04-01' AND r.actual_cents != c.mrr_cents" \
    | grep -q '| initech *|' \
    || { echo "FAIL: expected initech in MRR drift"; exit 1; }

echo
echo "OK — saas-metrics showcase passed."
