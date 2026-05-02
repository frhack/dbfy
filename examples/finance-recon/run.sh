#!/usr/bin/env bash
# finance-recon showcase — Excel × Parquet × REST in one SQL.

set -euo pipefail

cd "$(dirname "$0")/../.."
SHOWCASE=examples/finance-recon

# 1. Generate the three datasets if they don't exist.
if [[ ! -f "$SHOWCASE/data/deals.xlsx" ]]; then
    python3 "$SHOWCASE/data/generate.py"
fi

# 2. Serve payments.json over HTTP so the REST source can pull it.
#    A trivial python http server in the background; tear down on
#    EXIT so re-runs are clean.
python3 -m http.server 8765 --directory "$SHOWCASE/data" >/dev/null 2>&1 &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null || true" EXIT
sleep 0.5  # let http.server bind

# 3. Build CLI.
cargo build -q --release -p dbfy-cli

run_query() {
    local title="$1"
    local sql_file="$2"
    echo
    echo "==> $title"
    echo "    ($sql_file)"
    echo
    target/release/dbfy query \
        --config "$SHOWCASE/configs/finance.yaml" \
        -- "$(cat "$sql_file")"
}

run_query "Q1 — sales summary"          "$SHOWCASE/queries/01_sales_summary.sql"
run_query "Q2 — warehouse drift"        "$SHOWCASE/queries/02_warehouse_drift.sql"
run_query "Q3 — overdue payments"       "$SHOWCASE/queries/03_overdue_payments.sql"
run_query "Q4 — killer 3-way recon"     "$SHOWCASE/queries/04_killer_recon.sql"

echo
echo "==> validating expected findings"

# Q2: D008 (Massive Dynamic) is "closed" in sales but not in
# warehouse — the recon must surface it.
echo "    expecting D008 in warehouse-drift"
target/release/dbfy query --config "$SHOWCASE/configs/finance.yaml" -- \
    "SELECT s.deal_id FROM sales.deals s LEFT JOIN warehouse.orders w ON w.deal_id = s.deal_id WHERE s.status = 'closed' AND w.deal_id IS NULL" \
    | grep -q '| D008 *|' \
    || { echo "FAIL: expected D008 in warehouse drift"; exit 1; }

# Q3: D006 is in warehouse but not in payments — overdue.
echo "    expecting D006 in overdue-payments"
target/release/dbfy query --config "$SHOWCASE/configs/finance.yaml" -- \
    "SELECT w.deal_id FROM warehouse.orders w LEFT JOIN payments.transactions p ON p.deal_id = w.deal_id WHERE p.deal_id IS NULL" \
    | grep -q '| D006 *|' \
    || { echo "FAIL: expected D006 in overdue payments"; exit 1; }

echo
echo "OK — finance-recon showcase passed."
