#!/usr/bin/env bash
# auth-audit showcase — LDAP × Postgres × syslog joined in one SQL.
#
# Brings up an OpenLDAP + Postgres pair via docker compose, generates
# the syslog file, then runs four progressively harder queries. The
# fourth one (`04_killer_3way.sql`) is what the showcase actually
# advertises.
#
# Run:
#   bash examples/auth-audit/run.sh
#
# Tear down with `docker compose down -v` from `examples/auth-audit/`.

set -euo pipefail

cd "$(dirname "$0")/../.."
SHOWCASE=examples/auth-audit

# 1. Stack up.
( cd "$SHOWCASE" && docker compose up -d --wait )

# 2. Generate the syslog file.
python3 "$SHOWCASE/data/generate-syslog.py"

# 3. Build dbfy CLI release-mode (cached after first invocation).
cargo build -q --release -p dbfy-cli

# 4. Configure auth.
export LDAP_PASSWORD=adminpass

run_query() {
    local title="$1"
    local sql_file="$2"
    echo
    echo "==> $title"
    echo "    ($sql_file)"
    echo
    target/release/dbfy query \
        --config "$SHOWCASE/configs/auth.yaml" \
        -- "$(cat "$sql_file")"
}

run_query "Q1 — directory user count"        "$SHOWCASE/queries/01_directory_summary.sql"
run_query "Q2 — failed-login storms"         "$SHOWCASE/queries/02_failed_login_storms.sql"
run_query "Q3 — orphan audit records"        "$SHOWCASE/queries/03_orphan_audit_records.sql"
run_query "Q4 — killer: directory × audit × syslog" "$SHOWCASE/queries/04_killer_3way.sql"

# 5. Validators (these double the queries above as integration assertions).
echo
echo "==> validating expected findings"

# Q1 must report 8 users (the Bitnami seed list).
echo "    expecting 8 users in directory"
target/release/dbfy query --config "$SHOWCASE/configs/auth.yaml" -- \
    "SELECT count(*) FROM directory.users" | grep -q '^| 8 *|' \
    || { echo "FAIL: expected 8 users"; exit 1; }

# Q3 must spot exactly one orphan: 'mallory'.
echo "    expecting orphan: mallory"
target/release/dbfy query --config "$SHOWCASE/configs/auth.yaml" -- \
    "SELECT a.uid FROM audit.events a LEFT JOIN directory.users d ON d.uid = a.uid WHERE d.uid IS NULL GROUP BY a.uid" \
    | grep -q '| mallory *|' \
    || { echo "FAIL: expected mallory orphan"; exit 1; }

echo
echo "OK — auth-audit showcase passed."
