#!/usr/bin/env python3
"""Generate the three datasets the finance-recon showcase joins.

Story:
- The sales team tracks pending deals in a shared Excel workbook
  (because of course they do). Each row is a deal with an expected
  close date and amount.
- The data warehouse stores closed orders as a Parquet file (one
  row per closed order; this is what the BI team queries normally).
- The payments REST API is the source of truth for whether the
  customer actually paid.

The reconciliation question: for every deal the sales team marked
"closed", is there a matching warehouse entry, and was it paid?
Three sources, three formats, one SELECT.
"""

import pathlib

ROOT = pathlib.Path(__file__).resolve().parent

# --------------------------------------------------------------------
# Sales spreadsheet (Excel) — 12 rows, 3 columns. We use openpyxl
# directly (no rust_xlsxwriter) so the showcase doesn't need a
# Rust toolchain to regenerate data; openpyxl is in pyarrow's
# transitive deps for most environments. Falls back to a
# pre-committed file when openpyxl is unavailable.
# --------------------------------------------------------------------
def write_excel():
    try:
        from openpyxl import Workbook
    except ImportError:
        print("openpyxl not available, skipping xlsx generation")
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "deals"
    ws.append(["deal_id", "customer", "amount", "expected_close", "status"])

    rows = [
        ("D001", "ACME Corp",       12500.00, "2026-04-15", "closed"),
        ("D002", "Globex",           7800.50, "2026-04-20", "closed"),
        ("D003", "Initech",         44000.00, "2026-04-22", "closed"),
        ("D004", "Soylent",          3200.00, "2026-04-28", "pending"),
        ("D005", "Umbrella Corp",   91500.00, "2026-04-30", "closed"),
        ("D006", "Cyberdyne",       28000.00, "2026-05-05", "closed"),
        ("D007", "Tyrell",          15400.00, "2026-05-10", "pending"),
        ("D008", "Massive Dynamic", 67200.00, "2026-04-30", "closed"),
        ("D009", "Veidt Enterp.",    9900.00, "2026-05-15", "closed"),
        ("D010", "Pied Piper",       4500.00, "2026-05-01", "lost"),
        ("D011", "Hooli",            8700.00, "2026-04-29", "closed"),
        ("D012", "Aperture",        21000.00, "2026-05-03", "closed"),
    ]
    for r in rows:
        ws.append(r)

    out = ROOT / "deals.xlsx"
    wb.save(out)
    print(f"wrote {len(rows)} rows to {out}")


# --------------------------------------------------------------------
# Warehouse (Parquet) — what the BI team actually sees. NOT every
# deal lands here on time; some are delayed, some never make it.
# That's what the reconciliation will surface.
# --------------------------------------------------------------------
def write_parquet():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("pyarrow not available, skipping parquet generation")
        return

    rows = [
        # deal_id, amount_landed, landed_at
        ("D001", 12500.00, "2026-04-16T03:12:00"),
        ("D002",  7800.50, "2026-04-21T03:12:00"),
        ("D003", 44000.00, "2026-04-23T03:12:00"),
        # D004 is pending — not in warehouse yet
        ("D005", 91500.00, "2026-05-01T03:12:00"),
        ("D006", 28000.00, "2026-05-06T03:12:00"),
        # D007 is pending — not in warehouse
        # D008 (Massive Dynamic, 67200) is "closed" in sales but
        # missing from warehouse — this is what reconciliation
        # has to flag.
        ("D009",  9900.00, "2026-05-16T03:12:00"),
        # D010 was lost, no warehouse row
        ("D011",  8700.00, "2026-04-30T03:12:00"),
        ("D012", 21000.00, "2026-05-04T03:12:00"),
        # Extra ghost row in the warehouse: not a deal in sales
        # at all. Did it come from another channel? Recon flags it.
        ("D999", 50000.00, "2026-05-02T03:12:00"),
    ]

    table = pa.table({
        "deal_id":     [r[0] for r in rows],
        "amount":      [r[1] for r in rows],
        "landed_at":   [r[2] for r in rows],
    })
    out = ROOT / "warehouse.parquet"
    pq.write_table(table, out)
    print(f"wrote {len(rows)} rows to {out}")


# --------------------------------------------------------------------
# Payments REST snapshot — we don't actually run a server for the
# showcase; instead we serve a static JSON file via `python3 -m
# http.server`. The dbfy REST source treats it identically.
# --------------------------------------------------------------------
def write_rest_payload():
    import json
    payments = {
        "data": [
            {"deal_id": "D001", "paid_at": "2026-04-17T10:00:00", "amount": 12500.00, "status": "paid"},
            {"deal_id": "D002", "paid_at": "2026-04-22T11:30:00", "amount":  7800.50, "status": "paid"},
            {"deal_id": "D003", "paid_at": "2026-04-25T09:00:00", "amount": 44000.00, "status": "paid"},
            {"deal_id": "D005", "paid_at": "2026-05-03T14:15:00", "amount": 91500.00, "status": "paid"},
            # D006: warehouse has it landed, payments hasn't seen it
            #       yet. 5-day pending — recon flags it as overdue.
            {"deal_id": "D008", "paid_at": "2026-05-02T08:00:00", "amount": 67200.00, "status": "paid"},
            {"deal_id": "D009", "paid_at": "2026-05-18T12:00:00", "amount":  9900.00, "status": "paid"},
            {"deal_id": "D011", "paid_at": "2026-05-01T09:30:00", "amount":  8700.00, "status": "paid"},
            {"deal_id": "D012", "paid_at": "2026-05-06T10:00:00", "amount": 21000.00, "status": "paid"},
        ]
    }
    out = ROOT / "payments.json"
    out.write_text(json.dumps(payments, indent=2))
    print(f"wrote payments.json with {len(payments['data'])} records to {out}")


if __name__ == "__main__":
    write_excel()
    write_parquet()
    write_rest_payload()
