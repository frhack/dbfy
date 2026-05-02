#!/usr/bin/env python3
"""Generate the FY26 budget Excel — what finance has *committed* to
spend / earn per quarter per plan tier. The CFO's spreadsheet, in
other words. The showcase joins this with billing reality.
"""

import pathlib

ROOT = pathlib.Path(__file__).resolve().parent

try:
    from openpyxl import Workbook
except ImportError:
    print("openpyxl not available, skipping budget.xlsx generation")
    raise SystemExit(0)

wb = Workbook()
ws = wb.active
ws.title = "fy26_budget"
ws.append(["plan", "month", "target_revenue_cents"])

rows = [
    ("starter",     "2026-04-01",   30000),
    ("pro",         "2026-04-01",   80000),
    ("enterprise",  "2026-04-01",  200000),
]
for r in rows:
    ws.append(r)

out = ROOT / "budget.xlsx"
wb.save(out)
print(f"wrote {len(rows)} rows to {out}")
