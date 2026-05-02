# finance-recon — Excel × Parquet × REST in one SQL

Three different teams, three different formats, one truth. The
showcase reconciles a finance team's Excel workbook of deals
against the data warehouse's Parquet file and against a payments
REST API.

| Source                  | Format    | Carries                                |
|-------------------------|-----------|----------------------------------------|
| `sales.deals`           | `.xlsx`   | what the sales team thinks closed      |
| `warehouse.orders`      | parquet   | what the BI pipeline ingested          |
| `payments.transactions` | REST/JSON | what the bank actually paid            |

## Run

```bash
bash examples/finance-recon/run.sh
```

This generates the three datasets (12 rows in xlsx, 9 in parquet,
8 records in REST), starts a tiny `python3 -m http.server` for the
payments JSON, builds `dbfy-cli --release`, runs four queries and
two validators.

## What the showcase finds

**Q1** — sanity: 9 closed deals worth ~298k.

**Q2 — warehouse drift**: closed in sales, missing from warehouse.
Expected: `D008 Massive Dynamic 67200`. Question to finance:
*did the integration miss this?*

**Q3 — overdue payments**: landed in warehouse, no payment row yet.
Expected: `D006 Cyberdyne 28000`. Question to AR: *5 days
since landing, where's the money?*

**Q4 — killer query**: full per-deal status. Each row is one of:
- `warehouse_missing` (worst — sales says closed, warehouse never saw it)
- `payment_pending`
- `amount_mismatch`
- `reconciled`

## Pushdown each source contributes

| Source        | Pushdown                                                 |
|---------------|----------------------------------------------------------|
| sales (xlsx)  | predicate `WHERE status = 'closed'` evaluated during the calamine row stream — non-matching rows never enter the RecordBatch |
| warehouse     | parquet row-group pruning + projection (only `deal_id`, `amount` read) |
| payments      | REST GET cached in-memory; the query JSONPaths into `$.data[*]`                |

## Tear-down

The `python3 -m http.server` background process is killed by the
`trap` in `run.sh` on script exit. No persistent state to clean up.
