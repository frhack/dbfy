# saas-metrics — GraphQL × Postgres × Excel in one SQL

Three different teams, three views of "how is the SaaS doing".
The showcase joins:

| Source            | Speaks         | Carries                                       |
|-------------------|----------------|-----------------------------------------------|
| `product.usage`   | GraphQL POST   | per-customer events + MAU (live, this month)  |
| `billing`         | Postgres 16    | customers + their actual paid invoices        |
| `finance.budget`  | `.xlsx`        | CFO's per-tier revenue targets                |

Each source contributes a different pushdown:

- **GraphQL**: SQL `WHERE month = '2026-04-01'` becomes the
  GraphQL `$month` variable on the wire — the API only ships the
  right monthly slice.
- **Postgres**: every `WHERE … AND r.month = DATE '2026-04-01'`
  pushes into native `WHERE month = '2026-04-01'` server-side.
- **Excel**: row-iteration filter on `b.month` skips non-matching
  rows before they reach the RecordBatch.

## Run

```bash
bash examples/saas-metrics/run.sh
```

Brings up `postgres:16-alpine` (port 5433 to coexist with the
auth-audit showcase), starts a tiny `python3 graphql-server.py`
stub for the product API, generates `budget.xlsx`, builds
`dbfy-cli --release`, runs four queries plus a validator.

## What you'll see

**Q1** — total events ~26.6M, total MAU ~48k for April.

**Q2** — actuals vs target per tier. Pro tier collected 47k vs
80k target → behind plan by 33k.

**Q3 — MRR drift**: Initech paid 12 000 vs MRR 15 000 (−3 000).
Worth a CSM follow-up: discount? Plan change? Failed retry?

**Q4 — killer query**: ARPU per million events.
- `tyrell` is paying 2.5k for 2 100 events → enormous
  cents-per-million → potential churn risk (paying for
  shelf-ware).
- `umbrella` and `acme` are heavy users at ~6 ¢ per million
  events; healthy.

## Tear-down

```bash
( cd examples/saas-metrics && docker compose down -v )
```
