-- Q2: actual revenue per plan tier vs FY26 budget target. The
-- budget lives in Excel (the CFO's spreadsheet); the actuals live
-- in Postgres (billing system). Two-source aggregate join.

WITH actuals AS (
    SELECT
        c.plan,
        sum(r.actual_cents) AS actual_cents
    FROM billing.customers c
    JOIN billing.monthly_revenue r ON r.customer_id = c.id
    WHERE r.month = DATE '2026-04-01'
    GROUP BY c.plan
)
SELECT
    a.plan,
    a.actual_cents,
    CAST(b.target_revenue_cents AS bigint) AS target_cents,
    a.actual_cents - CAST(b.target_revenue_cents AS bigint) AS variance
FROM actuals a
JOIN finance.budget b
  ON b.plan = a.plan AND b.month = '2026-04-01'
ORDER BY a.plan;
