-- Q4: Killer query — three sources, ARPU vs usage cohort.
--
-- For each customer, what's their plan, what they pay, and how
-- much product do they actually use? The "cents per million events"
-- column is the ARPU/usage ratio: low ratio = great deal for the
-- customer, bad deal for us; high ratio = customer is paying for
-- shelf-ware (potential churn).
--
-- Pushdown layers:
--   product (GraphQL)  → variables pushdown for the month filter
--   billing (Postgres) → JOIN translates to native WHERE/SELECT
--   finance (Excel)    → row-iteration filter on `month`

SELECT
    c.slug,
    c.plan,
    c.mrr_cents,
    u.events,
    u.mau,
    CASE
        WHEN u.events > 0
        THEN CAST(c.mrr_cents AS double) / (u.events / 1000000.0)
        ELSE NULL
    END AS cents_per_million_events,
    -- How does this customer's slice compare to the budget for
    -- their tier? `b.share` is what fraction of the tier's
    -- target_revenue this customer represents.
    CAST(c.mrr_cents AS double) / CAST(b.target_revenue_cents AS double) AS share_of_tier_budget
FROM billing.customers c
JOIN product.usage u  ON u.customer_slug = c.slug AND u.month = '2026-04-01'
JOIN finance.budget b ON b.plan = c.plan         AND b.month = '2026-04-01'
ORDER BY cents_per_million_events DESC NULLS LAST;
