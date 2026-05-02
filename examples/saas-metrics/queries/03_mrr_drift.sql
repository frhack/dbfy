-- Q3: customers whose actual revenue this month doesn't match
-- their MRR. Common causes: discounts, mid-month plan changes,
-- billing failures. Whatever the reason, the CSM team wants to
-- know.

SELECT
    c.slug,
    c.name,
    c.plan,
    c.mrr_cents AS expected_cents,
    r.actual_cents,
    r.actual_cents - c.mrr_cents AS drift_cents
FROM billing.customers c
JOIN billing.monthly_revenue r ON r.customer_id = c.id
WHERE r.month = DATE '2026-04-01'
  AND abs(r.actual_cents - c.mrr_cents) > 0
ORDER BY abs(r.actual_cents - c.mrr_cents) DESC;
