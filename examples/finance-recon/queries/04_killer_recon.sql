-- Q4: Killer query — the full reconciliation across all three
-- sources. For every deal sales considers closed, what does the
-- warehouse and the payments API say? Three columns of "truth"
-- side by side.
--
-- Each source contributes a different layer:
--   sales      → Excel rows with WHERE status pushdown
--   warehouse  → Parquet row-group + projection pushdown
--   payments   → REST GET (cached) + JSONPath extraction
--
-- The output sorts by drift size so the rows that need the
-- finance team's attention float to the top.

SELECT
    s.deal_id,
    s.customer,
    s.amount AS sales_amount,
    w.amount AS warehouse_amount,
    p.amount AS paid_amount,
    p.paid_at,
    CASE
        WHEN w.deal_id IS NULL                         THEN 'warehouse_missing'
        WHEN p.deal_id IS NULL                         THEN 'payment_pending'
        WHEN abs(w.amount - p.amount) > 0.01           THEN 'amount_mismatch'
        ELSE 'reconciled'
    END AS status
FROM sales.deals s
LEFT JOIN warehouse.orders     w ON w.deal_id = s.deal_id
LEFT JOIN payments.transactions p ON p.deal_id = s.deal_id
WHERE s.status = 'closed'
ORDER BY
    CASE
        WHEN w.deal_id IS NULL THEN 0
        WHEN p.deal_id IS NULL THEN 1
        ELSE 2
    END,
    s.amount DESC;
