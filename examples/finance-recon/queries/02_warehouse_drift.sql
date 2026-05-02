-- Q2: Closed deals that haven't landed in the warehouse yet.
-- Each missing row is a finance question: did the integration
-- run? Did the deal actually close on the date sales says? Is
-- there a manual step nobody documented?
--
-- Cross-source LEFT JOIN: sales.deals (Excel) ⋈ warehouse.orders (Parquet).

SELECT
    s.deal_id,
    s.customer,
    s.amount,
    s.expected_close
FROM sales.deals s
LEFT JOIN warehouse.orders w ON w.deal_id = s.deal_id
WHERE s.status = 'closed'
  AND w.deal_id IS NULL
ORDER BY s.amount DESC;
