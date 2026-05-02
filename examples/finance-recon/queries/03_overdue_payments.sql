-- Q3: Warehouse-landed orders that the payments API doesn't yet
-- have a "paid" status for. These are the deals where money
-- should be in the bank but isn't.
--
-- The trick is a 2-source LEFT JOIN with NULL filtering on the
-- right side.

SELECT
    w.deal_id,
    w.amount,
    w.landed_at
FROM warehouse.orders w
LEFT JOIN payments.transactions p ON p.deal_id = w.deal_id
WHERE p.deal_id IS NULL
ORDER BY w.amount DESC;
