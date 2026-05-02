-- Q1: How many closed deals does the sales team think we have,
-- and what's the total value? (Excel pushdown: WHERE status =
-- 'closed' is evaluated during the row iteration, not after.)

SELECT
    count(*) AS closed_deals,
    sum(CAST(amount AS double)) AS total_amount
FROM sales.deals
WHERE status = 'closed';
