-- Q1: total events + MAU for the month, pulled live from the
-- GraphQL endpoint. Variables pushdown turns
--   WHERE month = '2026-04-01'
-- into the GraphQL `$month` variable, so the API only ships the
-- right slice.

SELECT
    sum(events) AS total_events,
    sum(mau)    AS total_mau
FROM product.usage
WHERE month = '2026-04-01';
