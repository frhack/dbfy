-- Q2: Which users hit > 3 failed logins in a 5-minute window from
-- the same IP? This is the classic brute-force signature; the
-- audit table makes it cheap to compute.
--
-- Pushdown opportunity: dbfy translates `WHERE event = 'login_fail'`
-- into a native Postgres `WHERE`, so the wire-result is already
-- pre-filtered. The window logic runs on the dbfy side because
-- DataFusion's window functions don't push down (yet).

SELECT
    uid,
    source_ip,
    count(*) AS failed_attempts,
    min(occurred_at) AS first_attempt,
    max(occurred_at) AS last_attempt
FROM audit.events
WHERE event = 'login_fail'
GROUP BY uid, source_ip
HAVING count(*) > 3
ORDER BY failed_attempts DESC;
