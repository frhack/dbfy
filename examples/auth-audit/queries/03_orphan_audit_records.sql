-- Q3: Audit-log entries for users that don't exist in the
-- directory. Should ALWAYS be empty in a healthy system; if it
-- isn't, either a former user wasn't deprovisioned, or — much
-- worse — an attacker created a record for an account that was
-- never real.
--
-- This is a 2-source LEFT JOIN with anti-semantics, expressed
-- naturally in SQL across LDAP and Postgres:

SELECT DISTINCT a.uid, a.source_ip, count(*) AS event_count
FROM audit.events a
LEFT JOIN directory.users d ON d.uid = a.uid
WHERE d.uid IS NULL
GROUP BY a.uid, a.source_ip
ORDER BY event_count DESC;
