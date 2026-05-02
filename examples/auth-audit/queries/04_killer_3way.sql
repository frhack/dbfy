-- Q4: The killer query — three sources in one SELECT.
--
-- For every user in the directory, summarise their day:
--   * how many login attempts the audit table saw
--   * how many sshd "Accepted" lines the OS actually logged
--   * the gap between the two
--
-- A non-zero gap is the interesting signal:
--   - audit > syslog: events recorded but no OS evidence (auth
--                     service ran but didn't reach the host?)
--   - syslog > audit: OS saw a login but the auth service didn't
--                     record it (auth service was offline?)
--   - both 0       : user wasn't around (PTO, terminated, etc)
--
-- Each source contributes the right pushdown:
--   directory.users:    LDAP filter (objectClass=inetOrgPerson)
--   audit.events:       Postgres WHERE event IN ('login','login_fail')
--   syslog.auth:        rows-file zone_map skip on `ts`

WITH audit_per_user AS (
    SELECT uid, count(*) AS audit_attempts
    FROM audit.events
    WHERE event IN ('login', 'login_fail')
    GROUP BY uid
),
syslog_per_user AS (
    -- Cheap-and-cheerful uid extraction: sshd's "Accepted" /
    -- "Failed password" lines have the username at a fixed
    -- position. We could push regex parsing into a dedicated
    -- syslog-with-regex table, but for the showcase the
    -- substring trick keeps the SQL self-contained.
    SELECT
        regexp_extract(message, ' for ([a-zA-Z0-9_]+) from', 1) AS uid,
        count(*) AS syslog_lines
    FROM syslog.auth
    WHERE message LIKE 'Accepted%'
       OR message LIKE 'Failed password%'
    GROUP BY 1
)
SELECT
    d.uid,
    d.cn,
    coalesce(a.audit_attempts, 0) AS audit_count,
    coalesce(s.syslog_lines,    0) AS syslog_count,
    coalesce(a.audit_attempts, 0) - coalesce(s.syslog_lines, 0) AS gap
FROM directory.users d
LEFT JOIN audit_per_user  a ON a.uid = d.uid
LEFT JOIN syslog_per_user s ON s.uid = d.uid
ORDER BY abs(gap) DESC, d.uid;
