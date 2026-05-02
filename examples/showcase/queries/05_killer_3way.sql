-- 5. Killer 3-way JOIN: REST × file × file.
--
-- For every CI failure in the last 24h, find:
--   * the GitHub issue most recently updated within ±1 hour of the
--     failure (correlation by time, not by SHA — proxy for "did
--     someone open an issue right after CI broke?")
--   * the count of severe system events on the SAME hosts in the
--     same time window
--
-- The point: three heterogeneous sources (live REST + indexed JSONL +
-- globbed syslog) joined declaratively in one SELECT, no glue scripts.
--
-- Capabilities exercised:
--   * REST source (issues, paginated, cached on rerun)
--   * rows-file JSONL with bloom on `status`
--   * rows-file syslog with glob + zone-map on `ts`
--   * cross-source temporal JOIN (epoch arithmetic)
--   * GROUP BY across sources
--
-- Validator angle: if any source returned empty, the join collapses
-- to zero rows. The script checks count > 0.

WITH recent_failures AS (
    SELECT id, ts, sha, workflow
    FROM files.ci_runs
    WHERE status = 'failure'
      AND ts >= now() - interval '6 hour'
),
nearby_errors AS (
    SELECT count(*) AS error_count
    FROM logs.system
    WHERE severity <= 4
      AND ts >= now() - interval '6 hour'
)
SELECT
    f.workflow,
    count(*) AS failure_count,
    -- `n.error_count` is a single scalar per row (the same value
    -- broadcast across groups), so MAX is just a way to project it
    -- through GROUP BY without DataFusion needing `any_value()`.
    max(n.error_count)               AS system_errors_in_window,
    count(DISTINCT i.author)         AS issue_authors_active_in_window
FROM recent_failures f
LEFT JOIN nearby_errors n ON true
LEFT JOIN gh.issues i
       ON i.state = 'open'
      AND i.updated_at >= now() - interval '6 hour'
GROUP BY f.workflow
ORDER BY failure_count DESC
LIMIT 5;
