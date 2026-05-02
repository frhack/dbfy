-- 4. Glob multi-file with per-file pruning.
--
-- 5 syslog files × 2k events each = 10k events. The zone-map on `ts`
-- is per-file: a query on a recent time window prunes 4 of 5 files
-- before any line is parsed. The bloom on `app` then trims further
-- inside the surviving file.
--
-- Capabilities exercised:
--   * rows-file glob expansion
--   * RFC 5424 syslog parser (priority decomposition into severity)
--   * per-file zone-map pruning
--   * bloom on a low-cardinality string column
--
-- Validator angle: if the glob isn't lexicographically sorted, the
-- order of system-N.log files matters and ts windows misalign;
-- result count is still correct but timing degrades.

SELECT app, count(*) AS error_count
FROM logs.system
WHERE severity <= 3                              -- ERR or worse
  AND ts >= now() - interval 6 hour
  AND app IN ('nginx', 'kube-apiserver', 'etcd')
GROUP BY app
ORDER BY error_count DESC;
