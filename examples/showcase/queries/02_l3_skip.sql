-- 2. L3 indexing skip ratio.
--
-- A point-range query against 50k JSONL rows. The zone-map index on
-- `id` lets the pruner skip every chunk whose [min,max] doesn't
-- intersect [49000, 49100], so we read 1 chunk of 1000 rows instead
-- of all 50.
--
-- Capabilities exercised:
--   * rows-file JSONL parser
--   * zone map pruning
--   * incremental EXTEND (if you re-run after appending, only new
--     chunks get parsed — verified by `dbfy index --table files.ci_runs`)
--
-- Validator angle: if zone maps regress, the query times out or
-- returns the wrong count. Expected: exactly 101 rows.

SELECT count(*) AS hot_window_rows,
       min(id) AS min_id,
       max(id) AS max_id
FROM files.ci_runs
WHERE id BETWEEN 49000 AND 49100;
