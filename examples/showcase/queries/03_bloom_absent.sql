-- 3. Bloom filter on absent key.
--
-- Look up a SHA that DEFINITELY isn't in the dataset. The bloom on
-- `sha` (high cardinality, 50k distinct values) catches the absence
-- across every chunk before any JSON parsing happens.
--
-- Capabilities exercised:
--   * rows-file bloom filter pruning
--   * fast-path "definitely absent" reply
--
-- Validator angle: if bloom regresses, query takes ~50× longer
-- because every chunk gets opened and parsed. Expected: 0 rows in
-- single-digit milliseconds.

SELECT count(*) AS sightings
FROM files.ci_runs
WHERE sha = 'cafef00ddeadbeef0000000000000000baadf00d';
