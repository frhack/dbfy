-- 1. Hello REST: count open issues from a real GitHub repo.
--
-- Capabilities exercised:
--   * REST source (live https://api.github.com)
--   * link-header pagination (GitHub uses `Link: <…>; rel="next"`)
--   * bearer auth via $GITHUB_TOKEN env var
--   * filter pushdown into the URL: WHERE state = 'open' becomes ?state=open
--     so we don't fetch closed issues at all.
--
-- Validator angle: if pagination is broken, this returns a multiple of
-- 30 (the GitHub default per_page) instead of the true count.

SELECT count(*) AS open_issues,
       count(DISTINCT author) AS distinct_authors,
       avg(comments) AS avg_comments
FROM gh.issues
WHERE state = 'open';
