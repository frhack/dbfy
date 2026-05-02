-- Q1: How many users does the directory have? Sanity check that
-- the LDAP source is reachable and the bind credentials work.

SELECT count(*) AS user_count FROM directory.users;
