-- Seed the `auth_events` table that the auth service writes to.
-- Each event corresponds to a successful or failed login that
-- syslog *should* also have a record for; the showcase joins the
-- two to find drift (e.g. logins in syslog without a matching audit
-- record, or audit records for users not in LDAP).

CREATE TABLE IF NOT EXISTS auth_events (
    id          BIGSERIAL PRIMARY KEY,
    uid         TEXT NOT NULL,
    event       TEXT NOT NULL,            -- 'login' | 'logout' | 'login_fail'
    source_ip   INET NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    session_id  UUID
);

CREATE INDEX IF NOT EXISTS idx_auth_events_uid ON auth_events (uid);
CREATE INDEX IF NOT EXISTS idx_auth_events_at  ON auth_events (occurred_at);

INSERT INTO auth_events (uid, event, source_ip, occurred_at, session_id) VALUES
  -- alice: normal day's work, two distinct sessions
  ('alice', 'login',      '10.0.1.5',   '2026-04-30 09:01:00+00', 'a1111111-1111-1111-1111-111111111111'),
  ('alice', 'logout',     '10.0.1.5',   '2026-04-30 12:30:00+00', 'a1111111-1111-1111-1111-111111111111'),
  ('alice', 'login',      '10.0.1.5',   '2026-04-30 13:45:00+00', 'a2222222-2222-2222-2222-222222222222'),
  ('alice', 'logout',     '10.0.1.5',   '2026-04-30 17:55:00+00', 'a2222222-2222-2222-2222-222222222222'),

  -- bob: failed logins followed by a success
  ('bob',   'login_fail', '198.51.100.7','2026-04-30 02:10:00+00', NULL),
  ('bob',   'login_fail', '198.51.100.7','2026-04-30 02:10:30+00', NULL),
  ('bob',   'login_fail', '198.51.100.7','2026-04-30 02:11:01+00', NULL),
  ('bob',   'login',      '198.51.100.7','2026-04-30 02:11:45+00', 'b1111111-1111-1111-1111-111111111111'),
  ('bob',   'logout',     '198.51.100.7','2026-04-30 02:13:00+00', 'b1111111-1111-1111-1111-111111111111'),

  -- carol: completely normal pattern
  ('carol', 'login',      '10.0.1.42',  '2026-04-30 08:55:00+00', 'c1111111-1111-1111-1111-111111111111'),
  ('carol', 'logout',     '10.0.1.42',  '2026-04-30 18:00:00+00', 'c1111111-1111-1111-1111-111111111111'),

  -- mallory: NOT in LDAP — orphan audit record (the showcase must spot this)
  ('mallory','login',     '203.0.113.99','2026-04-30 04:00:00+00', 'd1111111-1111-1111-1111-111111111111'),

  -- dave: present in LDAP but no audit events at all (the showcase must also spot this — covered by left join from LDAP side)
  -- (intentionally no rows for dave)

  -- eve: many failed logins, no eventual success
  ('eve',   'login_fail', '198.51.100.7','2026-04-30 03:00:00+00', NULL),
  ('eve',   'login_fail', '198.51.100.7','2026-04-30 03:00:30+00', NULL),
  ('eve',   'login_fail', '198.51.100.7','2026-04-30 03:01:00+00', NULL),
  ('eve',   'login_fail', '198.51.100.7','2026-04-30 03:01:30+00', NULL),
  ('eve',   'login_fail', '198.51.100.7','2026-04-30 03:02:00+00', NULL);
