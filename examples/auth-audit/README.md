# auth-audit — LDAP × Postgres × syslog in one SQL

A second canonical dbfy showcase, focused on the auth/security
narrative. Three sources answer the same question from three angles;
their join is the interesting bit.

| Source        | Speaks            | Carries                                 |
|---------------|-------------------|-----------------------------------------|
| `directory`   | LDAP (OpenLDAP)   | who exists in the org                   |
| `audit`       | Postgres 16       | what the auth service *recorded*        |
| `syslog.auth` | RFC-5424 file     | what the OS *actually saw*              |

## Run

```bash
bash examples/auth-audit/run.sh
```

This brings up `bitnami/openldap` + `postgres:16-alpine` via
`docker compose`, generates a 17-line syslog, builds
`dbfy-cli --release`, and runs four queries plus two assertion
validators.

## What you'll see

**Q1** — sanity check, 8 users in the directory.

**Q2** — failed-login storms (`> 3 fails` from one IP):
- `bob` did 3 fails before his eventual success → not flagged
- `eve` hammered 5× from `198.51.100.7` → **flagged**

**Q3** — orphan audit records (entries the directory doesn't know about):
- `mallory` shows up — the Postgres audit table has events for them
  but they're not in LDAP. Either deprovisioned + still active
  (→ revoke) or never real (→ incident).

**Q4 — the killer query**:
For every directory user, compare audit-table count with
syslog-line count. Non-zero gaps are the signal:
- `audit > syslog`: events recorded but no OS evidence
- `syslog > audit`: OS saw a login the auth service didn't record
- `dave` has 0 / 0 — was on PTO, not interesting

## What pushdown does for each source

| Source        | Pushdown                                                    |
|---------------|-------------------------------------------------------------|
| `directory`   | LDAP filter `(&(objectClass=inetOrgPerson)(uid=…))`         |
| `audit`       | native Postgres `WHERE event = '…'` over the wire           |
| `syslog.auth` | `dbfy_idx` zone-map skip on `ts` (the indexer creates a sidecar `.dbfy_idx` after the first run)  |

## Why this is hard (and why dbfy makes it easy)

The "right" way without dbfy is three queries to three systems plus
glue code that joins their results in your application. With dbfy:

```sql
SELECT d.uid, d.cn,
       count(distinct a.id)    AS audit_count,
       count(distinct s.ts)    AS syslog_count
  FROM directory.users d
  LEFT JOIN audit.events a   ON a.uid = d.uid
  LEFT JOIN syslog.auth   s  ON s.message LIKE '%for ' || d.uid || ' from%'
 GROUP BY d.uid, d.cn;
```

…and the federation engine handles dispatch + pushdown + assembly.

## Tear-down

```bash
( cd examples/auth-audit && docker compose down -v )
```

That removes the containers and their named volumes.
