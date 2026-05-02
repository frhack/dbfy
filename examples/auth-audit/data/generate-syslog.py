#!/usr/bin/env python3
"""Generate the syslog file the showcase joins against.

The Postgres `auth_events` table represents what the auth service
*recorded*. The syslog represents what the OS *saw*. In the real
world the two diverge (network glitches, log rotation, attacks
that bypass the auth service). The showcase exposes those gaps via
a SQL JOIN.

We hand-craft enough lines that the join shows interesting findings:
- alice / bob / carol have matching syslog + audit
- mallory has audit but no syslog (impossible in real life ⇒ red flag)
- dave has neither (he was on PTO)
- eve hammered ssh from a hostile IP (syslog matches audit fail records)
- a "ghost" entry: a syslog session for `nobody` that never made it
  into the audit log (suggests the auth service was down)
"""

import pathlib

OUT = pathlib.Path(__file__).resolve().parent / "auth.log"

# Each tuple: timestamp, host, app, message
events = [
    # alice morning session
    ("2026-04-30T09:01:00+00:00", "host01", "sshd[1234]", "Accepted publickey for alice from 10.0.1.5 port 51234 ssh2: RSA SHA256:..."),
    ("2026-04-30T12:30:00+00:00", "host01", "sshd[1234]", "Disconnected from user alice 10.0.1.5 port 51234"),
    ("2026-04-30T13:45:00+00:00", "host01", "sshd[1240]", "Accepted publickey for alice from 10.0.1.5 port 51240 ssh2: RSA SHA256:..."),
    ("2026-04-30T17:55:00+00:00", "host01", "sshd[1240]", "Disconnected from user alice 10.0.1.5 port 51240"),

    # bob: 3 failures + success
    ("2026-04-30T02:10:00+00:00", "host01", "sshd[2001]", "Failed password for bob from 198.51.100.7 port 49001 ssh2"),
    ("2026-04-30T02:10:30+00:00", "host01", "sshd[2001]", "Failed password for bob from 198.51.100.7 port 49001 ssh2"),
    ("2026-04-30T02:11:01+00:00", "host01", "sshd[2001]", "Failed password for bob from 198.51.100.7 port 49001 ssh2"),
    ("2026-04-30T02:11:45+00:00", "host01", "sshd[2001]", "Accepted password for bob from 198.51.100.7 port 49001 ssh2"),
    ("2026-04-30T02:13:00+00:00", "host01", "sshd[2001]", "Disconnected from user bob 198.51.100.7 port 49001"),

    # carol normal
    ("2026-04-30T08:55:00+00:00", "host02", "sshd[3030]", "Accepted publickey for carol from 10.0.1.42 port 51500 ssh2: RSA SHA256:..."),
    ("2026-04-30T18:00:00+00:00", "host02", "sshd[3030]", "Disconnected from user carol 10.0.1.42 port 51500"),

    # eve: full storm of failures (matching audit count of 5)
    ("2026-04-30T03:00:00+00:00", "host01", "sshd[4001]", "Failed password for eve from 198.51.100.7 port 49050 ssh2"),
    ("2026-04-30T03:00:30+00:00", "host01", "sshd[4001]", "Failed password for eve from 198.51.100.7 port 49050 ssh2"),
    ("2026-04-30T03:01:00+00:00", "host01", "sshd[4001]", "Failed password for eve from 198.51.100.7 port 49050 ssh2"),
    ("2026-04-30T03:01:30+00:00", "host01", "sshd[4001]", "Failed password for eve from 198.51.100.7 port 49050 ssh2"),
    ("2026-04-30T03:02:00+00:00", "host01", "sshd[4001]", "Failed password for eve from 198.51.100.7 port 49050 ssh2"),

    # mallory: NO syslog entry. The audit table has it; the OS doesn't.
    # This is the gap the killer query exposes.

    # ghost session: syslog says nobody logged in, audit table doesn't know.
    ("2026-04-30T05:30:00+00:00", "host03", "sshd[7777]", "Accepted password for nobody from 192.0.2.1 port 12345 ssh2"),
    ("2026-04-30T05:31:30+00:00", "host03", "sshd[7777]", "Disconnected from user nobody 192.0.2.1 port 12345"),
]


def emit(ts_iso: str, host: str, app: str, msg: str) -> str:
    """RFC 5424-ish line: <PRI>1 ts host app procid - - msg.
    PRI=86 means facility=auth(10) + severity=info(6) → 86. Good enough for our parser.
    """
    # PRI for auth.info = 10*8 + 6 = 86 → <86>
    return f"<86>1 {ts_iso} {host} {app} - - - {msg}\n"


def main() -> None:
    with OUT.open("w") as f:
        for ts, host, app, msg in events:
            f.write(emit(ts, host, app, msg))
    print(f"wrote {len(events)} lines to {OUT}")


if __name__ == "__main__":
    main()
