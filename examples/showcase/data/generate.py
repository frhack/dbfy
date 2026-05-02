#!/usr/bin/env python3
"""Synthesise the local datasets the showcase joins against:

  - ci-runs.jsonl     50,000 CI run records with realistic shape
                      (id, ts, sha, workflow, status, duration_ms,
                      runner, attempt). A handful of `sha` values
                      match real commits from the GitHub API source
                      so the 3-way JOIN actually finds matches.

  - system-N.log      5 rotated syslog RFC 5424 files, ~2k events
                      each. Severities follow a realistic
                      distribution (mostly INFO, rare WARN/ERROR).

The data is deterministic given the seed so the showcase output is
stable across runs. Re-running this script overwrites everything.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

# A handful of real DuckDB commit SHAs, picked to be ones we'll find
# referenced in issues/PRs from the GitHub API. Mixed with random
# SHAs so most CI rows don't match any real issue.
REAL_SHAS = [
    "fb7701fec0caf058f295aea6f5b8ef91b1d9a942",
    "3eaed75f0ec5500609b7a97aa05468493b229d1",  # truncated example
    "7e8f81b0000000000000000000000000",          # synthetic stand-in
]

WORKFLOWS = ["test", "lint", "release-build", "docs", "extension-ci"]
RUNNERS = ["ubuntu-latest", "macos-13", "macos-14", "windows-latest"]
STATUSES_W = [("success", 80), ("failure", 12), ("cancelled", 5), ("timed_out", 3)]


def rand_sha(rng: random.Random) -> str:
    return "".join(rng.choices("0123456789abcdef", k=40))


def weighted(rng: random.Random, choices_with_weights):
    items, weights = zip(*choices_with_weights)
    return rng.choices(items, weights=weights, k=1)[0]


def looks_like_rfc3339(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def gen_ci_runs(out: Path, n: int, seed: int) -> None:
    rng = random.Random(seed)
    # Spread CI runs over the last 24 hours (same window as syslog)
    # so a query like `WHERE ts >= now() - interval '6 hour'` matches
    # roughly the most recent 25% of records on both sources at once.
    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=24)
    step = timedelta(hours=24).total_seconds() / n
    with out.open("w") as f:
        for i in range(n):
            ts = base + timedelta(seconds=i * step + rng.uniform(0, step / 2))
            # ~1 in 200 rows references a "real" SHA, rest are random.
            sha = rng.choice(REAL_SHAS) if rng.random() < 0.005 else rand_sha(rng)
            workflow = rng.choice(WORKFLOWS)
            status = weighted(rng, STATUSES_W)
            duration = (
                rng.randint(2_000, 90_000)
                if status != "timed_out"
                else rng.randint(360_000, 600_000)
            )
            row = {
                "id": i,
                "ts": looks_like_rfc3339(ts),
                "sha": sha,
                "workflow": workflow,
                "status": status,
                "duration_ms": duration,
                "runner": rng.choice(RUNNERS),
                "attempt": rng.randint(1, 3),
            }
            f.write(json.dumps(row) + "\n")


def gen_syslog(out_dir: Path, files: int, per_file: int, seed: int) -> None:
    """RFC 5424 lines:
    <PRI>1 TIMESTAMP HOSTNAME APPNAME PROCID MSGID - MSG
    """
    rng = random.Random(seed + 1)
    base = datetime.now(timezone.utc) - timedelta(hours=24)
    # Spread `files * per_file` events evenly across the last 24 hours
    # so a query for "last 6 hours" matches roughly the last 25% of
    # records. With files=5, per_file=2k, total=10k → 1 event every
    # ~8.6 seconds.
    total = files * per_file
    step = timedelta(hours=24).total_seconds() / total
    apps = ["nginx", "kube-apiserver", "etcd", "scheduler", "redis"]
    hosts = [f"node-{i:02d}" for i in range(1, 6)]

    for fidx in range(files):
        path = out_dir / f"system-{fidx + 1}.log"
        with path.open("w") as f:
            for j in range(per_file):
                # Severity distribution: 75% INFO(6), 18% NOTICE(5),
                # 5% WARN(4), 2% ERR(3).
                roll = rng.random()
                if roll < 0.75:
                    severity = 6
                elif roll < 0.93:
                    severity = 5
                elif roll < 0.98:
                    severity = 4
                else:
                    severity = 3
                facility = 16  # local0
                pri = facility * 8 + severity

                global_seq = fidx * per_file + j
                ts = base + timedelta(seconds=global_seq * step)
                host = rng.choice(hosts)
                app = rng.choice(apps)
                proc_id = rng.randint(1000, 9999)
                msg_id = "ID" + str(rng.randint(1, 99))
                msg = f"event seq={j} src={app}@{host}"
                line = (
                    f"<{pri}>1 {looks_like_rfc3339(ts)} {host} {app} {proc_id}"
                    f" {msg_id} - {msg}"
                )
                f.write(line + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent))
    parser.add_argument("--ci-rows", type=int, default=50_000)
    parser.add_argument("--syslog-files", type=int, default=5)
    parser.add_argument("--syslog-per-file", type=int, default=2_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    gen_ci_runs(out / "ci-runs.jsonl", args.ci_rows, args.seed)
    gen_syslog(out, args.syslog_files, args.syslog_per_file, args.seed)
    total_syslog = args.syslog_files * args.syslog_per_file
    print(
        f"wrote {args.ci_rows} CI rows + "
        f"{total_syslog} syslog events across {args.syslog_files} files",
    )


if __name__ == "__main__":
    main()
