"""dbfy + DuckDB analytics demo — REST × REST × file in a single SQL query.

The point of the dual-track architecture is that once `dbfy.duckdb_extension`
is `LOAD`ed, REST APIs and line-delimited files become tables on the same
footing as Parquet, CSV, or in-memory data. You can `JOIN`, aggregate,
window, materialise into local columnar cache, and export — all in SQL,
without glue code.

This script:
  1. Spins up two in-process HTTP endpoints (`/sensors`, `/readings`).
  2. Synthesises a JSONL log of fleet operations events (`fleet-events.jsonl`).
  3. `LOAD`s dbfy from the release build.
  4. Demonstrates six progressively richer patterns:
       a. raw REST federation — `SELECT … FROM dbfy_rest(...)`
       b. multi-source JOIN — readings × sensors in one query
       c. file federation — `SELECT … FROM dbfy_rows_file(...)` with
          zone maps + bloom filters declared inline
       d. **3-way JOIN** — REST × REST × file in a single SELECT, the
          killer use case: correlate REST-sourced telemetry with
          locally-stored ops events
       e. materialised cache — `CREATE TABLE … AS SELECT …` snapshot
       f. window function + Parquet export

Run after building the extension::

    cargo build -p dbfy-frontend-duckdb --features loadable_extension --release --jobs 1
    strip target/release/libdbfy_duckdb.so
    python3 crates/dbfy-frontend-duckdb/scripts/append_metadata.py \\
        target/release/libdbfy_duckdb.so \\
        --output target/release/dbfy.duckdb_extension --duckdb-capi-version v1.2.0

    .venv/bin/python crates/dbfy-frontend-duckdb/examples/sensor_analytics.py
"""

from __future__ import annotations

import json
import random
import textwrap
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import duckdb


# ---------- fixtures: synthetic "iot fleet" served over HTTP ----------

ZONES = ["warehouse-a", "warehouse-b", "cold-room", "loading-dock"]
MODELS = ["BME280", "DHT22", "SHT31"]


def _build_sensors() -> dict:
    sensors = []
    for i in range(40):
        sensors.append(
            {
                "sensor_id": f"sensor-{i:03d}",
                "zone": ZONES[i % len(ZONES)],
                "model": MODELS[i % len(MODELS)],
                "installed_at": "2025-09-01",
            }
        )
    return {"data": sensors}


def _build_readings() -> dict:
    rng = random.Random(42)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    readings = []
    for i in range(40):
        sid = f"sensor-{i:03d}"
        # 12 readings per sensor over the last hour
        for k in range(12):
            ts = now - timedelta(minutes=k * 5)
            base_temp = 18.0 if i % 4 == 2 else 22.0  # cold-room runs colder
            readings.append(
                {
                    "sensor_id": sid,
                    "timestamp": ts.isoformat().replace("+00:00", "Z"),
                    "temperature": round(base_temp + rng.uniform(-2, 4), 2),
                    "humidity": rng.randint(35, 75),
                }
            )
    return {"data": readings}


SENSORS = _build_sensors()
READINGS = _build_readings()


def _write_events_jsonl(path: Path) -> None:
    """Synthesise a fleet-operations log: deploy / restart / alarm-acked
    events for the same sensor IDs the REST endpoints report.

    Realistic shape: one JSON object per line, RFC 3339 timestamps, an
    integer event id for ordering, a categorical `kind` for bloom-side
    pruning, and an `actor` field that names a human or service. This
    exercises every cell type the JSONL parser supports.
    """
    rng = random.Random(7)
    kinds = ["deploy", "restart", "alarm_acked", "calibration"]
    actors = ["alice", "bob", "carol", "ops-bot"]
    now = datetime.now(timezone.utc).replace(microsecond=0)
    with path.open("w") as f:
        for ev_id in range(2000):
            sid = f"sensor-{rng.randrange(40):03d}"
            kind = kinds[ev_id % len(kinds)]
            actor = actors[rng.randrange(len(actors))]
            ts = now - timedelta(seconds=ev_id * 7)
            row = {
                "event_id": ev_id,
                "ts": ts.isoformat().replace("+00:00", "Z"),
                "sensor_id": sid,
                "kind": kind,
                "actor": actor,
                "duration_ms": round(rng.uniform(50, 5000), 2),
            }
            f.write(json.dumps(row) + "\n")


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (stdlib API)
        if self.path.startswith("/sensors"):
            payload = SENSORS
        elif self.path.startswith("/readings"):
            payload = READINGS
        else:
            self.send_response(404); self.end_headers(); return
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a, **_k):  # silence
        pass


def _start_server() -> tuple[str, HTTPServer, threading.Thread]:
    s = HTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=s.serve_forever, daemon=True)
    t.start()
    return f"http://127.0.0.1:{s.server_address[1]}", s, t


# ---------- dbfy configs ----------

SENSORS_CFG = textwrap.dedent("""
    root: $.data[*]
    columns:
      sensor_id:    {path: "$.sensor_id",    type: string}
      zone:         {path: "$.zone",         type: string}
      model:        {path: "$.model",        type: string}
      installed_at: {path: "$.installed_at", type: date}
""").strip()

READINGS_CFG = textwrap.dedent("""
    root: $.data[*]
    columns:
      sensor_id:   {path: "$.sensor_id",   type: string}
      timestamp:   {path: "$.timestamp",   type: timestamp}
      temperature: {path: "$.temperature", type: float64}
      humidity:    {path: "$.humidity",    type: int64}
""").strip()

# Inline rows-file config for `dbfy_rows_file()`: parser declares typed
# columns, indexed_columns adds zone map + bloom for fast pushdown.
EVENTS_CFG = textwrap.dedent("""
    parser:
      format: jsonl
      columns:
        - { name: event_id,    path: "$.event_id",    type: int64     }
        - { name: ts,          path: "$.ts",          type: timestamp }
        - { name: sensor_id,   path: "$.sensor_id",   type: string    }
        - { name: kind,        path: "$.kind",        type: string    }
        - { name: actor,       path: "$.actor",       type: string    }
        - { name: duration_ms, path: "$.duration_ms", type: float64   }
    indexed_columns:
      - { name: event_id, kind: zone_map }
      - { name: ts,       kind: zone_map }
      - { name: kind,     kind: bloom    }
      - { name: actor,    kind: bloom    }
    chunk_rows: 200
""").strip()


# ---------- the demo ----------

def main() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    ext = repo_root / "target" / "release" / "dbfy.duckdb_extension"
    if not ext.exists():
        raise SystemExit(f"missing {ext}; see the docstring for the build command")

    # Synthesise the fleet-events log next to the script (cleaned at end).
    events_path = repo_root / "target" / "fleet-events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    _write_events_jsonl(events_path)

    base, server, thread = _start_server()
    try:
        con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        con.execute(f"LOAD '{ext}'")

        # ---------------------------------------------------------------
        # (a) Raw federation: REST → SQL with one function call.
        # ---------------------------------------------------------------
        n_sensors = con.execute(
            "SELECT count(*) FROM dbfy_rest(?, config := ?)",
            [f"{base}/sensors", SENSORS_CFG],
        ).fetchone()[0]
        print(f"(a) /sensors returned {n_sensors} rows")

        # ---------------------------------------------------------------
        # (b) Cross-source JOIN — two REST endpoints in a single query,
        # no glue code, no pandas, no `requests`. Result is plain SQL.
        # ---------------------------------------------------------------
        zone_summary = con.execute(
            """
            SELECT
                s.zone,
                count(*)                          AS samples,
                round(avg(r.temperature), 2)      AS avg_temp,
                max(r.humidity)                   AS peak_humidity
            FROM dbfy_rest(?, config := ?) r
            JOIN dbfy_rest(?, config := ?) s USING (sensor_id)
            GROUP BY s.zone
            ORDER BY avg_temp DESC
            """,
            [
                f"{base}/readings", READINGS_CFG,
                f"{base}/sensors",  SENSORS_CFG,
            ],
        ).fetchall()

        print("\n(b) Zone summary (REST × REST JOIN, single query):")
        print(f"    {'zone':<14} {'samples':>8} {'avg_temp':>10} {'peak_hum':>10}")
        for zone, samples, avg_t, peak_h in zone_summary:
            print(f"    {zone:<14} {samples:>8} {avg_t:>10} {peak_h:>10}")

        # ---------------------------------------------------------------
        # (c) File federation: same SQL story for line-delimited files.
        # The L3 index (zone maps + bloom filters declared in EVENTS_CFG)
        # is built lazily on first scan and persisted as a sidecar; later
        # queries on the same file are bounded by the relevant chunks.
        # ---------------------------------------------------------------
        kind_breakdown = con.execute(
            """
            SELECT kind, count(*) AS n,
                   round(avg(duration_ms), 1) AS avg_ms
            FROM dbfy_rows_file(?, config := ?)
            GROUP BY kind
            ORDER BY n DESC
            """,
            [str(events_path), EVENTS_CFG],
        ).fetchall()
        print("\n(c) Fleet events by kind (rows-file federation):")
        print(f"    {'kind':<14} {'n':>6} {'avg_ms':>9}")
        for kind, n, avg_ms in kind_breakdown:
            print(f"    {kind:<14} {n:>6} {avg_ms:>9}")

        # ---------------------------------------------------------------
        # (d) The killer pattern: REST × REST × FILE in one SELECT. Find
        # zones where a high-temperature reading lined up with an
        # alarm_acked event on the same sensor within ±90 seconds. This
        # is exactly the kind of query you'd otherwise glue together
        # with three scripts and a CSV intermediate.
        # ---------------------------------------------------------------
        correlations = con.execute(
            """
            SELECT
                s.zone,
                count(*)                       AS hot_acked_events,
                round(avg(r.temperature), 2)   AS avg_temp_at_event
            FROM dbfy_rest(?, config := ?) r
            JOIN dbfy_rest(?, config := ?) s
              USING (sensor_id)
            JOIN dbfy_rows_file(?, config := ?) e
              ON e.sensor_id = r.sensor_id
             AND e.kind = 'alarm_acked'
             AND abs(epoch(e.ts) - epoch(r.timestamp)) <= 90
            WHERE r.temperature > 22
            GROUP BY s.zone
            ORDER BY hot_acked_events DESC
            """,
            [
                f"{base}/readings", READINGS_CFG,
                f"{base}/sensors",  SENSORS_CFG,
                str(events_path),   EVENTS_CFG,
            ],
        ).fetchall()
        print("\n(d) Hot-and-acked correlations (REST × REST × FILE):")
        print(f"    {'zone':<14} {'hot_acked':>10} {'avg_temp':>10}")
        for zone, hot_acked, avg_t in correlations:
            print(f"    {zone:<14} {hot_acked:>10} {avg_t:>10}")

        # ---------------------------------------------------------------
        # (e) Materialise into a local DuckDB table (the "cache" pattern)
        # and re-query at columnar speed. Each subsequent query is
        # zero-HTTP and zero-file-IO.
        # ---------------------------------------------------------------
        con.execute(
            "CREATE TABLE readings_local AS SELECT * FROM dbfy_rest(?, config := ?)",
            [f"{base}/readings", READINGS_CFG],
        )
        con.execute(
            "CREATE TABLE sensors_local  AS SELECT * FROM dbfy_rest(?, config := ?)",
            [f"{base}/sensors", SENSORS_CFG],
        )
        con.execute(
            "CREATE TABLE events_local   AS SELECT * FROM dbfy_rows_file(?, config := ?)",
            [str(events_path), EVENTS_CFG],
        )

        t0 = time.perf_counter()
        for _ in range(20):
            con.execute(
                """
                SELECT s.model, avg(r.temperature), count(e.event_id)
                FROM readings_local r
                JOIN sensors_local  s USING (sensor_id)
                LEFT JOIN events_local e USING (sensor_id)
                GROUP BY s.model
                """
            ).fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(
            f"\n(e) 20× 3-way JOIN over the cached snapshot: {elapsed_ms:.1f} ms total "
            f"({elapsed_ms / 20:.2f} ms/query, zero HTTP, zero file IO)"
        )

        # ---------------------------------------------------------------
        # (f) Window function over the cached data + Parquet export.
        # ---------------------------------------------------------------
        anomalies = con.execute(
            """
            WITH delta AS (
                SELECT
                    sensor_id,
                    timestamp,
                    temperature,
                    temperature - avg(temperature) OVER (PARTITION BY sensor_id) AS deviation
                FROM readings_local
            )
            SELECT sensor_id, timestamp, temperature, round(deviation, 2) AS dev
            FROM delta
            WHERE abs(deviation) > 3
            ORDER BY abs(deviation) DESC
            LIMIT 5
            """
        ).fetchall()
        print("\n(f) Top deviations from each sensor's mean (window function):")
        print(f"    {'sensor_id':<12} {'timestamp':<22} {'temp':>6} {'dev':>6}")
        for sid, ts, temp, dev in anomalies:
            print(f"    {sid:<12} {ts!s:<22} {temp:>6} {dev:>6}")

        out = repo_root / "target" / "sensor_demo.parquet"
        con.execute(f"COPY readings_local TO '{out}' (FORMAT 'parquet')")
        print(f"\n(f) Wrote {out} ({out.stat().st_size:,} bytes)")

    finally:
        server.shutdown()
        thread.join(timeout=1.0)
        # Leave the .jsonl + its .dbfy_idx sidecar behind for inspection;
        # they live in `target/` so a `cargo clean` removes them.


if __name__ == "__main__":
    main()
