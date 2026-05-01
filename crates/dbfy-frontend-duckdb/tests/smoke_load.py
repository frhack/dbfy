"""Smoke test: LOAD the freshly-built dbfy.duckdb_extension into a vanilla
DuckDB Python connection (no Rust embedding!) and run an end-to-end query
against an in-process HTTP server.

This is the closest thing to "a real user using the community extension".

Run after::

    cargo build -p dbfy-frontend-duckdb --features loadable_extension --jobs 1
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import duckdb


PAYLOAD = {
    "data": [
        {"id": 1, "name": "Mario", "status": "active"},
        {"id": 2, "name": "Anna",  "status": "inactive"},
        {"id": 3, "name": "Luca",  "status": "active"},
    ]
}


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path.startswith("/customers"):
            body = json.dumps(PAYLOAD).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_args, **_kwargs):
        pass


def _start_server() -> tuple[str, HTTPServer, threading.Thread]:
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", server, thread


def main() -> None:
    import os
    override = os.environ.get("DBFY_EXT_PATH")
    repo_root = Path(__file__).resolve().parents[3]
    candidates = (
        [Path(override)] if override else [
            repo_root / "target" / "release" / "dbfy.duckdb_extension",
            repo_root / "target" / "debug"   / "dbfy.duckdb_extension",
        ]
    )
    ext_path = next((c for c in candidates if c.exists()), None)
    if ext_path is None:
        raise SystemExit(
            "dbfy.duckdb_extension not found; build with:\n"
            "  cargo build -p dbfy-frontend-duckdb --features loadable_extension --release --jobs 1\n"
            "  python3 crates/dbfy-frontend-duckdb/scripts/append_metadata.py \\\n"
            "    target/release/libdbfy_duckdb.so \\\n"
            "    --output target/release/dbfy.duckdb_extension --duckdb-capi-version v1.2.0\n"
            "Or set DBFY_EXT_PATH to override."
        )
    so_path = ext_path
    print(f"loading {ext_path} ({ext_path.stat().st_size:,} bytes)")

    base, server, thread = _start_server()
    try:
        con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        con.execute(f"LOAD '{so_path}'")
        rows = con.execute(
            f"""
            SELECT id, name FROM dbfy_rest(
                '{base}/customers',
                config := '
root: $.data[*]
columns:
  id:     {{path: "$.id",     type: int64}}
  name:   {{path: "$.name",   type: string}}
  status: {{path: "$.status", type: string}}
'
            )
            WHERE status = 'active'
            ORDER BY id
            """
        ).fetchall()
        assert rows == [(1, "Mario"), (3, "Luca")], rows
        print("OK loadable: rows =", rows)
    finally:
        server.shutdown()
        thread.join(timeout=1.0)


if __name__ == "__main__":
    main()
