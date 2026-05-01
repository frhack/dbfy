"""End-to-end smoke test for the dbfy Python bindings.

Spins up an in-process HTTP server, points the engine at it, and asserts that
queries return PyArrow batches with the expected rows.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pyarrow as pa

import dbfy


CUSTOMERS_PAYLOAD = {
    "data": [
        {"id": 1, "name": "Mario", "status": "active", "tags": ["alpha", "beta"]},
        {"id": 2, "name": "Anna", "status": "inactive", "tags": ["gamma"]},
        {"id": 3, "name": "Luca", "status": "active", "tags": ["delta"]},
    ]
}


class CustomersHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (stdlib API)
        if self.path.startswith("/customers"):
            body = json.dumps(CUSTOMERS_PAYLOAD).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_args, **_kwargs):  # silence noisy default logging
        pass


def start_server() -> tuple[str, HTTPServer, threading.Thread]:
    server = HTTPServer(("127.0.0.1", 0), CustomersHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", server, thread


def main() -> None:
    base_url, server, thread = start_server()
    try:
        config = f"""
version: 1
sources:
  crm:
    type: rest
    base_url: {base_url}
    tables:
      customers:
        endpoint:
          method: GET
          path: /customers
        root: "$.data[*]"
        columns:
          id:
            path: "$.id"
            type: int64
          name:
            path: "$.name"
            type: string
          status:
            path: "$.status"
            type: string
          first_tag:
            path: "$.tags[0]"
            type: string
"""
        engine = dbfy.Engine.from_yaml(config)

        assert engine.registered_tables() == ["crm.customers"], engine.registered_tables()

        batches = engine.query(
            "SELECT id, name, first_tag "
            "FROM crm.customers "
            "WHERE status = 'active' "
            "ORDER BY id"
        )
        assert all(isinstance(batch, pa.RecordBatch) for batch in batches)

        table = pa.Table.from_batches(batches)
        rows = table.to_pylist()
        assert rows == [
            {"id": 1, "name": "Mario", "first_tag": "alpha"},
            {"id": 3, "name": "Luca", "first_tag": "delta"},
        ], rows

        explanation = engine.explain("SELECT id FROM crm.customers WHERE status = 'active'")
        assert "provider: rest" in explanation, explanation
        assert "RestStreamExec" not in explanation  # explain shows logical plan

        try:
            dbfy.Engine.from_yaml("version: 1\nsources: {}")
        except dbfy.DbfyError as exc:
            assert "at least one source" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected DbfyError for empty sources")

        print("OK", "rows=", rows)
    finally:
        server.shutdown()
        thread.join(timeout=1.0)


if __name__ == "__main__":
    main()
