"""End-to-end async smoke test for the dbfy Python bindings.

Same fixture as smoke.py but drives the engine via `await engine.query_async(...)`.
"""

from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pyarrow as pa

import dbfy


CUSTOMERS_PAYLOAD = {
    "data": [
        {"id": 1, "name": "Mario", "status": "active"},
        {"id": 2, "name": "Anna", "status": "inactive"},
        {"id": 3, "name": "Luca", "status": "active"},
    ]
}


class CustomersHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
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

    def log_message(self, *_args, **_kwargs):
        pass


def start_server() -> tuple[str, HTTPServer, threading.Thread]:
    server = HTTPServer(("127.0.0.1", 0), CustomersHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", server, thread


async def run_queries(engine: dbfy.Engine) -> None:
    batches = await engine.query_async(
        "SELECT id, name FROM crm.customers WHERE status = 'active' ORDER BY id"
    )
    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    rows = pa.Table.from_batches(batches).to_pylist()
    assert rows == [
        {"id": 1, "name": "Mario"},
        {"id": 3, "name": "Luca"},
    ], rows

    explanation = await engine.explain_async(
        "SELECT id FROM crm.customers WHERE status = 'active'"
    )
    assert "provider: rest" in explanation, explanation

    print("OK async rows=", rows)


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
"""
        engine = dbfy.Engine.from_yaml(config)
        asyncio.run(run_queries(engine))
    finally:
        server.shutdown()
        thread.join(timeout=1.0)


if __name__ == "__main__":
    main()
