#!/usr/bin/env python3
"""Tiny GraphQL stub for the saas-metrics showcase.

Mimics the shape of a real product-analytics API (think Mixpanel /
Amplitude). Returns per-customer usage counts for the current
billing period when given a single `month` variable. Listening on
http://127.0.0.1:8766/graphql.

We don't bring in a full GraphQL stack — `aiohttp` + a hand-rolled
`/graphql` handler are enough to satisfy the dbfy pushdown contract:
the engine POSTs a body with `query` + `variables`, we read the
month variable and return the matching slice.
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

# Per-customer per-month usage counts. Each customer is keyed by
# the slug that appears as `slug` in the Postgres `customers`
# table — that's what the join is on.
USAGE = {
    "2026-04-01": [
        {"customer_slug": "acme",      "events": 8_421_330, "mau": 12340},
        {"customer_slug": "globex",    "events":   742_005, "mau":  1980},
        {"customer_slug": "initech",   "events": 1_055_300, "mau":  3120},
        {"customer_slug": "soylent",   "events":     8_900, "mau":   140},
        {"customer_slug": "umbrella",  "events": 9_910_550, "mau": 18420},
        {"customer_slug": "cyberdyne", "events":   980_410, "mau":  2810},
        {"customer_slug": "tyrell",    "events":     2_100, "mau":    34},
        {"customer_slug": "massive",   "events": 5_500_000, "mau":  9100},
    ],
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args):  # noqa: A002 — base sig
        return  # silence

    def do_POST(self):
        if self.path != "/graphql":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        # The GraphQL query shape doesn't matter for the showcase
        # — we just respect the `month` variable to scope the
        # response. dbfy pushes the SQL `WHERE month = '2026-04-01'`
        # predicate into this variable via the YAML `pushdown`
        # mapping.
        month = (body.get("variables") or {}).get("month", "2026-04-01")
        data = USAGE.get(month, [])
        resp = {"data": {"usage": data}}
        payload = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main():
    server = HTTPServer(("127.0.0.1", 8766), Handler)
    print("graphql-server: http://127.0.0.1:8766/graphql")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
