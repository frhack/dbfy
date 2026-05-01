"""Smoke test verifying predicate pushdown into a Python provider.

Provider declares filter capabilities for `n` and the `=` operator on
`status`. The test asserts that:

* `request['filters']` contains the pushed predicates,
* the provider only scans the rows it declares it will, not the full range,
* DataFusion still gets correct results (the filter is "Inexact" so DataFusion
  may double-check, but the provider has done its job).
"""

import pyarrow as pa

import dbfy


class FilterAwareProvider:
    """Yields integers in [start, stop), honouring pushed predicates."""

    def __init__(self, start: int = 0, stop: int = 1_000_000):
        self._start = start
        self._stop = stop
        self._scanned_rows = 0
        self._last_filters: list = []
        self._schema = pa.schema(
            [
                pa.field("n", pa.int64(), nullable=False),
                pa.field("status", pa.string(), nullable=False),
            ]
        )

    def schema(self) -> pa.Schema:
        return self._schema

    def capabilities(self) -> dict:
        return {
            "limit_pushdown": True,
            "projection_pushdown": True,
            "filters": {
                "<": ["n"],
                "<=": ["n"],
                ">": ["n"],
                ">=": ["n"],
                "=": ["n", "status"],
            },
        }

    def scan(self, request):
        self._last_filters = list(request["filters"])
        lo, hi = self._start, self._stop
        eq_status = None
        eq_n = None

        for f in request["filters"]:
            col, op, val = f["column"], f["operator"], f["value"]
            if col == "n":
                if op == "<":
                    hi = min(hi, val)
                elif op == "<=":
                    hi = min(hi, val + 1)
                elif op == ">":
                    lo = max(lo, val + 1)
                elif op == ">=":
                    lo = max(lo, val)
                elif op == "=":
                    eq_n = val
            elif col == "status" and op == "=":
                eq_status = val

        if eq_n is not None:
            lo, hi = max(lo, eq_n), min(hi, eq_n + 1)

        limit = request["limit"]
        wanted = [f.name for f in self._schema] if request["projection"] is None else request["projection"]

        rows = []
        for n in range(lo, hi):
            status = "active" if n % 2 == 0 else "inactive"
            if eq_status is not None and status != eq_status:
                continue
            self._scanned_rows += 1
            rows.append((n, status))
            if limit is not None and len(rows) >= limit:
                break

        cols = {
            "n": pa.array([r[0] for r in rows], type=pa.int64()),
            "status": pa.array([r[1] for r in rows], type=pa.string()),
        }
        out_schema = pa.schema([self._schema.field(name) for name in wanted])
        yield pa.record_batch([cols[name] for name in wanted], schema=out_schema)


def main() -> None:
    provider = FilterAwareProvider(0, 1_000_000)
    engine = dbfy.Engine.empty()
    engine.register_provider("nums", provider)

    # Case 1: range filter — should push `>= 100` and `< 110` to the provider.
    rows = pa.Table.from_batches(
        engine.query("SELECT n FROM nums WHERE n >= 100 AND n < 110 ORDER BY n")
    ).to_pylist()
    assert [r["n"] for r in rows] == list(range(100, 110)), rows
    pushed_ops = sorted(f["operator"] for f in provider._last_filters)
    assert pushed_ops == ["<", ">="], pushed_ops
    walked_after_range = provider._scanned_rows
    assert walked_after_range <= 10, f"walked {walked_after_range}, expected <= 10"

    # Case 2: equality on string column.
    provider._scanned_rows = 0
    rows = pa.Table.from_batches(
        engine.query(
            "SELECT n, status FROM nums WHERE n < 20 AND status = 'active' ORDER BY n"
        )
    ).to_pylist()
    assert [r["n"] for r in rows] == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18], rows
    eq_status_pushed = any(
        f["column"] == "status" and f["operator"] == "=" and f["value"] == "active"
        for f in provider._last_filters
    )
    assert eq_status_pushed, provider._last_filters

    # Case 3: explain shows the provider's capabilities.
    explanation = engine.explain("SELECT n FROM nums WHERE n < 5")
    assert "=:n|status" in explanation and "<:n" in explanation, explanation
    assert "accepted filters: n < 5" in explanation, explanation

    print(
        "OK pushdown:",
        f"range walked={walked_after_range},",
        f"pushed_filters={[f['operator'] for f in provider._last_filters]}",
    )


if __name__ == "__main__":
    main()
