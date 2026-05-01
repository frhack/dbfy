"""Smoke test for Python-defined programmatic providers.

Defines a Python provider that yields PyArrow batches lazily, registers it with
the engine, then runs a SQL query that exercises filter + projection over the
provider via DataFusion.
"""

from __future__ import annotations

from typing import Iterator

import pyarrow as pa

import dbfy


CUSTOMERS = [
    {"id": 1, "name": "Mario", "status": "active"},
    {"id": 2, "name": "Anna", "status": "inactive"},
    {"id": 3, "name": "Luca", "status": "active"},
    {"id": 4, "name": "Sara", "status": "active"},
]


class CustomersProvider:
    def __init__(self) -> None:
        self._schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string(), nullable=False),
                pa.field("status", pa.string(), nullable=False),
            ]
        )

    def schema(self) -> pa.Schema:
        return self._schema

    def scan(self, request: dbfy.ScanRequest) -> Iterator[pa.RecordBatch]:
        chunk_size = 2
        for offset in range(0, len(CUSTOMERS), chunk_size):
            chunk = CUSTOMERS[offset : offset + chunk_size]
            ids = pa.array([row["id"] for row in chunk], type=pa.int64())
            names = pa.array([row["name"] for row in chunk], type=pa.string())
            statuses = pa.array([row["status"] for row in chunk], type=pa.string())
            yield pa.record_batch([ids, names, statuses], schema=self._schema)


def main() -> None:
    engine = dbfy.Engine.empty()
    engine.register_provider("app.customers", CustomersProvider())

    assert engine.registered_tables() == ["app.customers"], engine.registered_tables()

    batches = engine.query(
        "SELECT id, name FROM app.customers WHERE status = 'active' ORDER BY id"
    )
    rows = pa.Table.from_batches(batches).to_pylist()
    assert rows == [
        {"id": 1, "name": "Mario"},
        {"id": 3, "name": "Luca"},
        {"id": 4, "name": "Sara"},
    ], rows

    explanation = engine.explain("SELECT id FROM app.customers")
    assert "provider: programmatic" in explanation, explanation

    print("OK provider rows=", rows)


if __name__ == "__main__":
    main()
