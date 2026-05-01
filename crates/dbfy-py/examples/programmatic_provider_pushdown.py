"""Idiomatic provider WITH predicate, limit, and projection pushdown.

The provider exposes ``metrics.readings`` and demonstrates the two
distinct wins that pushdown delivers:

A. **Iteration savings** — when a pushed predicate matches a column on
   which the provider keeps an index (here a ``dict[str, list]`` by
   sensor name), the loop never even touches non-matching rows. This is
   the same trick a real backend uses (DB index lookup, partition
   pruning, file skipping).

B. **Cross-language savings** — every row the provider emits crosses
   the Python→Arrow→Rust boundary. By filtering inline, we ship only
   matching rows, sparing DataFusion the work and reducing batch-build
   overhead.

Two counters expose both effects: ``rows_scanned`` (touched by the
Python loop) and ``rows_emitted`` (sent downstream as RecordBatch rows).

Run with::

    .venv/bin/python crates/dbfy-py/examples/programmatic_provider_pushdown.py
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, ClassVar, Dict, Iterable, Iterator, List

import pyarrow as pa

import dbfy


# Operator -> per-cell evaluation. Built once; values bound at compile time.
_OPS: Dict[str, Callable[[object, object], bool]] = {
    "=":  lambda v, x: v == x,
    "<":  lambda v, x: v <  x,
    "<=": lambda v, x: v <= x,
    ">":  lambda v, x: v >  x,
    ">=": lambda v, x: v >= x,
    "IN": lambda v, x: v in x,
}


@dataclass(slots=True)
class SensorReadingsProvider:
    """In-memory ``metrics.readings`` table with a sensor-name index."""

    by_sensor: Dict[str, List[dict]]
    chunk_size: int = 256
    rows_scanned: int = field(default=0, init=False)
    rows_emitted: int = field(default=0, init=False)

    _SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("sensor", pa.string(), nullable=False),
            pa.field("temperature", pa.float64(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
        ]
    )

    @classmethod
    def from_rows(cls, rows: Iterable[dict]) -> "SensorReadingsProvider":
        index: Dict[str, List[dict]] = defaultdict(list)
        for row in rows:
            index[row["sensor"]].append(row)
        return cls(by_sensor=dict(index))

    @property
    def total_rows(self) -> int:
        return sum(len(bucket) for bucket in self.by_sensor.values())

    # ---- dbfy.Provider protocol -----------------------------------

    def schema(self) -> pa.Schema:
        return self._SCHEMA

    def capabilities(self) -> dict:
        return {
            "projection_pushdown": True,
            "limit_pushdown": True,
            "filters": {
                "=":  ["sensor", "status"],
                "IN": ["sensor", "status"],
                "<":  ["id", "temperature"],
                "<=": ["id", "temperature"],
                ">":  ["id", "temperature"],
                ">=": ["id", "temperature"],
            },
        }

    def scan(self, request: dbfy.ScanRequest) -> Iterator[pa.RecordBatch]:
        self.rows_scanned = 0
        self.rows_emitted = 0

        source = self._narrow_source(request["filters"])
        predicate = self._compile_predicate(request["filters"])
        wanted = request["projection"] or [f.name for f in self._SCHEMA]
        out_schema = pa.schema([self._SCHEMA.field(name) for name in wanted])
        limit = request["limit"]

        buffer: List[dict] = []
        for row in source:
            self.rows_scanned += 1
            if not predicate(row):
                continue
            buffer.append(row)
            if limit is not None and self.rows_emitted + len(buffer) >= limit:
                self.rows_emitted += len(buffer)
                yield self._build_batch(buffer, wanted, out_schema)
                return
            if len(buffer) >= self.chunk_size:
                self.rows_emitted += len(buffer)
                yield self._build_batch(buffer, wanted, out_schema)
                buffer = []
        if buffer:
            self.rows_emitted += len(buffer)
            yield self._build_batch(buffer, wanted, out_schema)

    # ---- pushdown helpers -------------------------------------------

    def _narrow_source(self, filters: List[dict]) -> Iterator[dict]:
        """Use the sensor index to skip non-matching buckets entirely.

        Both ``=`` and ``IN`` are honoured. ``IN`` arrives even when
        DataFusion rewrites a short ``IN`` list to ``OR``-of-equality at
        planning time, because the core normalises the OR chain back into
        an ``InList`` filter (see ``try_or_chain_to_in_list``).
        """
        sensor_eq: str | None = None
        sensor_in: List[str] | None = None
        for f in filters:
            if f["column"] != "sensor":
                continue
            if f["operator"] == "=":
                sensor_eq = f["value"]
            elif f["operator"] == "IN":
                sensor_in = list(f["value"])

        if sensor_eq is not None:
            return iter(self.by_sensor.get(sensor_eq, ()))
        if sensor_in is not None:
            return (r for s in sensor_in for r in self.by_sensor.get(s, ()))
        return (r for bucket in self.by_sensor.values() for r in bucket)

    @staticmethod
    def _compile_predicate(filters: List[dict]) -> Callable[[dict], bool]:
        if not filters:
            return lambda _row: True
        # Pre-bind values; for IN convert to a set so per-row lookup is O(1).
        checks: List[tuple] = []
        for f in filters:
            value = set(f["value"]) if f["operator"] == "IN" else f["value"]
            checks.append((f["column"], value, _OPS[f["operator"]]))

        def predicate(row: dict) -> bool:
            return all(op(row[col], val) for col, val, op in checks)

        return predicate

    def _build_batch(
        self,
        rows: List[dict],
        wanted: List[str],
        out_schema: pa.Schema,
    ) -> pa.RecordBatch:
        arrays = [
            pa.array([r[name] for r in rows], type=self._SCHEMA.field(name).type)
            for name in wanted
        ]
        return pa.record_batch(arrays, schema=out_schema)


# ---- demo --------------------------------------------------------------

def _make_rows(n: int) -> Iterator[dict]:
    sensors = ("alpha", "beta", "gamma")
    for i in range(n):
        yield {
            "id": i,
            "sensor": sensors[i % 3],
            "temperature": 18.0 + (i % 23),
            "status": "ok" if i % 7 else "alarm",
        }


def _run(label: str, sql: str, provider: SensorReadingsProvider, engine: dbfy.Engine) -> None:
    rows = pa.Table.from_batches(engine.query(sql)).to_pylist()
    total = provider.total_rows
    print(f"=== {label}")
    print(f"  rows_scanned: {provider.rows_scanned:>9,} / {total:,}")
    print(f"  rows_emitted: {provider.rows_emitted:>9,}")
    print(f"  rows_returned: {len(rows)}")
    for r in rows[:5]:
        print(f"    {r}")
    print()


def main() -> None:
    provider = SensorReadingsProvider.from_rows(_make_rows(1_000_000))
    engine = dbfy.Engine.empty()
    engine.register_provider("metrics.readings", provider)

    # 1. Index hit (`sensor = 'alpha'`) + range + LIMIT.
    #    Index narrows source from 1,000,000 -> ~333,333.
    #    LIMIT pushes through (no ORDER BY), so the loop stops at 5 matches.
    _run(
        "indexed sensor + range + LIMIT",
        """
        SELECT id, sensor, temperature
        FROM metrics.readings
        WHERE sensor = 'alpha' AND temperature >= 35 AND status = 'ok'
        LIMIT 5
        """,
        provider,
        engine,
    )

    # 2. Index hit via IN over multiple sensors + range + aggregate.
    #    DataFusion rewrites short IN lists as OR-of-equality; the core
    #    rebuilds them into InList in `try_or_chain_to_in_list`, so the
    #    provider sees a real `IN` filter and walks only the alpha+beta
    #    buckets (~666,666). gamma is skipped entirely.
    _run(
        "indexed sensor IN + range + aggregate",
        """
        SELECT count(*) AS n, avg(temperature) AS avg_t
        FROM metrics.readings
        WHERE sensor IN ('alpha', 'beta') AND temperature >= 30
        """,
        provider,
        engine,
    )

    # 3. No sensor filter — index gives no help; predicate still prunes
    #    cross-language traffic to ~14% of the source.
    _run(
        "no index hit, predicate-only pushdown",
        """
        SELECT count(*) AS n
        FROM metrics.readings
        WHERE temperature >= 35 AND status = 'ok'
        """,
        provider,
        engine,
    )


if __name__ == "__main__":
    main()
