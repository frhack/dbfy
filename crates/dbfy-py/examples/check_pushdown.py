"""Empirical check: does predicate pushdown reach the Python provider?

Counts the rows the provider walks for several queries to expose what
DataFusion actually pushes down today.
"""

from dataclasses import dataclass, field
from typing import ClassVar, Iterator, List

import pyarrow as pa

import dbfy


@dataclass(slots=True)
class CountingRangeProvider:
    """Same as RangeProvider but counts rows walked and prints the request."""

    start: int
    stop: int
    chunk_size: int = 1024
    rows_walked: int = field(default=0, init=False)
    last_request: dict = field(default_factory=dict, init=False)

    _SCHEMA: ClassVar[pa.Schema] = pa.schema([pa.field("n", pa.int64(), nullable=False)])

    def schema(self) -> pa.Schema:
        return self._SCHEMA

    def scan(self, request) -> Iterator[pa.RecordBatch]:
        self.last_request = dict(request)
        self.rows_walked = 0
        buffer: List[int] = []
        remaining = request["limit"]

        for n in range(self.start, self.stop):
            buffer.append(n)
            self.rows_walked += 1
            if remaining is not None:
                remaining -= 1
                if remaining == 0:
                    yield self._batch(buffer)
                    return
            if len(buffer) >= self.chunk_size:
                yield self._batch(buffer)
                buffer = []
        if buffer:
            yield self._batch(buffer)

    def _batch(self, values: List[int]) -> pa.RecordBatch:
        return pa.record_batch([pa.array(values, type=pa.int64())], schema=self._SCHEMA)


def run(label: str, sql: str, provider: CountingRangeProvider, engine: dbfy.Engine) -> None:
    print(f"=== {label}")
    print(f"  SQL:        {sql}")
    print(f"  Plan:")
    for line in engine.explain(sql).splitlines():
        print(f"    {line}")
    rows = pa.Table.from_batches(engine.query(sql)).to_pylist()
    print(f"  request:    {provider.last_request}")
    print(f"  rows walked by provider: {provider.rows_walked:,}")
    print(f"  rows returned:           {len(rows)}")
    print()


def main() -> None:
    provider = CountingRangeProvider(0, 1_000_000)
    engine = dbfy.Engine.empty()
    engine.register_provider("math.numbers", provider)

    run("WHERE only — pushdown candidate", "SELECT n FROM math.numbers WHERE n < 10",
        provider, engine)
    run("LIMIT only — pushdown succeeds", "SELECT n FROM math.numbers LIMIT 5",
        provider, engine)
    run("WHERE + LIMIT — limit not pushed past filter",
        "SELECT n FROM math.numbers WHERE n < 10 LIMIT 5", provider, engine)


if __name__ == "__main__":
    main()
