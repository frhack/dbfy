"""Idiomatic Python programmatic table provider for dbfy.

Exposes a synthetic numeric range as a SQL table and demonstrates the
contract a provider must satisfy:

* ``schema()`` returns the PyArrow ``Schema`` of the rows the provider will
  yield. It is queried once, at registration time.
* ``scan(request)`` is called per query and must return an iterable of
  ``pyarrow.RecordBatch`` objects whose schema matches ``schema()``. The
  engine pulls batches lazily; when an upstream ``LIMIT`` is satisfied,
  the underlying iterator is dropped, propagating ``GeneratorExit`` into
  this generator on the next ``yield``.

Run with::

    .venv/bin/python crates/dbfy-py/examples/programmatic_provider.py
"""

from dataclasses import dataclass
from typing import ClassVar, Iterator, List

import pyarrow as pa

import dbfy


@dataclass(frozen=True, slots=True)
class RangeProvider:
    """Yield integers in ``[start, stop)`` together with their square.

    The provider is intentionally tiny but illustrates two performance
    patterns worth copying:

    * It honours ``request['limit']`` to stop walking the range as soon as
      enough rows have been emitted; this matters when DataFusion can push
      the SQL ``LIMIT`` straight to the provider.
    * It emits chunked ``RecordBatch`` values rather than one giant batch,
      so DataFusion can start work as soon as the first chunk lands and
      memory usage stays flat for huge ranges.
    """

    start: int
    stop: int
    step: int = 1
    chunk_size: int = 1024

    _SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            pa.field("n", pa.int64(), nullable=False),
            pa.field("squared", pa.int64(), nullable=False),
        ]
    )

    def schema(self) -> pa.Schema:
        return self._SCHEMA

    def scan(self, request: dbfy.ScanRequest) -> Iterator[pa.RecordBatch]:
        remaining = request["limit"]  # may be None
        buffer: List[int] = []

        for n in range(self.start, self.stop, self.step):
            buffer.append(n)
            if remaining is not None:
                remaining -= 1
                if remaining == 0:
                    yield self._build_batch(buffer)
                    return
            if len(buffer) >= self.chunk_size:
                yield self._build_batch(buffer)
                buffer = []

        if buffer:
            yield self._build_batch(buffer)

    def _build_batch(self, values: List[int]) -> pa.RecordBatch:
        return pa.record_batch(
            [
                pa.array(values, type=pa.int64()),
                pa.array([n * n for n in values], type=pa.int64()),
            ],
            schema=self._SCHEMA,
        )


def main() -> None:
    engine = dbfy.Engine.empty()
    engine.register_provider("math.numbers", RangeProvider(0, 1_000_000))

    # 1) Plain LIMIT — DataFusion pushes limit=5 to scan(); the generator
    #    walks at most 5 values out of one million before yielding control.
    print("=== SELECT n, squared FROM math.numbers LIMIT 5")
    for row in pa.Table.from_batches(
        engine.query("SELECT n, squared FROM math.numbers LIMIT 5")
    ).to_pylist():
        print(row)

    # 2) Filter without LIMIT — DataFusion consumes the whole stream and
    #    applies the predicate locally, demonstrating that residual filters
    #    work fine even when the provider exposes no pushdown capabilities.
    print("\n=== SELECT n, squared FROM math.numbers WHERE n IN (0, 7, 49) ORDER BY n")
    for row in pa.Table.from_batches(
        engine.query(
            "SELECT n, squared FROM math.numbers "
            "WHERE n IN (0, 7, 49) ORDER BY n"
        )
    ).to_pylist():
        print(row)

    # 3) Aggregate — nothing pushed; DataFusion consumes the whole stream.
    print("\n=== SELECT count(*), sum(squared) FROM math.numbers WHERE n < 1000")
    for row in pa.Table.from_batches(
        engine.query(
            "SELECT count(*) AS n_rows, sum(squared) AS sum_sq "
            "FROM math.numbers WHERE n < 1000"
        )
    ).to_pylist():
        print(row)


if __name__ == "__main__":
    main()
