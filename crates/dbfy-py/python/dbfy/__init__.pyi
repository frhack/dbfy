"""Type stubs for the dbfy federated SQL engine."""

from typing import Any, Awaitable, Final, Iterable, List, Mapping, Optional, Protocol, TypedDict

import pyarrow

__version__: Final[str]


class DbfyError(Exception):
    """Raised when the engine reports a config, REST, or execution error."""


class FilterDict(TypedDict):
    column: str
    operator: str
    value: Any


class ScanRequest(TypedDict):
    query_id: str
    projection: Optional[List[str]]
    filters: List[FilterDict]
    limit: Optional[int]


class Capabilities(TypedDict, total=False):
    """Optional dict returned by ``Provider.capabilities()`` to declare pushdown."""

    projection_pushdown: bool
    limit_pushdown: bool
    filters: Mapping[str, List[str]]


class Provider(Protocol):
    """Structural protocol for a Python-defined programmatic table provider.

    Implementing the optional ``capabilities()`` method (signature:
    ``() -> Capabilities``) opts the provider into pushdown of filters,
    projection, or limit. Without it, all predicates remain residual and
    DataFusion applies them locally after consuming the full scan stream.
    """

    def schema(self) -> pyarrow.Schema: ...
    def scan(self, request: ScanRequest) -> Iterable[pyarrow.RecordBatch]: ...


class Engine:
    """Embedded federated SQL engine over REST and programmatic providers."""

    @staticmethod
    def from_yaml(yaml: str) -> "Engine":
        """Build an engine from an inline YAML configuration string."""

    @staticmethod
    def from_path(path: str) -> "Engine":
        """Load and validate a YAML configuration file."""

    @staticmethod
    def empty() -> "Engine":
        """Build an engine with no REST sources, ready for `register_provider`."""

    def register_provider(self, table_name: str, provider: Provider) -> None:
        """Register a Python provider as a table reachable from SQL queries."""

    def registered_tables(self) -> List[str]:
        """Return the qualified names of registered tables (`source.table`)."""

    def query(self, sql: str) -> List[pyarrow.RecordBatch]:
        """Execute a SQL query and return the resulting Arrow record batches."""

    def explain(self, sql: str) -> str:
        """Render the optimized logical plan with pushdown details."""

    def query_async(self, sql: str) -> Awaitable[List[pyarrow.RecordBatch]]:
        """Execute a SQL query asynchronously, awaitable from asyncio."""

    def explain_async(self, sql: str) -> Awaitable[str]:
        """Render the optimized plan asynchronously, awaitable from asyncio."""

    def __repr__(self) -> str: ...
