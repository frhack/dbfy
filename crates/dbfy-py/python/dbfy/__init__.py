"""Public Python API for the dbfy federated SQL engine."""

from typing import Any, List, Optional, TypedDict

from ._dbfy import Engine, DbfyError, __version__


class FilterDict(TypedDict):
    """A predicate pushed down by DataFusion to a programmatic provider."""

    column: str
    operator: str        # "=" | "<" | "<=" | ">" | ">=" | "IN"
    value: Any           # bool | int | float | str | list[str]


class ScanRequest(TypedDict):
    """Request payload passed to a programmatic provider's ``scan`` method."""

    query_id: str
    projection: Optional[List[str]]
    filters: List[FilterDict]
    limit: Optional[int]


__all__ = ["Engine", "FilterDict", "DbfyError", "ScanRequest", "__version__"]
