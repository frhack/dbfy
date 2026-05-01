#!/usr/bin/env python3
"""Append a DuckDB extension metadata + signature footer to a cdylib so
it can be `LOAD`ed by a stock DuckDB instance.

The footer is 512 bytes appended to the end of the file:

    [256 bytes metadata][256 bytes signature]

Metadata is 8 × 32-byte fields, NUL-padded:
    0: METADATA_VERSION (literal "4")
    1: extension_version
    2: duckdb_version
    3: platform
    4: extension_abi (typically empty for C_API)
    5: function_index (empty)
    6: dependencies (empty)
    7: reserved (empty)

Signature is 256 bytes of zeros for an unsigned extension; loading then
requires `SET allow_unsigned_extensions = true;` on the DuckDB side.

Usage::

    python3 append_metadata.py target/debug/libdbfy_duckdb.so \\
        --output target/debug/dbfy.duckdb_extension \\
        --duckdb-version v1.5.2 --platform linux_amd64
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _field(s: str, size: int = 32) -> bytes:
    encoded = s.encode("utf-8")
    if len(encoded) > size:
        raise ValueError(f"field {s!r} exceeds {size} bytes")
    return encoded.ljust(size, b"\0")


def append_metadata(
    binary: Path,
    output: Path,
    *,
    extension_name: str,
    extension_version: str,
    duckdb_capi_version: str,
    platform: str,
    extension_abi: str = "C_STRUCT",
) -> None:
    """DuckDB reads 8 fields × 32 bytes from the file then reverses the
    array, so file order is the *reverse* of the logical field order:

        file offset       logical field after reverse    meaning
        ----------------- ------------------------------ -----------------------
        0..32             field[7]                       (unused)
        32..64            field[6]                       (unused)
        64..96            field[5]                       (unused)
        96..128           field[4]                       extension_abi
        128..160          field[3]                       extension_version
        160..192          field[2]                       duckdb_capi_version (when ABI=C_STRUCT)
        192..224          field[1]                       platform
        224..256          field[0]                       MAGIC = "4"

    The 32-byte MAGIC value is a literal "4" followed by 31 NULs (see
    EXPECTED_MAGIC_VALUE in `extension.hpp`).
    """
    _ = extension_name  # currently not encoded in the layout above
    metadata = b"".join([
        _field(""),                       # 0..32  (field[7])
        _field(""),                       # 32..64 (field[6])
        _field(""),                       # 64..96 (field[5])
        _field(extension_abi),            # 96..128 (field[4])
        _field(extension_version),        # 128..160 (field[3])
        _field(duckdb_capi_version),      # 160..192 (field[2])
        _field(platform),                 # 192..224 (field[1])
        _field("4"),                      # 224..256 (field[0]) MAGIC
    ])
    assert len(metadata) == 256, f"metadata is {len(metadata)} bytes"
    signature = b"\0" * 256

    payload = binary.read_bytes() + metadata + signature
    output.write_bytes(payload)
    output.chmod(0o755)
    print(f"Wrote {output} ({len(payload):,} bytes; +512 byte footer)")


def main() -> int:
    parser = argparse.ArgumentParser(description="append DuckDB extension metadata footer")
    parser.add_argument("binary", type=Path, help="cdylib produced by cargo build")
    parser.add_argument("--output", type=Path, required=True, help="path to write the .duckdb_extension file")
    parser.add_argument("--name", default="dbfy")
    parser.add_argument("--extension-version", default="v0.1.0")
    parser.add_argument("--duckdb-capi-version", default="v1.4.0")
    parser.add_argument("--platform", default="linux_amd64")
    parser.add_argument("--extension-abi", default="C_STRUCT")
    args = parser.parse_args()

    if not args.binary.exists():
        print(f"error: binary not found: {args.binary}", file=sys.stderr)
        return 1

    append_metadata(
        binary=args.binary,
        output=args.output,
        extension_name=args.name,
        extension_version=args.extension_version,
        duckdb_capi_version=args.duckdb_capi_version,
        platform=args.platform,
        extension_abi=args.extension_abi,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
