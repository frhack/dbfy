// dbfy DuckDB extension shim — bundled-mode-only filter pushdown bridge.
//
// This file is compiled by `build.rs` when the `duckdb` cargo feature is
// active, and linked into the rlib alongside the bundled libduckdb. It
// reaches into the C++ side of DuckDB to expose hooks that the C API
// does not (yet) cover — primarily filter pushdown for our table
// functions.
//
// Step-1 spike: verifies the build pipeline + that we can include and
// call into the bundled DuckDB C++ headers. No optimizer hooks yet;
// those land in step 2.

#include <cstdint>
#include <cstring>

#include "duckdb.hpp"

extern "C" {

/// Returns a known sentinel value. Used by the Rust side to confirm the
/// shim was actually compiled and linked when the `duckdb` feature is on.
uint32_t dbfy_shim_probe(void) {
    return 0xDBFEC5;
}

/// Returns the bundled DuckDB library version string. Proves the
/// `duckdb.hpp` include resolved against libduckdb-sys's extracted
/// headers and that the C++ symbols are reachable from our shim.
/// The returned pointer points at static storage; the caller must
/// not free it.
const char* dbfy_shim_duckdb_version(void) {
    return duckdb::DuckDB::LibraryVersion();
}

}  // extern "C"
