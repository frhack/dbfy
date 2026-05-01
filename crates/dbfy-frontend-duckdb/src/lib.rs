//! DuckDB extension frontend for `dbfy`.
//!
//! Two build modes:
//!
//! * **default** (no features) — empty cdylib, fast to compile, useful as a
//!   workspace member while the DuckDB integration is iterated.
//! * **`--features duckdb`** — pulls the [`duckdb`] crate (bundled DuckDB
//!   C++) and exposes a [`register`] function that installs the
//!   `dbfy_rest()` table function on a DuckDB [`Connection`].
//!
//! ```no_run
//! # #[cfg(feature = "duckdb")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use duckdb::Connection;
//!
//! let conn = Connection::open_in_memory()?;
//! dbfy_duckdb::register(&conn)?;
//!
//! let mut stmt = conn.prepare(
//!     "SELECT * FROM dbfy_rest('https://api.example.com/users', \
//!      root := '$.data[*]')",
//! )?;
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "duckdb"))] fn main() {}
//! ```

#![allow(clippy::missing_safety_doc)]

pub use dbfy_config as config;
pub use dbfy_provider as provider;
pub use dbfy_provider_rest as provider_rest;
pub use dbfy_provider_rows_file as provider_rows_file;

#[cfg(any(feature = "duckdb", feature = "loadable_extension"))]
mod arrow_to_duckdb;
#[cfg(any(feature = "duckdb", feature = "loadable_extension"))]
mod rest_vtab;
#[cfg(any(feature = "duckdb", feature = "loadable_extension"))]
mod rows_file_vtab;

/// Bundled-only C++ shim. Compiled by `build.rs` when the `duckdb`
/// feature is active; not linked in `loadable_extension` builds since
/// the loadable artefact only sees the C API. Step-1 spike: `probe`
/// just returns a sentinel to confirm the link works end-to-end.
#[cfg(all(feature = "duckdb", not(feature = "loadable_extension")))]
mod shim {
    use std::ffi::CStr;

    unsafe extern "C" {
        fn dbfy_shim_probe() -> u32;
        fn dbfy_shim_duckdb_version() -> *const std::os::raw::c_char;
    }

    /// Confirm the C++ shim was compiled and linked. Returns 0xDBFEC5
    /// on success. Calling this from a test proves the build pipeline
    /// is functional without depending on any DuckDB internals yet.
    pub fn probe() -> u32 {
        unsafe { dbfy_shim_probe() }
    }

    /// Bundled DuckDB library version, as reported by `duckdb::DuckDB::LibraryVersion()`.
    /// Confirms the shim can reach into the C++ side of DuckDB.
    pub fn duckdb_version() -> &'static str {
        unsafe {
            let p = dbfy_shim_duckdb_version();
            CStr::from_ptr(p).to_str().expect("duckdb version is valid utf-8")
        }
    }
}

#[cfg(all(feature = "duckdb", not(feature = "loadable_extension")))]
pub use shim::{duckdb_version as shim_duckdb_version, probe as shim_probe};

#[cfg(any(feature = "duckdb", feature = "loadable_extension"))]
pub use rest_vtab::register as register_rest;
#[cfg(any(feature = "duckdb", feature = "loadable_extension"))]
pub use rows_file_vtab::register as register_rows_file;

/// Register all dbfy table functions on a DuckDB connection.
#[cfg(any(feature = "duckdb", feature = "loadable_extension"))]
pub fn register(conn: &duckdb::Connection) -> duckdb::Result<()> {
    register_rest(conn)?;
    register_rows_file(conn)?;
    Ok(())
}

/// Loadable-extension entrypoint.
///
/// Compiled only when the `loadable_extension` feature is enabled. The
/// resulting cdylib (`target/<profile>/libdbfy_duckdb.{so,dylib,dll}`)
/// is the `.duckdb_extension` binary that any compatible DuckDB host
/// can `LOAD`. The macro generates the C ABI symbols DuckDB looks for
/// when it loads the file.
///
/// Build:
///
/// ```bash
/// cargo build -p dbfy-frontend-duckdb --features loadable_extension --release --jobs 1
/// cp target/release/libdbfy_duckdb.so dbfy.duckdb_extension
/// ```
///
/// Use from any DuckDB:
///
/// ```sql
/// LOAD 'dbfy.duckdb_extension';
/// SELECT value FROM dbfy_rest('https://api.example.com/users');
/// ```
#[cfg(feature = "loadable_extension")]
#[duckdb::duckdb_entrypoint_c_api(ext_name = "dbfy", min_duckdb_version = "v1.2.0")]
pub fn extension_init(con: duckdb::Connection) -> duckdb::Result<(), Box<dyn std::error::Error>> {
    rest_vtab::register(&con)?;
    rows_file_vtab::register(&con)?;
    Ok(())
}

/// Returns the crate version. Useful for runtime probes from C/C++ hosts.
#[unsafe(no_mangle)]
pub extern "C" fn dbfy_duckdb_version() -> *const std::ffi::c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const std::ffi::c_char
}
