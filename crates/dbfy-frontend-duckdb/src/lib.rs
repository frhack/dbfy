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
    use std::ffi::{CStr, c_void};

    use duckdb::ffi;
    use serde::Deserialize;

    unsafe extern "C" {
        fn dbfy_shim_probe() -> u32;
        fn dbfy_shim_duckdb_version() -> *const std::os::raw::c_char;
        fn dbfy_shim_install_optimizer_db(db: ffi::duckdb_database) -> i32;
        fn dbfy_shim_observations_peek() -> *const std::os::raw::c_char;
        fn dbfy_shim_observations_clear();
        fn dbfy_shim_take_filters(bind_data: *mut c_void) -> *mut std::os::raw::c_char;
        fn dbfy_shim_free_string(p: *mut std::os::raw::c_char);
    }

    /// One predicate the optimizer recognised as `column OP constant`
    /// (or its commuted form). Type is the DuckDB `LogicalType` name —
    /// e.g. `INTEGER`, `BIGINT`, `VARCHAR`, `BOOLEAN`. The Rust side
    /// uses it to coerce the string `value` into the right
    /// `ScalarValue` variant when forwarding to typed providers.
    #[derive(Debug, Clone, Deserialize)]
    pub struct PushdownFilter {
        pub column: String,
        pub op: String,
        pub value: String,
        #[serde(rename = "type")]
        pub duck_type: String,
    }

    /// Confirm the C++ shim was compiled and linked. Returns 0xDBFEC5
    /// on success. Calling this from a test proves the build pipeline
    /// is functional without depending on any DuckDB internals yet.
    pub fn probe() -> u32 {
        unsafe { dbfy_shim_probe() }
    }

    /// Bundled DuckDB library version, as reported by `duckdb::DuckDB::LibraryVersion()`.
    pub fn duckdb_version() -> &'static str {
        unsafe {
            let p = dbfy_shim_duckdb_version();
            CStr::from_ptr(p)
                .to_str()
                .expect("duckdb version is valid utf-8")
        }
    }

    /// Register dbfy's OptimizerExtension on the given raw DuckDB
    /// database handle. After this call, every query planned on any
    /// connection to this database walks through dbfy's optimizer hook.
    ///
    /// Returns `Ok(())` on success. Failure means either the handle is
    /// null or the DuckDB internal layout assumption (`DatabaseWrapper`
    /// from `capi_internal.hpp`) no longer matches — both are bugs.
    ///
    /// # Safety
    /// The caller must ensure `raw_db` is a valid `duckdb_database`
    /// returned by `ffi::duckdb_open` and not yet closed.
    pub unsafe fn install_optimizer_hook(raw_db: ffi::duckdb_database) -> Result<(), i32> {
        let code = unsafe { dbfy_shim_install_optimizer_db(raw_db) };
        if code == 0 { Ok(()) } else { Err(code) }
    }

    /// Take and clear the thread-local observation buffer. Returns the
    /// `\n`-joined "function_name | filter_expr" lines the optimizer
    /// hook recorded since the last drain on this thread. Test-only —
    /// production code shouldn't need this.
    pub fn drain_observations() -> String {
        unsafe {
            let p = dbfy_shim_observations_peek();
            let cstr = CStr::from_ptr(p);
            let owned = cstr.to_string_lossy().into_owned();
            dbfy_shim_observations_clear();
            owned
        }
    }

    /// Take ownership of the pushdown filters the optimizer extension
    /// stashed for `bind_data`. Returns `Ok(vec)` (possibly empty) on
    /// success, `Err(_)` only if the JSON shape is wrong (a bug). A
    /// `Some` empty vec vs `None` distinguishes "no pushdown opportunity
    /// at all" from "the JSON parsed but contained zero filters".
    ///
    /// # Safety
    /// `bind_data` must be the same `*const T` pointer DuckDB stashed
    /// in the `LogicalGet::bind_data` slot for this query — typically
    /// retrieved via `InitInfo::get_bind_data::<T>()` cast to
    /// `*mut c_void`.
    pub unsafe fn take_pushdown_filters(
        bind_data: *mut c_void,
    ) -> Result<Option<Vec<PushdownFilter>>, serde_json::Error> {
        let raw = unsafe { dbfy_shim_take_filters(bind_data) };
        if raw.is_null() {
            return Ok(None);
        }
        let result = unsafe {
            let json_str = CStr::from_ptr(raw).to_string_lossy().into_owned();
            dbfy_shim_free_string(raw);
            serde_json::from_str::<Vec<PushdownFilter>>(&json_str)
        };
        result.map(Some)
    }
}

#[cfg(all(feature = "duckdb", not(feature = "loadable_extension")))]
pub use shim::{
    PushdownFilter, drain_observations as shim_drain_observations,
    duckdb_version as shim_duckdb_version, install_optimizer_hook, probe as shim_probe,
    take_pushdown_filters,
};

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
