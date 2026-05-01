//! Build script for the optional C++ shim used in bundled-DuckDB mode.
//!
//! Layered behaviour:
//!
//! * No features → no-op. Default workspace `cargo check` stays fast.
//! * `--features loadable_extension` → no-op. The loadable extension uses
//!   only the C API and cannot link a C++ shim alongside the host DuckDB.
//! * `--features duckdb` (bundled) → compile `cpp/extension_shim.cpp`
//!   against `libduckdb-sys`'s extracted DuckDB headers and link the
//!   resulting object into the consumer rlib.
//!
//! Header discovery: when `DEP_DUCKDB_INCLUDE` is published by
//! `libduckdb-sys` (after <https://github.com/duckdb/duckdb-rs/pull/753>
//! lands and ships) we read it directly. Until then, `libduckdb-sys`
//! does not export its include path to downstream build scripts, so
//! we fall back to globbing
//! `target/<profile>/build/libduckdb-sys-<hash>/out/duckdb/src/include`.
//! When the upstream PR is in a published release, drop the glob
//! branch and treat `DEP_DUCKDB_INCLUDE` as required.

fn main() {
    println!("cargo:rerun-if-changed=cpp/extension_shim.cpp");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=DEP_DUCKDB_INCLUDE");

    if cfg!(feature = "duckdb") && !cfg!(feature = "loadable_extension") {
        bundled::compile_shim();
    }
}

#[cfg(feature = "duckdb")]
mod bundled {
    use std::path::PathBuf;

    pub fn compile_shim() {
        let include = locate_include().expect(
            "could not find libduckdb-sys's DuckDB headers — neither \
             DEP_DUCKDB_INCLUDE was set nor a libduckdb-sys-* build dir \
             was found in the target tree",
        );
        eprintln!("dbfy: compiling C++ shim against headers at {}", include.display());

        cc::Build::new()
            .cpp(true)
            .std("c++17")
            .file("cpp/extension_shim.cpp")
            .include(&include)
            .flag_if_supported("-Wno-unused-parameter")
            .flag_if_supported("-Wno-unused-variable")
            .compile("dbfy_duckdb_shim");
    }

    fn locate_include() -> Option<PathBuf> {
        if let Ok(s) = std::env::var("DEP_DUCKDB_INCLUDE") {
            return Some(PathBuf::from(s));
        }
        glob_libduckdb_sys_include()
    }

    /// Fallback: walk up from `OUT_DIR` to `target/<profile>/build/`,
    /// then find a `libduckdb-sys-*/out/duckdb/src/include` directory.
    /// Hack — drop once duckdb/duckdb-rs#753 is in a released libduckdb-sys.
    fn glob_libduckdb_sys_include() -> Option<PathBuf> {
        let out_dir = PathBuf::from(std::env::var_os("OUT_DIR")?);
        let build_dir = out_dir.parent()?.parent()?;
        for entry in std::fs::read_dir(build_dir).ok()?.flatten() {
            let s = entry.file_name().to_string_lossy().into_owned();
            if !s.starts_with("libduckdb-sys-") {
                continue;
            }
            let candidate = entry.path().join("out/duckdb/src/include");
            if candidate.is_dir() {
                return Some(candidate);
            }
        }
        None
    }
}
