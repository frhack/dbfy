//! Build script for the optional C++ shim used in bundled-DuckDB mode.
//!
//! Layered behaviour:
//!
//! * No features → no-op. Default workspace `cargo check` stays fast.
//! * `--features loadable_extension` → no-op. The loadable extension uses
//!   only the C API and cannot link a C++ shim alongside the host DuckDB.
//! * `--features duckdb` (bundled) → compile `cpp/extension_shim.cpp`
//!   against the headers `libduckdb-sys` extracted under
//!   `target/<profile>/build/libduckdb-sys-<hash>/out/duckdb/src/include`,
//!   and link the resulting object into the consumer rlib.
//!
//! Caveat — header discovery hack. `libduckdb-sys` does not currently
//! emit `cargo:include=...` (it lacks a `links = "duckdb"` directive in
//! its Cargo.toml). Until that lands upstream, downstream crates that
//! want the C++ headers have to glob the target tree to locate the
//! extracted `out/duckdb/src/include` directory. A proper fix is a PR
//! against `duckdb-rs` adding `links` + `cargo:include=$OUT_DIR/duckdb/src/include`.

fn main() {
    println!("cargo:rerun-if-changed=cpp/extension_shim.cpp");
    println!("cargo:rerun-if-changed=build.rs");

    if cfg!(feature = "duckdb") && !cfg!(feature = "loadable_extension") {
        bundled::compile_shim();
    }
}

#[cfg(feature = "duckdb")]
mod bundled {
    use std::path::PathBuf;

    pub fn compile_shim() {
        let include = locate_libduckdb_sys_include()
            .expect("could not find libduckdb-sys's extracted duckdb headers in the target tree");
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

    /// Walk up from `OUT_DIR` to the workspace `target/<profile>/build/`
    /// directory, then find a `libduckdb-sys-*/out/duckdb/src/include`
    /// path that exists. Returns the first match.
    fn locate_libduckdb_sys_include() -> Option<PathBuf> {
        let out_dir = PathBuf::from(std::env::var_os("OUT_DIR")?);
        // OUT_DIR layout: <target>/<profile>/build/<crate>-<hash>/out
        let build_dir = out_dir.parent()?.parent()?;
        let entries = std::fs::read_dir(build_dir).ok()?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let s = name.to_string_lossy();
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
