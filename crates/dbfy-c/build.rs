use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let include_dir = crate_dir.join("include");
    std::fs::create_dir_all(&include_dir).expect("create include dir");

    let config = cbindgen::Config::from_file(crate_dir.join("cbindgen.toml"))
        .expect("read cbindgen.toml");

    cbindgen::Builder::new()
        .with_crate(crate_dir.to_string_lossy().to_string())
        .with_config(config)
        .generate()
        .expect("cbindgen generation")
        .write_to_file(include_dir.join("dbfy.h"));

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    println!("cargo:rerun-if-changed=build.rs");
}
