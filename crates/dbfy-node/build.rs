// napi-build emits the Node-specific linker flags (e.g. `-undefined
// dynamic_lookup` on macOS) so the cdylib can resolve `napi_*`
// symbols against the host node binary at load time.

extern crate napi_build;

fn main() {
    napi_build::setup();
}
