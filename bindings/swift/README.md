# dbfy for Swift / Apple platforms

Swift Package Manager bindings for the embedded dbfy federated SQL
engine. Targets macOS 12+, iOS 15+, and Mac Catalyst 15+, distributed
as a `binaryTarget` xcframework.

## Install (consumer side)

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/typeeffect/dbfy", from: "0.4.1"),
],
targets: [
    .target(
        name: "MyApp",
        dependencies: [.product(name: "Dbfy", package: "dbfy")]
    )
]
```

## Quick taste

```swift
import Dbfy

let yaml = """
version: 1
sources:
  crm:
    type: rest
    base_url: https://api.example.com
    tables:
      customers:
        endpoint: { method: GET, path: /customers }
        root: "$.data[*]"
        columns:
          id:   { path: "$.id",   type: int64 }
          name: { path: "$.name", type: string }
"""

let engine = try Engine.fromYaml(yaml)
let result = try await engine.query("SELECT id, name FROM crm.customers LIMIT 10")
print("rows: \(result.rowCount), batches: \(result.batchCount)")
```

`Engine` is an `actor`, so it's safe to share across `Task`s. The
async `query` method spawns the work on the embedded tokio runtime
and resumes the caller's continuation when the result lands — never
on a tokio thread.

## Building the xcframework

The release flow does this automatically (see
`.github/workflows/swift.yml`); for local development you can run
the same script:

```bash
bash bindings/swift/build-xcframework.sh
```

It produces `bindings/swift/dbfy.xcframework.zip`. The script needs
**macOS** because `xcodebuild -create-xcframework` is Apple-only.

After that, switch the `binaryTarget` in `Package.swift` from
`url:`+`checksum:` to a `path:` reference for local development:

```swift
.binaryTarget(name: "DbfyNative", path: "dbfy.xcframework")
```

## Why not pure Swift?

The query engine is ~30k LoC of Rust + DataFusion + Arrow. Reimplementing
that on Swift would take years and lose feature parity. The C ABI exposed
by `dbfy-c` is small (~10 functions) and stable, and Swift's `import C`
support is excellent — async/await maps cleanly onto the FFI callback
pattern via `withCheckedThrowingContinuation`.

The Arrow C Data Interface used by `dbfy_result_export_batch` lets
callers convert results into Swift Arrow arrays once
[apache/arrow-swift](https://github.com/apache/arrow-swift) stabilises.
For now `QueryResult.rowCount` / `batchCount` plus a future IPC export
helper are sufficient for most workloads.

## Troubleshooting

**"Could not find module 'CDbfy' for target 'arm64-apple-ios'"**:
`build-xcframework.sh` hasn't been run, or the xcframework slice
for the device is missing. The bundled iOS slice is built from
`aarch64-apple-ios`; if you are on an Apple Silicon Mac and want
to also run on an x86_64 iOS Simulator, the script already builds
both `aarch64-apple-ios-sim` and `x86_64-apple-ios-sim` and
combines them via lipo — so the simulator slice is universal.

**Linker error about "undefined symbol _swift_*"**: SwiftPM picked
up the wrong toolchain. Ensure you're using Xcode 15+ with Swift
5.9+ (`swift --version`).
