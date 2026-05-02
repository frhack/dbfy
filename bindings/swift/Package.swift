// swift-tools-version: 5.9
//
// Swift Package Manager manifest for `Dbfy` — Swift bindings over
// the embedded dbfy federated SQL engine.
//
// The native engine ships as a binary `xcframework` attached to each
// GitHub release; SwiftPM downloads the matching xcframework zip
// based on the `version` tag and verifies the checksum below.
//
// To build the xcframework locally (or in CI on a macOS runner):
//   bash bindings/swift/build-xcframework.sh
//
// That script produces `bindings/swift/dbfy.xcframework.zip` and
// prints the SHA-256 checksum to update the `binaryTarget` below.

import PackageDescription

let package = Package(
    name: "Dbfy",
    platforms: [
        .macOS(.v12),
        .iOS(.v15),
        .macCatalyst(.v15),
    ],
    products: [
        .library(name: "Dbfy", targets: ["Dbfy"]),
    ],
    targets: [
        // The C ABI surface — header umbrella over `dbfy.h`.
        // SwiftPM imports it as `import CDbfy` from the Swift target.
        .target(
            name: "CDbfy",
            dependencies: ["DbfyNative"],
            path: "Sources/CDbfy",
            publicHeadersPath: "include"
        ),

        // The Rust-built native library, distributed as an
        // xcframework so a single dependency covers iOS device,
        // iOS simulator, macOS arm64 + x86_64, and Mac Catalyst.
        //
        // After `bash build-xcframework.sh`, switch this to a
        // `.binaryTarget(name: "DbfyNative", path: "dbfy.xcframework")`
        // for local development. The release flow uploads the zip
        // to GitHub and updates this stanza in-place via
        // `bindings/swift/release.sh`.
        .binaryTarget(
            name: "DbfyNative",
            url: "https://github.com/frhack/dbfy/releases/download/v0.4.0/dbfy.xcframework.zip",
            checksum: "0000000000000000000000000000000000000000000000000000000000000000"
        ),

        // Swift wrapper with async/await methods, NSException-style
        // bridging via `Result`-based throws, and a `RecordBatch`
        // type that opaque-wraps the Arrow IPC bytes.
        .target(
            name: "Dbfy",
            dependencies: ["CDbfy"],
            path: "Sources/Dbfy"
        ),

        .testTarget(
            name: "DbfyTests",
            dependencies: ["Dbfy"],
            path: "Tests/DbfyTests"
        ),
    ]
)
