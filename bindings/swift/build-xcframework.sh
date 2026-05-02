#!/usr/bin/env bash
# Build the dbfy.xcframework bundle from cargo-built static libs
# across the Apple target matrix, ready to ship as a SwiftPM
# binaryTarget. Must run on macOS — `xcodebuild -create-xcframework`
# does not exist on Linux.
#
# Usage:
#   bash bindings/swift/build-xcframework.sh
#
# Outputs:
#   bindings/swift/build/dbfy.xcframework/
#   bindings/swift/dbfy.xcframework.zip
#
# Exit non-zero if any cargo target fails; the script is meant to
# fail fast in CI.

set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
OUT="$(pwd)/build"
rm -rf "$OUT" && mkdir -p "$OUT"

# Cargo targets we care about. macOS arm64+x86_64 land in a single
# universal slice via `lipo`; iOS device + iOS simulator + Mac
# Catalyst stay separate because xcframework slices are keyed by
# (platform, arch).
TARGETS=(
    aarch64-apple-darwin
    x86_64-apple-darwin
    aarch64-apple-ios
    aarch64-apple-ios-sim
    x86_64-apple-ios-sim
)

echo "==> Adding rust targets"
for t in "${TARGETS[@]}"; do rustup target add "$t" >/dev/null; done

echo "==> Building libdbfy.a per target"
for t in "${TARGETS[@]}"; do
    echo "  - $t"
    (cd "$ROOT" && cargo build -p dbfy-c --release --target "$t")
done

echo "==> Lipo'ing macOS slices"
mkdir -p "$OUT/macos"
lipo -create \
    "$ROOT/target/aarch64-apple-darwin/release/libdbfy.a" \
    "$ROOT/target/x86_64-apple-darwin/release/libdbfy.a" \
    -output "$OUT/macos/libdbfy.a"

echo "==> Lipo'ing iOS Simulator slices"
mkdir -p "$OUT/ios-sim"
lipo -create \
    "$ROOT/target/aarch64-apple-ios-sim/release/libdbfy.a" \
    "$ROOT/target/x86_64-apple-ios-sim/release/libdbfy.a" \
    -output "$OUT/ios-sim/libdbfy.a"

echo "==> Staging headers"
mkdir -p "$OUT/headers"
cp "$ROOT/crates/dbfy-c/include/dbfy.h" "$OUT/headers/"
cat > "$OUT/headers/module.modulemap" <<'EOF'
module CDbfyNative {
    umbrella header "dbfy.h"
    export *
}
EOF

echo "==> Creating xcframework"
xcodebuild -create-xcframework \
    -library "$OUT/macos/libdbfy.a"             -headers "$OUT/headers" \
    -library "$OUT/ios-sim/libdbfy.a"           -headers "$OUT/headers" \
    -library "$ROOT/target/aarch64-apple-ios/release/libdbfy.a" -headers "$OUT/headers" \
    -output "$OUT/dbfy.xcframework"

echo "==> Zipping for release upload"
( cd "$OUT" && zip -ry dbfy.xcframework.zip dbfy.xcframework )
mv "$OUT/dbfy.xcframework.zip" .

CHECKSUM=$(swift package compute-checksum dbfy.xcframework.zip)
echo
echo "==> Done. xcframework ready."
echo "    bindings/swift/dbfy.xcframework.zip"
echo "    SHA-256:  $CHECKSUM"
echo
echo "Update the binaryTarget URL + checksum in Package.swift before"
echo "tagging. The release.yml workflow does this automatically."
