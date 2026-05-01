#!/usr/bin/env bash
# Compile and run the C smoke test against the freshly built static library.
# Usage: bash crates/dbfy-c/tests/run_smoke.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
CRATE="$ROOT/crates/dbfy-c"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
PROFILE="${CARGO_PROFILE:-debug}"

cargo build -p dbfy-c --profile "${RUST_BUILD_PROFILE:-dev}"

OUT="$TARGET_DIR/c_smoke"
gcc -std=c11 -Wall -Wextra -Werror \
    -I "$CRATE/include" \
    "$CRATE/tests/smoke.c" \
    "$TARGET_DIR/$PROFILE/libdbfy.a" \
    -lpthread -ldl -lm \
    -o "$OUT"

"$OUT"
