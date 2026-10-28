#!/usr/bin/env bash
# Explicit opt-in local experiment. Does not replace setup:celld or delete state.
set -euo pipefail
if [[ -n "${1:-}" && "${1:-}" != '--check-only' ]]; then echo 'Only optional argument is --check-only' >&2; exit 1; fi
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REV=8702f6d186c77698dbfb30cf29e3b1922a88f008
PATCH="$ROOT/patches/celld-prefix/local-store.patch"
EXPECTED=da05a76b2d9073b350436bf3f05f4ae2b94b55cca66940feed8b9aa0cff708c6
[[ $(sha256sum "$PATCH" | cut -d' ' -f1) = "$EXPECTED" ]] || { echo 'Patch identity mismatch' >&2; exit 1; }
command -v rustup >/dev/null || { echo 'Install official rustup and Rust 1.94.1 first; this script changes no global toolchain.' >&2; exit 1; }
rustup run 1.94.1 rustc --version
mkdir -p "$ROOT/.tools"
WORK=$(mktemp -d "$ROOT/.tools/celld-prefix-build.XXXXXX")
printf 'Build workspace (retained): %s\n' "$WORK"
export CARGO_TARGET_DIR="$WORK/target"
git clone --filter=blob:none --no-checkout https://github.com/denoland/celld.git "$WORK/source"
git -C "$WORK/source" checkout --detach "$REV"
[[ $(git -C "$WORK/source" rev-parse HEAD) = "$REV" ]]
mkdir -p "$WORK/check/src"
cp "$ROOT/tools/celld-prefix-check/Cargo.toml" "$ROOT/tools/celld-prefix-check/Cargo.lock" "$WORK/check/"
cp "$ROOT/tools/celld-prefix-check/src/main.rs" "$WORK/check/src/"
git -C "$WORK/source" show "$REV:crates/celld/local_store.rs" > "$WORK/check/src/baseline.rs"
git -C "$WORK/source" apply --check "$PATCH"
git -C "$WORK/source" apply "$PATCH"
cp "$WORK/source/crates/celld/local_store.rs" "$WORK/check/src/patched.rs"
rustup run 1.94.1 cargo run --locked --manifest-path "$WORK/check/Cargo.toml" -- "$WORK/semantic-test.sqlite"
if [[ "${1:-}" = '--check-only' ]]; then exit 0; fi
(cd "$WORK/source" && rustup run 1.94.1 cargo build --locked --profile lab -p celld --bin celld -j "${BUILD_JOBS:-2}")
OUT="$ROOT/.tools/celld-prefix-experimental"
mkdir -p "$OUT/bin"
# Keep dependency attribution alongside the explicitly selected binary.
cp "$WORK/source/LICENSE" "$WORK/source/NOTICE" "$OUT/"
cp "$CARGO_TARGET_DIR/lab/celld" "$OUT/bin/celld"
{
 printf 'EXPERIMENTAL, NOT OFFICIAL CELLD RELEASE\nsource=%s\nrust=1.94.1\nprofile=lab\n' "$REV"
 sha256sum "$PATCH" "$WORK/source/Cargo.lock" "$OUT/bin/celld"
} > "$OUT/identity.txt"
printf 'Default dependency unchanged. To opt in for a test:\nCELLD_BIN=%q npm run test:e2e\n' "$OUT/bin/celld"
