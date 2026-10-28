# Optional experimental celld LocalStore patch

**Not an official release or the default runtime.** `npm run setup:celld` still installs official celld 0.6.3. This local experiment does not change durability settings, ownership, remote S3 behavior, or application state. Do not use it against a live database without separate qualification.

Upstream: [denoland/celld](https://github.com/denoland/celld), v0.6.3, commit `8702f6d186c77698dbfb30cf29e3b1922a88f008`. Copyright 2026 Deno Land Inc.; Apache-2.0 LICENSE and upstream NOTICE are included. The accompanying patch changes `crates/celld/local_store.rs`; it is not upstream-approved.

Patch SHA-256: `da05a76b2d9073b350436bf3f05f4ae2b94b55cca66940feed8b9aa0cff708c6`.

## Scope

Hierarchical `list`/`list_with_delimiter` metadata lookup uses the existing SQLite BINARY key index with `[prefix + '/', prefix + '0')`, retaining the original Rust filters. Empty prefixes still scan; raw paginated listing is unchanged. This avoids scanning every object's metadata for each cell's epoch-chain lookup. It does **not** reduce retained LTX bytes or fix all recovery bottlenecks.

Equivalence is for valid LocalStore data. Unlike the original full enumeration, a prefix query does not incidentally decode corrupted metadata outside that prefix. It is not an equivalent corruption-detection scan.

## Reproduce locally

Requires Git, a native Rust build environment and official Rust **1.94.1** available through rustup. Toolchain installation is separate and explicit:

```sh
rustup toolchain install 1.94.1 --profile minimal
bash scripts/build-experimental-celld.sh --check-only # exact production-module equivalence
bash scripts/build-experimental-celld.sh              # equivalence, then lab-profile binary
CELLD_BIN="$PWD/.tools/celld-prefix-experimental/bin/celld" npm run test:e2e
```

Each invocation uses a fresh retained workspace under `.tools/`; it neither edits an existing source checkout nor changes the default runtime. The build uses upstream `cargo build --locked --profile lab -p celld --bin celld -j 2` (override `BUILD_JOBS`), with an unchanged upstream lockfile. Output attribution and actual binary hash are recorded beside the optional binary. No binary reproducibility guarantee is implied across build hosts.

## Evidence and boundaries

The independent Rust harness compiles both exact original and patched production modules and passed **19,210 comparisons across 810 keys**, including Unicode, encoded paths, wildcard characters, delimiter grouping, offsets and raw pagination. SQLite's query plan uses its key index.

On identical copies of a preserved synthetic 500-update Murmur checkpoint, both official 0.6.3 and a clean unpatched **same lab profile** exceeded the unchanged 10-second first-query deadline. The patched lab-profile repeat returned the first query in 780 ms and passed split/index/idempotency verification in 1.032 s. A fresh 500-update workload also passed split and merge restart checks. These are controlled local observations, not throughput or hosted-Workers guarantees. Latest-application qualification is recorded separately; official-runtime loaded recovery remains an open gate.
