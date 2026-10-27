#!/usr/bin/env bash
# Isolated real-runtime integration and crash recovery; never resets developer data.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CELLD=${CELLD_BIN:-}
if [[ -z "$CELLD" ]]; then
  for candidate in "$ROOT/.tools/bin/celld" "$ROOT/../celld-tools/bin/celld"; do
    if [[ -x "$candidate" ]]; then CELLD=$candidate; break; fi
  done
fi
CELLD=${CELLD:-celld}
export PATH="$ROOT/node_modules/.bin:$PATH"
export BASE_URL="http://127.0.0.1:${PORT:-9876}"
WORK=$(mktemp -d)
PID=''
cleanup() {
  if [[ -n "$PID" ]]; then kill -TERM "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi
  rm -rf "$WORK"
}
trap cleanup EXIT
cp -R "$ROOT/src" "$WORK/src"
cp "$ROOT/wrangler.json" "$WORK/wrangler.json"
printf 'ALLOW_FAULTS=true\nTABLET_SPLIT_BYTES=1024\nTABLET_MERGE_BYTES=256\n' > "$WORK/.dev.vars"
start() {
  "$CELLD" dev "$WORK" --host 127.0.0.1 --port "${PORT:-9876}" --no-watch > "$WORK/runtime.log" 2>&1 & PID=$!
  for _ in $(seq 1 100); do
    if curl --silent --fail "$BASE_URL/health" >/dev/null; then return; fi
    if ! kill -0 "$PID" 2>/dev/null; then cat "$WORK/runtime.log"; exit 1; fi
    sleep .2
  done
  cat "$WORK/runtime.log"; echo 'Runtime readiness timeout' >&2; exit 1
}
start
node "$ROOT/scripts/integration.mjs"
node "$ROOT/scripts/phase2-integration.mjs"
node "$ROOT/scripts/integration.mjs" --restart-seed "$WORK/restart.json"
kill -KILL "$PID"
wait "$PID" 2>/dev/null || true
PID=''
start
node "$ROOT/scripts/integration.mjs" --restart-verify "$WORK/restart.json"
node "$ROOT/scripts/integration.mjs" --abort-seed "$WORK/abort.json"
kill -KILL "$PID"
wait "$PID" 2>/dev/null || true
PID=''
start
node "$ROOT/scripts/integration.mjs" --abort-verify "$WORK/abort.json"
node "$ROOT/scripts/phase2-integration.mjs" --migration-seed "$WORK/migration.json"
kill -KILL "$PID"
wait "$PID" 2>/dev/null || true
PID=''
start
node "$ROOT/scripts/phase2-integration.mjs" --migration-verify "$WORK/migration.json"
node "$ROOT/scripts/demo.mjs"
