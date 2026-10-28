#!/usr/bin/env bash
# Real celld composite table regressions; isolated state never touches developer data.
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
WORK=$(mktemp -d); PID=''
cleanup(){
 status=$?
 if [[ -n "$PID" ]];then kill -TERM "$PID" 2>/dev/null || true;wait "$PID" 2>/dev/null || true;fi
 if [[ "$status" != 0 ]];then
  artifacts=${TEST_ARTIFACT_DIR:-/tmp/actor-db-test-artifacts}
  mkdir -p "$artifacts"
  error_log="$artifacts/$(basename "${BASH_SOURCE[0]}" .sh)-${PORT:-9876}-$(date +%s).runtime.error.log"
  cp "$WORK/runtime.log" "$error_log" || true
  echo "Full runtime failure log: $error_log" >&2
  tail -80 "$WORK/runtime.log" >&2 || true
 fi
 rm -rf "$WORK"
}
trap cleanup EXIT
cp -R "$ROOT/src" "$WORK/src";cp "$ROOT/wrangler.json" "$WORK/wrangler.json"
printf 'ALLOW_FAULTS=true\nTABLET_SPLIT_BYTES=1024\nTABLET_MERGE_BYTES=256\n' > "$WORK/.dev.vars"
start(){
 "$CELLD" dev "$WORK" --host 127.0.0.1 --port "${PORT:-9876}" --no-watch > "$WORK/runtime.log" 2>&1 & PID=$!
 for _ in $(seq 1 100);do
  if curl --silent --fail "$BASE_URL/health" >/dev/null;then return;fi
  if ! kill -0 "$PID" 2>/dev/null;then cat "$WORK/runtime.log";exit 1;fi
  sleep .2
 done
 echo 'Runtime readiness timeout' >&2;exit 1
}
start
node "$ROOT/scripts/composite-integration.mjs" --seed "$WORK/replay.json"
kill -KILL "$PID";wait "$PID" 2>/dev/null || true;PID=''
start
node "$ROOT/scripts/composite-integration.mjs" --verify "$WORK/replay.json"
