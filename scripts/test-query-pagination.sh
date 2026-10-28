#!/usr/bin/env bash
# Serial real-celld test: frozen source, scripts and fixtures; disposable state.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CELLD=${CELLD_BIN:-}
if [[ -z "$CELLD" ]]; then
 for candidate in "$ROOT/.tools/bin/celld" "$ROOT/../celld-tools/bin/celld"; do
  if [[ -x "$candidate" ]]; then CELLD=$candidate; break; fi
 done
fi
CELLD=${CELLD:-celld}
if [[ "$CELLD" == */* ]]; then CELLD=$(realpath "$CELLD"); fi
export PATH="$ROOT/node_modules/.bin:$PATH"
export BASE_URL="http://127.0.0.1:${PORT:-9876}"
WORK=$(mktemp -d); PID=''
cleanup(){
 status=$?
 if [[ -n "$PID" ]];then
  kill -TERM "$PID" 2>/dev/null || true
  for _ in $(seq 1 50);do if ! kill -0 "$PID" 2>/dev/null;then break;fi;sleep .1;done
  if kill -0 "$PID" 2>/dev/null;then kill -KILL "$PID" 2>/dev/null || true;fi
  wait "$PID" 2>/dev/null || true
 fi
 if [[ "$status" != 0 ]];then
  artifacts=${TEST_ARTIFACT_DIR:-/tmp/actor-db-test-artifacts}
  mkdir -p "$artifacts"
  prefix="$artifacts/query-pagination-${PORT:-9876}-$(date +%s)"
  cp "$WORK/runtime.log" "$prefix.runtime.error.log" 2>/dev/null || true
  cp "$WORK/client.log" "$prefix.client.error.log" 2>/dev/null || true
  echo "Preserved failure logs: $prefix.{runtime,client}.error.log" >&2
  tail -80 "$WORK/runtime.log" >&2 || true
  mv "$WORK" "$prefix.state"
  echo "Preserved full failure source/config/state: $prefix.state" >&2
 else rm -rf "$WORK";fi
}
trap cleanup EXIT
# All application/test inputs are captured before startup. No script below is
# loaded from mutable ROOT after the snapshot has been taken.
cp -R "$ROOT/src" "$ROOT/scripts" "$WORK/"
for directory in fixtures test/fixtures; do
 if [[ -d "$ROOT/$directory" ]];then mkdir -p "$WORK/$(dirname "$directory")";cp -R "$ROOT/$directory" "$WORK/$directory";fi
 done
cp "$ROOT/wrangler.json" "$ROOT/package.json" "$WORK/"
printf 'ALLOW_FAULTS=true\nTABLET_SPLIT_BYTES=512\nTABLET_MERGE_BYTES=128\nTABLET_MAX_COUNT=256\nCURSOR_MAX_CACHE_PAGES=2\n' > "$WORK/.dev.vars"
"$CELLD" dev "$WORK" --host 127.0.0.1 --port "${PORT:-9876}" --no-watch --logs > "$WORK/runtime.log" 2>&1 & PID=$!
for _ in $(seq 1 100);do
 if curl --silent --max-time 1 --fail "$BASE_URL/health" >/dev/null;then break;fi
 if ! kill -0 "$PID" 2>/dev/null;then cat "$WORK/runtime.log";exit 1;fi
 sleep .2
 done
curl --silent --max-time 1 --fail "$BASE_URL/health" >/dev/null || { echo 'Runtime readiness timeout' >&2;exit 1; }
node "$WORK/scripts/query-pagination-integration.mjs" 2>&1 | tee "$WORK/client.log"
