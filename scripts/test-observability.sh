#!/usr/bin/env bash
# Isolated actual-runtime/browser probe; synthetic loopback data only, no CI/deploy.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CELLD=${CELLD_BIN:-};if [[ -z "$CELLD" ]];then if [[ -x "$ROOT/.tools/bin/celld" ]];then CELLD="$ROOT/.tools/bin/celld";else CELLD=celld;fi;fi
ARTIFACTS=${TEST_ARTIFACT_DIR:-$(mktemp -d /tmp/murmur-observe-artifacts.XXXXXX)}
mkdir -p "$ARTIFACTS";ARTIFACTS=$(realpath "$ARTIFACTS")
WORK=$(mktemp -d);PID=''
stop(){ if [[ -n "$PID" ]];then kill -TERM "$PID" 2>/dev/null || true;for _ in $(seq 1 60);do kill -0 "$PID" 2>/dev/null || break;sleep .1;done;kill -KILL "$PID" 2>/dev/null || true;wait "$PID" 2>/dev/null || true;PID='';fi; }
cleanup(){ status=$?;stop;cp "$WORK/runtime.log" "$ARTIFACTS/runtime.log" 2>/dev/null || true;if [[ "$status" != 0 ]];then mv "$WORK" "$ARTIFACTS/failure-state";echo "Failure state preserved: $ARTIFACTS/failure-state" >&2;else rm -rf "$WORK";fi;echo "Observability artifacts: $ARTIFACTS"; }
trap cleanup EXIT
cp -R "$ROOT/src" "$ROOT/scripts" "$WORK/";cp "$ROOT/wrangler.json" "$ROOT/package.json" "$WORK/"
export PATH="$ROOT/node_modules/.bin:$PATH" BASE_URL="http://127.0.0.1:${PORT:-9996}" OBSERVE_ARTIFACT_DIR="$ARTIFACTS"
export API_TOKEN=synthetic-observe-test-token-only
if [[ "${BACKEND_ONLY:-${OBSERVE_BACKEND_ONLY:-0}}" != 1 ]];then
 export PLAYWRIGHT_CORE_PATH=${PLAYWRIGHT_CORE_PATH:-$(cd "$ROOT" && node -p "require.resolve('playwright-core')")}
fi
{ printf 'source=%s\n' "${SOURCE_COMMIT:-worktree-development-not-release}";"$CELLD" --version;sha256sum "$WORK/src/"*.js "$WORK/scripts/observability-browser.mjs"; } > "$ARTIFACTS/identity.txt"
start(){ "$CELLD" dev "$WORK" --host 127.0.0.1 --port "${PORT:-9996}" --no-watch --logs >> "$WORK/runtime.log" 2>&1 & PID=$!;for _ in $(seq 1 100);do curl -sf --max-time 1 "$BASE_URL/health" >/dev/null && return;kill -0 "$PID" 2>/dev/null || { tail -40 "$WORK/runtime.log";return 1; };sleep .2;done;return 1; }
printf 'ALLOW_FAULTS=true\nTABLET_SPLIT_BYTES=512\nTABLET_MERGE_BYTES=128\n' > "$WORK/.dev.vars"
start
node --input-type=module <<'JS'
import assert from'node:assert/strict';const r=await fetch(process.env.BASE_URL+'/observe/api',{method:'POST',headers:{'content-type':'application/json'},body:'{"action":"overview"}',signal:AbortSignal.timeout(10000)});assert.equal(r.status,403);assert.equal((await r.json()).code,'DIAGNOSTICS_DISABLED');console.log('PASS diagnostics require configured token even on localhost');
JS
stop
printf 'API_TOKEN=%s\nBUILD_ID=local-observability-test\n' "$API_TOKEN" >> "$WORK/.dev.vars"
start
node "$WORK/scripts/observability-browser.mjs" 2>&1 | tee "$ARTIFACTS/client.log"
