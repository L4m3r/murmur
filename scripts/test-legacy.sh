#!/usr/bin/env bash
# Real celld upgrade rejection and rollback; all state is in isolated temp dirs.
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
export BASE_URL="http://127.0.0.1:${PORT:-9877}"
WORK=$(mktemp -d)
PID=''
stop() {
  if [[ -n "$PID" ]]; then kill -TERM "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; PID=''; fi
}
cleanup() { stop; rm -rf "$WORK"; }
trap cleanup EXIT
start() {
  "$CELLD" dev "$APP" --host 127.0.0.1 --port "${PORT:-9877}" --no-watch >> "$APP/runtime.log" 2>&1 & PID=$!
  for _ in $(seq 1 100); do
    if curl --silent --fail "$BASE_URL/health" >/dev/null; then return; fi
    if ! kill -0 "$PID" 2>/dev/null; then cat "$APP/runtime.log"; exit 1; fi
    sleep .2
  done
  cat "$APP/runtime.log"; echo 'Runtime readiness timeout' >&2; exit 1
}
load_version() {
  rm -rf "$APP/src"
  cp -R "$1/src" "$APP/src"
  cp "$1/wrangler.json" "$APP/wrangler.json"
}
for CASE in committed after-prepare after-decision; do
  export CASE
  APP="$WORK/$CASE"; mkdir -p "$APP"
  load_version "$ROOT/test/fixtures/legacy-v1"
  printf 'ALLOW_FAULTS=true\n' > "$APP/.dev.vars"
  start
  node --input-type=module <<'JS'
import assert from 'node:assert/strict';
const fault = process.env.CASE === 'committed' ? undefined : process.env.CASE;
const res = await fetch(`${process.env.BASE_URL}/query`, {method:'POST', body:JSON.stringify({sql:"CREATE TABLE legacy_items (id TEXT PRIMARY KEY, value TEXT); INSERT INTO legacy_items (id, value) VALUES ('old', 'preserved');",idempotencyKey:'legacy-seed',fault})});
const body = await res.json();
assert.equal(res.status,fault?503:200,JSON.stringify(body));
console.log(`v1 ${process.env.CASE} seed: ${res.status}`);
JS
  stop
  load_version "$ROOT"
  start
  node --input-type=module <<'JS'
import assert from 'node:assert/strict';
const res=await fetch(`${process.env.BASE_URL}/query`,{method:'POST',body:JSON.stringify({sql:'SELECT * FROM legacy_items;',idempotencyKey:'upgrade-read'})});
const body=await res.json();
assert.equal(res.status,409,JSON.stringify(body));
assert.equal(body.code,'LEGACY_STATE',JSON.stringify(body));
console.log(`v2 ${process.env.CASE}: rejected LEGACY_STATE without mutation`);
JS
  stop
  load_version "$ROOT/test/fixtures/legacy-v1"
  start
  node --input-type=module <<'JS'
import assert from 'node:assert/strict';
async function query(sql,key){const res=await fetch(`${process.env.BASE_URL}/query`,{method:'POST',body:JSON.stringify({sql,idempotencyKey:key})}); const body=await res.json();assert.equal(res.status,200,JSON.stringify(body));return body;}
await query("CREATE TABLE legacy_items (id TEXT PRIMARY KEY, value TEXT); INSERT INTO legacy_items (id, value) VALUES ('old', 'preserved');",'legacy-seed');
const body=await query('SELECT * FROM legacy_items;','rollback-read');
assert.deepEqual(body.results[0].rows,[{id:'old',value:'preserved'}]);
console.log(`v1 ${process.env.CASE} rollback: original data/recovery intact`);
JS
  stop
done
echo 'PASS: v1 committed + prepared + commit-decided state fails closed in v2 and survives rollback'
