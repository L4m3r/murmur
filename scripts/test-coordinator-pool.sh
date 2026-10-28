#!/usr/bin/env bash
# Isolated real-runtime pooled-alarm and persistence probe. Never touches dev state.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CELLD=${CELLD_BIN:-}
if [[ -z "$CELLD" ]]; then
 for candidate in "$ROOT/.tools/bin/celld" "$ROOT/../celld-tools-v063/bin/celld" "$ROOT/../celld-tools/bin/celld"; do
  if [[ -x "$candidate" ]]; then CELLD=$candidate; break; fi
 done
fi
CELLD=${CELLD:-celld}
VERSION=$("$CELLD" --version)
node --input-type=module - "$VERSION" <<'VERSION_JS'
const match=/v?(\d+)\.(\d+)\.(\d+)/.exec(process.argv[2]||'');
if(!match||(+match[1]===0&&(+match[2]<6||(+match[2]===6&&+match[3]<3)))){console.error('celld >=0.6.3 required for ownership-loss safety');process.exit(1);}
VERSION_JS
export PATH="$ROOT/node_modules/.bin:$PATH"
export BASE_URL="http://127.0.0.1:${PORT:-9884}"
WORK=$(mktemp -d)
PID=''
stop(){
 if [[ -z "$PID" ]]; then return; fi
 kill -TERM "$PID" 2>/dev/null || true
 for _ in $(seq 1 50); do if ! kill -0 "$PID" 2>/dev/null; then break; fi; sleep .1; done
 if kill -0 "$PID" 2>/dev/null; then kill -KILL "$PID" 2>/dev/null || true; fi
 wait "$PID" 2>/dev/null || true; PID=''
}
cleanup(){
 status=$?
 set +e
 stop
 if [[ "$status" != 0 ]]; then
  tail -100 "$WORK/runtime.log" >&2 || true
  artifacts=${TEST_ARTIFACT_DIR:-/tmp/actor-db-test-artifacts}
  mkdir -p "$artifacts"
  target="$artifacts/coordinator-pool-${PORT:-9884}-$(date +%s)-$$"
  mv "$WORK" "$target"
  echo "Pool fixture failure state/log/source/identity preserved: $target" >&2
 else rm -rf "$WORK"; fi
}
trap cleanup EXIT
mkdir -p "$WORK/src"
cp "$ROOT/src/coordinator-service.js" "$ROOT/src/coordinator-routing.js" "$ROOT/src/records.js" "$WORK/src/"
if [[ -n "${POOL_SERVICE_REF:-}" ]]; then
 git -C "$ROOT" show "$POOL_SERVICE_REF:src/coordinator-service.js" > "$WORK/src/coordinator-service.js"
 git -C "$ROOT" show "$POOL_SERVICE_REF:src/coordinator-routing.js" > "$WORK/src/coordinator-routing.js"
fi
cp "$ROOT/test/fixtures/coordinator-pool/worker.js" "$ROOT/test/fixtures/coordinator-pool/wrangler.json" "$WORK/"
{ date -u +%FT%TZ; printf '%s\n' "$VERSION"; if [[ -n "${SOURCE_COMMIT:-}" ]]; then printf 'source_commit=%s\n' "$SOURCE_COMMIT"; elif [[ "$(git -C "$ROOT" rev-parse --show-toplevel 2>/dev/null || true)" = "$ROOT" ]]; then git -C "$ROOT" rev-parse HEAD; else printf 'source_commit=archive (exact source hashes below)\n'; fi; sha256sum "$WORK/src/"*.js "$WORK/worker.js" "$WORK/wrangler.json"; } > "$WORK/identity.txt"
start(){
 "$CELLD" dev "$WORK" --host 127.0.0.1 --port "${PORT:-9884}" --no-watch --logs >>"$WORK/runtime.log" 2>&1 & PID=$!
 deadline=$((SECONDS+20))
 while (( SECONDS < deadline )); do
  if curl --silent --max-time 1 --fail "$BASE_URL/health" >/dev/null; then return; fi
  if ! kill -0 "$PID" 2>/dev/null; then cat "$WORK/runtime.log"; exit 1; fi
  sleep .2
 done
 echo 'Pool fixture readiness timeout' >&2; exit 1
}
if curl --silent --max-time 1 "$BASE_URL/health" >/dev/null; then echo 'Pool test port already occupied' >&2; exit 1; fi
start
if [[ "${POOL_TEST_MODE:-}" == 'transactions' ]]; then
 node --input-type=module <<'TX_JS'
import assert from 'node:assert/strict';
const base=process.env.BASE_URL;
const post=async(path,body={})=>{const r=await fetch(base+path,{signal:AbortSignal.timeout(5000),method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});const value=await r.json();assert.equal(r.status,200,JSON.stringify(value));return value;};
const first=post('/work',{owner:0,key:'local-tx',hold:true,barrier:'local-tx',transactions:20});
const second=post('/work',{owner:1,key:'local-tx',hold:true,barrier:'local-tx',transactions:20});
const deadline=Date.now()+4000;let ready=false;
while(Date.now()<deadline){const r=await fetch(base+'/inspect',{signal:AbortSignal.timeout(1000)});const state=await r.json();if(state.pendingCounts['local-tx']===2){ready=true;break;}await new Promise(r=>setTimeout(r,25));}
assert.ok(ready,'both separate input events reached external barrier');
await post('/release',{label:'local-tx'});
const replies=await Promise.all([first,second]);assert.ok(replies.every(reply=>reply.n===20));
const state=await(await fetch(base+'/inspect',{signal:AbortSignal.timeout(1000)})).json();assert.ok(state.owners.every(owner=>owner.txCount===20));
console.log('PASS actual runtime: two concurrent entered scopes each complete20 local KV/crypto transactions after shared external barrier');
TX_JS
 exit 0
fi
node --input-type=module <<'JS'
import assert from 'node:assert/strict';
const base=process.env.BASE_URL;
const post=async(path,body={})=>{const r=await fetch(base+path,{signal:AbortSignal.timeout(5000),method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});const value=await r.json();assert.equal(r.status,200,JSON.stringify(value));return value;};
const status=async()=>{const r=await fetch(base+'/inspect',{signal:AbortSignal.timeout(5000)});assert.equal(r.status,200);return r.json();};
async function until(fn,label){const end=Date.now()+12000;while(Date.now()<end){const value=await fn();if(value)return value;await new Promise(r=>setTimeout(r,100));}throw Error('Timed out: '+label);}
await post('/setup');await until(async()=>{const s=await status();return s.pending.includes('alarm')&&s;},'held first alarm RPC');
const fair=await until(async()=>{const s=await status();return s.owners[1].firedAt&&s;},'later logical alarm while first RPC remains held');
assert.equal(fair.owners[0].firedAt,null);assert.ok(fair.pending.includes('alarm'));console.log('PASS actual celld: later alarm delivered while first alarm RPC and waitUntil remain pending');
await post('/release',{label:'alarm'});await until(async()=>(await status()).owners[0].firedAt,'held alarm completion');
const first=post('/work',{owner:0,key:'same',hold:true});await until(async()=>(await status()).pending.includes('work'),'held scoped request');
const retry=post('/work',{owner:0,key:'same'});await until(async()=>(await status()).owners[0].refs===2,'same scope queued retry');
assert.equal((await post('/work',{owner:1,key:'other'})).n,1);await post('/release',{label:'work'});assert.deepEqual(await first,await retry);
assert.equal((await status()).owners[0].counter,1);console.log('PASS actual celld: unrelated owner progresses; same-scope retries share serialization and receipt');
JS
stop
start
node --input-type=module <<'JS'
import assert from 'node:assert/strict';
const base=process.env.BASE_URL;
for(const [owner,key]of [[0,'same'],[1,'other']]){const r=await fetch(base+'/work',{signal:AbortSignal.timeout(5000),method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({owner,key})});assert.equal(r.status,200);assert.equal((await r.json()).n,1);}
const state=await(await fetch(base+'/inspect',{signal:AbortSignal.timeout(5000)})).json();assert.ok(state.owners.every(owner=>owner.firedAt&&owner.counter===1&&owner.alarm===null));
console.log('PASS actual celld: scoped receipts, counters and alarm completion survive process restart');
JS
