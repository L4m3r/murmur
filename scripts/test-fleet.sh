#!/usr/bin/env bash
# Local two-process celld smoke using Moto S3 emulator. Not a production storage qualification.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CELLD=${CELLD_BIN:-};MOTO=${MOTO_BIN:-}
if [[ -z "$MOTO" ]];then
 for p in "$ROOT/.tools/moto/bin/moto_server" "$ROOT/../fleet-tools/moto-venv/bin/moto_server";do if [[ -x "$p" ]];then MOTO=$p;break;fi;done
fi
MOTO=${MOTO:-moto_server}
if [[ -z "$CELLD" ]];then for p in "$ROOT/.tools/bin/celld" "$ROOT/../celld-tools/bin/celld";do if [[ -x "$p" ]];then CELLD=$p;break;fi;done;fi
CELLD=${CELLD:-celld};PYTHON=${MOTO_PYTHON:-"$(dirname "$MOTO")/python"}
[[ -x "$MOTO" && -x "$PYTHON" ]] || { echo 'Install moto[server]==5.1.15 in an isolated venv and set MOTO_BIN/MOTO_PYTHON.' >&2;exit 1; }
WORK=$(mktemp -d);PIDS=''
cleanup(){ status=$?;for p in $PIDS;do kill -TERM "$p" 2>/dev/null || true;done;for p in $PIDS;do wait "$p" 2>/dev/null || true;done;if [[ $status != 0 ]];then artifacts=${TEST_ARTIFACT_DIR:-/tmp/actor-db-test-artifacts};mkdir -p "$artifacts";for node in a b;do cp "$WORK/$node.log" "$artifacts/fleet-$node-$(date +%s).log" || true;done;tail -n 60 "$WORK/a.log" "$WORK/b.log" >&2 || true;fi;rm -rf "$WORK"; }
trap cleanup EXIT
export AWS_ACCESS_KEY_ID=synthetic-test-key AWS_SECRET_ACCESS_KEY=synthetic-test-secret AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
export PATH="$ROOT/node_modules/.bin:$PATH"
"$MOTO" -H 127.0.0.1 -p 5000 > "$WORK/moto.log" 2>&1 & PIDS=$!
for _ in $(seq 1 100);do curl -sf http://127.0.0.1:5000 >/dev/null && break;sleep .1;done
"$PYTHON" - <<'PY'
import boto3
s=boto3.client('s3',endpoint_url='http://127.0.0.1:5000');s.create_bucket(Bucket='celld-test')
s.put_object(Bucket='celld-test',Key='conditional-probe',Body=b'a',IfNoneMatch='*')
try:s.put_object(Bucket='celld-test',Key='conditional-probe',Body=b'b',IfNoneMatch='*');raise RuntimeError('Conditional create was not enforced')
except s.exceptions.ClientError as e:assert e.response['ResponseMetadata']['HTTPStatusCode']==412
old=s.head_object(Bucket='celld-test',Key='conditional-probe')['ETag']
s.put_object(Bucket='celld-test',Key='conditional-probe',Body=b'12345',IfMatch=old)
try:s.put_object(Bucket='celld-test',Key='conditional-probe',Body=b'stale',IfMatch=old);raise RuntimeError('Conditional overwrite was not enforced')
except s.exceptions.ClientError as e:assert e.response['ResponseMetadata']['HTTPStatusCode']==412
part=s.get_object(Bucket='celld-test',Key='conditional-probe',Range='bytes=1-3')
assert part['ResponseMetadata']['HTTPStatusCode']==206 and part['Body'].read()==b'234'
print('PASS emulator conditional create/overwrite reject stale writes with412; byte range exact')
PY
mkdir "$WORK/project";cp -R "$ROOT/src" "$WORK/project/src";cp "$ROOT/wrangler.json" "$WORK/project/wrangler.json"
"$CELLD" deploy "$WORK/project" --bucket s3://celld-test --endpoint http://127.0.0.1:5000 --region us-east-1
CELLD_NODE=test-a CELLD_WATCH="$WORK/a" CELLD_READY_FLEET_GATE_MS=0 "$CELLD" --bucket s3://celld-test --endpoint http://127.0.0.1:5000 --listen 127.0.0.1:9877 --internal-listen 127.0.0.1:9887 --advertise 127.0.0.1:9887 > "$WORK/a.log" 2>&1 & A=$!;PIDS="$PIDS $A"
CELLD_NODE=test-b CELLD_WATCH="$WORK/b" CELLD_READY_FLEET_GATE_MS=0 "$CELLD" --bucket s3://celld-test --endpoint http://127.0.0.1:5000 --listen 127.0.0.1:9878 --internal-listen 127.0.0.1:9888 --advertise 127.0.0.1:9888 > "$WORK/b.log" 2>&1 & B=$!;PIDS="$PIDS $B"
for port in 9877 9878;do for _ in $(seq 1 100);do curl -sf --max-time 2 "http://127.0.0.1:$port/health" >/dev/null && break;sleep .2;done;done
node "$ROOT/scripts/fleet-integration.mjs" seed "$WORK/state.json"
curl -fsS http://127.0.0.1:9887/state > "$WORK/a-state.json";curl -fsS http://127.0.0.1:9888/state > "$WORK/b-state.json"
"$PYTHON" - "$WORK" <<'PY'
import json,sys
p=sys.argv[1];a=json.load(open(p+'/a-state.json'));b=json.load(open(p+'/b-state.json'))
assert a['owned_cells']>0 and b['owned_cells']>0,(a['owned_cells'],b['owned_cells'])
print(f"PASS verified actor ownership on both processes: A={a['owned_cells']}, B={b['owned_cells']}")
PY
kill -KILL "$A";wait "$A" 2>/dev/null || true
node "$ROOT/scripts/fleet-integration.mjs" verify "$WORK/state.json"
