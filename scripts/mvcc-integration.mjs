import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile,writeFile } from 'node:fs/promises';
import { keyHash } from '../src/tablets.js';
const base=process.env.BASE_URL||'http://127.0.0.1:9876';
const auth=process.env.API_TOKEN?{authorization:`Bearer ${process.env.API_TOKEN}`} : {};
async function post(path,body={}){const response=await fetch(`${base}${path}`,{method:'POST',headers:{'content-type':'application/json',...auth},body:JSON.stringify(body)});return{status:response.status,body:await response.json()};}
async function query(sql,extra={},status=200){const result=await post('/query',{sql,idempotencyKey:randomUUID(),...extra});assert.equal(result.status,status,`${sql.slice(0,200)} ${JSON.stringify(result.body)}`);return result.body;}
async function snapshot(){const result=await post('/snapshot');assert.equal(result.status,200);return result.body;}
async function recover(){assert.equal((await post('/recover')).status,200);}
const rows=result=>result.results.at(-1).rows;
const otherKey=keys=>{let key=randomUUID();while(keys.some(old=>keyHash(old)%4===keyHash(key)%4))key=randomUUID();return key;};
const mode=process.argv[2],manifest=process.argv[3];
if(mode==='--ready-seed'){
  const table=`ready_${Date.now().toString(36)}`;
  await query(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO ${table}(id,n) VALUES (1,0)`);
  const sql=`UPDATE ${table} SET n=n+1 WHERE id=1`,idempotencyKey=randomUUID();
  await query(sql,{idempotencyKey,fault:'after-commit-ready'},503);
  await writeFile(manifest,JSON.stringify({table,sql,idempotencyKey}));
  console.log('PASS irrevocable commit-ready before plan staged for SIGKILL');
}else if(mode==='--ready-verify'){
  const{table,sql,idempotencyKey}=JSON.parse(await readFile(manifest,'utf8'));
  await recover();
  assert.deepEqual(rows(await query(`SELECT * FROM ${table}`)),[{id:1,n:1}]);
  const first=await query(sql,{idempotencyKey});assert.deepEqual(await query(sql,{idempotencyKey}),first);
  console.log('PASS SIGKILL restart plans commit-ready intent and applies it exactly once');
}else{
  const table=`mvcc_${Date.now().toString(36)}`;
  await query(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY,balance INTEGER,email TEXT); INSERT INTO ${table}(id,balance,email) VALUES (1,100,'one'),(2,100,'two'); CREATE UNIQUE INDEX email_unique ON ${table}(email)`);
  const before=(await snapshot()).frontier;
  const transfer=`UPDATE ${table} SET balance=balance-7 WHERE id=1; UPDATE ${table} SET balance=balance+7 WHERE id=2`;
  const transferKey=randomUUID();
  await query(transfer,{idempotencyKey:transferKey,fault:'after-first-commit'},503);
  assert.deepEqual(rows(await query(`SELECT balance FROM ${table} ORDER BY id`,{idempotencyKey:otherKey([transferKey])})),[{balance:100},{balance:100}]);
  const hole=await snapshot();assert.ok(hole.allocated>hole.frontier);assert.equal(hole.frontier,before);
  await recover();
  assert.deepEqual(rows(await query(`SELECT balance FROM ${table} ORDER BY id`)),[{balance:93},{balance:107}]);
  assert.deepEqual(rows(await query(`SELECT balance FROM ${table} ORDER BY id`,{snapshot:before})),[{balance:100},{balance:100}]);
  console.log('PASS stable frontier hides partially applied versions; historical snapshot survives commit');
  const baseline=(await snapshot()).frontier;
  const leftKey=randomUUID();let rightKey=randomUUID();while(keyHash(leftKey)%4===keyHash(rightKey)%4)rightKey=randomUUID();
  const pending=[query(`UPDATE ${table} SET balance=balance+1 WHERE id=1`,{idempotencyKey:leftKey,pauseAfterPlanMs:900}),query(`UPDATE ${table} SET balance=balance+1 WHERE id=2`,{idempotencyKey:rightKey,pauseAfterPlanMs:900})];
  let overlap=false;
  for(let i=0;i<100;i++){const state=await snapshot();if(state.allocated>=baseline+2&&state.frontier===baseline){overlap=true;break;}await new Promise(resolve=>setTimeout(resolve,5));}
  const during=await query(`SELECT balance FROM ${table} ORDER BY id`,{idempotencyKey:otherKey([leftKey,rightKey])});
  assert.deepEqual(rows(during),[{balance:93},{balance:107}]);
  const completed=await Promise.all(pending);assert.ok(overlap,'disjoint same-table writes must have two planned steps simultaneously');
  assert.notEqual(completed[0].coordinatorId,completed[1].coordinatorId);
  console.log('PASS distinct coordinators plan same-table disjoint-key writes concurrently while reads keep old snapshot');
  await Promise.all(Array.from({length:12},()=>query(`UPDATE ${table} SET balance=balance+1 WHERE id=1`,{pauseAfterReadMs:20})));
  assert.deepEqual(rows(await query(`SELECT balance FROM ${table} ORDER BY id`)),[{balance:106},{balance:108}]);
  console.log('PASS conflicting increments validate and retry without lost updates');
  const claims=await Promise.all([200,201].map(id=>post('/query',{sql:`INSERT INTO ${table}(id,balance,email) VALUES (${id},0,'claim')`,idempotencyKey:randomUUID(),pauseAfterReadMs:50})));
  assert.deepEqual(claims.map(result=>result.status).sort(),[200,400]);
  assert.equal(rows(await query(`SELECT id FROM ${table} WHERE email='claim'`)).length,1);
  console.log('PASS concurrent unique-index claims commit exactly one row');
  const old=(await snapshot()).frontier;
  const batch=await query(`UPDATE ${table} SET balance=balance+3 WHERE id=1; SELECT balance FROM ${table} WHERE id=1; DELETE FROM ${table} WHERE id=2; SELECT id FROM ${table} WHERE id=2`);
  assert.deepEqual(batch.results[1].rows,[{balance:109}]);assert.deepEqual(batch.results[3].rows,[]);
  assert.deepEqual(rows(await query(`SELECT balance FROM ${table} WHERE id=2`)),[]);
  assert.deepEqual(rows(await query(`SELECT balance FROM ${table} WHERE id=2`,{snapshot:old})),[{balance:108}]);
  assert.deepEqual(rows(await query(`SELECT id FROM ${table} WHERE email='two'`,{snapshot:old})),[{id:2}]);
  assert.deepEqual(rows(await query(`SELECT id FROM ${table} WHERE email='two'`)),[]);
  console.log('PASS read-your-writes and MVCC base/index tombstones retain historical reads');
  await query(`UPDATE ${table} SET balance=0`,{snapshot:old},400);
  await query(`SELECT * FROM ${table}`,{snapshot:(await snapshot()).allocated+1000},400);
  console.log('PASS explicit snapshots reject writes and unpublished future steps');
  const skew=`skew_${Date.now().toString(36)}`;
  await query(`CREATE TABLE ${skew}(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO ${skew}(id,n) VALUES (1,1),(2,1)`);
  const firstKey=randomUUID(),secondKey=otherKey([firstKey]);
  const skewResult=await Promise.all([
    query(`SELECT n FROM ${skew} WHERE id=2; UPDATE ${skew} SET n=0 WHERE id=1`,{idempotencyKey:firstKey,pauseAfterReadMs:50}),
    query(`SELECT n FROM ${skew} WHERE id=1; UPDATE ${skew} SET n=0 WHERE id=2`,{idempotencyKey:secondKey,pauseAfterReadMs:50})
  ]);
  assert.deepEqual(skewResult.map(result=>result.results[0].rows[0].n).sort(),[0,1]);
  console.log('PASS write-skew dependency cycle retries one transaction to a serial read result');
  await query(`UPDATE ${skew} SET n=0`);
  const rangeKey=randomUUID(),insertKey=otherKey([rangeKey]);
  const phantom=await Promise.all([
    query(`SELECT id FROM ${skew} WHERE id>=10; UPDATE ${skew} SET n=n+1 WHERE id=1`,{idempotencyKey:rangeKey,pauseAfterReadMs:50}),
    query(`SELECT n FROM ${skew} WHERE id=1; INSERT INTO ${skew}(id,n) VALUES (10,0)`,{idempotencyKey:insertKey,pauseAfterReadMs:50})
  ]);
  assert.ok(phantom[0].results[0].rows.length>0||phantom[1].results[0].rows[0].n>0,'range phantom dependency cycle must not let both transactions observe prewrite state');
  console.log('PASS empty-range reservation prevents phantom dependency anomaly');
  await query(`UPDATE ${skew} SET n=n+1 WHERE id=1`,{fault:'after-plan'},503);
  const planned=(await snapshot()).allocated;
  let recovered=false;
  for(let i=0;i<100;i++){if((await snapshot()).frontier>=planned){recovered=true;break;}await new Promise(resolve=>setTimeout(resolve,100));}
  assert.ok(recovered,'durable alarm must resolve abandoned plan without query/recover assistance');
  console.log('PASS celld durable alarm advances abandoned plan frontier without explicit recovery');
  console.log('9 MVCC integration tests passed');
}
