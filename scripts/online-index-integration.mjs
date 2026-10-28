import {queryRequest} from './http-query.mjs';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile,writeFile} from 'node:fs/promises';
const base=process.env.BASE_URL||'http://127.0.0.1:9876';
async function post(path,body,timeoutMs){const r=await fetch(base+path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body),...(timeoutMs===undefined?{}:{signal:AbortSignal.timeout(timeoutMs)})});return{status:r.status,body:await r.json()};}
async function q(sql,extra={},status=200){const r=await queryRequest(base,{sql,idempotencyKey:randomUUID(),...extra},{expected:status});assert.equal(r.status,status,JSON.stringify(r.body));return r.body;}
const rows=r=>r.results.at(-1).rows;
async function start(sql){const r=await post('/schema',{action:'start',sql,idempotencyKey:randomUUID()});assert.ok([200,202].includes(r.status),JSON.stringify(r.body));return r.body;}
// Availability budgets are real wall-clock bounds, including the first request.
// Each resume operation: 30s total, <=10s per attempt; whole backfill: 10min.
// Only explicit retryable503 replies retry. A transport timeout fails visibly.
async function finish(id,expected='ready'){
 const started=Date.now(),finishDeadline=started+600000;let attempts=0,retries=0,pendingResponses=0;
 for(let page=0;page<1000;page++){
  const operationStarted=Date.now(),operationDeadline=Math.min(operationStarted+30000,finishDeadline);
  let r;
  while(true){
   const remaining=operationDeadline-Date.now();
   assert.ok(remaining>0,`Schema resume wall-clock budget exhausted job=${id} attempts=${attempts} retries=${retries} finish_elapsed_ms=${Date.now()-started}`);
   const timeoutMs=Math.min(10000,remaining),attemptStarted=Date.now();attempts++;
   try{r=await post('/schema',{action:'resume',id},timeoutMs);}
   catch(error){
    console.error(`SCHEMA_RESUME_FAILURE job=${id} attempt=${attempts} timeout_ms=${timeoutMs} attempt_ms=${Date.now()-attemptStarted} operation_elapsed_ms=${Date.now()-operationStarted} finish_elapsed_ms=${Date.now()-started} error=${error.name}:${error.message}`);
    throw error;
   }
   console.log(`SCHEMA_RESUME_ATTEMPT job=${id} attempt=${attempts} status=${r.status} timeout_ms=${timeoutMs} attempt_ms=${Date.now()-attemptStarted} operation_elapsed_ms=${Date.now()-operationStarted} finish_elapsed_ms=${Date.now()-started}`);
   assert.ok(Date.now()<=operationDeadline,`Schema resume exceeded operation wall-clock deadline job=${id} attempt=${attempts} elapsed_ms=${Date.now()-operationStarted}`);
   if(r.status===503&&r.body.retryable===true&&Date.now()<operationDeadline){
    const pending=r.body.code==='PLAN_PENDING'&&r.body.outcome==='PENDING'&&r.body.faultReached!==true;
    if(pending)pendingResponses++;else retries++;
    console.error(`${pending?'PLAN_PENDING':'RETRY'} schema resume same job=${id} retries=${retries} pending=${pendingResponses} elapsed_ms=${Date.now()-started}: ${JSON.stringify(r.body)}`);
    const retryAfterMs=Number.isFinite(r.body.retryAfterMs)&&r.body.retryAfterMs>0?r.body.retryAfterMs:0;
    await new Promise(resolve=>setTimeout(resolve,Math.min(Math.max(250,retryAfterMs),Math.max(0,operationDeadline-Date.now()))));continue;
   }
   break;
  }
  assert.ok([200,202].includes(r.status),JSON.stringify(r.body));
  if(r.body.job.state===expected){console.log(`SCHEMA_FINISH job=${id} attempts=${attempts} retries=${retries} pending=${pendingResponses} elapsed_ms=${Date.now()-started}`);return r.body;}
  if(['failed','cancelled','ready','dropped'].includes(r.body.job.state))assert.equal(r.body.job.state,expected,JSON.stringify(r.body));
 }
 throw Error('Schema job did not finish');
}
async function seed(table,count){await q(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY,label TEXT,n INTEGER)`);for(let n=0;n<count;n+=100){const values=Array.from({length:Math.min(100,count-n)},(_,i)=>`(${n+i},'v${n+i}',0)`);await q(`INSERT INTO ${table}(id,label,n) VALUES${values.join(',')}`);}}
const file=process.argv[3];
if(process.argv[2]==='--verify'){
 const{job}=JSON.parse(await readFile(file,'utf8'));const result=await finish(job);
 assert.ok(result.job.pages>1);
 const found=await q("SELECT id FROM crash_build WHERE label='v299'");assert.deepEqual(rows(found),[{id:299}]);assert.equal(found.plan.type,'index-range');
 console.log('PASS SIGKILL resumes durable online-index cursor and publishes complete index');
}else{
 await seed('online_rows',4096);
 const building=await start('CREATE INDEX by_label ON online_rows(label)');assert.equal(building.job.state,'building');
 assert.notEqual((await q("SELECT id FROM online_rows WHERE label='v4095'")).plan.type,'index-range');
 await q("UPDATE online_rows SET label='moved' WHERE id=0; DELETE FROM online_rows WHERE id=1; INSERT INTO online_rows(id,label,n) VALUES(5000,'new',0)");
 const ready=await finish(building.job.id);assert.ok(ready.job.pages>=64);
 assert.ok(ready.routing[0].indexes.find(i=>i.name==='by_label').tablets.length>16,'backfill must partition beyond the former 16-tablet ceiling');
 for(const[value,ids]of [['moved',[0]],['v0',[]],['v1',[]],['new',[5000]],['v4095',[4095]]]){const result=await q('SELECT id FROM online_rows WHERE label=:value',{params:{value}});assert.deepEqual(rows(result),ids.map(id=>({id})));assert.equal(result.plan.type,'index-range');}
 console.log('PASS resumable 4096-row index build with concurrent update/delete/insert never resurrects old entries');
 await seed('unique_build',300);
 const unique=await start('CREATE UNIQUE INDEX uq ON unique_build(label)');
 await q("INSERT INTO unique_build(id,label,n) VALUES(999,'v299',0)");
 await finish(unique.job.id,'failed');
 assert.deepEqual(rows(await q("SELECT id FROM unique_build WHERE label='v299' ORDER BY id")),[{id:299},{id:999}]);
 await q('DELETE FROM unique_build WHERE id=999');
 const constrained=await start('CREATE UNIQUE INDEX uq ON unique_build(label)');let validating=constrained;
 for(let i=0;i<1000&&!['validating','validation-cutover','ready'].includes(validating.job.state);i++){const r=await post('/schema',{action:'resume',id:constrained.job.id});assert.ok([200,202].includes(r.status),JSON.stringify(r.body));validating=r.body;}
 assert.ok(['validating','validation-cutover'].includes(validating.job.state),'Observe complete index constraint before READY');
 await q("INSERT INTO unique_build(id,label,n) VALUES(999,'v299',0)",{},400);
 await finish(constrained.job.id);
 assert.deepEqual(rows(await q("SELECT id FROM unique_build WHERE label='v299'")),[{id:299}]);
 console.log('PASS BUILDING allows duplicates, frozen validation fails build, VALIDATING enforces prefix uniqueness');
 await q("UPDATE unique_build SET n=1 WHERE id=0");
 await seed('duplicate_build',100);await q("UPDATE duplicate_build SET label='duplicate' WHERE id<2");
 const duplicate=await start('CREATE UNIQUE INDEX bad ON duplicate_build(label)');await finish(duplicate.job.id,'failed');
 assert.equal(rows(await q('SELECT id FROM duplicate_build')).length,100);
 assert.equal((await q("SELECT id FROM duplicate_build WHERE label='duplicate'")).routing[0].indexes.length,0);
 console.log('PASS failed unique build removes only its own generation and preserves base rows');
 const cancel=await start('CREATE INDEX cancelled ON online_rows(n)');await post('/schema',{action:'cancel',id:cancel.job.id});await finish(cancel.job.id,'cancelled');
 assert.ok(!(await q('SELECT id FROM online_rows WHERE id=0')).routing[0].indexes.some(i=>i.name==='cancelled'));
 console.log('PASS cancellation resolves batch state and retires only the building generation');
 const dropped=await start('DROP INDEX by_label ON online_rows');await finish(dropped.job.id,'dropped');assert.notEqual((await q("SELECT id FROM online_rows WHERE label='v4095'")).plan.type,'index-range');assert.equal(rows(await q('SELECT id FROM online_rows')).length,4096);
 console.log('PASS resumable DROP removes routing and retires generation without base deletion');
 await seed('crash_build',300);const crash=await start('CREATE INDEX crash_idx ON crash_build(label)');await writeFile(file,JSON.stringify({job:crash.job.id}));
 console.log('5 online index cases passed; durable backfill staged for SIGKILL');
}
