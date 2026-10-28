import {queryRequest} from './http-query.mjs';
/** Run against isolated celld with TABLET_SPLIT_BYTES=1048576,
 * TABLET_MERGE_BYTES=262144, ALLOW_FAULTS=true. These thresholds ensure the
 * chunking test exceeds the legacy-KV ceiling on one participant; SQLite-backed
 * DOs have a larger 2 MB combined key/value ceiling. Unit tests also cover >2 MB. */
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile,writeFile} from 'node:fs/promises';
const base=process.env.BASE_URL||'http://127.0.0.1:9876';
const auth=process.env.API_TOKEN?{authorization:`Bearer ${process.env.API_TOKEN}`} : {};
const stamp=Date.now().toString(36);
const largeText='界'.repeat(200);
const rows=result=>result.results.at(-1).rows;
async function post(path,body={}){
 const response=await fetch(`${base}${path}`,{method:'POST',headers:{'content-type':'application/json',...auth},body:JSON.stringify(body)});
 return{status:response.status,body:await response.json()};
}
async function query(sql,extra={},expected=200){
 const result=await queryRequest(base,{sql,idempotencyKey:randomUUID(),...extra},{headers:auth,expected});
 assert.equal(result.status,expected,`${sql.slice(0,180)}: ${JSON.stringify(result.body).slice(0,1000)}`);return result.body;
}
async function seed(table,count,withText=false){
 await query(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY,n INTEGER${withText?',body TEXT':''})`);
 for(let start=0;start<count;start+=100){
  const values=Array.from({length:Math.min(100,count-start)},(_,i)=>`(${start+i},0${withText?",'x'":''})`).join(',');
  await query(`INSERT INTO ${table}(id,n${withText?',body':''}) VALUES ${values}`);
 }
}
const mode=process.argv[2],manifest=process.argv[3];
if(mode==='--chunk-seed'){
 const table=`chunk_crash_${stamp}`;await seed(table,400,true);
 const sql=`UPDATE ${table} SET body='${largeText}'`,idempotencyKey=randomUUID();
 await query(sql,{idempotencyKey,fault:'after-commit-ready'},503);
 await writeFile(manifest,JSON.stringify({table,sql,idempotencyKey}));
 console.log('PASS >128KiB single-participant commit-ready transaction staged for SIGKILL');
}else if(mode==='--chunk-verify'){
 const{table,sql,idempotencyKey}=JSON.parse(await readFile(manifest,'utf8'));
 assert.equal((await post('/recover')).status,200);
 const replay=await query(sql,{idempotencyKey});
 assert.equal(replay.participants.filter(p=>p.name.startsWith('base/')).length,1);
 const result=await query(`SELECT * FROM ${table} ORDER BY id`);
 assert.equal(rows(result).length,400);assert.ok(rows(result).every(row=>row.body===largeText));
 assert.deepEqual(await query(sql,{idempotencyKey}),replay);
 console.log('PASS SIGKILL restart reconstructs chunked transaction and result without double application');
}else{
 const chunk=`chunk_${stamp}`;await seed(chunk,400,true);
 const sql=`UPDATE ${chunk} SET body='${largeText}'`,idempotencyKey=randomUUID();
 const changed=await query(sql,{idempotencyKey});
 assert.equal(changed.changes.baseRows,400);
 assert.equal(changed.participants.filter(p=>p.name.startsWith('base/')).length,1,'use documented 1MiB split threshold for single-participant chunk test');
 assert.deepEqual(await query(sql,{idempotencyKey}),changed);
 const select=`SELECT * FROM ${chunk} ORDER BY id`,selectKey=randomUUID();
 const large=await query(select,{idempotencyKey:selectKey});
 assert.equal(rows(large).length,400);assert.ok(rows(large).every(row=>row.body===largeText));
 assert.ok(Buffer.byteLength(JSON.stringify(large))>131072);
 assert.deepEqual(await query(select,{idempotencyKey:selectKey}),large);
 console.log('PASS >128KiB prepared participant, coordinator state and replayable SELECT result');
 const history=`history_${stamp}`;await seed(history,1);
 let oldSnapshot;
 for(let i=1;i<=1000;i++){
  const result=await query(`UPDATE ${history} SET n=n+1 WHERE id=0`);
  if(i===500)oldSnapshot=result.step;
  if(i%250===0)console.log(`  history fixture: ${i} committed versions`);
 }
 const current=await query(`SELECT n FROM ${history} WHERE id=0`);
 assert.deepEqual(rows(current),[{n:1000}]);assert.equal(current.plan.versionsRead,1);
 const old=await query(`SELECT n FROM ${history} WHERE id=0`,{snapshot:oldSnapshot});
 assert.deepEqual(rows(old),[{n:500}]);assert.equal(old.plan.versionsRead,1);
 console.log('PASS current and historical point lookups read one version after 1000 updates');
 const wide=`wide_${stamp}`;await seed(wide,2600);
 const mutation=await query(`UPDATE ${wide} SET n=n+1 WHERE id=2599`);
 assert.equal(mutation.changes.baseRows,1);
 assert.ok(mutation.plan.rowsRead<=2,`point mutation loaded ${mutation.plan.rowsRead} rows`);
 assert.deepEqual(rows(await query(`SELECT * FROM ${wide} WHERE id=2599`)),[{id:2599,n:1}]);
 console.log('PASS point mutation of >2000-row table does not materialize the table');
 const scan=await query(`SELECT id FROM ${wide} WHERE id>=250 AND id<2350 ORDER BY id`);
 assert.deepEqual(rows(scan),Array.from({length:2100},(_,i)=>({id:i+250})));
 assert.equal(scan.plan.rowsRead,2100);
 console.log('PASS scan across more than eight 256-row pages has no missing or duplicate rows');
 const leaseId=`storage_lease_${stamp}`;
 const lease=await post('/snapshot',{action:'lease',id:leaseId,snapshot:oldSnapshot,ttlMs:60000});
 assert.equal(lease.status,200,JSON.stringify(lease.body));
 async function compact(){
  let continuation,deleted=0,floor;
  do{const result=await post('/maintenance',{retainSteps:0,continuation});assert.equal(result.status,200,JSON.stringify(result.body));deleted+=result.body.deletedVersions||0;floor=result.body.retentionFloor;continuation=result.body.continuation;if(result.body.done)break;}while(continuation);
  return{deleted,floor};
 }
 const protectedGC=await compact();
 assert.ok(protectedGC.deleted>0,'compaction must physically remove historical versions');
 assert.equal(protectedGC.floor,oldSnapshot);
 assert.deepEqual(rows(await query(`SELECT n FROM ${history} WHERE id=0`,{snapshot:oldSnapshot})),[{n:500}]);
 assert.equal((await post('/snapshot',{action:'release',id:leaseId})).status,200);
 const advancedGC=await compact();assert.ok(advancedGC.floor>oldSnapshot);
 const expired=await query(`SELECT n FROM ${history} WHERE id=0`,{snapshot:oldSnapshot},410);
 assert.equal(expired.code,'SNAPSHOT_EXPIRED');
 assert.deepEqual(rows(await query(`SELECT n FROM ${history} WHERE id=0`)),[{n:1000}]);
 console.log('PASS bounded GC physically deletes history, protects leased snapshot and rejects expired reads');
 console.log('5 storage integration tests passed');
}
