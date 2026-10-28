import {queryRequest} from './http-query.mjs';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile,writeFile} from 'node:fs/promises';
const base=process.env.BASE_URL||'http://127.0.0.1:9876';
async function post(path,body={}){const r=await fetch(base+path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});return{status:r.status,body:await r.json()};}
async function query(sql,extra={},status=200){const r=await queryRequest(base,{sql,idempotencyKey:randomUUID(),...extra},{expected:status});assert.equal(r.status,status,JSON.stringify(r.body));return r.body;}
const rows=r=>r.results.at(-1).rows;
const mode=process.argv[2],file=process.argv[3];
if(mode==='--verify'){
 const m=JSON.parse(await readFile(file,'utf8'));
 assert.equal((await post('/query',{sql:m.sql,idempotencyKey:m.key})).status,410);
 assert.deepEqual(await query(m.nextSql,{idempotencyKey:m.nextKey,requestEpoch:1}),m.replay);
 assert.deepEqual(rows(await query('SELECT n FROM replay WHERE id=1',{requestEpoch:1})),[{n:12}]);
 console.log('PASS SIGKILL restart preserves epoch rejection and retained current-epoch receipt');
}else{
 await query('CREATE TABLE replay(id INTEGER PRIMARY KEY,n INTEGER,note TEXT); INSERT INTO replay(id,n,note) VALUES(1,0,\'a\'),(2,0,\'b\'); CREATE UNIQUE INDEX note_idx ON replay(note)');
 const key=randomUUID(),sql='UPDATE replay SET n=n+:inc WHERE id=:id';
 const first=await query(sql,{idempotencyKey:key,params:{inc:1,id:1}});
 assert.deepEqual(await query(sql,{idempotencyKey:key,params:{id:1,inc:1}}),first);
 await query(sql,{idempotencyKey:key,params:{id:1,inc:9}},409);
 const hostile="x'; DELETE FROM replay; --";
 await query('UPDATE replay SET note=:value WHERE id=:id',{params:{value:hostile,id:2}});
 assert.deepEqual(rows(await query('SELECT note FROM replay WHERE id=2')),[{note:hostile}]);
 console.log('PASS typed named parameters are injection-safe and canonical replay identity includes bindings');
 const holeKey=randomUUID();
 await query('UPDATE replay SET n=n+10 WHERE id=1',{idempotencyKey:holeKey,fault:'after-plan-held'},503);
 const allocated=(await post('/snapshot')).body;
 assert.ok(allocated.allocated>allocated.frontier);
 await query('UPDATE replay SET n=n+3 WHERE id=2');
 assert.deepEqual(rows(await query('SELECT n FROM replay WHERE id=2')),[{n:3}]);
 const unresolved=(await post('/snapshot')).body;
 assert.ok(unresolved.pending.some(p=>p.id));
 console.log('PASS disjoint same-table write/read completes behind an unresolved planned transaction');
 const rotated=await post('/snapshot',{action:'rotateEpoch'});assert.equal(rotated.status,200);assert.equal(rotated.body.currentEpoch,1);
 const busy=await post('/maintenance',{action:'retireEpochs',before:1});assert.equal(busy.status,409,JSON.stringify(busy.body));
 assert.deepEqual(rows(await query('SELECT n FROM replay WHERE id=1')),[{n:11}]);
 await query('UPDATE replay SET note=\'new-index-value\' WHERE id=2',{fault:'after-first-commit'},503);
 assert.deepEqual(rows(await query('SELECT id FROM replay WHERE note=\'new-index-value\'')),[{id:2}]);
 console.log('PASS relevant read recovers partial base/index apply, and epoch retirement refuses active work');
 const old=(await post('/snapshot')).body.snapshot;
 await query('DROP INDEX note_idx ON replay');
 assert.deepEqual(rows(await query('SELECT id FROM replay WHERE note=\'new-index-value\'')),[{id:2}]);
 assert.deepEqual(rows(await query('SELECT id FROM replay WHERE note=\'new-index-value\'',{snapshot:old})),[{id:2}]);
 console.log('PASS transactional DROP INDEX preserves historical catalog/index reads');
 const retired=await post('/maintenance',{action:'retireEpochs',before:1});assert.equal(retired.status,200,JSON.stringify(retired.body));
 let cursor,ownerCursor,bucket=0,bucketEpochFloor;
 for(let n=0;n<100;n++){
  const r=await post('/maintenance',{action:'collectEpochs',cursor,ownerCursor,bucket,bucketEpochFloor});assert.equal(r.status,200,JSON.stringify(r.body));
  cursor=r.body.nextCursor;ownerCursor=r.body.ownerCursor;bucket=r.body.nextBucket;bucketEpochFloor=r.body.nextBucketEpochFloor;
  if(r.body.done&&r.body.ownersCollected===0)break;
  assert.ok(n<99,'epoch collection converges');
 }
 let continuation;
 for(let n=0;n<100;n++){const r=await post('/maintenance',{retainSteps:0,continuation});assert.equal(r.status,200,JSON.stringify(r.body));continuation=r.body.continuation;if(r.body.done)break;assert.ok(n<99);}
 await query(sql,{idempotencyKey:key,requestEpoch:0,params:{inc:1,id:1}},410);
 await query(sql,{idempotencyKey:key,params:{inc:1,id:1}},410);
 const nextKey=randomUUID(),nextSql='UPDATE replay SET n=n+1 WHERE id=1';
 const replay=await query(nextSql,{idempotencyKey:nextKey,requestEpoch:1});
 assert.deepEqual(rows(await query('SELECT n FROM replay WHERE id=1',{requestEpoch:1})),[{n:12}]);
 await query('UPDATE replay SET n=999 WHERE id=1',{idempotencyKey:nextKey,requestEpoch:1},409);
 await writeFile(file,JSON.stringify({sql,key,nextSql,nextKey,replay}));
 console.log('PASS retired request epochs remain rejected after receipt/marker GC; current-epoch replay remains exact');
 console.log('5 visibility/retention/schema integration tests passed');
}
