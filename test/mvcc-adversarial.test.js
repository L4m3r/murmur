import {storage} from './pool-storage-fixture.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import { cluster as peerCluster } from './peer-fixture.js';
import { encodeKey } from '../src/keys.js';
const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const delta=(key,n=key)=>({key,row:{id:key,n}});


import Worker, { Coordinator, DataShard, PlanAllocator as Mediator, PlanBucket, shardFor } from '../src/index.js';
function workerModel() {
  const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'4096',TABLET_MERGE_BYTES:'1024'};
  const all=[];let hook=async()=>{},sequence=0;
  function namespace(Type){const states=new Map(),actors=new Map();const ns={idFromName:n=>n,get(name){if(!states.has(name))states.set(name,{storage:storage(),id:name});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return {fetch:async(url,init)=>{const input=JSON.parse(init.body);await hook('before',Type,name,input);const response=await actors.get(name).fetch(new Request(url,init));await hook('after',Type,name,input,response);return response;}};},reset(){actors.clear();}};all.push(ns);return ns;}
  env.SHARDS=namespace(DataShard);env.ALLOCATOR=namespace(Mediator);env.MEDIATORS=namespace(PlanBucket);env.COORDINATOR=namespace(Coordinator);
  return {env,hook(fn){hook=fn;},reset(){all.forEach(ns=>ns.reset());},async query(sql,extra={}){const response=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify({sql,idempotencyKey:`independent-${++sequence}`,...extra})}),env);return {status:response.status,body:await response.json()};}};
}
function ownerKeys(){const first='independent-owner-0';let n=1,second;do{second=`independent-owner-${n++}`;}while(shardFor(first)===shardFor(second));return [first,second];}
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
async function deadline(promise){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Independent transaction did not make progress')),2000);})]);}finally{clearTimeout(timer);}}

test('MVCC Worker independent: abandoned unplanned proposals do not block another coordinator without alarms',async()=>{
  const db=workerModel(),[first,second]=ownerKeys();
  assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,0)')).status,200);
  assert.equal((await db.query('UPDATE a SET n=n+1 WHERE id=1',{idempotencyKey:first,fault:'after-prepare'})).status,503);
  db.reset();
  const result=await deadline(db.query('UPDATE a SET n=n+1 WHERE id=1',{idempotencyKey:second}));
  assert.equal(result.status,200,JSON.stringify(result));
  assert.deepEqual((await db.query('SELECT * FROM a')).body.results[0].rows,[{id:1,n:1}]);
});

test('MVCC Worker independent: disjoint same-table writes finish while another prepared transaction is held',async()=>{
  const db=workerModel(),[first,second]=ownerKeys(),prepared=deferred(),release=deferred();let held=false;
  assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,0),(2,0)')).status,200);
  db.hook(async(phase,Type,name,input)=>{if(!held&&phase==='after'&&Type===DataShard&&input.op==='propose'&&name.startsWith('base/')){held=true;prepared.resolve();await release.promise;}});
  const a=db.query('UPDATE a SET n=n+1 WHERE id=1',{idempotencyKey:first});await deadline(prepared.promise);
  let b;try{b=await deadline(db.query('UPDATE a SET n=n+1 WHERE id=2',{idempotencyKey:second}));assert.equal(b.status,200,JSON.stringify(b));}finally{release.resolve();}
  assert.equal((await a).status,200);
  assert.deepEqual((await db.query('SELECT * FROM a ORDER BY id')).body.results[0].rows,[{id:1,n:1},{id:2,n:1}]);
});

test('MVCC Worker independent: lost plan allocation response recovers same committed intent exactly once',async()=>{
  const db=workerModel();assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,0)')).status,200);
  let lost=false;db.hook(async(phase,Type,name,input)=>{if(!lost&&phase==='after'&&Type===Mediator&&input.op==='assign'){lost=true;throw new Error('Lost allocation response after persistence');}});
  const sql='UPDATE a SET n=n+1 WHERE id=1';
  assert.equal((await db.query(sql,{idempotencyKey:'lost-plan-response'})).status,503);db.reset();
  const replay=await db.query(sql,{idempotencyKey:'lost-plan-response'});assert.equal(replay.status,200,JSON.stringify(replay));
  assert.deepEqual(await db.query(sql,{idempotencyKey:'lost-plan-response'}),replay);
  assert.deepEqual((await db.query('SELECT * FROM a')).body.results[0].rows,[{id:1,n:1}]);
});

test('MVCC Worker independent: concurrent cross-row read/write transactions have a serializable result',async()=>{
  const db=workerModel(),[first,second]=ownerKeys(),ready=deferred();const ids=new Set();
  assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,1),(2,1)')).status,200);
  db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard&&input.op==='propose'&&ids.size<2){ids.add(input.id);if(ids.size===2)ready.resolve();await ready.promise;}});
  const results=await deadline(Promise.all([
    db.query('SELECT n FROM a WHERE id=2; UPDATE a SET n=0 WHERE id=1',{idempotencyKey:first}),
    db.query('SELECT n FROM a WHERE id=1; UPDATE a SET n=0 WHERE id=2',{idempotencyKey:second})
  ]));
  for(const result of results)assert.equal(result.status,200,JSON.stringify(result));
  assert.deepEqual(results.map(r=>r.body.results[0].rows[0].n).sort(),[0,1]);
});

test('MVCC Worker independent: unique-index racing inserts with distinct PKs cannot both commit',async()=>{
  const db=workerModel(),[first,second]=ownerKeys(),ready=deferred(),ids=new Set();
  assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,email TEXT); CREATE UNIQUE INDEX email_idx ON a(email)')).status,200);
  db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard&&input.op==='propose'&&ids.size<2){ids.add(input.id);if(ids.size===2)ready.resolve();await ready.promise;}});
  const result=await deadline(Promise.all([
    db.query("INSERT INTO a(id,email) VALUES (1,'same')",{idempotencyKey:first}),
    db.query("INSERT INTO a(id,email) VALUES (2,'same')",{idempotencyKey:second})
  ]));
  assert.deepEqual(result.map(r=>r.status).sort(),[200,400]);
  const rows=(await db.query('SELECT * FROM a')).body.results[0].rows;assert.equal(rows.length,1);assert.equal(rows[0].email,'same');
});

for(const phantom of [false,true])test(`MVCC Worker independent: indexed SELECT validates ${phantom?'empty-range phantoms':'base-row contents even when index entry is unchanged'}`,async()=>{
  const db=workerModel(),[first,second]=ownerKeys(),waiting=deferred(),release=deferred();let held=false;
  assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT,n INTEGER); CREATE INDEX tag_idx ON a(tag); INSERT INTO a(id,tag,n) VALUES (1,'other',0),(2,'existing',1)")).status,200);
  db.hook(async(phase,Type,name,input)=>{if(!held&&phase==='before'&&Type===DataShard&&input.op==='propose'){held=true;waiting.resolve();await release.promise;}});
  const reader=db.query(`SELECT n FROM a WHERE tag='${phantom?'new':'existing'}'; UPDATE a SET n=3 WHERE id=1`,{idempotencyKey:first});
  await deadline(waiting.promise);
  try{
    const writer=await deadline(db.query(phantom?"INSERT INTO a(id,tag,n) VALUES (3,'new',5)":"UPDATE a SET n=5 WHERE id=2",{idempotencyKey:second}));
    assert.equal(writer.status,200,JSON.stringify(writer));
  }finally{release.resolve();}
  const result=await reader;assert.equal(result.status,200,JSON.stringify(result));
  assert.deepEqual(result.body.results[0].rows,[{n:5}]);
});

test('MVCC independent: overlapping proposals coexist but write-skew fails at planned vote',async()=>{
 const db=peerCluster(),a=await db.plan('e0_a',0,{base:{changes:[delta(1)],reads:[point(2)]}}),b=await db.plan('e0_b',0,{base:{changes:[delta(2)],reads:[point(1)]}}),c=await db.plan('e0_c',0,{base:{changes:[delta(3)],reads:[point(3)]}});
 db.reset();assert.equal((await db.finish(b)).decision.outcome,'ABORT');assert.equal((await db.finish(c)).decision.outcome,'COMMIT');assert.equal((await db.finish(a)).decision.outcome,'COMMIT');
});

test('MVCC independent: absent unique prefix conflicts across all PK suffixes at vote',async()=>{
 const db=peerCluster(),prefix=encodeKey(['same']),range={lower:prefix,upper:prefix+'~',lowerInclusive:true,upperInclusive:false};
 const a=await db.plan('e0_unique_a',0,{index:{changes:[{key:['same',1],row:{value:'same',pk:1}}],reads:[range]}}),b=await db.plan('e0_unique_b',0,{index:{changes:[{key:['same',999],row:{value:'same',pk:999}}],reads:[range]}});
 assert.equal((await db.finish(b)).decision.outcome,'ABORT');assert.equal((await db.finish(a)).decision.outcome,'COMMIT');
});

test('MVCC independent: newer applied row behind earlier hole rejects stale dependent vote',async()=>{
 const db=peerCluster(),hole=await db.plan('e0_hole',0,{a:{changes:[delta(1)]}}),later=await db.plan('e0_later',0,{b:{changes:[delta(2)]}});await db.finish(later);
 const stale=await db.plan('e0_stale',0,{b:{changes:[delta(3)],reads:[point(2)]}});assert.equal((await db.finish(stale)).decision.outcome,'ABORT');
 assert.deepEqual((await db.call('SHARDS','b',{op:'scan',snapshot:0})).rows,[]);assert.deepEqual((await db.call('SHARDS','b',{op:'scan',snapshot:later.step})).rows,[{id:2,n:2}]);
 assert.equal((await db.raw('SHARDS','a',{op:'scan',snapshot:later.step})).status,409);await db.finish(hole);
});

test('MVCC independent: delete tombstones preserve historical snapshots and replay cannot resurrect',async()=>{
 const db=peerCluster(),plans=[];let snapshot=0;
 for(const [id,changes]of [['e0_insert',[delta(7,10)]],['e0_update',[delta(7,20)]],['e0_delete',[{key:7,row:null}]]]){const a=await db.plan(id,snapshot,{base:{changes,reads:[point(7)]}});assert.equal((await db.finish(a)).decision.outcome,'COMMIT');plans.push(a);snapshot=a.step;}
 db.reset();assert.deepEqual((await db.call('SHARDS','base',{op:'scan',snapshot:1})).rows,[{id:7,n:10}]);assert.deepEqual((await db.call('SHARDS','base',{op:'scan',snapshot:2})).rows,[{id:7,n:20}]);assert.deepEqual((await db.call('SHARDS','base',{op:'scan',snapshot:3})).rows,[]);
 await db.call('SHARDS','base',{op:'drive',id:plans[0].id});assert.deepEqual((await db.call('SHARDS','base',{op:'scan',snapshot:3})).rows,[]);
});

test('MVCC independent: durable unplanned abort fences delayed proposal after reset',async()=>{
 const db=peerCluster(),request=await db.propose('e0_dead',0,{base:{changes:[delta(1)],reads:[point(2)]}});await db.call('SHARDS','base',{op:'abort',id:request.id});db.reset();
 assert.equal((await db.raw('SHARDS','base',{op:'propose',id:request.id,name:'base',epoch:0,owner:'owner',snapshot:0,participants:['base'],changes:[delta(1)],reads:[point(2)]})).status,409);
 const next=await db.plan('e0_next',0,{base:{changes:[delta(2)]}});assert.equal((await db.finish(next)).decision.outcome,'COMMIT');
});

test('MVCC independent: retirement preserves old snapshot route while rejecting stale-route vote',async()=>{
 const db=peerCluster(),seed=await db.plan('e0_seed',0,{old:{changes:[delta(1)],expectedEpoch:0,meta:{epoch:1,retired:false}}});await db.finish(seed);
 const move=await db.plan('e0_move',seed.step,{old:{expectedEpoch:1,meta:{epoch:2,retired:true}}});await db.finish(move);db.reset();
 assert.deepEqual((await db.call('SHARDS','old',{op:'scan',snapshot:seed.step,epoch:1})).rows,[{id:1,n:1}]);assert.equal((await db.raw('SHARDS','old',{op:'scan',snapshot:move.step,epoch:1})).status,409);
 const stale=await db.plan('e0_staleroute',seed.step,{old:{changes:[delta(3)],expectedEpoch:1}});assert.equal((await db.finish(stale)).decision.outcome,'ABORT');
});

test('MVCC independent: durable append with lost ACK repairs by peer replay after reset',async()=>{
 const db=peerCluster(),a=await db.plan('e0_ackloss',0,{base:{changes:[delta(9)]}});let once=false;
 db.hook(async(phase,binding,name,input)=>{if(!once&&phase==='before'&&binding==='MEDIATORS'&&input.op==='applied'){once=true;throw new Error('Lost apply acknowledgment');}});
 assert.equal((await db.raw('SHARDS','base',{op:'drive',id:a.id})).status,503);assert.deepEqual((await db.call('SHARDS','base',{op:'scan',snapshot:a.step})).rows,[{id:9,n:9}]);db.reset();
 await db.finish(a);await db.call('ALLOCATOR','global',{op:'completion',id:a.id});assert.equal((await db.call('ALLOCATOR','global',{op:'snapshot'})).frontier,a.step);
});

test('MVCC independent: immutable proposed payload cannot be substituted after planning',async()=>{
 const db=peerCluster(),a=await db.plan('e0_frozen',0,{base:{changes:[delta(1)]}});
 assert.equal((await db.raw('SHARDS','base',{op:'propose',id:a.id,name:'base',epoch:0,owner:'owner',snapshot:0,participants:['base'],changes:[delta(2)],reads:[]})).status,409);
 assert.equal((await db.raw('SHARDS','base',{op:'scan',snapshot:a.step})).status,409);await db.finish(a);assert.deepEqual((await db.call('SHARDS','base',{op:'scan',snapshot:a.step})).rows,[{id:1,n:1}]);
});
