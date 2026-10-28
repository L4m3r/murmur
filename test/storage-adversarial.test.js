import test from 'node:test';
import assert from 'node:assert/strict';
import { readRecord, writeRecord, deleteRecord } from '../src/records.js';
import { DataShard } from '../src/datashard.js';
import { encodeKey } from '../src/keys.js';
import { scopedStorage } from '../src/coordinator-service.js';
import { coordinatorName } from '../src/coordinator-routing.js';

// Independent bounded-IO/rollback model, not a substitute for celld process-loss tests.
function storage() {
  const data = new Map(), io = []; let failure = Infinity, mutations = 0, failDeletes = false;
  const api = { data, io,
    async setAlarm(value) { api.alarm=value; }, async deleteAlarm() { api.alarm=null; }, async getAlarm() { return api.alarm??null; },
    failAt(n) { failure = n; mutations = 0; },
    failDeletes(value) { failDeletes = value; },
    async get(k) { io.push({op:'get',key:k}); return structuredClone(data.get(k)); },
    async put(k,v) { if (++mutations === failure) throw new Error('injected mutation failure'); data.set(k,structuredClone(v)); },
    async delete(k) { if (failDeletes || ++mutations === failure) throw new Error('injected mutation failure'); return data.delete(k); },
    async list(options={}) {
      const {prefix='',start,end,limit,reverse=false}=options;
      let rows=[...data].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);
      if(reverse) rows.reverse(); if(limit!==undefined) rows=rows.slice(0,limit);
      io.push({op:'list',options,count:rows.length}); return new Map(structuredClone(rows));
    },
    async transaction(fn) { const old=structuredClone(data), oldAlarm=api.alarm; try { return await fn(api); } catch(e) { api.alarm=oldAlarm;data.clear();for(const[k,v]of old)data.set(k,v);throw e; } },
  }; return api;
}
const point=k=>({lower:encodeKey(k),upper:encodeKey(k),lowerInclusive:true,upperInclusive:true});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false};
async function peerModel({initialShardStore,authorityFloor}={}) {
  const {PlanAllocator}=await import('../src/mediator.js'),{PlanBucket}=await import('../src/plan-bucket.js'),{manifestHash}=await import('../src/protocol.js');
  const env={PLAN_BUCKETS:'2',ALLOW_FAULTS:'true'},states=new Map(),actors=new Map();
  if(initialShardStore)states.set('SHARDS:s',{storage:initialShardStore,id:'s'});
  for(const[binding,Class]of[['ALLOCATOR',PlanAllocator],['MEDIATORS',PlanBucket],['SHARDS',DataShard]])env[binding]={idFromName:x=>x,get:name=>({fetch:async(url,init)=>{const key=binding+':'+name;if(!states.has(key))states.set(key,{storage:storage(),id:name});if(!actors.has(key))actors.set(key,new Class(states.get(key),env));const response=await actors.get(key).fetch(new Request(url,init));if(authorityFloor&&binding==='ALLOCATOR'&&JSON.parse(init.body).op==='status'){return Response.json({...await response.json(),retentionFloor:authorityFloor()},{status:response.status});}return response;}})};
  const call=async(binding,name,input)=>{const r=await env[binding].get(name).fetch('https://test',{method:'POST',body:JSON.stringify(input)});return{status:r.status,...await r.json()};};
  const med=input=>call('ALLOCATOR','global',input);
  const result={states,actors,env,call,med,restart(){actors.clear();},async plan(id,snapshot,bodies){
    assert.equal((await med({op:'pin',id,owner:'c0',snapshot})).status,200);
    const names=Object.keys(bodies).sort(),participants=[];
    for(const name of names){const p=await call('SHARDS',name,{op:'propose',id,name,owner:'c0',epoch:0,snapshot,participants:names,reads:[],changes:[],...bodies[name]});assert.equal(p.status,200,JSON.stringify(p));participants.push({name,bodyHash:p.bodyHash,publicKey:p.publicKey});}
    const {status,...association}=await med({op:'assign',id,owner:'c0',epoch:0,snapshot,participants,manifestHash:await manifestHash(participants)});assert.equal(status,200);
    assert.equal((await call('MEDIATORS',association.bucket,{op:'register',association})).status,200);
    for(const name of names){const response=await call('SHARDS',name,{op:'acceptPlan',association});assert.equal(response.status,200);assert.equal((await call('MEDIATORS',association.bucket,{op:'delivered',id,ack:response.ack})).status,200);}
    return association;
  },async finish(a){let state,rounds=0;do{for(const p of a.participants)assert.equal((await call('SHARDS',p.name,{op:'drive',id:a.id,release:true})).status,200);state=await call('MEDIATORS',a.bucket,{op:'get',id:a.id});assert.ok(++rounds<20);}while(state.applied.length!==a.participants.length);assert.equal((await med({op:'completion',id:a.id})).applied,true);await med({op:'unpin',id:a.id});return state;}};
  return result;
}
function shardModel() {
  const store=storage();let floor=0;const ready=peerModel({initialShardStore:store,authorityFloor:()=>floor});
  return {store,setFloor(n){floor=n;},async restart(){(await ready).restart();},async call(body){const model=await ready;return model.call('SHARDS','s',body);},
    async validate(snapshot,reads){const model=await ready;await model.call('SHARDS','s',{op:'stats'});try{await store.transaction(tx=>model.actors.get('SHARDS:s').validateHeads(tx,snapshot,reads,[]));return{status:200};}catch(e){return{status:e.status||503,code:e.code};}},
    async commit(id,step,changes,extra={}){const model=await ready,a=await model.plan(id,step-1,{s:{changes,...extra}});assert.equal(a.step,step);assert.equal((await model.finish(a)).decision.outcome,'COMMIT');}};
}

test('independent chunks: every replacement/deletion interruption preserves original generation exactly',async()=>{
  const original={text:'😀界'.repeat(30000)}, next={text:'🧬'.repeat(40000)};
  const baseline=storage();await baseline.transaction(tx=>writeRecord(tx,'record',original));
  const pristine=structuredClone(baseline.data);
  // Covers each new chunk, manifest publication and each old-chunk cleanup.
  let success=false;
  for(let n=1;n<=30;n++) {
    const s=storage();for(const[k,v]of pristine)s.data.set(k,structuredClone(v));s.failAt(n);
    try { await s.transaction(tx=>writeRecord(tx,'record',next));success=true;assert.deepEqual(await readRecord(s,'record'),next);break; }
    catch(e) { assert.match(e.message,/injected mutation failure/);assert.deepEqual(s.data,pristine);assert.deepEqual(await readRecord(s,'record'),original); }
  }
  assert.equal(success,true);
  success=false;
  for(let n=1;n<=30;n++) {
    const s=storage();for(const[k,v]of pristine)s.data.set(k,structuredClone(v));s.failAt(n);
    try { await s.transaction(tx=>deleteRecord(tx,'record'));success=true;assert.equal(s.data.size,0);break; }
    catch(e) { assert.match(e.message,/injected mutation failure/);assert.deepEqual(s.data,pristine);assert.deepEqual(await readRecord(s,'record'),original); }
  }
  assert.equal(success,true);
});

test('independent heads: empty historical pages advance across future keys and restart without skipping visible tail',async()=>{
  const db=shardModel();await db.commit('old',1,[{key:1000,row:{id:1000}}]);
  await db.commit('future',2,Array.from({length:300},(_,key)=>({key,row:{id:key}})));
  let cursor,rows=[],pages=0,empty=0;
  do {
    const page=await db.call({op:'scan',snapshot:1,range:all,pageSize:17,cursor});assert.equal(page.status,200);
    assert.ok(page.keysRead<=17);if(!page.rows.length)empty++;rows.push(...page.rows);pages++;
    if(page.nextCursor!==null&&cursor)assert.ok(page.nextCursor>cursor);
    cursor=page.nextCursor;await db.restart();assert.ok(pages<30,'cursor must make progress');
  } while(cursor!==null);
  assert.ok(empty>=17);assert.deepEqual(rows,[{id:1000}]);
});

test('independent heads: second-page deleted phantom rejects stale predicate despite no visible row',async()=>{
  const db=shardModel();await db.commit('seed',1,Array.from({length:300},(_,key)=>({key,row:{id:key}})));
  await db.commit('delete-tail',2,[{key:299,row:null}]);await db.restart();db.store.io.length=0;
  const stale=await db.validate(1,[all]);
  assert.equal(stale.status,409);assert.equal(stale.code,'CONFLICT');
  const headPages=db.store.io.filter(x=>x.op==='list'&&x.options.prefix==='head:');
  assert.ok(headPages.length>=2);assert.ok(headPages.every(x=>x.options.limit<=256&&x.count<=256));
  assert.ok(!db.store.io.some(x=>x.op==='list'&&x.options.prefix==='row:'));
  const current=await db.validate(2,[point(299)]);assert.equal(current.status,200);
});

test('independent admission: compact-first rejects stale pin; pin-first survives lease expiry and actor restart',async()=>{
  const { PlanAllocator: Mediator }=await import('../src/mediator.js');const s=storage();
  await s.put('frontier',20);await s.put('allocated',20);
  let now=1000,actor=new Mediator({storage:s});actor.now=()=>now;
  const call=async input=>{const r=await actor.fetch(new Request('https://test',{method:'POST',body:JSON.stringify(input)}));return {status:r.status,...await r.json()};};
  assert.equal((await call({op:'lease',id:'lease-a',snapshot:3,ttlMs:50})).status,200);
  assert.equal((await call({op:'pin',id:'reading',owner:'c0',snapshot:3,leaseId:'lease-a'})).status,200);
  now=100000;actor=new Mediator({storage:s});actor.now=()=>now;
  assert.equal((await call({op:'compact',retainSteps:0})).retentionFloor,3);
  assert.equal((await call({op:'pin',id:'reading',owner:'c0',snapshot:3,leaseId:'lease-a'})).status,200,'admitted pin does not expire with public lease');
  assert.equal((await call({op:'pin',id:'new-reader',owner:'c1',snapshot:3,leaseId:'lease-a'})).status,410);
  assert.equal((await call({op:'unpin',id:'reading'})).status,200);
  assert.equal((await call({op:'compact',retainSteps:0})).retentionFloor,20);
  assert.equal((await call({op:'pin',id:'too-old',owner:'c0',snapshot:3})).code,'SNAPSHOT_EXPIRED');
  assert.equal((await call({op:'pin',id:'at-floor',owner:'c0',snapshot:20})).status,200);
});

test('independent admission: delayed pin after unpin is fenced even before original admission',async()=>{
  const { PlanAllocator: Mediator }=await import('../src/mediator.js');const s=storage();
  let actor=new Mediator({storage:s});
  const call=async input=>{const r=await actor.fetch(new Request('https://test',{method:'POST',body:JSON.stringify(input)}));return {status:r.status,...await r.json()};};
  assert.equal((await call({op:'unpin',id:'lost-request'})).status,200);
  actor=new Mediator({storage:s});
  assert.equal((await call({op:'pin',id:'lost-request',owner:'c0',snapshot:0})).code,'PIN_CLOSED');
  assert.equal((await call({op:'snapshot'})).pins,0);
});

test('independent GC: bounded continuation preserves floor anchor and tombstone, fences old RPCs after reset',async()=>{
  const db=shardModel();
  for(let step=1;step<=8;step++)await db.commit(`v${step}`,step,[{key:1,row:step===4?null:{id:1,n:step,text:'界'.repeat(18000)}}]);
  db.setFloor(4);let cursor,rounds=0,deleted=0;
  do {
    const page=await db.call({op:'compact',limit:1,pageSize:1,cursor});assert.equal(page.status,200,JSON.stringify(page));
    assert.ok(page.deletedVersions+page.deletedMetadata<=1);deleted+=page.deletedVersions;
    cursor=page.nextCursor;rounds++;await db.restart();assert.ok(rounds<20);
  } while(cursor!==null);
  assert.equal(deleted,3);
  assert.equal((await db.call({op:'get',key:1,snapshot:3})).code,'SNAPSHOT_EXPIRED');
  assert.equal((await db.call({op:'propose',name:'s',epoch:0,participants:['s'],id:'delayed',owner:'c1',snapshot:3,reads:[],changes:[]})).code,'SNAPSHOT_EXPIRED');
  assert.equal((await db.call({op:'get',key:1,snapshot:4})).row,null);
  assert.equal((await db.call({op:'get',key:1,snapshot:5})).row.n,5);
  assert.equal((await db.call({op:'get',key:1,snapshot:8})).row.n,8);
  const roots=[...db.store.data.keys()].filter(k=>k.startsWith('row:'));assert.equal(roots.length,5);
  const liveGenerations=new Set([...db.store.data.values()].filter(v=>v?.kind==='murmur.chunked-json').map(v=>v.generation));
  for(const key of db.store.data.keys())if(key.startsWith('__records_chunk_v1/'))assert.ok(liveGenerations.has(key.split('/')[2]),'GC must not leave obsolete chunks');
});

test('independent GC: active proposal snapshot clamps local floor until authoritative abort',async()=>{
  const db=shardModel();await db.commit('v1',1,[{key:1,row:{id:1,n:1}}]);
  const p=await db.call({op:'propose',name:'s',epoch:0,participants:['s'],id:'held',owner:'c0',snapshot:1,reads:[point(1)],changes:[]});assert.equal(p.status,200);
  await db.commit('v2',2,[{key:2,row:{id:2,n:2}}]);db.setFloor(2);
  assert.equal((await db.call({op:'compact'})).collectionFloor,1);
  await db.restart();assert.equal((await db.call({op:'get',key:1,snapshot:1})).row.n,1);
  assert.equal((await db.call({op:'abort',id:'held'})).status,200);
  assert.equal((await db.call({op:'compact'})).collectionFloor,2);
  assert.equal((await db.call({op:'get',key:1,snapshot:1})).code,'SNAPSHOT_EXPIRED');
});

test('independent GC: interruption after durable floor but during deletion rolls back physical prune safely',async()=>{
  const db=shardModel();for(let step=1;step<=3;step++)await db.commit(`v${step}`,step,[{key:1,row:{id:1,n:step}}]);
  db.setFloor(3);const original=[...db.store.data.keys()].filter(k=>k.startsWith('row:'));
  db.store.failDeletes(true);assert.equal((await db.call({op:'compact'})).status,503);db.store.failDeletes(false);await db.restart();
  assert.equal(await db.store.get('collectionFloor'),3);
  assert.deepEqual([...db.store.data.keys()].filter(k=>k.startsWith('row:')),original);
  assert.equal((await db.call({op:'get',key:1,snapshot:2})).code,'SNAPSHOT_EXPIRED');
  assert.equal((await db.call({op:'get',key:1,snapshot:3})).row.n,3);
  assert.equal((await db.call({op:'compact'})).status,200);
  assert.equal([...db.store.data.keys()].filter(k=>k.startsWith('row:')).length,1);
});

test('independent lease identity: released lease id cannot silently bind a delayed pin to a newer snapshot',async()=>{
  const { PlanAllocator: Mediator }=await import('../src/mediator.js');const s=storage();await s.put('frontier',2);await s.put('allocated',2);
  const actor=new Mediator({storage:s});
  const call=async input=>{const r=await actor.fetch(new Request('https://test',{method:'POST',body:JSON.stringify(input)}));return {status:r.status,...await r.json()};};
  assert.equal((await call({op:'lease',id:'lease-reused',snapshot:1})).status,200);
  assert.equal((await call({op:'release',id:'lease-reused'})).status,200);
  const reused=await call({op:'lease',id:'lease-reused',snapshot:2});
  assert.notEqual(reused.status,200,'released lease identities must be fenced or require a new generation');
  const delayed=await call({op:'pin',id:'delayed-old-lease',owner:'c0',leaseId:'lease-reused'});
  assert.notEqual(delayed.status,200,'delayed old lease admission must not attach to new snapshot');
});

test('independent GC: metadata floor anchor preserves historical routing on retired shard',async()=>{
  const db=shardModel();
  await db.commit('epoch1',1,[{key:1,row:{id:1,n:1}}],{expectedEpoch:0,meta:{epoch:1,retired:false}});
  await db.commit('epoch2',2,[{key:1,row:{id:1,n:2}}],{expectedEpoch:1,meta:{epoch:2,retired:false}});
  await db.commit('retire',3,[],{expectedEpoch:2,meta:{epoch:3,retired:true}});
  db.setFloor(2);let cursor,rounds=0;
  do { const page=await db.call({op:'compact',limit:1,pageSize:1,cursor});assert.equal(page.status,200);cursor=page.nextCursor;await db.restart();assert.ok(++rounds<20); } while(cursor!==null);
  assert.equal([...db.store.data.keys()].filter(k=>k.startsWith('meta:')).length,2);
  assert.equal((await db.call({op:'get',snapshot:2,epoch:2,key:1})).row.n,2);
  assert.equal((await db.call({op:'get',snapshot:3,epoch:3,key:1})).code,'STALE_EPOCH');
  assert.equal((await db.call({op:'get',snapshot:1,epoch:1,key:1})).code,'SNAPSHOT_EXPIRED');
});

test('independent scoped GC: partial plan plus allocated pin retains old anchors until both hole and old pin resolve',async()=>{
  const db=await peerModel(),read=(name,snapshot)=>db.call('SHARDS',name,{op:'get',key:1,snapshot});
  const seed=await db.plan('seed',0,{a:{changes:[{key:1,row:{id:1,n:1}}]},b:{changes:[{key:1,row:{id:1,n:1}}]}});await db.finish(seed);
  const hole=await db.plan('hole',seed.step,{a:{changes:[{key:1,row:{id:1,n:2}}]},b:{changes:[{key:1,row:{id:1,n:2}}],holdFinalization:true}});
  for(let i=0;i<2;i++)for(const p of hole.participants)assert.equal((await db.call('SHARDS',p.name,{op:'drive',id:hole.id})).status,200);
  const later=await db.plan('later',seed.step,{c:{changes:[{key:1,row:{id:1,n:3}}]}});await db.finish(later);
  assert.equal((await db.med({op:'pin',id:'old-read',owner:'old',snapshot:seed.step})).status,200);
  assert.equal((await db.med({op:'pin',id:'allocated-read',owner:'new',snapshot:later.step})).status,200);
  assert.equal((await db.med({op:'compact',retainSteps:0})).retentionFloor,seed.step);
  for(const name of ['a','b','c'])assert.equal((await db.call('SHARDS',name,{op:'compact'})).status,200);
  await db.restart();
  assert.equal((await read('a',seed.step)).row.n,1);assert.equal((await read('b',seed.step)).row.n,1);
  assert.equal((await read('a',later.step)).row.n,2);assert.equal((await read('b',later.step)).code,'VISIBILITY_PENDING');
  await db.finish(hole);
  assert.equal((await db.med({op:'compact',retainSteps:0})).retentionFloor,seed.step);
  await db.med({op:'unpin',id:'old-read'});
  assert.equal((await db.med({op:'compact',retainSteps:0})).retentionFloor,later.step);
  for(const name of ['a','b','c']){assert.equal((await db.call('SHARDS',name,{op:'compact'})).status,200);assert.equal((await read(name,seed.step)).code,'SNAPSHOT_EXPIRED');}
  assert.equal((await read('a',later.step)).row.n,2);assert.equal((await read('b',later.step)).row.n,2);assert.equal((await read('c',later.step)).row.n,3);
});

test('independent epoch identity: rejected wrong-epoch unpin must not poison the live operation',async()=>{
  const { PlanAllocator: Mediator }=await import('../src/mediator.js');const s=storage(),actor=new Mediator({storage:s});
  const call=async input=>{const r=await actor.fetch(new Request('https://test',{method:'POST',body:JSON.stringify(input)}));return {status:r.status,...await r.json()};};
  const original={op:'pin',id:'plain-operation-id',owner:'c0',snapshot:0,epoch:0};
  assert.equal((await call(original)).status,200);
  assert.equal((await call({op:'rotateEpoch'})).currentEpoch,1);
  const before=structuredClone(s.data);
  assert.equal((await call({op:'unpin',id:original.id,epoch:1})).status,409);
  assert.deepEqual(s.data,before,'rejected identity mismatch must not mutate fences or indexes');
  assert.equal((await call(original)).status,200);
});

test('independent GC: reclaim latest tombstone and head without resurrection at exact floor',async()=>{
  const db=shardModel();await db.commit('insert',1,[{key:7,row:{id:7,n:1}}]);await db.commit('delete',2,[{key:7,row:null}]);db.setFloor(2);
  let cursor,rounds=0;do{const page=await db.call({op:'compact',limit:1,pageSize:1,cursor});assert.equal(page.status,200);cursor=page.nextCursor;await db.restart();assert.ok(++rounds<20);}while(cursor!==null);
  assert.equal([...db.store.data.keys()].filter(k=>k.startsWith('row:')||k.startsWith('head:')).length,0);
  assert.equal((await db.call({op:'get',snapshot:2,key:7})).row,null);
  assert.equal((await db.call({op:'propose',name:'s',epoch:0,participants:['s'],id:'stale-after-head-gc',owner:'c1',snapshot:1,reads:[point(7)],changes:[{key:7,row:{id:7,n:9}}]})).code,'SNAPSHOT_EXPIRED');
  await db.commit('reinsert',3,[{key:7,row:{id:7,n:3}}]);
  assert.equal((await db.call({op:'get',snapshot:2,key:7})).row,null);
  assert.equal((await db.call({op:'get',snapshot:3,key:7})).row.n,3);
});

test('independent epoch replay: collected successful receipt cannot execute again under expired request epoch',async()=>{
  const { default:Worker,Coordinator,PlanAllocator,PlanBucket }=await import('../src/index.js');
  const env={ALLOW_FAULTS:'false',TABLET_SPLIT_BYTES:'16384',TABLET_MERGE_BYTES:'1024'};
  const spaces=[];
  const ns=Type=>{const states=new Map(),actors=new Map();const namespace={states,idFromName:x=>x,get(name){if(!states.has(name))states.set(name,{storage:storage()});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return{fetch:(url,init)=>actors.get(name).fetch(new Request(url,init))};},reset(){actors.clear();}};spaces.push(namespace);return namespace;};
  env.ALLOCATOR=ns(PlanAllocator);env.MEDIATORS=ns(PlanBucket);env.COORDINATOR=ns(Coordinator);env.SHARDS=ns(DataShard);
  const post=async(path,input)=>{const r=await Worker.fetch(new Request('http://127.0.0.1'+path,{method:'POST',body:JSON.stringify(input)}),env);return{status:r.status,...await r.json()};};
  const query=(id,sql,epoch=0)=>post('/query',{idempotencyKey:id,sql,requestEpoch:epoch});
  assert.equal((await query('setup','CREATE TABLE replay_a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO replay_a(id,n) VALUES (1,0)')).status,200);
  const sql='UPDATE replay_a SET n=n+1 WHERE id=1';
  {const result=await query('increment',sql);assert.equal(result.status,200,JSON.stringify(result));}
  {const result=await query('increment',sql);assert.equal(result.status,200,JSON.stringify(result));}
  assert.equal((await post('/snapshot',{action:'rotateEpoch'})).currentEpoch,1);
  assert.equal((await post('/maintenance',{action:'retireEpochs',before:1})).status,200);
  let page,rounds=0;do{page=await post('/maintenance',{action:'collectEpochs',cursor:page?.nextCursor,ownerCursor:page?.ownerCursor,bucket:page?.nextBucket});assert.equal(page.status,200);assert.ok(++rounds<20);}while(!page.done||page.ownerCursor);
  for(const state of env.COORDINATOR.states.values())assert.equal([...state.storage.data.keys()].filter(k=>k.startsWith('request:')).length,0,'retired request receipts were actually collected');
  for(const state of env.MEDIATORS.states.values())assert.equal([...state.storage.data.keys()].filter(k=>k.startsWith('plan:')).length,0,'done must cover retired plans in every mediator bucket');
  spaces.forEach(space=>space.reset());
  const expired=await query('increment',sql);assert.equal(expired.status,410);assert.equal(expired.code,'REQUEST_EXPIRED');
  const value=await query('read-new','SELECT n FROM replay_a WHERE id=1',1);assert.equal(value.status,200);assert.deepEqual(value.results[0].rows,[{n:1}]);
});

test('independent retired GC: old-floor cursor cannot orphan versions when retirement becomes collectible',async()=>{
  const db=shardModel();await db.commit('born',1,[{key:1,row:{id:1}},{key:2,row:{id:2}}],{expectedEpoch:0,meta:{epoch:1,retired:false}});
  const oldPage=await db.call({op:'compact',pageSize:1});assert.equal(oldPage.status,200);assert.ok(oldPage.nextCursor);
  await db.commit('retire',2,[],{expectedEpoch:1,meta:{epoch:2,retired:true}});db.setFloor(2);
  let cursor=oldPage.nextCursor,rounds=0;
  do{const page=await db.call({op:'compact',pageSize:1,cursor});assert.equal(page.status,200);cursor=page.nextCursor;await db.restart();assert.ok(++rounds<20);}while(cursor!==null);
  // A fresh pass collects keys preceding the old cursor. No deleted head may
  // strand a version that every future head-driven pass would permanently miss.
  do{const page=await db.call({op:'compact',pageSize:1,cursor});assert.equal(page.status,200);cursor=page.nextCursor;assert.ok(++rounds<40);}while(cursor!==null);
  assert.equal([...db.store.data.keys()].filter(k=>k.startsWith('row:')||k.startsWith('head:')).length,0,'retired row roots and heads must all be reclaimed');
  assert.equal((await db.call({op:'get',key:2,snapshot:2})).code,'STALE_EPOCH');
});

test('independent peer admission: mismatched durable proposal metadata cannot allocate an irrecoverable step',async()=>{
  const {PlanAllocator}=await import('../src/mediator.js');const {generateIdentity,manifestHash}=await import('../src/protocol.js');
  const identity=await generateIdentity();
  for(const mismatch of [{participants:['a','omitted']},{snapshot:1},{epoch:1},{owner:'other-owner'}]) {
    const s=storage(),proposal={id:'e0_bad',name:'a',owner:'owner',epoch:0,snapshot:0,participants:['a'],bodyHash:'a'.repeat(64),publicKey:identity.publicKey,...mismatch};
    const env={SHARDS:{idFromName:x=>x,get:()=>({fetch:async()=>Response.json(proposal)})}},actor=new PlanAllocator({storage:s},env);
    const call=async input=>{const r=await actor.fetch(new Request('https://test',{method:'POST',body:JSON.stringify(input)}));return{status:r.status,...await r.json()};};
    assert.equal((await call({op:'pin',id:'e0_bad',owner:'owner',epoch:0,snapshot:0})).status,200);
    const participants=[{name:'a',bodyHash:proposal.bodyHash,publicKey:identity.publicKey}];
    const result=await call({op:'assign',id:'e0_bad',owner:'owner',epoch:0,snapshot:0,participants,manifestHash:await manifestHash(participants)});
    assert.notEqual(result.status,200,`metadata mismatch was assigned: ${JSON.stringify(mismatch)}`);
    assert.equal((await s.get('allocated'))||0,0,'invalid association must not consume an irreversible plan step');
    assert.equal(await s.get('plan:e0_bad'),undefined);
  }
});

test('independent peer GC: terminal evidence collects only after retirement and delayed messages cannot recreate it',async()=>{
  const {PlanAllocator}=await import('../src/mediator.js'),{PlanBucket}=await import('../src/plan-bucket.js'),{manifestHash}=await import('../src/protocol.js');
  const env={PLAN_BUCKETS:'2'},states=new Map(),actors=new Map();
  for(const[binding,Class]of[['ALLOCATOR',PlanAllocator],['MEDIATORS',PlanBucket],['SHARDS',DataShard]])env[binding]={idFromName:x=>x,get:name=>({fetch:async(url,init)=>{const key=binding+':'+name;if(!states.has(key))states.set(key,{storage:storage(),id:name});if(!actors.has(key))actors.set(key,new Class(states.get(key),env));return actors.get(key).fetch(new Request(url,init));}})};
  const call=async(binding,name,input)=>{const r=await env[binding].get(name).fetch('https://test',{method:'POST',body:JSON.stringify(input)});return{status:r.status,...await r.json()};};
  const med=input=>call('ALLOCATOR','global',input),id='e0_peer_gc',names=['a','b'];
  assert.equal((await med({op:'pin',id,owner:'c0',snapshot:0})).status,200);
  const participants=[],bodies=new Map();
  for(const name of names){const body={op:'propose',id,name,owner:'c0',epoch:0,snapshot:0,participants:names,reads:[],changes:[{key:1,row:{id:1,name}}]};bodies.set(name,body);const p=await call('SHARDS',name,body);assert.equal(p.status,200);participants.push({name,bodyHash:p.bodyHash,publicKey:p.publicKey});}
  const {status,...association}=await med({op:'assign',id,owner:'c0',epoch:0,snapshot:0,participants,manifestHash:await manifestHash(participants)});assert.equal(status,200);
  assert.equal((await call('MEDIATORS',association.bucket,{op:'register',association})).status,200);
  for(const name of names)assert.equal((await call('SHARDS',name,{op:'acceptPlan',association})).status,200);
  await med({op:'rotateEpoch'});assert.equal((await med({op:'retireEpochs',before:1})).code,'EPOCH_BUSY');
  let bucket,rounds=0;do{for(const name of names)assert.equal((await call('SHARDS',name,{op:'drive',id})).status,200);bucket=await call('MEDIATORS',association.bucket,{op:'get',id});assert.ok(++rounds<20);}while(bucket.applied.length!==names.length);
  assert.equal(bucket.decision.outcome,'COMMIT');
  const readSet=await states.get('SHARDS:a').storage.get(`readset:${id}`);
  assert.equal((await med({op:'completion',id})).applied,true);
  assert.equal((await med({op:'status'})).pins,0,'authoritative terminal participant ACKs must release execution pin without origin coordinator');assert.equal((await med({op:'retireEpochs',before:1})).status,200);
  const prefixes=['proposal:','association:','readset:','inbox:','outbox:','terminal:','hold:','pending:','foot:','applied:','aborted:'];
  for(const name of names){let cursor,batches=0;do{const page=await call('SHARDS',name,{op:'compact',limit:1,pageSize:1,cursor});assert.equal(page.status,200,JSON.stringify(page));cursor=page.nextCursor;actors.delete('SHARDS:'+name);assert.ok(++batches<50);}while(cursor!==null);assert.equal([...states.get('SHARDS:'+name).storage.data.keys()].filter(k=>prefixes.some(prefix=>k.startsWith(prefix))).length,0);}
  assert.equal((await call('MEDIATORS',association.bucket,{op:'collect'})).deleted,1);
  let collection,collections=0;do{collection=await med({op:'collectEpochs',cursor:collection?.nextCursor});assert.equal(collection.status,200);assert.ok(++collections<30);}while(!collection.done);
  actors.clear();const before=structuredClone(states.get('SHARDS:a').storage.data);
  for(const input of[bodies.get('a'),{op:'proposal',id},{op:'acceptPlan',association},{op:'readSet',association,readSet},{op:'finalize',association,decision:bucket.decision},{op:'drive',id}]){const late=await call('SHARDS','a',input);assert.equal(late.status,410,JSON.stringify({op:input.op,late}));assert.equal(late.code,'REQUEST_EXPIRED');}
  assert.deepEqual(states.get('SHARDS:a').storage.data,before,'late traffic must not recreate collected protocol records');
  assert.equal((await call('SHARDS','a',{op:'get',key:1,snapshot:association.step})).row.name,'a');
});

test('independent peer GC: delayed aborted lookup cannot recreate a fence after local epoch collection',async()=>{
  const db=await peerModel(),id='e0_delayed_abort';
  assert.equal((await db.med({op:'pin',id,owner:'c0',snapshot:0})).status,200);
  assert.equal((await db.call('SHARDS','s',{op:'propose',id,name:'s',owner:'c0',epoch:0,snapshot:0,participants:['s'],reads:[],changes:[]})).status,200);
  assert.equal((await db.call('SHARDS','s',{op:'abort',id})).status,200);
  await db.med({op:'unpin',id});
  const original=db.env.ALLOCATOR;let entered,release,once=false;
  const reached=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);
  db.env.ALLOCATOR={...original,get:name=>({fetch:async(url,init)=>{const response=await original.get(name).fetch(url,init),input=JSON.parse(init.body);if(!once&&input.op==='lookup'&&input.id===id){once=true;entered();await gate;}return response;}})};
  const delayed=db.call('SHARDS','s',{op:'drive',id});await reached;
  try{
    await db.med({op:'rotateEpoch'});assert.equal((await db.med({op:'retireEpochs',before:1})).status,200);
    let cursor,rounds=0;do{const page=await db.call('SHARDS','s',{op:'compact',cursor});assert.equal(page.status,200);cursor=page.nextCursor;assert.ok(++rounds<20);}while(cursor!==null);
    const before=structuredClone(db.states.get('SHARDS:s').storage.data);
    release();const result=await delayed;assert.equal(result.status,410,JSON.stringify(result));assert.equal(result.code,'REQUEST_EXPIRED');
    assert.deepEqual(db.states.get('SHARDS:s').storage.data,before,'stale abort evidence must not recreate any retired record');
  }finally{release();}
});

test('independent peer GC: lost peer ACK resumed after collection cannot recreate an outbox',async()=>{
  const db=await peerModel(),id='e0_delayed_peer_ack';
  const a=await db.plan(id,0,{a:{changes:[{key:1,row:{id:1}}]},b:{changes:[{key:2,row:{id:2}}]}});
  const original=db.env.SHARDS;let entered,release,once=false;
  const reached=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);
  db.env.SHARDS={...original,get:name=>({fetch:async(url,init)=>{const response=await original.get(name).fetch(url,init),input=JSON.parse(init.body);if(!once&&name==='b'&&input.op==='readSet'&&input.association.id===id){once=true;entered();await gate;}return response;}})};
  const delayed=db.call('SHARDS','a',{op:'drive',id});await reached;
  try{
    assert.equal((await db.call('SHARDS','b',{op:'drive',id})).status,200);
    assert.equal((await db.call('MEDIATORS',a.bucket,{op:'drive',id})).status,200);
    assert.equal((await db.med({op:'completion',id})).applied,true);
    await db.med({op:'unpin',id});await db.med({op:'rotateEpoch'});assert.equal((await db.med({op:'retireEpochs',before:1})).status,200);
    let cursor,rounds=0;do{const page=await db.call('SHARDS','a',{op:'compact',cursor});assert.equal(page.status,200);cursor=page.nextCursor;assert.ok(++rounds<20);}while(cursor!==null);
    const before=structuredClone(db.states.get('SHARDS:a').storage.data);
    release();const result=await delayed;assert.equal(result.status,410,JSON.stringify(result));
    assert.deepEqual(db.states.get('SHARDS:a').storage.data,before,'late peer acknowledgement must not recreate outbox:old=[]');
  }finally{release();}
});

test('independent peer GC: admitted ReadSet paused during signature verification rechecks the final local epoch fence',async t=>{
  const db=await peerModel(),id='e0_verifying_readset';
  const a=await db.plan(id,0,{a:{changes:[{key:1,row:{id:1}}]},b:{changes:[{key:2,row:{id:2}}]}});await db.finish(a);
  const readSet=await db.states.get('SHARDS:b').storage.get(`readset:${id}`);
  let entered,release,once=false;const reached=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);
  const original=crypto.subtle.verify.bind(crypto.subtle);
  t.mock.method(crypto.subtle,'verify',async(...args)=>{const verified=await original(...args);if(!once){once=true;entered();await gate;}return verified;});
  const delayed=db.call('SHARDS','a',{op:'readSet',association:a,readSet});await reached;
  try{
    await db.med({op:'rotateEpoch'});assert.equal((await db.med({op:'retireEpochs',before:1})).status,200);
    let cursor,rounds=0;do{const page=await db.call('SHARDS','a',{op:'compact',cursor});assert.equal(page.status,200);cursor=page.nextCursor;assert.ok(++rounds<20);}while(cursor!==null);
    const before=structuredClone(db.states.get('SHARDS:a').storage.data);
    release();const result=await delayed;assert.equal(result.status,410,JSON.stringify(result));assert.equal(result.code,'REQUEST_EXPIRED');
    assert.deepEqual(db.states.get('SHARDS:a').storage.data,before,'verified but retired ReadSet must not recreate its inbox');
  }finally{release();}
});

test('independent coordinator GC: late alarm after peer completion and epoch retirement cleans without protocol replay',async()=>{
  const {Coordinator}=await import('../src/index.js');const db=await peerModel(),raw=storage(),s=scopedStorage(raw,'origin');let now=Date.now();db.env.NOW=()=>now;const coordinator=new Coordinator({storage:raw,id:coordinatorName('origin')},db.env);
  const request={scope:'origin',op:'applyTargets',idempotencyKey:'origin-down',coordinatorId:'origin',requestEpoch:0,snapshot:0,targets:[{name:'a',reads:[],changes:[{key:1,row:{id:1}}]}],response:{results:[{rowsAffected:1}]},fault:'after-plan'};
  const call=async input=>{const r=await coordinator.fetch(new Request('https://test',{method:'POST',body:JSON.stringify({scope:'origin',...input})}));return{status:r.status,...await r.json()};};
  assert.equal((await call(request)).status,503);
  const active=await readRecord(s,'active');assert.ok(active);
  const before=structuredClone(raw.data);
  assert.equal((await call({op:'collect',epoch:99})).status,409,'caller-provided retired epoch cannot authorize deletion');
  assert.deepEqual(raw.data,before);
  const found=await db.med({op:'lookup',id:active.id});assert.equal(found.state,'assigned');assert.equal((await db.med({op:'drive'})).status,200);await db.finish(found.association);
  assert.ok(await readRecord(s,'active'),'origin remains unaware of peer completion');
  await db.med({op:'rotateEpoch'});assert.equal((await db.med({op:'retireEpochs',before:1})).status,200);
  const original=db.env.ALLOCATOR,operations=[];
  db.env.ALLOCATOR={...original,get:name=>({fetch:async(url,init)=>{operations.push(JSON.parse(init.body).op);return original.get(name).fetch(url,init);}})};
  now+=100000;await coordinator.alarm();now+=100000;await coordinator.alarm();
  assert.equal(await readRecord(s,'active'),undefined);assert.equal(await readRecord(s,'operation'),undefined);
  assert.ok(!operations.some(op=>['assign','lookup','abortIfUnassigned'].includes(op)),'retired recovery must not replay protocol');
  const owners=await db.med({op:'retiredOwners'});assert.ok(owners.owners.some(entry=>entry.owner==='origin'),'receipt-owner remains discoverable until collection acknowledgement');
  assert.equal((await call({op:'collect',epoch:0})).status,200);
  await db.med({op:'forgetOwner',owner:'origin',epoch:0});assert.ok(!(await db.med({op:'retiredOwners'})).owners.some(entry=>entry.owner==='origin'));
});
