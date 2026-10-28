import {coordinatorName} from '../src/coordinator-routing.js';
import {storage} from './pool-storage-fixture.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import Worker, { Coordinator, DataShard, PlanAllocator as Mediator, PlanBucket } from '../src/index.js';
import { manifestHash } from '../src/protocol.js';
import { encodeKey } from '../src/keys.js';

// Deterministic scheduling, durable actor-state replacement, no simulated alarms.

const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false};
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
async function deadline(promise){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Visibility progress deadline exceeded')),2500);})]);}finally{clearTimeout(timer);}}
function cluster() {
 const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'16384',TABLET_MERGE_BYTES:'1024'};let hook=async()=>{},sequence=0;const spaces=[],proposed=new Map();
 function namespace(Type){const states=new Map(),actors=new Map();const ns={idFromName:x=>x,get(name){if(!states.has(name))states.set(name,{storage:storage(),id:name});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return {fetch:async(url,init)=>{const input=JSON.parse(init.body);await hook('before',Type,name,input);const response=await actors.get(name).fetch(new Request(url,init));await hook('after',Type,name,input,response);return response;}};},reset(){actors.clear();}};spaces.push(ns);return ns;}
 env.SHARDS=namespace(DataShard);env.ALLOCATOR=namespace(Mediator);env.MEDIATORS=namespace(PlanBucket);env.COORDINATOR=namespace(Coordinator);
 const rpc=async(ns,name,input)=>{const response=await ns.get(name).fetch('https://internal',{method:'POST',body:JSON.stringify(input)});return {status:response.status,...await response.json()};};
 return {env,hook(fn){hook=fn;},reset(){spaces.forEach(s=>s.reset());},shard:(name,input)=>rpc(env.SHARDS,name,input),mediator:input=>rpc(env.ALLOCATOR,'global',input),
  async post(path,input){const response=await Worker.fetch(new Request('http://127.0.0.1'+path,{method:'POST',body:JSON.stringify(input)}),env);return {status:response.status,...await response.json()};},
  query(sql,extra={}){return this.post('/query',{sql,idempotencyKey:`visibility_${++sequence}`,...extra});},
  async prepare(name,id,changes=[],reads=[],snapshot=0,extra={}){await rpc(env.ALLOCATOR,'global',{op:'pin',id,owner:`owner-${id}`,snapshot});const result=await rpc(env.SHARDS,name,{op:'propose',id,name,epoch:0,owner:`owner-${id}`,snapshot,participants:[name],changes,reads,...extra});assert.equal(result.status,200,JSON.stringify(result));proposed.set(id,snapshot);return {name,bodyHash:result.bodyHash,publicKey:result.publicKey};},
  async plan(id,votes){const {status,...result}=await rpc(env.ALLOCATOR,'global',{op:'assign',id,epoch:0,owner:`owner-${id}`,snapshot:proposed.get(id),participants:votes,manifestHash:await manifestHash(votes)});assert.equal(status,200,JSON.stringify(result));await rpc(env.MEDIATORS,result.bucket,{op:'register',association:result});return result;},
  async apply(id,plan,vote){const result=await rpc(env.SHARDS,vote.name,{op:'drive',id});assert.equal(result.status,200,JSON.stringify(result));assert.equal(result.outcome,'COMMIT');await rpc(env.ALLOCATOR,'global',{op:'completion',id});await rpc(env.ALLOCATOR,'global',{op:'unpin',id});}

 };
}

test('visibility independent: allocated snapshot bypasses unrelated hole, not affected unapplied keys',async()=>{
 const db=cluster(),a=await db.prepare('same','hole',[{key:1,row:{id:1}}]),b=await db.prepare('same','later',[{key:2,row:{id:2}}]);
 const first=await db.plan('hole',[a]),second=await db.plan('later',[b]);await db.apply('later',second,b);
 const status=await db.mediator({op:'snapshot'});assert.equal(status.snapshot,second.step);assert.equal(status.frontier,0);
 assert.deepEqual((await db.shard('same',{op:'get',key:2,snapshot:status.snapshot})).rows,[{id:2}]);
 const affected=await db.shard('same',{op:'get',key:1,snapshot:status.snapshot});assert.equal(affected.status,409);assert.ok(affected.dependencies.some(d=>d.id==='hole'));
 assert.equal((await db.shard('same',{op:'scan',snapshot:status.snapshot,range:all})).status,409);
 db.reset();await db.apply('hole',first,a);
 assert.deepEqual((await db.shard('same',{op:'scan',snapshot:second.step,range:all})).rows,[{id:1},{id:2}]);
});

test('visibility independent: read-only reservations and unplanned writes do not block stable snapshot',async()=>{
 const db=cluster();await db.prepare('s','read-reservation',[],[all]);await db.prepare('other','unplanned',[{key:1,row:{id:1}}]);
 assert.equal((await db.shard('s',{op:'scan',range:all,snapshot:0})).status,200);
 assert.deepEqual((await db.shard('other',{op:'get',key:1,snapshot:0})).rows,[]);
});

test('visibility independent: allocation during reservation lookup cannot change fixed older snapshot',async()=>{
 const db=cluster(),vote=await db.prepare('s','racing',[{key:1,row:{id:1}}]);let plan,once=false;
 db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='before'&&Type===Mediator&&input.op==='lookup'&&input.id==='racing'){once=true;plan=await db.plan('racing',[vote]);}});
 const read=await db.shard('s',{op:'get',key:1,snapshot:0});assert.equal(once,true);assert.equal(read.status,200);assert.deepEqual(read.rows,[]);
 await db.apply('racing',plan,vote);assert.deepEqual((await db.shard('s',{op:'get',key:1,snapshot:0})).rows,[]);
 assert.deepEqual((await db.shard('s',{op:'get',key:1,snapshot:plan.step})).rows,[{id:1}]);
});

test('visibility independent: pending metadata fences reads before expected-epoch validation',async()=>{
 const db=cluster(),vote=await db.prepare('new','create',[{key:1,row:{id:1}}],[],0,{expectedEpoch:0,meta:{epoch:1,retired:false}}),plan=await db.plan('create',[vote]);
 const blocked=await db.shard('new',{op:'get',key:1,snapshot:plan.step,epoch:1});assert.equal(blocked.status,409);assert.ok(blocked.dependencies.some(d=>d.id==='create'));
 await db.apply('create',plan,vote);assert.deepEqual((await db.shard('new',{op:'get',key:1,snapshot:plan.step,epoch:1})).rows,[{id:1}]);
});

test('visibility independent: newer applied versions and scan pages never bleed into fixed snapshot',async()=>{
 const db=cluster(),seed=await db.prepare('s','seed',Array.from({length:5},(_,key)=>({key,row:{id:key,n:0}}))),p1=await db.plan('seed',[seed]);await db.apply('seed',p1,seed);
 const page1=await db.shard('s',{op:'scan',range:all,snapshot:p1.step,pageSize:2});assert.deepEqual(page1.rows,[{id:0,n:0},{id:1,n:0}]);
 const later=await db.prepare('s','later',[{key:3,row:{id:3,n:9}},{key:6,row:{id:6,n:9}}],[],p1.step),p2=await db.plan('later',[later]);await db.apply('later',p2,later);
 let cursor=page1.nextCursor,rows=[...page1.rows];while(cursor){const page=await db.shard('s',{op:'scan',range:all,snapshot:p1.step,pageSize:2,cursor});assert.equal(page.status,200);rows.push(...page.rows);cursor=page.nextCursor;}
 assert.deepEqual(rows,Array.from({length:5},(_,id)=>({id,n:0})));
});

test('visibility independent: allocated lease above apply frontier does not move GC across a hole',async()=>{
 const db=cluster(),a=await db.prepare('a','hole',[{key:1,row:{id:1}}]),b=await db.prepare('b','later',[{key:2,row:{id:2}}]);
 const p1=await db.plan('hole',[a]),p2=await db.plan('later',[b]);await db.apply('later',p2,b);
 assert.equal((await db.mediator({op:'lease',id:'old',snapshot:0})).status,200);
 assert.equal((await db.mediator({op:'lease',id:'latest',snapshot:p2.step})).status,200);
 assert.equal((await db.mediator({op:'pin',id:'reader',owner:'reader',snapshot:p2.step,leaseId:'latest'})).status,200);
 assert.equal((await db.mediator({op:'compact',retainSteps:0})).retentionFloor,0);
 await db.apply('hole',p1,a);assert.equal((await db.mediator({op:'compact',retainSteps:0})).retentionFloor,0);
 await db.mediator({op:'release',id:'old'});assert.equal((await db.mediator({op:'compact',retainSteps:0})).retentionFloor,p2.step);
 assert.deepEqual((await db.shard('b',{op:'get',key:2,snapshot:p2.step})).rows,[{id:2}]);
});

test('visibility Worker independent: held planned write does not delay unrelated write or fresh read',async()=>{
 const db=cluster(),held=deferred(),release=deferred();let once=false;
 assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,0),(2,0)')).status,200);
 db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='before'&&Type===DataShard&&input.op==='finalize'){once=true;held.resolve();await release.promise;}});
 const first=db.query('UPDATE a SET n=1 WHERE id=1');await deadline(held.promise);
 try{
  const second=await deadline(db.query('UPDATE a SET n=2 WHERE id=2'));assert.equal(second.status,200,JSON.stringify(second));
  const status=await db.mediator({op:'snapshot'});assert.ok(status.frontier<second.step);assert.ok(status.snapshot>=second.step);
  const read=await deadline(db.query('SELECT n FROM a WHERE id=2'));assert.equal(read.status,200);assert.deepEqual(read.results[0].rows,[{n:2}]);
 }finally{release.resolve();}
 assert.equal((await first).status,200);
 assert.deepEqual((await db.query('SELECT n FROM a ORDER BY id')).results[0].rows,[{n:1},{n:2}]);
});

for(const indexed of [false,true])test(`visibility Worker independent: partial apply ${indexed?'base/index lookup':'cross-table snapshot'} helps the plan without mixed data`,async()=>{
 const db=cluster(),held=deferred(),release=deferred(),recovery=deferred();let once=false,finished=false;
 const setup=indexed?"CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); CREATE INDEX tag_idx ON a(tag); INSERT INTO a(id,tag) VALUES (1,'old')":"CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); CREATE TABLE b(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,0); INSERT INTO b(id,n) VALUES (1,0)";
 assert.equal((await db.query(setup)).status,200);
 db.hook(async(phase,Type,name,input)=>{
  if(!once&&phase==='before'&&(indexed ? Type===PlanBucket&&input.op==='applied'&&input.ack.name.startsWith('index/') : Type===DataShard&&input.op==='finalize'&&name.startsWith('base/b/'))){once=true;held.resolve();await release.promise;}
  if(once&&phase==='before'&&Type===PlanBucket&&input.op==='drive')recovery.resolve();
 });
 const writer=db.query(indexed?"UPDATE a SET tag='new' WHERE id=1":"UPDATE a SET n=1 WHERE id=1; UPDATE b SET n=1 WHERE id=1");
 await deadline(held.promise);
 const read=db.query(indexed?"SELECT id FROM a WHERE tag='new'":"SELECT n FROM a; SELECT n FROM b").then(result=>{finished=true;return result;});
 let result;try{result=await deadline(read);assert.equal(result.status,200,JSON.stringify(result));}finally{release.resolve();}
 assert.equal((await writer).status,200);
 if(indexed)assert.deepEqual(result.results[0].rows,[{id:1}]);
 else assert.deepEqual(result.results.map(r=>r.rows),[[{n:1}],[{n:1}]]);
});

test('visibility Worker independent: affected read completes planned transaction with origin coordinator unavailable',async()=>{
 const db=cluster();assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,0)')).status,200);
 let owner;db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===Coordinator&&input.sql==='UPDATE a SET n=9 WHERE id=1')owner=input.scope;});
 const interrupted=await db.query('UPDATE a SET n=9 WHERE id=1',{fault:'after-plan',idempotencyKey:'origin-down'});assert.equal(interrupted.status,503);assert.ok(owner);
 db.reset();let touchedOrigin=false;db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===Coordinator&&input.scope===owner){touchedOrigin=true;throw new Error('Origin coordinator unavailable');}});
 await assert.rejects(db.env.COORDINATOR.get(coordinatorName(owner)).fetch('https://coordinator',{method:'POST',body:JSON.stringify({op:'recover',scope:owner})}),/Origin coordinator unavailable/);assert.equal(touchedOrigin,true);touchedOrigin=false;
 const read=await deadline(db.query('SELECT n FROM a WHERE id=1'));assert.equal(read.status,200,JSON.stringify(read));assert.deepEqual(read.results[0].rows,[{n:9}]);assert.equal(touchedOrigin,false);
 assert.equal((await db.mediator({op:'snapshot'})).pins,0,'terminal peer ACKs must release execution pin without origin');
 await db.mediator({op:'rotateEpoch'});assert.equal((await db.mediator({op:'retireEpochs',before:1})).status,200);
 db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===Coordinator&&input.scope===owner&&input.op!=='collect')throw new Error('Only retired receipt collection may reach old origin');});
 const collected=await db.env.COORDINATOR.get(coordinatorName(owner)).fetch('https://coordinator',{method:'POST',body:JSON.stringify({op:'collect',scope:owner,epoch:0})});
 assert.equal(collected.status,200,JSON.stringify(await collected.json()));
});
