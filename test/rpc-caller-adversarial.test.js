import test from 'node:test';
import assert from 'node:assert/strict';
import Worker,{Coordinator,DataShard,PlanAllocator,PlanBucket} from '../src/index.js';
import {coordinatorName} from '../src/coordinator-routing.js';
import {scopedStorage} from '../src/coordinator-service.js';
import {readRecord} from '../src/records.js';
import {storage} from './pool-storage-fixture.js';
function cluster(){const env={ALLOW_FAULTS:'true'},stores=new Map(),actors=new Map(),calls=[];let suppressed=0,inject=null;for(const[binding,Type]of Object.entries({COORDINATOR:Coordinator,SHARDS:DataShard,ALLOCATOR:PlanAllocator,MEDIATORS:PlanBucket}))env[binding]={idFromName:n=>n,get(name){const key=`${binding}:${name}`;if(!stores.has(key))stores.set(key,storage());if(!actors.has(key))actors.set(key,new Type({storage:stores.get(key),id:name,blockConcurrencyWhile:fn=>fn()},env));return {async fetch(url,init){const input=JSON.parse(init.body);calls.push({binding,name,input});if(inject?.(binding,name,input))return new Response("provider overloaded "+"x".repeat(6000),{status:503,headers:{"x-celld-overload":"1","retry-after":"2"}});if(binding==='MEDIATORS'&&input.op==='drive'&&suppressed>0){suppressed--;return Response.json({processed:0});}return actors.get(key).fetch(new Request(url,init));}};}};return {env,stores,calls,inject(fn){inject=fn;},suppress(n){suppressed=n;},reset(){actors.clear();},clear(){calls.length=0;},async query(body){const r=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify(body)}),env);return {status:r.status,...await r.json()};},async rpc(binding,name,input){const r=await env[binding].get(name).fetch('https://actor',{method:'POST',body:JSON.stringify(input)});return {status:r.status,...await r.json()};},async raw(body){return this.rpc('COORDINATOR',coordinatorName(body.coordinatorId),{scope:body.coordinatorId,...body});}};}


for(const where of ['allocator-admission','allocator-pin','planner-read','proposal'])test(`independent RPC caller: ${where} plaintext overload retains diagnostics and same-key recovery`,async()=>{
 const db=cluster();assert.equal((await db.query({sql:'CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES(1,0)',idempotencyKey:'seed'})).status,200);
 let hits=0;db.inject((binding,name,input)=>{
  const matches=where==='allocator-admission'?binding==='ALLOCATOR'&&input.op==='epoch':where==='allocator-pin'?binding==='ALLOCATOR'&&input.op==='pin':where==='planner-read'?binding==='SHARDS'&&name.startsWith('base/')&&['scan','getMany'].includes(input.op):binding==='SHARDS'&&input.op==='propose';
  if(matches){hits++;return true;}return false;
 });
 const body={sql:'UPDATE a SET n=n+1 WHERE id=1',idempotencyKey:'retry'};const unavailable=await db.query(body);
 assert.ok(hits>0);assert.equal(unavailable.status,503,JSON.stringify(unavailable));assert.equal(unavailable.code,'RPC_BACKPRESSURE');assert.equal(unavailable.retryable,true);assert.equal(unavailable.rpc.responseStatus,503);assert.equal(unavailable.rpc.overload,'1');assert.equal(unavailable.retryAfterMs,2000);assert.ok(unavailable.rpc.bodyExcerpt.length<=512);assert.equal(unavailable.rpc.truncated,true);
 db.inject(null);db.reset();const done=await db.query(body);assert.equal(done.status,200,JSON.stringify(done));assert.deepEqual(await db.query(body),done);assert.deepEqual((await db.query({sql:'SELECT n FROM a',idempotencyKey:'read'})).results[0].rows,[{n:1}]);
});

test('independent RPC caller: internal schema transaction pin overload remains retryable without a receipt',async()=>{
 const db=cluster(),body={op:'applyTargets',coordinatorId:'schema/rpc-diagnostic',idempotencyKey:'batch',snapshot:0,targets:[{name:'internal/a',reads:[],changes:[{key:1,row:{id:1}}]}],response:{results:[]}};
 db.inject((binding,name,input)=>binding==='ALLOCATOR'&&input.op==='pin');
 const unavailable=await db.raw(body);assert.equal(unavailable.status,503);assert.equal(unavailable.code,'RPC_BACKPRESSURE');assert.equal(unavailable.retryable,true);assert.equal(unavailable.rpc.overload,'1');assert.equal(unavailable.retryAfterMs,2000);
 const store=scopedStorage(db.stores.get(`COORDINATOR:${coordinatorName(body.coordinatorId)}`),body.coordinatorId);assert.equal(await readRecord(store,'request:batch'),undefined);
 db.inject(null);db.reset();const done=await db.raw(body);assert.equal(done.status,200,JSON.stringify(done));assert.deepEqual(await db.raw(body),done);
});
