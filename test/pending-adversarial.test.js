import test from 'node:test';
import assert from 'node:assert/strict';
import Worker,{Coordinator,DataShard,PlanAllocator,PlanBucket} from '../src/index.js';
import {coordinatorName} from '../src/coordinator-routing.js';
import {scopedStorage} from '../src/coordinator-service.js';
import {readRecord} from '../src/records.js';
import {storage} from './pool-storage-fixture.js';
function cluster(){const env={ALLOW_FAULTS:'true'},stores=new Map(),actors=new Map(),calls=[];let suppressed=0;for(const[binding,Type]of Object.entries({COORDINATOR:Coordinator,SHARDS:DataShard,ALLOCATOR:PlanAllocator,MEDIATORS:PlanBucket}))env[binding]={idFromName:n=>n,get(name){const key=`${binding}:${name}`;if(!stores.has(key))stores.set(key,storage());if(!actors.has(key))actors.set(key,new Type({storage:stores.get(key),id:name,blockConcurrencyWhile:fn=>fn()},env));return {async fetch(url,init){const input=JSON.parse(init.body);calls.push({binding,name,input});if(binding==='MEDIATORS'&&input.op==='drive'&&suppressed>0){suppressed--;return Response.json({processed:0});}return actors.get(key).fetch(new Request(url,init));}};}};return {env,stores,calls,suppress(n){suppressed=n;},reset(){actors.clear();},clear(){calls.length=0;},async query(body){const r=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify(body)}),env);return {status:r.status,...await r.json()};},async rpc(binding,name,input){const r=await env[binding].get(name).fetch('https://actor',{method:'POST',body:JSON.stringify(input)});return {status:r.status,...await r.json()};},async raw(body){return this.rpc('COORDINATOR',coordinatorName(body.coordinatorId),{scope:body.coordinatorId,...body});}};}

test('independent pending: lost reply/reset never publishes receipt before all participants apply',async()=>{
 const db=cluster(),owner='independent/wide',input={op:'applyTargets',coordinatorId:owner,idempotencyKey:'immutable',snapshot:0,targets:Array.from({length:11},(_,i)=>({name:`wide/${i}`,reads:[],changes:[{key:1,row:{id:1,n:i}}]})),response:{results:[{rowsAffected:11}]}};
 let pending=await db.raw(input);assert.equal(pending.code,'PLAN_PENDING');assert.equal(pending.outcome,'PENDING');const id=pending.transactionId;
 const store=scopedStorage(db.stores.get(`COORDINATOR:${coordinatorName(owner)}`),owner);
 assert.equal(await readRecord(store,'request:immutable'),undefined);db.reset();db.clear();
 const mismatch=await db.raw({...input,targets:input.targets.slice(1)});assert.equal(mismatch.code,'IDEMPOTENCY_CONFLICT');assert.equal(db.calls.filter(c=>c.binding==='MEDIATORS').length,0);
 let complete;
 for(let turn=0;turn<128;turn++){
  db.clear();const result=await db.raw(input);
  for(const op of ['drive','readSet','finalize'])assert.ok(db.calls.filter(c=>c.binding==='SHARDS'&&c.input.op===op).length<=1);
  if(result.status===200){complete=result;break;}
  assert.equal(result.code,'PLAN_PENDING');assert.equal(result.transactionId,id);assert.equal(await readRecord(store,'request:immutable'),undefined);
  // This fixture has no timer delivery: explicitly execute a bucket background turn.
  const found=await db.rpc('ALLOCATOR','global',{op:'lookup',id});await db.rpc('MEDIATORS',found.association.bucket,{op:'drive',id});db.reset();
 }
 assert.ok(complete);assert.equal(complete.transactionId,id);assert.equal(complete.version,1);assert.deepEqual(await db.raw(input),complete);
 const found=await db.rpc('ALLOCATOR','global',{op:'lookup',id}),plan=await db.rpc('MEDIATORS',found.association.bucket,{op:'get',id});assert.equal(plan.applied.length,input.targets.length);assert.equal((await readRecord(store,'request:immutable')).response.transactionId,id);
});
