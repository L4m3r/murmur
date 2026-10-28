import test from 'node:test';
import assert from 'node:assert/strict';
import {storage} from './pool-storage-fixture.js';
import {CoordinatorService} from '../src/coordinator-service.js';
import {coordinatorName} from '../src/coordinator-routing.js';
import Worker,{Coordinator,DataShard,PlanAllocator,PlanBucket} from '../src/index.js';
const gate=()=>{let release,enter;return {pending:new Promise(r=>release=r),entered:new Promise(r=>enter=r),release:()=>release(),enter:()=>enter()};};
function scopes(n){const out=[];for(let i=0;out.length<n;i++){const s=`scope-${i}`;if(coordinatorName(s)===coordinatorName('scope-0'))out.push(s);}return out;}
class Logical {
 constructor(state,env){this.storage=state.storage;this.env=env;this.tail=Promise.resolve();}
 run(fn){const p=this.tail.then(fn);this.tail=p.catch(()=>{});return p;}
 fetch(request){return this.run(async()=>{const q=await request.json();await this.storage.put('scope',q.scope);await this.storage.setAlarm(q.deadline);return Response.json({ok:true});});}
 alarm(){return this.run(async()=>{const scope=await this.storage.get('scope');await this.env.recover(scope,this.storage);});}
}
function fixture(){const root=storage(),ids=scopes(4),env={NOW:()=>100,COORDINATOR:{idFromName:x=>x},recover:async()=>{}};const state={storage:root,id:coordinatorName(ids[0])};let service=new CoordinatorService(state,env,Logical);return {root,ids,env,get service(){return service;},reset(){service=new CoordinatorService(state,env,Logical);},arm(scope,deadline){return service.fetch(new Request('https://pool',{method:'POST',body:JSON.stringify({scope,coordinatorId:scope,deadline})}));}};}

test('independent pool: failed recovery cannot erase concurrent alarm incarnation after restart',async()=>{
 const f=fixture(),[a,b]=f.ids,g=gate();await f.arm(a,10);await f.arm(b,20);
 f.env.recover=async(scope,store)=>{if(scope===a){g.enter();await g.pending;throw Error('late remote failure');}await store.put('complete',true);};
 const running=f.service.alarm();await g.entered;
 await f.service.scopedStorage(a).deleteAlarm();await f.service.scopedStorage(a).setAlarm(9000);g.release();await running;
 assert.equal(await f.service.scopedStorage(a).getAlarm(),9000);assert.equal(await f.service.scopedStorage(b).get('complete'),true);f.reset();assert.equal(await f.service.scopedStorage(a).getAlarm(),9000);assert.equal(f.root.alarm,9000);
});

test('independent pool: rollback cannot publish another scope alarm or clobber existing due state',async()=>{
 const f=fixture(),[a,b]=f.ids;await f.arm(a,5000);await f.arm(b,7000);
 await assert.rejects(f.service.scopedStorage(a).transaction(async tx=>{await tx.put('operation',{id:'not-admitted'});await tx.deleteAlarm();await tx.setAlarm(2);throw Error('crash before commit');}));
 f.reset();assert.equal(await f.service.scopedStorage(a).get('operation'),undefined);assert.equal(await f.service.scopedStorage(a).getAlarm(),5000);assert.equal(await f.service.scopedStorage(b).getAlarm(),7000);assert.equal(f.root.alarm,5000);
});

test('independent Worker: caller scope and owner cannot override immutable pool routing',async()=>{
 const env={},calls=[];
 function namespace(Type){const states=new Map(),actors=new Map();return {idFromName:x=>x,get(name){if(!states.has(name))states.set(name,{storage:storage(),id:name});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return {fetch:async(url,init)=>{const input=JSON.parse(init.body);if(Type===Coordinator)calls.push({name,input});return actors.get(name).fetch(new Request(url,init));}};}}};env.SHARDS=namespace(DataShard);env.ALLOCATOR=namespace(PlanAllocator);env.MEDIATORS=namespace(PlanBucket);env.COORDINATOR=namespace(Coordinator);
 const send=async extra=>{const response=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify({sql:'CREATE TABLE a(id INTEGER PRIMARY KEY)',idempotencyKey:'routing',...extra})}),env);return {status:response.status,body:await response.json()};};
 const original=await send({scope:'attacker',coordinatorId:'attacker'});assert.equal(original.status,200,JSON.stringify(original));assert.notEqual(calls[0].input.scope,'attacker');assert.equal(calls[0].input.scope,calls[0].input.coordinatorId);assert.equal(calls[0].name,coordinatorName(calls[0].input.scope));
 const replay=await send({scope:'different',coordinatorId:'different'});assert.deepEqual(replay,original);assert.equal(calls[1].name,calls[0].name);assert.equal(calls[1].input.scope,calls[0].input.scope);
});

test('independent pool: serial alarm callbacks return while held scope retains exclusive context',async()=>{
 const f=fixture(),[a,b]=f.ids,g=gate();f.env.COORDINATOR_ALARM_BATCH=1;f.env.COORDINATOR_ALARM_SLICE_MS=10;f.reset();await f.arm(a,10);await f.arm(b,20);
 f.env.recover=async(scope,store)=>{if(scope===a){g.enter();await g.pending;}await store.put('complete',true);};
 let timer;try{
  await Promise.race([f.service.alarm(),new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('Alarm callback awaited stalled owner')),500))]);
  await g.entered;assert.equal(f.service.contexts.get(a).refs,1);assert.ok(f.root.alarm!==null);
  await f.service.alarm();assert.equal(await f.service.scopedStorage(b).get('complete'),true);assert.equal(await f.service.scopedStorage(a).get('complete'),undefined);assert.equal(f.service.contexts.get(a).refs,1);
 }finally{clearTimeout(timer);g.release();}
 // Underlying task, not timer expiry, releases its scope ownership.
 for(let i=0;i<20&&f.service.contexts.size;i++)await new Promise(r=>setTimeout(r,1));
 assert.equal(f.service.contexts.size,0);assert.equal(await f.service.scopedStorage(a).get('complete'),true);
});

test('independent pool: unsupported pool count fails closed before logical admission',()=>{
 const f=fixture();assert.throws(()=>new CoordinatorService({storage:f.root,id:coordinatorName(f.ids[0])},{...f.env,COORDINATOR_POOL_SIZE:32},Logical));assert.equal(f.root.map.size,0);
});

test('independent pool: short storage queue isolates ordinary KV and nested transaction/alarm operations',async()=>{
 const f=fixture(),[a,b]=f.ids,g=gate(),sa=f.service.scopedStorage(a),sb=f.service.scopedStorage(b);
 let entries=0,maxEntries=0;const original=f.root.transaction;
 f.root.transaction=async fn=>{entries++;maxEntries=Math.max(maxEntries,entries);try{return await original(fn);}finally{entries--;}};
 const first=sa.transaction(async tx=>{await tx.put('value','uncommitted');await tx.transaction(async nested=>{await nested.put('nested',true);await nested.setAlarm(5000);});g.enter();await g.pending;throw Error('rollback local batch');});
 await g.entered;
 let ordinaryDone=false;const ordinary=sb.put('value','committed').then(()=>ordinaryDone=true);
 const second=sb.transaction(async tx=>{assert.equal(await tx.get('value'),'committed');await tx.setAlarm(6000);});
 await new Promise(r=>setTimeout(r,0));assert.equal(ordinaryDone,false);assert.equal(entries,1);g.release();
 await assert.rejects(first,/rollback local batch/);await ordinary;await second;
 assert.equal(maxEntries,1);assert.equal(await sa.get('value'),undefined);assert.equal(await sa.get('nested'),undefined);assert.equal(await sa.getAlarm(),null);assert.equal(await sb.get('value'),'committed');assert.equal(await sb.getAlarm(),6000);
});
