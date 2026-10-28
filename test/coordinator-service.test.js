import test from 'node:test';
import assert from 'node:assert/strict';
import {CoordinatorService,scopedStorage} from '../src/coordinator-service.js';
import {coordinatorName,COORDINATOR_POOL_SIZE} from '../src/coordinator-routing.js';
import {readRecord,writeRecord,deleteRecord} from '../src/records.js';
function storage(){const map=new Map();let tail=Promise.resolve();const api={map,alarm:null,async get(key){if(Array.isArray(key))return new Map(key.filter(k=>map.has(k)).map(k=>[k,structuredClone(map.get(k))]));return structuredClone(map.get(key));},async put(key,value){if(typeof key!=='string'){for(const[k,v]of Object.entries(key))map.set(k,structuredClone(v));}else map.set(key,structuredClone(value));},async delete(key){if(Array.isArray(key)){let n=0;for(const k of key)n+=Number(map.delete(k));return n;}return map.delete(key);},async list({prefix='',start,startAfter,end,reverse=false,limit}={}){let rows=[...map].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(startAfter===undefined||k>startAfter)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:1);if(reverse)rows.reverse();if(limit!==undefined)rows=rows.slice(0,limit);return new Map(rows.map(([k,v])=>[k,structuredClone(v)]));},transaction(fn){const task=tail.then(async()=>{const backup=structuredClone(map),alarm=api.alarm;try{return await fn(api);}catch(e){map.clear();for(const[k,v]of backup)map.set(k,v);api.alarm=alarm;throw e;}});tail=task.catch(()=>{});return task;},async setAlarm(time){api.alarm=time;},async getAlarm(){return api.alarm;},async deleteAlarm(){api.alarm=null;}};return api;}
const gate=()=>{let release,entered;return{promise:new Promise(r=>release=r),waiting:new Promise(r=>entered=r),release:()=>release(),enter:()=>entered()};};
function matchingScopes(count=4){const route=coordinatorName('tx/a'),out=[];for(let n=0;out.length<count;n++){const scope=`tx/test-${n}`;if(coordinatorName(scope)===route)out.push(scope);}return out;}
class FakeCoordinator{
 constructor(state,env){this.storage=state.storage;this.env=env;this.tail=Promise.resolve();env.created++;}
 run(fn){const task=this.tail.then(fn);this.tail=task.catch(()=>{});return task;}
 fetch(request){return this.run(async()=>{const input=await request.json();await this.storage.put('name',input.scope);if(input.op==='setAlarm'){await this.storage.setAlarm(input.deadline);return Response.json({armed:true});}if(input.op==='deleteAlarm'){await this.storage.deleteAlarm();return Response.json({deleted:true});}const old=await this.storage.get(`request:${input.key}`);if(old)return Response.json(old);if(input.op==='block'){const g=this.env.gates.get(input.scope);g.enter();await g.promise;}const n=(await this.storage.get('counter')||0)+1;await this.storage.put('counter',n);const result={n,scope:input.scope};await this.storage.put(`request:${input.key}`,result);return Response.json(result);});}
 alarm(){return this.run(async()=>{const scope=await this.storage.get('name');if(this.env.onAlarm)await this.env.onAlarm(scope,this.storage);await this.storage.put('fires',(await this.storage.get('fires')||0)+1);});}
}
function fixture(config={}){const root=storage(),scopes=matchingScopes(6),route=coordinatorName(scopes[0]);const env={created:0,gates:new Map(),NOW:()=>100,COORDINATOR:{idFromName:x=>x},...config};const background=[];const state={storage:root,id:route,waitUntil:task=>background.push(task)};let service=new CoordinatorService(state,env,FakeCoordinator);return{root,env,scopes,background,get service(){return service;},restart(){service=new CoordinatorService(state,env,FakeCoordinator);},async call(scope,body={}){const r=await service.fetch(new Request('https://coordinator',{method:'POST',body:JSON.stringify({scope,coordinatorId:scope,...body})}));return{status:r.status,body:await r.json()};}};}

test('routing is fixed16 UTF8 FNV physicalservices and scopes cannot mismatch physical/logical identity',async()=>{
 assert.equal(COORDINATOR_POOL_SIZE,16);for(const scope of ['tx/a','schema/hello','tx/😀']){let hash=2166136261;for(const byte of Buffer.from(scope,'utf8'))hash=Math.imul(hash^byte,16777619)>>>0;assert.equal(coordinatorName(scope),`coordinator-pool/v1/${hash%16}`);}
 assert.throws(()=>coordinatorName('\ud800'));const f=fixture();assert.equal((await f.call(f.scopes[0],{coordinatorId:'other'})).status,400);assert.equal((await f.call('wrong-physical-owner')).status,400);
});

test('scoped KV batch/list/transaction and chunk manifests cannot leak between owners',async()=>{
 const root=storage(),a=scopedStorage(root,'tx/a'),b=scopedStorage(root,'tx/b');
 await a.put({a:1,b:2,c:3});await b.put({a:9,b:8});assert.deepEqual([...await a.get(['a','c','missing'])],[['a',1],['c',3]]);
 assert.deepEqual([...await a.list({start:'a',end:'c',reverse:true,limit:1})],[['b',2]]);assert.deepEqual([...await a.list({startAfter:'a'})],[['b',2],['c',3]]);assert.deepEqual([...await b.list({prefix:'a'})],[['a',9]]);
 const dataA={payload:'a'.repeat(180000)},dataB={payload:'b'.repeat(180000)};
 await a.transaction(txn=>writeRecord(txn,'active',dataA));await b.transaction(txn=>writeRecord(txn,'active',dataB));assert.deepEqual(await readRecord(a,'active'),dataA);assert.deepEqual(await readRecord(b,'active'),dataB);
 await assert.rejects(a.transaction(async txn=>{await writeRecord(txn,'active',{payload:'broken'.repeat(30000)});throw Error('rollback');}));assert.deepEqual(await readRecord(a,'active'),dataA);
 await a.transaction(txn=>deleteRecord(txn,'active'));assert.deepEqual(await readRecord(b,'active'),dataB);assert.equal(await readRecord(a,'active'),undefined);assert.equal(await a.delete(['a','b']),2);assert.equal(await b.get('a'),9);
 assert.ok([...root.map.keys()].every(k=>k.startsWith('scope:')));
});

test('stalled logical scope does not block another and queued same-scope retry shares one instance',async()=>{
 const f=fixture(),[a,b]=f.scopes,g=gate();f.env.gates.set(a,g);const first=f.call(a,{op:'block',key:'same'});await g.waiting;
 const retry=f.call(a,{op:'increment',key:'same'});await new Promise(r=>setTimeout(r,0));assert.equal(f.service.contexts.get(a).refs,2);
 const other=await f.call(b,{op:'increment',key:'other'});assert.equal(other.status,200);assert.equal(other.body.n,1);assert.equal(f.service.contexts.get(a).refs,2);assert.equal(f.env.created,2);
 g.release();assert.deepEqual((await first).body,(await retry).body);assert.equal(f.service.contexts.size,0);assert.equal(f.service.totalRefs,0);assert.equal(f.service.totalBytes,0);
});

test('scope/context/aggregate admission caps reject before logical state changes',async()=>{
 const f=fixture({COORDINATOR_MAX_CONTEXTS:1,COORDINATOR_MAX_SCOPE_QUEUED:2}),[a,b]=f.scopes,g=gate();f.env.gates.set(a,g);const first=f.call(a,{op:'block',key:'one'});await g.waiting;const queued=f.call(a,{op:'increment',key:'two'});await new Promise(r=>setTimeout(r,0));
 assert.equal((await f.call(a,{op:'increment',key:'three'})).body.code,'COORDINATOR_BACKPRESSURE');assert.equal((await f.call(b,{op:'increment',key:'other'})).status,503);assert.equal(await scopedStorage(f.root,b).get('name'),undefined);
 g.release();await first;await queued;assert.equal(await scopedStorage(f.root,a).get('counter'),2);
 const bytes=fixture({COORDINATOR_MAX_QUEUED_BYTES:100});assert.equal((await bytes.call(bytes.scopes[0],{op:'increment',key:'x',payload:'x'.repeat(100)})).status,503);assert.equal(bytes.root.map.size,0);
});

test('virtual alarms isolate owners and atomically rollback scoped alarm mutations',async()=>{
 const f=fixture(),[a,b]=f.scopes;await f.call(a,{op:'setAlarm',deadline:5000});await f.call(b,{op:'setAlarm',deadline:3000});assert.equal(f.root.alarm,3000);
 await f.call(a,{op:'deleteAlarm'});assert.equal(f.root.alarm,3000);assert.equal(await f.service.scopedStorage(a).getAlarm(),null);assert.equal(await f.service.scopedStorage(b).getAlarm(),3000);
 await assert.rejects(f.service.scopedStorage(b).transaction(async txn=>{await txn.put('active',1);await txn.setAlarm(9000);throw Error('rollback');}));assert.equal(await f.service.scopedStorage(b).get('active'),undefined);assert.equal(await f.service.scopedStorage(b).getAlarm(),3000);assert.equal(f.root.alarm,3000);
});

test('alarm delivery schedules rediscovery first and newer delete/set revision survives old completion',async()=>{
 const f=fixture(),[a,b]=f.scopes,g=gate(),seen=[];await f.call(a,{op:'setAlarm',deadline:50});await f.call(b,{op:'setAlarm',deadline:50});
 f.env.onAlarm=async(scope,store)=>{seen.push(scope);assert.ok(f.root.alarm>100);if(scope===a){await store.deleteAlarm();await store.setAlarm(5000);g.enter();await g.promise;}};
 const delivery=f.service.alarm();await g.waiting;await new Promise(r=>setTimeout(r,0));assert.ok(seen.includes(b));assert.equal(await f.service.scopedStorage(b).get('fires'),1);
 g.release();await delivery;assert.equal(await f.service.scopedStorage(a).getAlarm(),5000);assert.equal(await f.service.scopedStorage(b).getAlarm(),null);assert.equal(f.root.alarm,5000);
});

test('failed earliest alarm cannot starve later due scope and restart rediscovers retained due',async()=>{
 const f=fixture({COORDINATOR_ALARM_BATCH:1}),[a,b]=f.scopes;await f.call(a,{op:'setAlarm',deadline:10});await f.call(b,{op:'setAlarm',deadline:20});f.env.onAlarm=async scope=>{if(scope===a)throw Error('unavailable peer');};
 await f.service.alarm();assert.equal(await f.service.scopedStorage(a).getAlarm(),10);await f.service.alarm();assert.equal(await f.service.scopedStorage(b).get('fires'),1);
 f.restart();f.env.onAlarm=null;await f.service.alarm();assert.equal(await f.service.scopedStorage(a).get('fires'),1);assert.equal(f.root.alarm,null);
});

test('busy context retains due alarm while unrelated due scope progresses',async()=>{
 const f=fixture(),[a,b]=f.scopes;await f.call(a,{op:'setAlarm',deadline:50});await f.call(b,{op:'setAlarm',deadline:50});const g=gate();f.env.gates.set(a,g);const work=f.call(a,{op:'block',key:'held'});await g.waiting;
 await f.service.alarm();assert.equal(await f.service.scopedStorage(a).getAlarm(),50);assert.equal(await f.service.scopedStorage(b).get('fires'),1);g.release();await work;await f.service.alarm();assert.equal(await f.service.scopedStorage(a).get('fires'),1);
});

test('alarm handler yields after bounded slice while stalled task keeps refs and later alarm dispatches healthy scopes',async()=>{
 const f=fixture({COORDINATOR_ALARM_BATCH:1,COORDINATOR_ALARM_SLICE_MS:10}),[a,b]=f.scopes,g=gate();
 await f.call(a,{op:'setAlarm',deadline:10});await f.call(b,{op:'setAlarm',deadline:20});
 f.env.onAlarm=async scope=>{if(scope===a){g.enter();await g.promise;}};
 const first=f.service.alarm();await g.waiting;await Promise.race([first,new Promise((_,reject)=>setTimeout(()=>reject(Error('alarm handler blocked on remote work')),500))]);
 assert.equal(f.service.contexts.get(a).refs,1);assert.ok(f.root.alarm>100);await f.service.alarm();assert.equal(await f.service.scopedStorage(b).get('fires'),1);assert.equal(f.service.contexts.get(a).refs,1);
 assert.ok(f.background.length>=2);g.release();await Promise.all(f.background);assert.equal(f.service.contexts.has(a),false);assert.equal(await f.service.scopedStorage(a).getAlarm(),null);
});

test('unsupported mutable pool size is rejected instead of silently rerouting durable owners',()=>{
 assert.throws(()=>fixture({COORDINATOR_POOL_SIZE:32}),/routing version/);
});

test('physical storage transactions are entered serially across logical scopes and root KV cannot join an open savepoint',async()=>{
 const f=fixture(),[a,b]=f.scopes,g=gate(),original=f.root.transaction.bind(f.root);let entered=0,maxEntered=0,bStarted=false,bRead=false;
 f.root.transaction=fn=>{entered++;maxEntered=Math.max(maxEntered,entered);return original(fn).finally(()=>entered--);};
 const sa=f.service.scopedStorage(a),sb=f.service.scopedStorage(b);
 const first=sa.transaction(async txn=>{await txn.put('value','A');g.enter();await g.promise;await txn.setAlarm(5000);});await g.waiting;
 const second=sb.transaction(async txn=>{bStarted=true;await writeRecord(txn,'value',{payload:'B'.repeat(180000)});});
 const read=sb.get('value').then(value=>{bRead=true;return value;});await new Promise(r=>setTimeout(r,0));
 assert.equal(maxEntered,1,'must queue BEFORE entering runtime transaction/input gate');assert.equal(bStarted,false);assert.equal(bRead,false);
 g.release();await Promise.all([first,second,read]);assert.equal(maxEntered,1);assert.deepEqual(await readRecord(sb,'value'),{payload:'B'.repeat(180000)});assert.equal(await sa.get('value'),'A');
});
