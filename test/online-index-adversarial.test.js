import {storage} from './pool-storage-fixture.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import { SchemaManager } from '../src/schema-manager.js';
import Worker, { Coordinator, DataShard, PlanAllocator as Mediator, PlanBucket } from '../src/index.js';
import { encodeKey } from '../src/keys.js';

// Deterministic scheduling, durable actor-state replacement, no simulated alarms.

const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false};
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
async function deadline(promise){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Visibility progress deadline exceeded')),2500);})]);}finally{clearTimeout(timer);}}
function cluster() {
 const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'16384',TABLET_MERGE_BYTES:'1024'};let hook=async()=>{},sequence=0;const spaces=[];
 function namespace(Type){const states=new Map(),actors=new Map();const ns={idFromName:x=>x,get(name){if(!states.has(name))states.set(name,{storage:storage(),id:name});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return {fetch:async(url,init)=>{const input=JSON.parse(init.body);await hook('before',Type,name,input);const response=await actors.get(name).fetch(new Request(url,init));await hook('after',Type,name,input,response);return response;}};},reset(){actors.clear();}};spaces.push(ns);return ns;}
 env.SCHEMA=namespace(SchemaManager);env.SHARDS=namespace(DataShard);env.ALLOCATOR=namespace(Mediator);env.MEDIATORS=namespace(PlanBucket);env.COORDINATOR=namespace(Coordinator);
 const rpc=async(ns,name,input)=>{const response=await ns.get(name).fetch('https://internal',{method:'POST',body:JSON.stringify(input)});return {status:response.status,...await response.json()};};
 return {env,hook(fn){hook=fn;},reset(){spaces.forEach(s=>s.reset());},shard:(name,input)=>rpc(env.SHARDS,name,input),mediator:input=>rpc(env.ALLOCATOR,'global',input),
  async post(path,input){const response=await Worker.fetch(new Request('http://127.0.0.1'+path,{method:'POST',body:JSON.stringify(input)}),env);return {status:response.status,...await response.json()};},
  async schema(sql,key){return this.post('/schema',{action:'start',sql,idempotencyKey:key});},
  query(sql,extra={}){return this.post('/query',{sql,idempotencyKey:`visibility_${++sequence}`,...extra});},
 };
}


async function finish(db,id){let result;for(let i=0;i<100;i++){result=await db.post('/schema',{action:'resume',id});assert.ok([200,202].includes(result.status),JSON.stringify(result));if(['ready','dropped','failed','cancelled'].includes(result.job.state))return result;}assert.fail('Schema job failed to reach terminal state');}
async function jobId(key){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode('0:'+key))),b=>b.toString(16).padStart(2,'0')).join('');}

test('online index independent: captured batch conflicts with concurrent update/delete/insert and never resurrects old entries',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'old'),(2,'deleted'),(3,'stable')")).status,200);
 const started=await db.schema('CREATE INDEX lookup ON a(tag)','build-mutate');assert.equal(started.status,202,JSON.stringify(started));
 const read=await db.query("SELECT id FROM a WHERE tag='stable'");assert.deepEqual(read.results[0].rows,[{id:3}]);assert.notEqual(read.plan.type,'index-range','partial index must be hidden from optimizer');
 assert.equal((await db.query("UPDATE a SET tag='new' WHERE id=1; DELETE FROM a WHERE id=2; INSERT INTO a(id,tag) VALUES (4,'fresh')")).status,200);db.reset();
 const result=await finish(db,started.job.id);assert.equal(result.job.state,'ready',JSON.stringify(result));
 for(const [tag,rows]of [['old',[]],['deleted',[]],['new',[{id:1}]],['stable',[{id:3}]],['fresh',[{id:4}]]]){const read=await db.query(`SELECT id FROM a WHERE tag='${tag}'`);assert.equal(read.status,200);assert.equal(read.plan.type,'index-range');assert.deepEqual(read.results[0].rows,rows);}
});

test('online index independent: preexisting duplicates fail UNIQUE build and cleanup never deletes base rows',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'same'),(2,'same')")).status,200);
 const started=await db.schema('CREATE UNIQUE INDEX lookup ON a(tag)','build-duplicate');assert.ok([200,202].includes(started.status),JSON.stringify(started));
 const done=await finish(db,started.job.id);assert.equal(done.job.state,'failed');
 assert.deepEqual((await db.query('SELECT * FROM a ORDER BY id')).results[0].rows,[{id:1,tag:'same'},{id:2,tag:'same'}]);
 const schema=await db.shard('catalog/a',{op:'get',key:'schema',snapshot:(await db.mediator({op:'snapshot'})).snapshot});assert.equal(schema.row.indexes.length,0);
 assert.equal((await db.mediator({op:'snapshot'})).pins,0);
});

test('online index independent: BUILDING UNIQUE accepts duplicates and complete validation fails asynchronously',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'same')")).status,200);
 const started=await db.schema('CREATE UNIQUE INDEX lookup ON a(tag)','build-race');assert.equal(started.status,202,JSON.stringify(started));
 assert.equal((await db.query("INSERT INTO a(id,tag) VALUES (2,'same')")).status,200);
 const done=await finish(db,started.job.id);assert.equal(done.job.state,'failed');
 assert.deepEqual((await db.query("SELECT id FROM a WHERE tag='same' ORDER BY id")).results[0].rows,[{id:1},{id:2}]);
});

test('online index independent: lost unpin response during cutover cannot reuse a closed planning snapshot after GC',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'old')")).status,200);
 let once=false;db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='after'&&Type===Mediator&&input.op==='unpin'&&input.id.startsWith('sj-')&&!input.id.endsWith('-B')){once=true;throw new Error('Lost unpin response after durable close');}});
 const started=await db.schema('CREATE INDEX lookup ON a(tag)','cutover-unpin');assert.equal(started.status,503);assert.equal(once,true);db.reset();
 const before=await db.mediator({op:'compact',retainSteps:0});assert.ok(before.retentionFloor>=2);
 const done=await finish(db,await jobId('cutover-unpin'));assert.equal(done.job.state,'ready',JSON.stringify(done));
 assert.deepEqual((await db.query("SELECT id FROM a WHERE tag='old'")).results[0].rows,[{id:1}]);assert.equal((await db.mediator({op:'snapshot'})).pins,0);
});

test('online index independent: failed install still releases its pin after lost cleanup response and restart',async()=>{
 const db=cluster();assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT)')).status,200);
 let once=false;db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='before'&&Type===Mediator&&input.op==='unpin'&&input.id.startsWith('sj-')){once=true;throw new Error('Cleanup transport failed before unpin');}});
 const started=await db.schema('CREATE INDEX lookup ON a(missing)','install-failure');assert.equal(started.status,503);db.reset();
 const done=await finish(db,await jobId('install-failure'));assert.equal(done.job.state,'failed');assert.equal((await db.mediator({op:'snapshot'})).pins,0);
});

test('online index independent: lost committed batch receipt replays exact batch before cursor advances',async()=>{
 const db=cluster();const rows=Array.from({length:100},(_,id)=>`(${id},'v${id}')`).join(',');
 assert.equal((await db.query(`CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES ${rows}`)).status,200);
 const started=await db.schema('CREATE INDEX lookup ON a(tag)','batch-receipt');assert.equal(started.status,202);
 let once=false;db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='after'&&Type===Coordinator&&input.op==='applyTargets'&&input.targets.some(t=>t.name.startsWith('index/')&&t.changes.length)){once=true;throw new Error('Lost successful backfill receipt');}});
 const interrupted=await db.post('/schema',{action:'resume',id:started.job.id});assert.equal(interrupted.status,503);assert.equal(once,true);db.reset();
 const done=await finish(db,started.job.id);assert.equal(done.job.state,'ready');assert.equal(done.job.keys,100);
 for(const id of [0,63,64,99])assert.deepEqual((await db.query(`SELECT id FROM a WHERE tag='v${id}'`)).results[0].rows,[{id}]);
 assert.equal((await db.mediator({op:'snapshot'})).pins,0);
});

test('online index independent: stale job after DROP/recreate cannot publish or cleanup the replacement generation',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'old')")).status,200);
 const old=await db.schema('CREATE INDEX lookup ON a(tag)','old-job');assert.equal(old.status,202);
 assert.equal((await db.query("DROP INDEX lookup ON a; UPDATE a SET tag='new' WHERE id=1; CREATE INDEX lookup ON a(tag)")).status,200);db.reset();
 const done=await finish(db,old.job.id);assert.equal(done.job.state,'failed');
 const read=await db.query("SELECT id FROM a WHERE tag='new'");assert.equal(read.plan.type,'index-range');assert.deepEqual(read.results[0].rows,[{id:1}]);
 assert.deepEqual((await db.query("SELECT id FROM a WHERE tag='old'")).results[0].rows,[]);
 assert.equal((await db.mediator({op:'snapshot'})).pins,0);
});

test('online index independent: cancellation settles in-flight batch then removes only its generation and releases pins',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'old')")).status,200);
 const started=await db.schema('CREATE INDEX lookup ON a(tag)','cancel-job');assert.equal(started.status,202);
 const cancel=await db.post('/schema',{action:'cancel',id:started.job.id});assert.ok([200,202].includes(cancel.status));db.reset();
 const done=await finish(db,started.job.id);assert.equal(done.job.state,'cancelled');
 assert.deepEqual((await db.query('SELECT * FROM a')).results[0].rows,[{id:1,tag:'old'}]);
 const schema=await db.shard('catalog/a',{op:'get',key:'schema',snapshot:(await db.mediator({op:'snapshot'})).snapshot});assert.equal(schema.row.indexes.length,0);assert.equal((await db.mediator({op:'snapshot'})).pins,0);
});

test('online index independent: unknown batch outcome keeps request epoch alive until receipt and cursor are durable',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'old')")).status,200);
 const started=await db.schema('CREATE INDEX lookup ON a(tag)','epoch-receipt');assert.equal(started.status,202);
 let once=false;db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='after'&&Type===Coordinator&&input.op==='applyTargets'&&input.targets.some(t=>t.name.startsWith('index/')&&t.changes.length)){once=true;throw new Error('Unknown committed batch outcome');}});
 assert.equal((await db.post('/schema',{action:'resume',id:started.job.id})).status,503);
 assert.equal((await db.mediator({op:'rotateEpoch'})).currentEpoch,1);
 const retirement=await db.mediator({op:'retireEpochs',before:1});assert.equal(retirement.status,409);assert.equal(retirement.code,'EPOCH_BUSY');assert.ok(retirement.pins>0);
 db.reset();const done=await finish(db,started.job.id);assert.equal(done.job.state,'ready');assert.equal(done.job.keys,1);
 assert.equal((await db.mediator({op:'retireEpochs',before:1})).status,200);
 assert.deepEqual((await db.query("SELECT id FROM a WHERE tag='old'",{requestEpoch:1})).results[0].rows,[{id:1}]);
});

test('online index independent: concurrent duplicates during BUILDING commit but validation rejects the index',async()=>{
 const db=cluster();assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT)')).status,200);
 const started=await db.schema('CREATE UNIQUE INDEX lookup ON a(tag)','new-duplicates');assert.equal(started.status,202);
 const results=await Promise.all([db.query("INSERT INTO a(id,tag) VALUES (1,'same')"),db.query("INSERT INTO a(id,tag) VALUES (2,'same')")]);assert.deepEqual(results.map(r=>r.status).sort(),[200,200]);
 const done=await finish(db,started.job.id);assert.equal(done.job.state,'failed');assert.equal((await db.query("SELECT id FROM a WHERE tag='same'")).results[0].rows.length,2);
});

test('online index independent: oversized tuple-key page is subdivided without cursor gaps or duplicate accounting',async()=>{
 const db=cluster();db.env.TABLET_SPLIT_BYTES='1000000';
 const entries=Array.from({length:50},(_,i)=>({id:'x'.repeat(390)+String(i).padStart(3,'0'),n:i}));
 assert.equal((await db.query(`CREATE TABLE a(id TEXT PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES ${entries.map(r=>`('${r.id}',${r.n})`).join(',')}`)).status,200);
 const started=await db.schema('CREATE INDEX lookup ON a(n)','large-keys');assert.equal(started.status,202);
 const done=await finish(db,started.job.id);assert.equal(done.job.state,'ready',JSON.stringify(done));assert.equal(done.job.keys,50);assert.ok(done.job.pages>=2);
 for(const n of [0,15,16,31,32,49])assert.deepEqual((await db.query(`SELECT id FROM a WHERE n=${n}`)).results[0].rows,[{id:entries[n].id}]);
});

test('online index independent: concurrent duplicates after VALIDATING fence cannot both commit',async()=>{
 const db=cluster();assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT)')).status,200);
 const started=await db.schema('CREATE UNIQUE INDEX lookup ON a(tag)','validating-race');assert.equal(started.status,202);
 const installed=deferred(),release=deferred();let once=false;
 db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='after'&&Type===Coordinator&&input.op==='applyTargets'&&input.targets.some(t=>t.name==='catalog/a'&&t.changes.some(c=>c.row?.indexes?.some(i=>i.state==='validating')))){once=true;installed.resolve();await release.promise;}});
 const progress=(async()=>{for(let n=0;n<20&&!once;n++)await db.post('/schema',{action:'resume',id:started.job.id});})();
 await deadline(installed.promise);
 try{
  const results=await Promise.all([db.query("INSERT INTO a(id,tag) VALUES (1,'same')"),db.query("INSERT INTO a(id,tag) VALUES (2,'same')")]);
  assert.deepEqual(results.map(r=>r.status).sort(),[200,400]);
  const read=await db.query("SELECT id FROM a WHERE tag='same'");assert.notEqual(read.plan.type,'index-range');assert.equal(read.results[0].rows.length,1);
 }finally{release.resolve();await progress;}
 const done=await finish(db,started.job.id);assert.equal(done.job.state,'ready');assert.equal((await db.query("SELECT id FROM a WHERE tag='same'")).results[0].rows.length,1);
});

test('online index independent: Worker DROP schema job preserves base and returns dropped',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES (1,'keep'); CREATE INDEX lookup ON a(tag)")).status,200);
 const start=await db.schema('DROP INDEX lookup ON a','drop-async');assert.ok([200,202].includes(start.status),JSON.stringify(start));
 const done=await finish(db,start.job.id);assert.equal(done.job.state,'dropped');assert.deepEqual((await db.query('SELECT * FROM a')).results[0].rows,[{id:1,tag:'keep'}]);
 assert.equal((await db.query("SELECT id FROM a WHERE tag='keep'")).routing[0].indexes.length,0);
});

test('table generation independent: atomic DROP/recreate preserves old snapshot and never aliases row/index storage',async()=>{
 const db=cluster();const old=await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES(1,'old'); CREATE INDEX lookup ON a(tag)");assert.equal(old.status,200,JSON.stringify(old));
 const recreated=await db.query("DROP TABLE a; CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); INSERT INTO a(id,tag) VALUES(1,'new'); CREATE INDEX lookup ON a(tag)");assert.equal(recreated.status,200,JSON.stringify(recreated));
 assert.deepEqual((await db.query("SELECT * FROM a WHERE tag='new'")).results[0].rows,[{id:1,tag:'new'}]);
 assert.deepEqual((await db.query("SELECT * FROM a WHERE tag='old'",{snapshot:old.version})).results[0].rows,[{id:1,tag:'old'}]);
 const schema=await db.shard('catalog/a',{op:'get',key:'schema',snapshot:(await db.mediator({op:'snapshot'})).snapshot});assert.equal(schema.row.generation,1);assert.equal(schema.row.storageName,'a~g1');
 assert.equal((await db.query('DROP TABLE a; CREATE TABLE a(id INTEGER PRIMARY KEY); INSERT INTO a(id) VALUES(NULL)')).status,400);
 assert.deepEqual((await db.query('SELECT * FROM a')).results[0].rows,[{id:1,tag:'new'}]);
});
test('table generation independent: Worker asynchronous DROP TABLE retains historical routes',async()=>{
 const db=cluster();const old=await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY); INSERT INTO a(id) VALUES(7)");assert.equal(old.status,200);
 const start=await db.schema('DROP TABLE a','drop-table-job');assert.ok([200,202].includes(start.status),JSON.stringify(start));const done=await finish(db,start.job.id);assert.equal(done.job.state,'dropped');
 assert.equal((await db.query('SELECT * FROM a')).status,400);assert.deepEqual((await db.query('SELECT * FROM a',{snapshot:old.version})).results[0].rows,[{id:7}]);
 assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY); INSERT INTO a(id) VALUES(8)')).status,200);
 assert.deepEqual((await db.query('SELECT * FROM a')).results[0].rows,[{id:8}]);
});
