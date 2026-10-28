import {queryWithBackgroundTurns} from './pending-query-fixture.js';
import {storage} from './pool-storage-fixture.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import Worker, { Coordinator, DataShard, PlanAllocator as Mediator, PlanBucket, QueryCursor } from '../src/index.js';

// Deterministic scheduling, durable actor-state replacement, no simulated alarms.

function cluster(options={}) {
 const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'16384',TABLET_MERGE_BYTES:'1024',...options};let hook=async()=>{},sequence=0;const spaces=[];
 function namespace(Type){const states=new Map(),actors=new Map();const ns={idFromName:x=>x,get(name){if(!states.has(name))states.set(name,{storage:storage(),id:name});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));if(Type===Mediator)actors.get(name).now=()=>env.NOW?.()??Date.now();return {fetch:async(url,init)=>{const input=JSON.parse(init.body);await hook('before',Type,name,input);const response=await actors.get(name).fetch(new Request(url,init));await hook('after',Type,name,input,response);return response;}};},reset(){actors.clear();}};spaces.push(ns);return ns;}
 env.SHARDS=namespace(DataShard);env.ALLOCATOR=namespace(Mediator);env.MEDIATORS=namespace(PlanBucket);env.COORDINATOR=namespace(Coordinator);env.CURSORS=namespace(QueryCursor);
 const rpc=async(ns,name,input)=>{const response=await ns.get(name).fetch('https://internal',{method:'POST',body:JSON.stringify(input)});return {status:response.status,...await response.json()};};
 return {env,hook(fn){hook=fn;},reset(){spaces.forEach(s=>s.reset());},shard:(name,input)=>rpc(env.SHARDS,name,input),mediator:input=>rpc(env.ALLOCATOR,'global',input),
  async post(path,input){const response=await Worker.fetch(new Request('http://127.0.0.1'+path,{method:'POST',body:JSON.stringify(input)}),env);return {status:response.status,...await response.json()};},
  query(sql,extra={}){return queryWithBackgroundTurns(env,{sql,idempotencyKey:`pagination_${++sequence}`,...extra});}

 };
}

async function seed(db,count=48){
 const values=Array.from({length:count},(_,id)=>`(${id},'${id<24?'same':'z'}',${id%3},'${'x'.repeat(40)}')`);
 const result=await db.query(`CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT,n INTEGER,payload TEXT); INSERT INTO a(id,tag,n,payload) VALUES ${values.join(',')}; CREATE INDEX tags ON a(tag)`);
 assert.equal(result.status,200,JSON.stringify(result));return result;
}
function trace(db){const calls=[];db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard&&!name.startsWith('catalog/'))calls.push({name,input});});return calls;}
for(const desc of [false,true])test(`LIMIT physical ${desc?'DESC':'ASC'} skips residuals/tombstones and stops early`,async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'800',TABLET_MERGE_BYTES:'100'});await seed(db);
 assert.equal((await db.query('DELETE FROM a WHERE id=3; DELETE FROM a WHERE id=45')).status,200);
 const calls=trace(db),result=await db.query(`SELECT id FROM a WHERE n=0 ORDER BY id ${desc?'DESC':'ASC'} LIMIT 3`);
 assert.equal(result.status,200,JSON.stringify(result));assert.deepEqual(result.results[0].rows,desc?[{id:42},{id:39},{id:36}]:[{id:0},{id:6},{id:9}]);
 const scans=calls.filter(c=>c.input.op==='scan');assert.ok(scans.length);assert.ok(scans.every(c=>c.input.pageSize<=3&&c.input.reverse===desc));assert.ok(new Set(scans.map(c=>c.name)).size<result.routing[0].tablets.length);
});
test('LIMIT zero validates projection, predicate and order without base/index IO',async()=>{
 const db=cluster();await seed(db);const calls=trace(db);
 for(const sql of ['SELECT missing FROM a LIMIT 0','SELECT id FROM a WHERE missing=1 LIMIT 0','SELECT id FROM a ORDER BY missing LIMIT 0'])assert.equal((await db.query(sql)).status,400,sql);
 const valid=await db.query("SELECT id FROM a WHERE tag='same' ORDER BY tag LIMIT 0");assert.equal(valid.status,200);assert.deepEqual(valid.results[0].rows,[]);assert.equal(calls.length,0);
});
for(const desc of [false,true])test(`index LIMIT ${desc?'DESC':'ASC'} duplicates across partitions retain tuple order`,async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'800',TABLET_MERGE_BYTES:'100'});await seed(db);const calls=trace(db);
 const result=await db.query(`SELECT id,tag FROM a WHERE tag='same' AND n=0 ORDER BY tag ${desc?'DESC':'ASC'} LIMIT 5`);assert.equal(result.status,200,JSON.stringify(result));
 assert.deepEqual(result.results[0].rows.map(r=>r.id),desc?[21,18,15,12,9]:[0,3,6,9,12]);
 const scans=calls.filter(c=>c.input.op==='scan');assert.ok(scans.length);assert.ok(scans.every(c=>c.name.startsWith('index/')&&c.input.pageSize<=5&&c.input.reverse===desc));assert.ok(calls.some(c=>c.input.op==='getMany'));
});
test('composite leading-column LIMIT uses physical tuple order without full scan',async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'400',TABLET_MERGE_BYTES:'100'}),values=[];for(let tenant=0;tenant<4;tenant++)for(let id=0;id<8;id++)values.push(`(${tenant},${id},'${'x'.repeat(40)}')`);
 assert.equal((await db.query(`CREATE TABLE a(tenant INTEGER,id INTEGER,payload TEXT,PRIMARY KEY(tenant,id)); INSERT INTO a(tenant,id,payload) VALUES ${values.join(',')}`)).status,200);
 const calls=trace(db),r=await db.query('SELECT tenant,id FROM a ORDER BY tenant DESC LIMIT 3');assert.equal(r.status,200,JSON.stringify(r));assert.deepEqual(r.results[0].rows,[{tenant:3,id:7},{tenant:3,id:6},{tenant:3,id:5}]);assert.ok(calls.filter(c=>c.input.op==='scan').every(c=>c.input.pageSize<=3&&c.input.reverse));
});
test('public index cursor keeps duplicate tuples across split/drop/recreate, restart and lost response',async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'800',TABLET_MERGE_BYTES:'100'});const initial=await seed(db);assert.ok(initial.routing[0].indexes[0].tablets.length>1);
 const start=await db.post('/cursor',{action:'start',idempotencyKey:'stable',sql:"SELECT id,tag FROM a WHERE tag='same' ORDER BY tag DESC",pageSize:3});assert.equal(start.status,200,JSON.stringify(start));
 const page=token=>db.post('/cursor',{action:'page',id:start.id,token});
 const first=await page(start.token);assert.equal(first.status,200,JSON.stringify(first));assert.deepEqual(first.rows.map(r=>r.id),[23,22,21]);
 assert.equal((await db.query("DROP INDEX tags ON a; DELETE FROM a WHERE id>=6; UPDATE a SET tag='changed' WHERE id=1; CREATE INDEX tags ON a(tag)")).status,200);db.reset();
 assert.deepEqual(await page(start.token),first); // lost response/replay across actor replacement
 let token=first.nextToken,rows=[...first.rows],count=0;
 await db.post('/cursor',{action:'ack',id:start.id,throughSeq:first.page});assert.equal((await page(start.token)).status,410);
 while(token){const current=await page(token);assert.equal(current.status,200,JSON.stringify(current));rows.push(...current.rows);await db.post('/cursor',{action:'ack',id:start.id,throughSeq:current.page});token=current.nextToken;assert.ok(++count<30);}
 assert.deepEqual(rows.map(r=>r.id),Array.from({length:24},(_,i)=>23-i));assert.ok(rows.every(r=>r.tag==='same'));
 assert.equal((await db.post('/cursor',{action:'close',id:start.id})).status,200);
});
test('public cursor lease expiry refuses cached page and future continuation',async()=>{
 let now=10000;const db=cluster({NOW:()=>now});await seed(db,8);
 const start=await db.post('/cursor',{action:'start',idempotencyKey:'expires',sql:'SELECT id FROM a ORDER BY id',pageSize:2,ttlMs:100});assert.equal(start.status,200,JSON.stringify(start));
 const first=await db.post('/cursor',{action:'page',id:start.id,token:start.token});assert.equal(first.status,200);now+=101;
 for(const token of [start.token,first.nextToken])assert.equal((await db.post('/cursor',{action:'page',id:start.id,token})).status,410);
});
