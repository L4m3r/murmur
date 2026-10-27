import test from 'node:test';
import assert from 'node:assert/strict';
import { DataShard } from '../src/datashard.js';
import { Mediator } from '../src/mediator.js';
import { encodeKey } from '../src/keys.js';

// Independent durable-state model: reset destroys actor instances, not storage.
// Real process-loss durability is tested by the celld integration suite.
function storage() {
  const data = new Map();
  const api = {
    get: async k => structuredClone(data.get(k)),
    put: async (k,v) => { data.set(k, structuredClone(v)); },
    delete: async k => data.delete(k),
    list: async ({prefix='',start,end,limit,reverse=false}={}) => {
      let entries=[...data].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);
      if(reverse) entries.reverse(); if(limit!==undefined) entries=entries.slice(0,limit);
      return new Map(entries.map(([k,v])=>[k,structuredClone(v)]));
    },
    transaction: async fn => { const old=structuredClone(data);try{return await fn(api);}catch(e){data.clear();for(const [k,v]of old)data.set(k,v);throw e;} }
  }; return api;
}
async function call(actor,input) { const response=await actor.fetch(new Request('https://test/internal',{method:'POST',body:JSON.stringify(input)}));return {status:response.status,body:await response.json()}; }
function model() {
  const mediatorState={storage:storage()},states=new Map(); let mediator=new Mediator(mediatorState),shards=new Map();
  let loseAck=false;
  const env={MEDIATOR:{idFromName:x=>x,get:()=>({fetch:async(url,init)=>{if(loseAck&&JSON.parse(init.body).op==='applied'){loseAck=false;return Response.json({error:'lost apply acknowledgment'},{status:503});}return mediator.fetch(new Request(url,init));}})}};
  const shard=name=>{if(!states.has(name))states.set(name,{storage:storage()});if(!shards.has(name))shards.set(name,new DataShard(states.get(name),env));return shards.get(name);};
  return {shard, loseNextApplyAck(){loseAck=true;}, mediator:input=>call(mediator,input),reset(){mediator=new Mediator(mediatorState);shards=new Map();},
    async prepare(name,id,changes,reads=[],snapshot=0,extra={}){return call(shard(name),{op:'prepare',id,owner:'coordinator:0',snapshot,changes,reads,...extra});},
    async plan(id,votes){const result=await call(mediator,{op:'plan',id,owner:'coordinator:0',participants:votes});assert.equal(result.status,200,JSON.stringify(result));return result.body;},
    async apply(name,id,step,vote){const result=await call(shard(name),{op:'apply',id,step,shard:name,vote});assert.equal(result.status,200,JSON.stringify(result));},
    async read(name,snapshot,extra={}){const result=await call(shard(name),{op:'scan',snapshot,...extra});assert.equal(result.status,200,JSON.stringify(result));return result.body.rows;}
  };
}
const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const delta=(key,n=key)=>({key,row:{id:key,n}});

test('MVCC independent: disjoint same-tablet reservations coexist; write skew cannot prepare',async()=>{
  const db=model();
  assert.equal((await db.prepare('base','a',[delta(1)],[point(2)])).status,200);
  assert.equal((await db.prepare('base','b',[delta(2)],[point(1)])).status,409);
  assert.equal((await db.prepare('base','c',[delta(3)],[point(3)])).status,200);
  db.reset();
  assert.equal((await db.prepare('base','d',[delta(2)],[])).status,409,'read reservation survives actor reset');
  assert.equal((await db.prepare('base','e',[delta(4)],[])).status,200,'unrelated keys remain available');
});

test('MVCC independent: absent unique prefix reserves all PK suffixes, including empty reads',async()=>{
  const db=model(),prefix=encodeKey(['same']);
  const claim={lower:prefix,upper:prefix+'~',lowerInclusive:true,upperInclusive:false};
  const first=await db.prepare('index','u1',[{key:['same',1],row:{value:'same',pk:1}}],[claim]);
  assert.equal(first.status,200);
  assert.equal((await db.prepare('index','u2',[{key:['same',999],row:{value:'same',pk:999}}],[claim])).status,409);
  assert.equal((await db.prepare('index','u3',[{key:['other',1],row:{value:'other',pk:1}}],[])).status,200);
});

test('MVCC independent: applied plan behind a hole stays invisible but rejects stale dependent writers',async()=>{
  const db=model();
  const a=await db.prepare('a','first',[delta(1)]),b=await db.prepare('b','second',[delta(2)]);
  assert.equal(a.status,200);assert.equal(b.status,200);
  const p1=await db.plan('first',[{name:'a',vote:a.body.vote}]);
  const p2=await db.plan('second',[{name:'b',vote:b.body.vote}]);
  await db.apply('b','second',p2.step,b.body.vote);
  db.reset();
  const status=await db.mediator({op:'snapshot'});
  assert.equal(status.body.snapshot,0);assert.equal(status.body.allocated,2);
  assert.deepEqual(await db.read('b',status.body.snapshot),[]);
  assert.equal((await db.prepare('b','stale',[delta(3)],[point(2)],0)).status,409);
  assert.equal((await db.prepare('b','unrelated',[delta(4)],[point(4)],0)).status,200);
  await db.apply('a','first',p1.step,a.body.vote);
  assert.equal((await db.mediator({op:'snapshot'})).body.snapshot,2);
  assert.deepEqual(await db.read('b',2),[{id:2,n:2}]);
});

test('MVCC independent: delete tombstones preserve old snapshots; replay cannot resurrect',async()=>{
  const db=model();let snapshot=0;const plans=[];
  for(const [id,changes]of [['insert',[delta(7,10)]],['update',[delta(7,20)]],['delete',[{key:7,row:null}]]]){
    const vote=await db.prepare('base',id,changes,[point(7)],snapshot);assert.equal(vote.status,200);
    const plan=await db.plan(id,[{name:'base',vote:vote.body.vote}]);
    await db.apply('base',id,plan.step,vote.body.vote);snapshot=plan.step;plans.push({id,step:plan.step,vote:vote.body.vote});
  }
  db.reset();
  assert.deepEqual(await db.read('base',1),[{id:7,n:10}]);
  assert.deepEqual(await db.read('base',2),[{id:7,n:20}]);
  assert.deepEqual(await db.read('base',3),[]);
  await db.apply('base',plans[0].id,plans[0].step,plans[0].vote);
  assert.deepEqual(await db.read('base',3),[]);
});

test('MVCC independent: abort durably fences delayed prepare and releases its predicate reservation',async()=>{
  const db=model();assert.equal((await db.prepare('base','dead',[delta(1)],[point(2)])).status,200);
  assert.equal((await call(db.shard('base'),{op:'abort',id:'dead'})).status,200);db.reset();
  assert.equal((await db.prepare('base','dead',[delta(1)],[point(2)])).status,409);
  assert.equal((await db.prepare('base','next',[delta(2)],[])).status,200);
});

test('MVCC independent: retirement is snapshot versioned, not a blanket historical-read rejection',async()=>{
  const db=model();
  const first=await db.prepare('old','seed',[delta(1)],[],0,{expectedEpoch:0,meta:{epoch:1,retired:false}});
  assert.equal(first.status,200);const p1=await db.plan('seed',[{name:'old',vote:first.body.vote}]);await db.apply('old','seed',p1.step,first.body.vote);
  const retired=await db.prepare('old','move',[],[],1,{expectedEpoch:1,meta:{epoch:2,retired:true}});
  assert.equal(retired.status,200);const p2=await db.plan('move',[{name:'old',vote:retired.body.vote}]);await db.apply('old','move',p2.step,retired.body.vote);db.reset();
  assert.deepEqual(await db.read('old',1,{epoch:1}),[{id:1,n:1}]);
  assert.equal((await call(db.shard('old'),{op:'scan',snapshot:2,epoch:1})).status,409);
  assert.equal((await db.prepare('old','stale-route',[delta(3)],[],1,{expectedEpoch:1})).status,409);
});


test('MVCC independent: durable append with lost apply ACK repairs on replay after actor reset',async()=>{
  const db=model();const prepared=await db.prepare('base','ack-loss',[delta(9)]);
  assert.equal(prepared.status,200);const plan=await db.plan('ack-loss',[{name:'base',vote:prepared.body.vote}]);
  db.loseNextApplyAck();
  assert.equal((await call(db.shard('base'),{op:'apply',id:'ack-loss',step:plan.step,shard:'base',vote:prepared.body.vote})).status,503);
  assert.equal((await db.mediator({op:'snapshot'})).body.snapshot,0);
  db.reset();await db.apply('base','ack-loss',plan.step,prepared.body.vote);
  assert.equal((await db.mediator({op:'snapshot'})).body.snapshot,plan.step);
  assert.deepEqual(await db.read('base',plan.step),[{id:9,n:9}]);
});

test('MVCC independent: immutable votes and manifests reject payload substitution',async()=>{
  const db=model();const prepared=await db.prepare('base','frozen',[delta(1)]);
  assert.equal(prepared.status,200);
  assert.equal((await db.prepare('base','frozen',[delta(2)])).status,409);
  const plan=await db.plan('frozen',[{name:'base',vote:prepared.body.vote}]);
  assert.equal((await db.mediator({op:'plan',id:'frozen',owner:'coordinator:0',participants:[{name:'base',vote:'different'}]})).status,409);
  assert.equal((await call(db.shard('base'),{op:'apply',id:'frozen',step:plan.step,shard:'base',vote:'different'})).status,503);
  assert.deepEqual(await db.read('base',plan.step),[]);
  await db.apply('base','frozen',plan.step,prepared.body.vote);
  assert.deepEqual(await db.read('base',plan.step),[{id:1,n:1}]);
});

import Worker, { Coordinator, shardFor } from '../src/index.js';
function workerModel() {
  const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'4096',TABLET_MERGE_BYTES:'1024'};
  const all=[];let hook=async()=>{},sequence=0;
  function namespace(Type){const states=new Map(),actors=new Map();const ns={idFromName:n=>n,get(name){if(!states.has(name))states.set(name,{storage:storage()});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return {fetch:async(url,init)=>{const input=JSON.parse(init.body);await hook('before',Type,name,input);const response=await actors.get(name).fetch(new Request(url,init));await hook('after',Type,name,input,response);return response;}};},reset(){actors.clear();}};all.push(ns);return ns;}
  env.SHARDS=namespace(DataShard);env.MEDIATOR=namespace(Mediator);env.COORDINATOR=namespace(Coordinator);
  return {env,hook(fn){hook=fn;},reset(){all.forEach(ns=>ns.reset());},async query(sql,extra={}){const response=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify({sql,idempotencyKey:`independent-${++sequence}`,...extra})}),env);return {status:response.status,body:await response.json()};}};
}
function ownerKeys(){const first='independent-owner-0';let n=1,second;do{second=`independent-owner-${n++}`;}while(shardFor(first)===shardFor(second));return [first,second];}
function deferred(){let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};}
async function deadline(promise){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Independent transaction did not make progress')),2000);})]);}finally{clearTimeout(timer);}}

test('MVCC Worker independent: another coordinator recovers abandoned unplanned reservations without alarms',async()=>{
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
  db.hook(async(phase,Type,name,input)=>{if(!held&&phase==='after'&&Type===DataShard&&input.op==='prepare'&&name.startsWith('base/')){held=true;prepared.resolve();await release.promise;}});
  const a=db.query('UPDATE a SET n=n+1 WHERE id=1',{idempotencyKey:first});await deadline(prepared.promise);
  let b;try{b=await deadline(db.query('UPDATE a SET n=n+1 WHERE id=2',{idempotencyKey:second}));assert.equal(b.status,200,JSON.stringify(b));}finally{release.resolve();}
  assert.equal((await a).status,200);
  assert.deepEqual((await db.query('SELECT * FROM a ORDER BY id')).body.results[0].rows,[{id:1,n:1},{id:2,n:1}]);
});

test('MVCC Worker independent: lost plan allocation response recovers same committed intent exactly once',async()=>{
  const db=workerModel();assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,0)')).status,200);
  let lost=false;db.hook(async(phase,Type,name,input)=>{if(!lost&&phase==='after'&&Type===Mediator&&input.op==='plan'){lost=true;throw new Error('Lost allocation response after persistence');}});
  const sql='UPDATE a SET n=n+1 WHERE id=1';
  assert.equal((await db.query(sql,{idempotencyKey:'lost-plan-response'})).status,503);db.reset();
  const replay=await db.query(sql,{idempotencyKey:'lost-plan-response'});assert.equal(replay.status,200,JSON.stringify(replay));
  assert.deepEqual(await db.query(sql,{idempotencyKey:'lost-plan-response'}),replay);
  assert.deepEqual((await db.query('SELECT * FROM a')).body.results[0].rows,[{id:1,n:1}]);
});

test('MVCC Worker independent: concurrent cross-row read/write transactions have a serializable result',async()=>{
  const db=workerModel(),[first,second]=ownerKeys(),ready=deferred();const ids=new Set();
  assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,1),(2,1)')).status,200);
  db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard&&input.op==='prepare'&&ids.size<2){ids.add(input.id);if(ids.size===2)ready.resolve();await ready.promise;}});
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
  db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard&&input.op==='prepare'&&ids.size<2){ids.add(input.id);if(ids.size===2)ready.resolve();await ready.promise;}});
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
  db.hook(async(phase,Type,name,input)=>{if(!held&&phase==='before'&&Type===DataShard&&input.op==='prepare'){held=true;waiting.resolve();await release.promise;}});
  const reader=db.query(`SELECT n FROM a WHERE tag='${phantom?'new':'existing'}'; UPDATE a SET n=3 WHERE id=1`,{idempotencyKey:first});
  await deadline(waiting.promise);
  try{
    const writer=await deadline(db.query(phantom?"INSERT INTO a(id,tag,n) VALUES (3,'new',5)":"UPDATE a SET n=5 WHERE id=2",{idempotencyKey:second}));
    assert.equal(writer.status,200,JSON.stringify(writer));
  }finally{release.resolve();}
  const result=await reader;assert.equal(result.status,200,JSON.stringify(result));
  assert.deepEqual(result.body.results[0].rows,[{n:5}]);
});
