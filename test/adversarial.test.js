import test from 'node:test';
import assert from 'node:assert/strict';
import Worker, {Coordinator, Catalog, RowShard, IndexShard, LockManager, shardFor} from '../src/index.js';

// Deliberately in-memory scheduling model, not a multiprocess/durability claim.
const clone = structuredClone;
function storage() {
  const data = new Map();
  const api = {get:async k=>clone(data.get(k)),put:async(k,v)=>data.set(k,clone(v)),delete:async k=>data.delete(k),list:async({prefix='' }={})=>new Map([...data].filter(([k])=>k.startsWith(prefix)).map(([k,v])=>[k,clone(v)]))};
  api.transaction=async fn=>{ const saved=clone(data); try{return await fn(api);}catch(e){data.clear();for(const [k,v]of saved)data.set(k,v);throw e;} };
  return api;
}
const req = body=>new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify(body)});
async function invoke(actor,body){const r=await actor.fetch(req(body));return {status:r.status,...await r.json()};}
function deferred(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};}
function cluster(){
  const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'512',TABLET_MERGE_BYTES:'128'};
  const hooks=[];
  function ns(Type,kind){const actors=new Map(),states=new Map();return {idFromName:n=>n,state(name){if(!states.has(name))states.set(name,storage());return states.get(name);},get(name){if(!actors.has(name))actors.set(name,new Type({storage:this.state(name)},env));return {async fetch(url,init){const body=JSON.parse(init.body);for(const hook of hooks)await hook({kind,name,body});return actors.get(name).fetch(new Request(url,init));}};}};}
  for(const [binding,Type] of Object.entries({CATALOG:Catalog,ROWS:RowShard,INDEXES:IndexShard,LOCKS:LockManager,COORDINATOR:Coordinator}))env[binding]=ns(Type,binding);
  let seq=0;
  return {hooks,env,async query(sql,key=`q${++seq}`,extra={}){const response=await Worker.fetch(req({sql,idempotencyKey:key,...extra}),env);return {status:response.status,...await response.json()};}};
}
function keys(){const out=[];for(let i=0;out.filter(Boolean).length<4;i++){const k=`owner${i}`;out[shardFor(k)]??=k;}return out;}
const ownerKeys=keys();
const rows=r=>r.results.at(-1).rows;

test('lock acquisition is all-or-nothing, replay stable, release fences delayed grants',async()=>{
 const actor=new LockManager({storage:storage()});
 const a=await invoke(actor,{op:'acquire',id:'a',owner:'c0',tables:['z','a']});
 assert.equal((await invoke(actor,{op:'acquire',id:'a',owner:'c0',tables:['z','a']})).version,a.version);
 assert.equal((await invoke(actor,{op:'acquire',id:'b',owner:'c1',tables:['free','z']})).busy,true);
 const c=await invoke(actor,{op:'acquire',id:'c',owner:'c2',tables:['free']});assert.equal(c.busy,undefined);
 await invoke(actor,{op:'release',id:'never-granted'});
 assert.equal((await invoke(actor,{op:'acquire',id:'never-granted',owner:'c3',tables:['new']})).released,true);
 await invoke(actor,{op:'release',id:'a'});
 assert.equal((await invoke(actor,{op:'acquire',id:'a',owner:'c0',tables:['z']})).released,true);
 assert.equal((await invoke(actor,{op:'acquire',id:'d',owner:'c1',tables:['z']})).busy,undefined);
});

test('a paused transaction cannot block disjoint table work on another coordinator', {timeout:5000}, async()=>{
 const db=cluster();assert.equal((await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO a (id,n) VALUES (1,0); CREATE TABLE b (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO b (id,n) VALUES (1,0)')).status,200);
 const entered=deferred(),release=deferred();let paused=false;
 db.hooks.push(async({kind,name,body})=>{if(!paused&&kind==='ROWS'&&name.startsWith('a/')&&body.op==='prepare'){paused=true;entered.resolve();await release.promise;}});
 const first=db.query('UPDATE a SET n=n+1',ownerKeys[0]);await entered.promise;
 try {const other=await db.query('UPDATE b SET n=n+1',ownerKeys[1]);assert.equal(other.status,200);assert.equal(other.coordinatorId,'c1');} finally {release.resolve();}
 assert.equal((await first).status,200);
});

test('partial index/base commit is recovered before a different coordinator observes either',async()=>{
 const db=cluster();assert.equal((await db.query("CREATE TABLE a (id INTEGER PRIMARY KEY, tag TEXT); INSERT INTO a (id,tag) VALUES (1,'old'); CREATE UNIQUE INDEX tag_idx ON a (tag)")).status,200);
 const changed=await db.query("UPDATE a SET tag='new'",ownerKeys[0],{fault:'after-first-commit'});assert.equal(changed.status,503);
 const indexed=await db.query("SELECT * FROM a WHERE tag='new'",ownerKeys[1]);assert.equal(indexed.status,200);assert.equal(indexed.plan.type,'index');assert.deepEqual(rows(indexed),[{id:1,tag:'new'}]);
 assert.deepEqual(rows(await db.query("SELECT * FROM a WHERE tag='old'",ownerKeys[2])),[]);
 assert.equal((await db.query("INSERT INTO a (id,tag) VALUES (2,'new')",ownerKeys[3])).status,400);
 assert.deepEqual(rows(await db.query('SELECT * FROM a')),[{id:1,tag:'new'}]);
});

test('cross-coordinator recovery releases crash-after-lock and rejected transaction locks',async()=>{
 const db=cluster();await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO a (id,n) VALUES (1,0)');
 assert.equal((await db.query('UPDATE a SET n=99',ownerKeys[0],{fault:'after-lock'})).status,503);
 assert.deepEqual(rows(await db.query('SELECT * FROM a',ownerKeys[1])),[{id:1,n:0}]);
 assert.equal((await db.query('UPDATE a SET n=1; INSERT INTO a (id,n) VALUES (1,7)',ownerKeys[2])).status,400);
 assert.deepEqual(rows(await db.query('SELECT * FROM a',ownerKeys[3])),[{id:1,n:0}]);
});

test('split publication retires old tablets and rejects delayed old-epoch prepare',async()=>{
 const db=cluster();const created=await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, payload TEXT)');
 const old=created.routing[0];assert.equal(old.tablets.length,1);
 const values=Array.from({length:40},(_,i)=>`(${i},'${'x'.repeat(60)}')`).join(',');
 const split=await db.query(`INSERT INTO a (id,payload) VALUES ${values}`);
 assert.equal(split.status,200);assert.ok(split.events.some(e=>e.type==='split'));assert.ok(split.routing[0].epoch>old.epoch);
 const target=db.env.ROWS.get(`a/${old.tablets[0].id}`);
 async function call(body){const r=await target.fetch('https://internal',{method:'POST',body:JSON.stringify(body)});return {status:r.status,...await r.json()};}
 assert.equal((await call({op:'read',epoch:old.epoch})).code,'STALE_EPOCH');
 assert.equal((await call({op:'prepare',id:'delayed-old-route',version:split.version+100,expectedEpoch:old.epoch,epoch:old.epoch,primaryKey:'id',value:[{id:999,payload:'bad'}]})).code,'STALE_EPOCH');
 assert.equal(rows(await db.query('SELECT * FROM a')).length,40);
});

for(const fault of ['after-prepare','after-first-commit']) test(`split+index ${fault} resolves all old/new routes before another owner reads`,async()=>{
 const db=cluster();const initial=await db.query("CREATE TABLE a (id INTEGER PRIMARY KEY, tag TEXT); INSERT INTO a (id,tag) VALUES (999,'seed'); CREATE INDEX tag_idx ON a (tag)");
 const values=Array.from({length:40},(_,i)=>`(${i},'${'x'.repeat(60)}')`).join(',');const sql=`INSERT INTO a (id,tag) VALUES ${values}`;
 assert.equal((await db.query(sql,ownerKeys[0],{fault})).status,503);
 const read=await db.query('SELECT * FROM a',ownerKeys[1]);assert.equal(read.status,200);assert.equal(rows(read).length,fault==='after-prepare'?1:41);
 if(fault==='after-prepare')assert.equal(read.routing[0].epoch,initial.routing[0].epoch);else assert.ok(read.routing[0].epoch>initial.routing[0].epoch);
 const replay=await db.query(sql,ownerKeys[0]);assert.equal(replay.status,200);
 const indexed=await db.query(`SELECT * FROM a WHERE tag='${'x'.repeat(60)}'`,ownerKeys[2]);assert.equal(indexed.plan.type,'index');assert.equal(rows(indexed).length,40);
 assert.equal(rows(await db.query('SELECT * FROM a')).length,41);
});

test('concurrent different-owner unique-index claims admit exactly one row',async()=>{
 const db=cluster();await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, tag TEXT); CREATE UNIQUE INDEX tag_idx ON a (tag)');
 const claims=await Promise.all([db.query("INSERT INTO a (id,tag) VALUES (1,'same')",ownerKeys[0]),db.query("INSERT INTO a (id,tag) VALUES (2,'same')",ownerKeys[1])]);
 assert.deepEqual(claims.map(r=>r.status).sort(),[200,400]);
 const scan=rows(await db.query('SELECT * FROM a'));const lookup=rows(await db.query("SELECT * FROM a WHERE tag='same'"));assert.equal(scan.length,1);assert.deepEqual(lookup,scan);
});


test('legacy pending first CREATE with empty catalog fails closed and is untouched',async()=>{
 const db=cluster();const legacy={id:'v1-pending',key:'old',sql:'CREATE TABLE old (id INTEGER PRIMARY KEY)',version:1,decision:'preparing',targets:[{kind:'catalog',name:'catalog',value:{old:{columns:[{name:'id',type:'INTEGER',primaryKey:true}],primaryKey:'id',shards:4}}}]};
 const state=db.env.COORDINATOR.state('global');await state.put('active',legacy);
 const response=await db.query('CREATE TABLE fresh (id INTEGER PRIMARY KEY)');assert.equal(response.status,409);assert.equal(response.code,'LEGACY_STATE');
 assert.deepEqual(await state.get('active'),legacy);
 assert.equal(await db.env.CATALOG.state('table:fresh').get('data'),undefined);
});

test('index fast path still validates predicate types and unknown columns with no hits',async()=>{
 const db=cluster();await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER); CREATE INDEX n_idx ON a (n)');
 assert.equal((await db.query("SELECT * FROM a WHERE n='wrong-type'")).status,400);
 assert.equal((await db.query('SELECT missing FROM a WHERE n=999')).status,400);
 assert.equal((await db.query('SELECT * FROM a WHERE n=999 AND missing=1')).status,400);
});
