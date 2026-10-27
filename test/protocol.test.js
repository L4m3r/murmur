import test from 'node:test';
import assert from 'node:assert/strict';
import Worker, { Coordinator, RowShard, Catalog, IndexShard, LockManager, shardFor } from '../src/index.js';

// Deterministic protocol model. Runtime durability is exercised separately by
// celld process-restart integration tests, not asserted by this memory double.
function storage() {
  const data = new Map();
  const api = {
    get: async key => structuredClone(data.get(key)),
    put: async (key, value) => { data.set(key, structuredClone(value)); },
    delete: async key => data.delete(key),
    list: async ({prefix = ''} = {}) => new Map([...data].filter(([k]) => k.startsWith(prefix)).sort().map(([k,v])=>[k,structuredClone(v)])),
    transaction: async fn => {
      const backup = structuredClone(data);
      try { return await fn(api); }
      catch (error) { data.clear(); for (const [k,v] of backup) data.set(k,v); throw error; }
    },
  };
  return api;
}
const request = body => new Request('https://actor/query', {method:'POST',body:JSON.stringify(body)});
async function invoke(actor, body) { const response = await actor.fetch(request(body)); return { status:response.status, body:await response.json() }; }
function cluster() {
  const env = { ALLOW_FAULTS:'true', TABLET_SPLIT_BYTES:'1024', TABLET_MERGE_BYTES:'256' };
  const all = []; let hook = async()=>{};
  function namespace(Type) {
    const actors = new Map(), states = new Map();
    const namespace = { idFromName:name=>name, get(name) {
      if (!states.has(name)) states.set(name, {storage:storage(), id:{name,toString:()=>name}});
      if (!actors.has(name)) actors.set(name,new Type(states.get(name), env));
      return {fetch:async(_url,init)=>{await hook(Type,name,JSON.parse(init.body));return actors.get(name).fetch(new Request(_url,init));}};
    }, reset(){actors.clear();} };
    all.push(namespace); return namespace;
  }
  env.CATALOG=namespace(Catalog); env.ROWS=namespace(RowShard); env.INDEXES=namespace(IndexShard);
  env.LOCKS=namespace(LockManager); env.COORDINATOR=namespace(Coordinator);
  let sequence=0;
  return {env,setHook(fn){hook=fn;},restart(){for(const namespace of all) namespace.reset();},async query(sql,extra={}){
    const response=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify({sql,idempotencyKey:`k${++sequence}`,...extra})}),env);
    return {status:response.status,body:await response.json()};
  }};
}
test('durable abort fences a delayed prepare even after participant reset',async()=>{
  const state={storage:storage()}; let actor=new RowShard(state);
  assert.equal((await invoke(actor,{op:'abort',id:'old',version:1})).status,200);
  actor=new RowShard(state);
  assert.equal((await invoke(actor,{op:'prepare',id:'old',version:1,primaryKey:'id',epoch:1,expectedEpoch:0,value:[{id:1}]})).status,503);
  assert.equal((await invoke(actor,{op:'prepare',id:'new',version:1,primaryKey:'id',epoch:1,expectedEpoch:0,value:[{id:2}]})).status,200);
  assert.equal((await invoke(actor,{op:'commit',id:'new',version:1})).status,200);
  assert.deepEqual((await invoke(actor,{op:'read'})).body.value,[{id:2}]);
});
test('participant commit replay cannot revert newer rows; deleted row keys disappear',async()=>{
  const actor=new RowShard({storage:storage()});
  for (const [id,version,value] of [['a',1,[{id:1},{id:2}]],['b',2,[{id:2}]]]) {
    assert.equal((await invoke(actor,{op:'prepare',id,version,primaryKey:'id',epoch:1,expectedEpoch:version===1?0:1,value})).status,200);
    assert.equal((await invoke(actor,{op:'commit',id,version})).status,200);
  }
  assert.equal((await invoke(actor,{op:'commit',id:'a',version:1})).status,200);
  assert.deepEqual((await invoke(actor,{op:'read'})).body,{version:2,epoch:1,retired:false,value:[{id:2}]});
});
for (const fault of ['after-prepare','after-decision','after-first-commit']) test(`coordinator reset ${fault}: recovery precedes read, replay exactly once`,async()=>{
  const db=cluster();
  assert.equal((await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, balance INTEGER); INSERT INTO a (id,balance) VALUES (1,100),(2,100)')).status,200);
  const sql='UPDATE a SET balance=balance-7 WHERE id=1; UPDATE a SET balance=balance+7 WHERE id=2';
  assert.equal((await db.query(sql,{fault,idempotencyKey:'transfer'})).status,503);
  db.restart();
  const read=await db.query('SELECT * FROM a ORDER BY id');
  assert.deepEqual(read.body.results[0].rows.map(r=>r.balance),fault==='after-prepare'?[100,100]:[93,107]);
  const first=await db.query(sql,{idempotencyKey:'transfer'});
  assert.equal(first.status,200);
  assert.deepEqual(await db.query(sql,{idempotencyKey:'transfer'}),first);
  assert.equal((await db.query('SELECT * FROM a',{idempotencyKey:'transfer'})).status,409);
  assert.deepEqual((await db.query('SELECT * FROM a ORDER BY id')).body.results[0].rows.map(r=>r.balance),[93,107]);
});
test('concurrent transfers and reads expose only whole transactions',async()=>{
  const db=cluster();
  await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, balance INTEGER); INSERT INTO a (id,balance) VALUES (1,100),(2,100)');
  const tasks=[];
  for(let i=0;i<20;i++) {
    tasks.push(db.query('UPDATE a SET balance=balance-1 WHERE id=1; UPDATE a SET balance=balance+1 WHERE id=2').then(r=>assert.equal(r.status,200)));
    tasks.push(db.query('SELECT * FROM a ORDER BY id').then(r=>assert.equal(r.body.results[0].rows.reduce((n,row)=>n+row.balance,0),200)));
  }
  await Promise.all(tasks);
  assert.deepEqual((await db.query('SELECT * FROM a ORDER BY id')).body.results[0].rows.map(r=>r.balance),[80,120]);
});
test('UTF8 oversized durable envelope rejected before any write',async()=>{
  const db=cluster();
  const text='界'.repeat(8000);
  const r=await db.query(`CREATE TABLE a (id INTEGER PRIMARY KEY, text TEXT); INSERT INTO a (id,text) VALUES (1,'${text}'),(2,'${text}'); SELECT * FROM a`);
  assert.equal(r.status,400);
  assert.equal((await db.query('SELECT * FROM a')).status,400);
});

test('separate coordinators execute disjoint table transactions concurrently',async()=>{
  const db=cluster();
  await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER); CREATE TABLE b (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO a (id,n) VALUES (1,1); INSERT INTO b (id,n) VALUES (1,1)');
  let release; const gate=new Promise(resolve=>{release=resolve;});
  let both; const observed=new Promise(resolve=>{both=resolve;}); const reads=new Set();
  db.setHook(async(Type,name,input)=>{
    if(Type===Catalog && ['table:a','table:b'].includes(name) && input.op==='read'){
      reads.add(name); if(reads.size===2) both(); await gate;
    }
  });
  const first='parallel0';let second='parallel1';for(let i=1;shardFor(first)===shardFor(second);i++)second=`parallel${i}`;
  const transactions=[db.query('UPDATE a SET n=n+1',{idempotencyKey:first}),db.query('UPDATE b SET n=n+1',{idempotencyKey:second})];
  let timer;try {await Promise.race([observed,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Disjoint transactions did not overlap')),1000);})]);}
  finally {clearTimeout(timer);release();}
  for(const result of await Promise.all(transactions))assert.equal(result.status,200);
});
test('lock release fences delayed acquisition and leaves no orphan',async()=>{
  const actor=new LockManager({storage:storage()});
  await invoke(actor,{op:'release',id:'orphan'});
  assert.equal((await invoke(actor,{op:'acquire',id:'orphan',owner:'c0',tables:['a']})).body.released,true);
  assert.deepEqual((await invoke(actor,{op:'inspect'})).body.locks,{});
});
test('retired tablets reject stale reads and preparations',async()=>{
  const actor=new RowShard({storage:storage()});
  await invoke(actor,{op:'prepare',id:'a',version:1,primaryKey:'id',epoch:1,expectedEpoch:0,value:[{id:1}]});
  await invoke(actor,{op:'commit',id:'a',version:1});
  await invoke(actor,{op:'prepare',id:'b',version:2,primaryKey:'id',epoch:2,expectedEpoch:1,retired:true,value:[]});
  await invoke(actor,{op:'commit',id:'b',version:2});
  assert.equal((await invoke(actor,{op:'read',epoch:1})).status,409);
  assert.equal((await invoke(actor,{op:'read',epoch:2})).status,409);
  assert.equal((await invoke(actor,{op:'prepare',id:'c',version:3,primaryKey:'id',epoch:3,expectedEpoch:2,value:[{id:2}]})).body.code,'STALE_EPOCH');
});
test('nonempty V1 catalog fails closed without erasing old state',async()=>{
  const db=cluster();
  const legacy=db.env.CATALOG.get('catalog');
  const old={accounts:{columns:[{name:'id',type:'INTEGER',primaryKey:true}],primaryKey:'id',shards:4}};
  const call=async body=>legacy.fetch('https://actor/internal',{method:'POST',body:JSON.stringify(body)});
  assert.equal((await call({op:'prepare',id:'v1',version:1,epoch:1,expectedEpoch:0,value:old})).status,200);
  assert.equal((await call({op:'commit',id:'v1',version:1})).status,200);
  const result=await db.query('SELECT * FROM accounts');
  assert.equal(result.status,409);assert.equal(result.body.code,'LEGACY_STATE');
  assert.deepEqual((await (await call({op:'read'})).json()).value,old);
});
