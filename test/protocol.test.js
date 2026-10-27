import test from 'node:test';
import assert from 'node:assert/strict';
import { Coordinator, RowShard, Catalog } from '../src/index.js';

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
  function namespace(Type) {
    const actors = new Map();
    return { idFromName:name=>name, get(name) {
      if (!actors.has(name)) actors.set(name,new Type({storage:storage()}));
      return {fetch:(_url,init)=>actors.get(name).fetch(new Request('https://actor',init))};
    }};
  }
  const state={storage:storage()}, env={CATALOG:namespace(Catalog),ROWS:namespace(RowShard),ALLOW_FAULTS:'true'};
  let actor=new Coordinator(state,env), sequence=0;
  return {restart(){actor=new Coordinator(state,env);},query(sql,extra={}){return invoke(actor,{sql,idempotencyKey:`k${++sequence}`,...extra});}};
}
test('durable abort fences a delayed prepare even after participant reset',async()=>{
  const state={storage:storage()}; let actor=new RowShard(state);
  assert.equal((await invoke(actor,{op:'abort',id:'old',version:1})).status,200);
  actor=new RowShard(state);
  assert.equal((await invoke(actor,{op:'prepare',id:'old',version:1,primaryKey:'id',value:[{id:1}]})).status,500);
  assert.equal((await invoke(actor,{op:'prepare',id:'new',version:1,primaryKey:'id',value:[{id:2}]})).status,200);
  assert.equal((await invoke(actor,{op:'commit',id:'new',version:1})).status,200);
  assert.deepEqual((await invoke(actor,{op:'read'})).body.value,[{id:2}]);
});
test('participant commit replay cannot revert newer rows; deleted row keys disappear',async()=>{
  const actor=new RowShard({storage:storage()});
  for (const [id,version,value] of [['a',1,[{id:1},{id:2}]],['b',2,[{id:2}]]]) {
    assert.equal((await invoke(actor,{op:'prepare',id,version,primaryKey:'id',value})).status,200);
    assert.equal((await invoke(actor,{op:'commit',id,version})).status,200);
  }
  assert.equal((await invoke(actor,{op:'commit',id:'a',version:1})).status,200);
  assert.deepEqual((await invoke(actor,{op:'read'})).body,{version:2,value:[{id:2}]});
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
