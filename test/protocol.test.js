import test from 'node:test';
import assert from 'node:assert/strict';
import Worker,{Coordinator,DataShard,PlanAllocator,PlanBucket} from '../src/index.js';

// Memory protocol model only; process durability is covered by celld E2E tests.
function storage(){
 const data=new Map();let alarm=null;
 const api={
  async get(key){return Array.isArray(key)?new Map(key.filter(k=>data.has(k)).map(k=>[k,structuredClone(data.get(k))])):structuredClone(data.get(key));},
  async put(key,value){if(typeof key==='object'){for(const[k,v]of Object.entries(key))data.set(k,structuredClone(v));}else data.set(key,structuredClone(value));},
  async delete(key){if(Array.isArray(key)){let count=0;for(const k of key)count+=data.delete(k);return count;}return data.delete(key);},
  async list({prefix='',start,end,limit,reverse=false}={}){let result=[...data].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);if(reverse)result.reverse();if(limit!==undefined)result=result.slice(0,limit);return new Map(result.map(([k,v])=>[k,structuredClone(v)]));},
  async transaction(fn){const backup=structuredClone(data);try{return await fn(api);}catch(error){data.clear();for(const[k,v]of backup)data.set(k,v);throw error;}},
  async setAlarm(value){alarm=value;},async getAlarm(){return alarm;},async deleteAlarm(){alarm=null;}
 };return api;
}
function cluster(){
 const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'1024',TABLET_MERGE_BYTES:'256'},namespaces=[];
 function namespace(Type){const states=new Map(),actors=new Map();const ns={idFromName:name=>name,get(name){return{async fetch(url,init){if(!states.has(name))states.set(name,{storage:storage(),id:{name,toString:()=>name},blockConcurrencyWhile:fn=>fn()});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return actors.get(name).fetch(new Request(url,init));}};},reset(){actors.clear();}};namespaces.push(ns);return ns;}
 env.SHARDS=namespace(DataShard);env.ALLOCATOR=namespace(PlanAllocator);env.MEDIATORS=namespace(PlanBucket);env.COORDINATOR=namespace(Coordinator);
 let sequence=0;return{restart(){namespaces.forEach(ns=>ns.reset());},async post(path,body={}){const response=await Worker.fetch(new Request(`http://127.0.0.1${path}`,{method:'POST',body:JSON.stringify(body)}),env);return{status:response.status,body:await response.json()};},query(sql,extra={}){return this.post('/query',{sql,idempotencyKey:`request_${++sequence}`,...extra});}};
}
for(const fault of ['after-prepare','after-commit-ready','after-plan','after-first-commit'])test(`MVCC coordinator reset ${fault} preserves snapshot and resolves durable intent`,async()=>{
 const db=cluster();let result=await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,10),(2,10)');assert.equal(result.status,200,JSON.stringify(result.body));
 const before=(await db.post('/snapshot')).body.frontier;
 const sql='UPDATE a SET n=n-1 WHERE id=1; UPDATE a SET n=n+1 WHERE id=2';
 result=await db.query(sql,{fault,idempotencyKey:'transfer'});assert.equal(result.status,503,JSON.stringify(result.body));
 db.restart();
 assert.deepEqual((await db.query('SELECT n FROM a ORDER BY id',{snapshot:before})).body.results[0].rows,[{n:10},{n:10}]);
 assert.equal((await db.post('/recover')).status,200);
 assert.deepEqual((await db.query('SELECT n FROM a ORDER BY id')).body.results[0].rows,fault==='after-prepare'?[{n:10},{n:10}]:[{n:9},{n:11}]);
 const replay=await db.query(sql,{idempotencyKey:'transfer'});assert.equal(replay.status,200);assert.deepEqual(await db.query(sql,{idempotencyKey:'transfer'}),replay);
 assert.equal((await db.query('SELECT * FROM a',{idempotencyKey:'transfer'})).status,409);
});
test('MVCC tombstone leaves old snapshot readable after actor resets',async()=>{
 const db=cluster();assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,3)')).status,200);
 const before=(await db.post('/snapshot')).body.frontier;
 assert.equal((await db.query('DELETE FROM a WHERE id=1')).status,200);db.restart();
 assert.deepEqual((await db.query('SELECT * FROM a')).body.results[0].rows,[]);
 assert.deepEqual((await db.query('SELECT * FROM a',{snapshot:before})).body.results[0].rows,[{id:1,n:3}]);
});
