import {queryWithBackgroundTurns} from './pending-query-fixture.js';
import {storage} from './pool-storage-fixture.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import Worker, { Coordinator, DataShard, PlanAllocator as Mediator, PlanBucket } from '../src/index.js';
import { encodeKey, primaryRangeForFilters } from '../src/keys.js';

// Deterministic scheduling, durable actor-state replacement, no simulated alarms.

const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false};
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
async function deadline(promise){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Visibility progress deadline exceeded')),2500);})]);}finally{clearTimeout(timer);}}
function cluster(options={}) {
 const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'16384',TABLET_MERGE_BYTES:'1024',...options};let hook=async()=>{},sequence=0;const spaces=[];
 function namespace(Type){const states=new Map(),actors=new Map();const ns={idFromName:x=>x,get(name){if(!states.has(name))states.set(name,{storage:storage(),id:name});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return {fetch:async(url,init)=>{const input=JSON.parse(init.body);await hook('before',Type,name,input);const response=await actors.get(name).fetch(new Request(url,init));await hook('after',Type,name,input,response);return response;}};},reset(){actors.clear();}};spaces.push(ns);return ns;}
 env.SHARDS=namespace(DataShard);env.ALLOCATOR=namespace(Mediator);env.MEDIATORS=namespace(PlanBucket);env.COORDINATOR=namespace(Coordinator);
 const rpc=async(ns,name,input)=>{const response=await ns.get(name).fetch('https://internal',{method:'POST',body:JSON.stringify(input)});return {status:response.status,...await response.json()};};
 return {env,hook(fn){hook=fn;},reset(){spaces.forEach(s=>s.reset());},shard:(name,input)=>rpc(env.SHARDS,name,input),mediator:input=>rpc(env.ALLOCATOR,'global',input),
  async post(path,input){const response=await Worker.fetch(new Request('http://127.0.0.1'+path,{method:'POST',body:JSON.stringify(input)}),env);return {status:response.status,...await response.json()};},
  query(sql,extra={}){return queryWithBackgroundTurns(env,{sql,idempotencyKey:`visibility_${++sequence}`,...extra});},
  async prepare(name,id,changes=[],reads=[],snapshot=0,extra={}){const result=await rpc(env.SHARDS,name,{op:'prepare',id,owner:`owner-${id}`,snapshot,changes,reads,...extra});assert.equal(result.status,200,JSON.stringify(result));return {name,vote:result.vote};},
  async plan(id,votes){const result=await rpc(env.ALLOCATOR,'global',{op:'plan',id,owner:`owner-${id}`,participants:votes});assert.equal(result.status,200,JSON.stringify(result));return result;},
  async apply(id,plan,vote){const result=await rpc(env.SHARDS,vote.name,{op:'apply',id,step:plan.step,shard:vote.name,vote:vote.vote});assert.equal(result.status,200,JSON.stringify(result));}
 };
}


const setup="CREATE TABLE a(tenant INTEGER,id INTEGER,email TEXT,n INTEGER,PRIMARY KEY(tenant,id)); INSERT INTO a(tenant,id,email,n) VALUES (1,1,'one',11),(1,2,'two',12),(2,1,'three',21)";

test('composite independent: duplicate leading PK allowed, entire tuple unique and immutable',async()=>{
 const db=cluster();assert.equal((await db.query(setup)).status,200);
 assert.equal((await db.query("INSERT INTO a(tenant,id,email,n) VALUES (1,1,'duplicate',9)")).status,400);
 assert.equal((await db.query("INSERT INTO a(tenant,id,email,n) VALUES (1,NULL,'null',9)")).status,400);
 assert.equal((await db.query('UPDATE a SET id=3 WHERE tenant=1 AND id=2')).status,400);
 assert.equal((await db.query('UPDATE a SET tenant=3 WHERE tenant=1 AND id=2')).status,400);
 assert.deepEqual((await db.query('SELECT n FROM a WHERE tenant=1 ORDER BY id')).results[0].rows,[{n:11},{n:12}]);
 assert.equal((await db.query("UPSERT INTO a(tenant,id,email,n) VALUES (1,2,'two',99)")).status,200);
 assert.deepEqual((await db.query('SELECT n FROM a WHERE tenant=1 AND id=2')).results[0].rows,[{n:99}]);
 assert.deepEqual((await db.query('SELECT n FROM a WHERE tenant=2 AND id=1')).results[0].rows,[{n:21}]);
});

test('composite independent: prefix and second-component range physically prune split tablets',async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'200',TABLET_MERGE_BYTES:'100'});
 const rows=[];for(let tenant=1;tenant<=3;tenant++)for(let id=1;id<=12;id++)rows.push(`(${tenant},${id},'${'x'.repeat(35)}')`);
 const created=await db.query(`CREATE TABLE a(tenant INTEGER,id INTEGER,payload TEXT,PRIMARY KEY(tenant,id)); INSERT INTO a(tenant,id,payload) VALUES ${rows.join(',')}; CREATE INDEX lookup ON a(id)`);assert.equal(created.status,200,JSON.stringify(created));
 const visits=[];db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard&&name.startsWith('base/')&&['scan','getMany'].includes(input.op))visits.push({name,input});});
 const result=await db.query('SELECT tenant,id FROM a WHERE tenant=2 AND id>=4 AND id<7 ORDER BY id');assert.equal(result.status,200,JSON.stringify(result));
 assert.deepEqual(result.results[0].rows,[{tenant:2,id:4},{tenant:2,id:5},{tenant:2,id:6}]);
 assert.ok(new Set(visits.map(v=>v.name)).size<created.routing[0].tablets.length);
 const wanted=primaryRangeForFilters({primaryKeys:['tenant','id']},[{column:'tenant',op:'=',value:2},{column:'id',op:'>=',value:4},{column:'id',op:'<',value:7}]);
 assert.ok(visits.every(v=>v.input.op==='getMany'||JSON.stringify(v.input.range)===JSON.stringify(wanted)));
 assert.deepEqual((await db.query('SELECT tenant,id FROM a WHERE id=4 ORDER BY tenant')).results[0].rows,[{tenant:1,id:4},{tenant:2,id:4},{tenant:3,id:4}]);
 const merged=await db.query('DELETE FROM a WHERE tenant!=2; DELETE FROM a WHERE id!=6');assert.equal(merged.status,200,JSON.stringify(merged));
 assert.equal(merged.routing[0].tablets.length,1);assert.equal(merged.routing[0].indexes[0].tablets.length,1);db.reset();
 assert.deepEqual((await db.query('SELECT tenant,id FROM a WHERE id=6')).results[0].rows,[{tenant:2,id:6}]);
 assert.deepEqual((await db.query('SELECT tenant,id FROM a WHERE id=4 ORDER BY tenant',{snapshot:created.step})).results[0].rows,[{tenant:1,id:4},{tenant:2,id:4},{tenant:3,id:4}]);
});

for(const column of ['email','tenant','id'])test(`composite independent: index on ${column} preserves full base tuples and deduplicates PK suffix`,async()=>{
 const db=cluster();assert.equal((await db.query(setup+`; CREATE INDEX lookup ON a(${column})`)).status,200);
 const calls=[];db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard)calls.push({name,input});});
 const predicate=column==='email'?"email='two'":column==='tenant'?'tenant=1':'id=1';
 const result=await db.query(`SELECT tenant,id,n FROM a WHERE ${predicate} ORDER BY tenant`);assert.equal(result.status,200,JSON.stringify(result));
 const expected=column==='email'?[{tenant:1,id:2,n:12}]:column==='tenant'?[{tenant:1,id:1,n:11},{tenant:1,id:2,n:12}]:[{tenant:1,id:1,n:11},{tenant:2,id:1,n:21}];
 assert.deepEqual(result.results[0].rows,expected);
 const index=result.routing[0].indexes[0];assert.deepEqual(index.keyColumns,column==='email'?['value','pk0','pk1']:column==='tenant'?['value','pk1']:['value','pk0']);
 if(column!=='tenant')assert.ok(calls.some(c=>c.name.startsWith('base/')&&c.input.op==='getMany'&&c.input.keys.every(k=>Array.isArray(k)&&k.length===2)));
});

test('composite independent: racing unique value with different full PK tuples allows only one commit',async()=>{
 const db=cluster(),ready=deferred(),ids=new Set();
 assert.equal((await db.query('CREATE TABLE a(tenant INTEGER,id INTEGER,email TEXT,PRIMARY KEY(tenant,id)); CREATE UNIQUE INDEX unique_email ON a(email)')).status,200);
 db.hook(async(phase,Type,name,input)=>{if(phase==='before'&&Type===DataShard&&input.op==='prepare'&&ids.size<2){ids.add(input.id);if(ids.size===2)ready.resolve();await ready.promise;}});
 const results=await deadline(Promise.all([db.query("INSERT INTO a(tenant,id,email) VALUES (1,1,'same')"),db.query("INSERT INTO a(tenant,id,email) VALUES (2,1,'same')")]));
 assert.deepEqual(results.map(r=>r.status).sort(),[200,400]);
 assert.equal((await db.query("SELECT * FROM a WHERE email='same'")).results[0].rows.length,1);
});

test('composite independent: dropped/recreated index generations preserve historical tuple routes after restart',async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'200',TABLET_MERGE_BYTES:'100'});
 assert.equal((await db.query(setup+'; CREATE INDEX lookup ON a(email)')).status,200);
 const old=(await db.mediator({op:'snapshot'})).snapshot;
 assert.equal((await db.query('DROP INDEX lookup ON a')).status,200);
 assert.equal((await db.query("UPDATE a SET email='changed' WHERE tenant=1 AND id=2; CREATE INDEX lookup ON a(email)")).status,200);db.reset();
 assert.deepEqual((await db.query("SELECT tenant,id FROM a WHERE email='two'",{snapshot:old})).results[0].rows,[{tenant:1,id:2}]);
 assert.deepEqual((await db.query("SELECT tenant,id FROM a WHERE email='two'")).results[0].rows,[]);
 assert.deepEqual((await db.query("SELECT tenant,id FROM a WHERE email='changed'")).results[0].rows,[{tenant:1,id:2}]);
});

test('composite independent: three-component mixed-type prefix bounds match lexicographic predicate semantics',()=>{
 const schema={primaryKeys:['a','b','c']},domains=[[-1,0,2],['','a','a\0','界','😀'],[-2,0,3]],rows=[];
 for(const a of domains[0])for(const b of domains[1])for(const c of domains[2])rows.push({a,b,c});
 const ops=['=','<','<=','>','>='];
 const matches=(value,op,bound)=>{const a=encodeKey(value),b=encodeKey(bound);return op==='='?a===b:op==='<'?a<b:op==='<='?a<=b:op==='>'?a>b:a>=b;};
 const cases=[];
 for(const op of ops)for(const a of domains[0])cases.push([{column:'a',op,value:a}]);
 for(const a of domains[0])for(const op of ops)for(const b of domains[1])cases.push([{column:'a',op:'=',value:a},{column:'b',op,value:b}]);
 for(const b of domains[1])for(const op of ops)for(const c of domains[2])cases.push([{column:'a',op:'=',value:0},{column:'b',op:'=',value:b},{column:'c',op,value:c}]);
 for(const filters of cases){const range=primaryRangeForFilters(schema,filters);const actual=rows.filter(row=>{const key=encodeKey([row.a,row.b,row.c]);return !range.empty&&(range.lower===null||key>range.lower||(key===range.lower&&range.lowerInclusive))&&(range.upper===null||key<range.upper||(key===range.upper&&range.upperInclusive));});const expected=rows.filter(row=>filters.every(f=>matches(row[f.column],f.op,f.value)));assert.deepEqual(actual,expected,JSON.stringify(filters));}
});
