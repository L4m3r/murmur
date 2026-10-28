import test from 'node:test';
import assert from 'node:assert/strict';
import Worker,{Coordinator,DataShard,PlanAllocator,PlanBucket,SchemaManager} from '../src/index.js';
function storage(){const data=new Map();const api={data,setAlarm:async value=>{api.alarm=value;},deleteAlarm:async()=>{api.alarm=null;},getAlarm:async()=>api.alarm??null,get:async k=>structuredClone(data.get(k)),put:async(k,v)=>data.set(k,structuredClone(v)),delete:async k=>data.delete(k),list:async({prefix='',start,end,limit,reverse=false}={})=>{let rows=[...data].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);if(reverse)rows.reverse();if(limit!==undefined)rows=rows.slice(0,limit);return new Map(structuredClone(rows));},transaction:async fn=>{const old=structuredClone(data),oldAlarm=api.alarm;try{return await fn(api);}catch(e){api.alarm=oldAlarm;data.clear();for(const[k,v]of old)data.set(k,v);throw e;}}};return api;}
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
function cluster(options={}){
 const env={ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'16384',TABLET_MERGE_BYTES:'1024',...options};let hook=async()=>{},sequence=0;const namespaces=[];
 const namespace=Type=>{const states=new Map(),actors=new Map();const ns={states,idFromName:x=>x,get(name){if(!states.has(name))states.set(name,{storage:storage(),id:name});if(!actors.has(name))actors.set(name,new Type(states.get(name),env));return{fetch:async(url,init)=>{const input=JSON.parse(init.body);await hook('before',Type,name,input);const response=await actors.get(name).fetch(new Request(url,init));await hook('after',Type,name,input,response);return response;}};},reset(){actors.clear();}};namespaces.push(ns);return ns;};
 env.ALLOCATOR=namespace(PlanAllocator);env.MEDIATORS=namespace(PlanBucket);env.SHARDS=namespace(DataShard);env.COORDINATOR=namespace(Coordinator);env.SCHEMA=namespace(SchemaManager);
 const post=async(path,input={})=>{const r=await Worker.fetch(new Request('http://127.0.0.1'+path,{method:'POST',body:JSON.stringify(input)}),env);return{status:r.status,...await r.json()};};
 return{env,post,hook(fn){hook=fn;},reset(){namespaces.forEach(n=>n.reset());},async query(sql,extra={}){const body={sql,idempotencyKey:`lifecycle_${++sequence}`,...extra};for(let attempt=0;attempt<128;attempt++){const result=await post('/query',body);if(extra.fault||result.status!==503||result.code!=='PLAN_PENDING')return result;const rpc=async(binding,name,input)=>{const response=await binding.get(binding.idFromName(name)).fetch('https://background/',{method:'POST',body:JSON.stringify(input)});assert.equal(response.status,200);return response.json();};const found=await rpc(env.ALLOCATOR,'global',{op:'lookup',id:result.transactionId});assert.equal(found.state,'assigned');await rpc(env.MEDIATORS,found.association.bucket,{op:'drive',id:result.transactionId,release:true});}assert.fail('same-identity PLAN_PENDING retry did not complete within128 calls');},async compact(){let page,rounds=0;do{page=await post('/maintenance',{retainSteps:0,continuation:page?.continuation});assert.equal(page.status,200,JSON.stringify(page));assert.ok(++rounds<1000);}while(!page.done);return page;},async schema(sql,key){return post('/schema',{action:'start',sql,idempotencyKey:key});},async finish(id){let result,rounds=0;do{result=await post('/schema',{action:'resume',id});assert.ok([200,202].includes(result.status),JSON.stringify(result));assert.ok(++rounds<200);}while(result.status===202);return result;}};
}
const rows=result=>result.results[0].rows;
const oldActors=db=>[...db.env.SHARDS.states.keys()].filter(name=>name.startsWith('base/')||name.startsWith('index/'));
const roots=(db,names)=>names.reduce((n,name)=>n+[...db.env.SHARDS.states.get(name).storage.data.keys()].filter(k=>k.startsWith('row:')).length,0);
const setup="CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT,n INTEGER); INSERT INTO a(id,tag,n) VALUES (1,'old',1),(2,'other',2); CREATE INDEX lookup ON a(tag)";

test('table lifecycle independent: leased base/index snapshots survive DROP/recreate and sources collect only after release',async()=>{
 const db=cluster(),created=await db.query(setup);assert.equal(created.status,200,JSON.stringify(created));
 const names=oldActors(db),lease=await db.post('/snapshot',{action:'lease',id:'old-generation',snapshot:created.step,ttlMs:600000});assert.equal(lease.status,200);
 assert.equal((await db.query('DROP TABLE a')).status,200);
 assert.equal((await db.query('SELECT * FROM a')).status,400);
 const fresh=await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT,n INTEGER); INSERT INTO a(id,tag,n) VALUES (1,'new',9); CREATE INDEX lookup ON a(tag)");assert.equal(fresh.status,200,JSON.stringify(fresh));
 const newNames=oldActors(db).filter(name=>!names.includes(name));assert.ok(newNames.some(name=>name.startsWith('base/')));assert.ok(newNames.some(name=>name.startsWith('index/')));
 db.reset();await db.compact();assert.ok(roots(db,names)>0);
 assert.deepEqual(rows(await db.query("SELECT n FROM a WHERE tag='old'",{snapshot:created.step,snapshotLease:lease.id})),[{n:1}]);
 assert.deepEqual(rows(await db.query('SELECT n FROM a WHERE id=1')),[{n:9}]);
 await db.post('/snapshot',{action:'release',id:lease.id});await db.compact();assert.equal(roots(db,names),0);
 assert.ok(roots(db,newNames)>0);assert.deepEqual(rows(await db.query("SELECT n FROM a WHERE tag='new'")),[{n:9}]);
 assert.equal((await db.query('SELECT * FROM a',{snapshot:created.step})).code,'SNAPSHOT_EXPIRED');
});

test('table lifecycle independent: failed DROP/CREATE transaction rolls back and successful replacement is atomic across recovery',async()=>{
 const db=cluster(),created=await db.query(setup);assert.equal(created.status,200);
 const lease=await db.post('/snapshot',{action:'lease',id:'atomic-old',snapshot:created.step,ttlMs:600000});assert.equal(lease.status,200);
 const failed=await db.query("DROP TABLE a; CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (3,'wrong-type')");assert.equal(failed.status,400);
 assert.deepEqual(rows(await db.query("SELECT n FROM a WHERE tag='old'")),[{n:1}]);
 const sql='DROP TABLE a; CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (3,33)';
 const planned=await db.query(sql,{idempotencyKey:'atomic-replace',fault:'after-plan'});assert.equal(planned.status,503);
 db.reset();assert.deepEqual(rows(await db.query("SELECT n FROM a WHERE tag='old'",{snapshot:created.step,snapshotLease:lease.id})),[{n:1}]);
 assert.equal((await db.post('/recover')).status,200);
 const replay=await db.query(sql,{idempotencyKey:'atomic-replace'});assert.equal(replay.status,200,JSON.stringify(replay));
 assert.deepEqual(rows(await db.query('SELECT id,n FROM a')),[{id:3,n:33}]);
 assert.deepEqual(rows(await db.query('SELECT id,n FROM a ORDER BY id',{snapshot:created.step,snapshotLease:lease.id})),[{id:1,n:1},{id:2,n:2}]);
});

test('table lifecycle independent: a writer planned from old schema cannot cross a committed DROP',async()=>{
 const db=cluster();assert.equal((await db.query(setup)).status,200);
 const entered=deferred(),release=deferred();let once=false;
 db.hook(async(phase,Type,name,input)=>{if(!once&&phase==='before'&&Type===DataShard&&input.op==='propose'&&input.changes?.some(c=>c.row?.n===77)){once=true;entered.resolve();await release.promise;}});
 const writer=db.query('UPDATE a SET n=77 WHERE id=1');await entered.promise;
 try{assert.equal((await db.query('DROP TABLE a')).status,200);}finally{release.resolve();}
 const result=await writer;assert.equal(result.status,400,JSON.stringify(result));
 assert.equal((await db.query('CREATE TABLE a(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO a(id,n) VALUES (1,8)')).status,200);
 assert.deepEqual(rows(await db.query('SELECT n FROM a WHERE id=1')),[{n:8}]);
});

test('table lifecycle independent: interrupted wide DROP cleanup cannot retire a recreated generation',async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'512',TABLET_MERGE_BYTES:'128'});
 const values=Array.from({length:64},(_,id)=>`(${id},'${'x'.repeat(80)}',${id})`).join(',');
 assert.equal((await db.query(`CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT,n INTEGER); INSERT INTO a(id,tag,n) VALUES ${values}`)).status,200);
 for(const index of ['i1','i2','i3']){const created=await db.query(`CREATE INDEX ${index} ON a(tag)`);assert.equal(created.status,200,JSON.stringify(created));}
 const names=oldActors(db);assert.ok(names.length>48,`wide job requires >48 old actors; got ${names.length}`);
 const snapshot=(await db.post('/snapshot')).snapshot;
 const lease=await db.post('/snapshot',{action:'lease',id:'wide-old',snapshot,ttlMs:600000});assert.equal(lease.status,200);
 const start=await db.schema('DROP TABLE a','wide-drop');assert.equal(start.status,202,JSON.stringify(start));assert.ok(start.job.id);
 assert.equal((await db.query('SELECT id FROM a')).status,400,'first batch logically drops before wide physical cleanup completes');
 db.reset();
 const replacement=await db.query("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT,n INTEGER); INSERT INTO a(id,tag,n) VALUES (999,'replacement',99); CREATE INDEX i1 ON a(tag)");assert.equal(replacement.status,200,JSON.stringify(replacement));
 const newNames=oldActors(db).filter(name=>!names.includes(name));
 const done=await db.finish(start.job.id);assert.equal(done.job.state,'dropped',JSON.stringify(done));db.reset();
 assert.deepEqual(rows(await db.query("SELECT id,n FROM a WHERE tag='replacement'")),[{id:999,n:99}]);
 assert.equal(rows(await db.query('SELECT id FROM a',{snapshot,snapshotLease:lease.id})).length,64);
 await db.compact();assert.ok(roots(db,names)>0);assert.ok(roots(db,newNames)>0);
 await db.post('/snapshot',{action:'release',id:lease.id});await db.compact();assert.equal(roots(db,names),0);assert.ok(roots(db,newNames)>0);
 assert.deepEqual(rows(await db.query('SELECT id,n FROM a')),[{id:999,n:99}]);
});
