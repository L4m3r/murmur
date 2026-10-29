import test from 'node:test';
import assert from 'node:assert/strict';
import Worker,{Coordinator,DataShard,PlanAllocator,PlanBucket} from '../src/index.js';
import {observeRequest,metricsSnapshot} from '../src/observability.js';
import {storage} from './pool-storage-fixture.js';
function cluster(options={}){
 const env={API_TOKEN:'super-secret-bearer',ALLOW_FAULTS:'true',...options},stores=new Map(),actors=new Map(),calls=[];let before=async()=>{};
 for(const [binding,Type]of Object.entries({COORDINATOR:Coordinator,SHARDS:DataShard,ALLOCATOR:PlanAllocator,MEDIATORS:PlanBucket}))env[binding]={idFromName:x=>x,get(name){const key=binding+':'+name;if(!stores.has(key))stores.set(key,storage());if(!actors.has(key))actors.set(key,new Type({storage:stores.get(key),id:name},env));return{async fetch(url,init){const call={binding,name,input:JSON.parse(init.body)};calls.push(call);await before(call);return actors.get(key).fetch(new Request(url,init));}};}};
 return {env,stores,calls,hook(fn){before=fn;},async post(input,options={}){const request=new Request('http://127.0.0.1'+(options.path||'/observe/api'),{method:options.method||'POST',headers:{'content-type':'application/json',authorization:`Bearer ${env.API_TOKEN}`,...options.headers},...((options.method||'POST')==='POST'?{body:options.raw??JSON.stringify(input)}:{})});const response=await Worker.fetch(request,env);return{status:response.status,headers:response.headers,body:await response.json()};},snapshot(){return structuredClone([...stores].map(([key,s])=>[key,[...s.map],s.alarm]));}};
}
test('observability independent: localhost still requires configured token, exact bearer and same origin before RPC',async()=>{
 const disabled=cluster({API_TOKEN:undefined});assert.equal((await disabled.post({action:'overview'})).body.code,'DIAGNOSTICS_DISABLED');assert.equal(disabled.calls.length,0);
 const db=cluster();for(const authorization of ['', 'Bearer wrong','bearer super-secret-bearer'])assert.equal((await db.post({action:'overview'},{headers:{authorization}})).status,401);
 assert.equal((await db.post({action:'overview'},{headers:{origin:'https://evil.example'}})).body.code,'ORIGIN_DENIED');assert.equal((await db.post({action:'overview'},{method:'GET'})).status,405);assert.equal(db.calls.length,0);
 const good=await db.post({action:'overview'},{headers:{origin:'http://127.0.0.1'}});assert.equal(good.status,200);assert.equal(good.headers.get('cache-control'),'no-store');
});
test('observability independent: malformed/mutating inputs reject before actor calls with bounded body',async()=>{
 const db=cluster();for(const input of [{action:'recover'},{action:'overview',sql:'secret'},{action:'table',table:'a',limit:9},{action:'table',table:'../a'},{action:'table',table:'__proto__'},{action:'transaction',transactionId:'secret-key'}])assert.equal((await db.post(input)).status,400);
 assert.equal((await db.post(null,{raw:'x'.repeat(4097)})).status,413);assert.equal((await db.post(null,{raw:'not json'})).status,400);assert.equal((await db.post({action:'overview'},{headers:{'content-type':'text/plain'}})).status,415);assert.equal(db.calls.length,0);
});
test('observability independent: real actor overview/table/transaction reads never mutate durable state or reveal row content',async()=>{
 const db=cluster(),secret='private-row-value-98342';
 const seed=await db.post({sql:`CREATE TABLE a(id TEXT PRIMARY KEY,payload TEXT); INSERT INTO a(id,payload) VALUES('${secret}','secret-payload-798')`,idempotencyKey:'secret-request-key'},{path:'/query'});assert.equal(seed.status,200,JSON.stringify(seed.body));
 const before=db.snapshot();db.calls.length=0;
 const overview=await db.post({action:'overview'}),table=await db.post({action:'table',table:'a',limit:8}),transaction=await db.post({action:'transaction',transactionId:seed.body.transactionId});
 for(const reply of [overview,table,transaction]){assert.equal(reply.status,200,JSON.stringify(reply.body));const encoded=JSON.stringify(reply.body);for(const forbidden of [secret,'secret-payload-798','secret-request-key','super-secret-bearer','INSERT INTO'])assert.ok(!encoded.includes(forbidden),forbidden);assert.ok(Buffer.byteLength(encoded)<=65536);}
 assert.ok(db.calls.every(c=>c.input.op==='diagnostics'));assert.ok(db.calls.length<=13);assert.deepEqual(db.snapshot(),before);assert.ok(table.body.topology.tablets.length<=8);
});
test('observability independent: isolate ring bounds codes and discards arbitrary payloads',()=>{
 const env={},other={};for(let i=0;i<150;i++)observeRequest(env,'/query',503,i,{code:'secret-error-code',sql:'secret-sql',params:{password:'secret'},rows:['secret'],routing:[{table:'safe'}],transactionId:'secret-tx'});
 const snapshot=metricsSnapshot(env);assert.equal(snapshot.observedRequests,150);assert.equal(snapshot.sampleCount,128);assert.equal(snapshot.recent.length,32);assert.equal(snapshot.codeCounts.OTHER,128);assert.equal(metricsSnapshot(other).observedRequests,0);assert.ok(!JSON.stringify(snapshot).includes('secret'));
});
test('observability independent: overview admission exhaustion never performs extra actor RPC',async()=>{
 const db=cluster();for(let i=0;i<24;i++)assert.equal((await db.post({action:'overview'})).status,200);
 const before=db.calls.length,reply=await db.post({action:'overview'});assert.equal(reply.status,429);assert.equal(reply.body.code,'DIAGNOSTICS_BACKPRESSURE');assert.equal(db.calls.length,before);
});

test('observability independent: large real topology samples only eight tablets and never scans row data',async()=>{
 const db=cluster({TABLET_SPLIT_BYTES:'200',TABLET_MERGE_BYTES:'100'}),rows=Array.from({length:48},(_,i)=>`(${i},'${'x'.repeat(60)}')`),body={sql:`CREATE TABLE many(id INTEGER PRIMARY KEY,payload TEXT); INSERT INTO many(id,payload) VALUES ${rows.join(',')}`,idempotencyKey:'large-topology'};
 let result;
 for(let i=0;i<128;i++){
  result=await db.post(body,{path:'/query'});if(result.status===200)break;assert.equal(result.body.code,'PLAN_PENDING',JSON.stringify(result.body));
  const rpc=async(binding,name,input)=>{const r=await db.env[binding].get(name).fetch('https://background',{method:'POST',body:JSON.stringify(input)});assert.equal(r.status,200);return r.json();};
  const found=await rpc('ALLOCATOR','global',{op:'lookup',id:result.body.transactionId});await rpc('MEDIATORS',found.association.bucket,{op:'drive',id:result.body.transactionId});
 }
 assert.equal(result.status,200);assert.ok(result.body.routing[0].tablets.length>8);const before=db.snapshot();db.calls.length=0;
 const reply=await db.post({action:'table',table:'many'});assert.equal(reply.status,200,JSON.stringify(reply.body));assert.equal(reply.body.topology.tablets.length,8);assert.equal(reply.body.topology.nextOffset,8);assert.equal(db.calls.length,10);assert.ok(db.calls.every(c=>c.input.op==='diagnostics'));assert.deepEqual(db.snapshot(),before);
});

test('observability independent: timed-out reads retain active admission until settlement and cannot fan out later',async()=>{
 const db=cluster({DIAGNOSTICS_TIMEOUT_MS:100});const seed=await db.post({sql:'CREATE TABLE a(id INTEGER PRIMARY KEY)',idempotencyKey:'timeout-seed'},{path:'/query'});assert.equal(seed.status,200);
 let release;const held=new Promise(r=>release=r);let arrived=0;
 db.calls.length=0;db.hook(async call=>{if(call.binding==='ALLOCATOR'&&call.input.op==='diagnostics'){arrived++;await held;}});
 const input={action:'transaction',transactionId:seed.body.transactionId};
 try{
  const replies=await Promise.all(Array.from({length:4},()=>db.post(input)));
  assert.equal(arrived,4);for(const reply of replies){assert.equal(reply.status,504,JSON.stringify(reply.body));assert.equal(reply.body.code,'DIAGNOSTICS_TIMEOUT');}
  assert.equal(db.calls.length,4);const denied=await db.post({action:'overview'});assert.equal(denied.status,429);assert.equal(denied.body.code,'DIAGNOSTICS_BACKPRESSURE');assert.equal(db.calls.length,4);
 }finally{release();}
 // Allow the actual allocator reads to settle and release their slots. No
 // mediator progress read may follow a transaction lookup after its deadline.
 await new Promise(resolve=>setTimeout(resolve,0));
 assert.equal(db.calls.length,4);assert.ok(db.calls.every(c=>c.binding==='ALLOCATOR'));
 db.hook(async()=>{});const recovered=await db.post({action:'overview'});assert.equal(recovered.status,200,JSON.stringify(recovered.body));assert.equal(db.calls.length,5);
});
