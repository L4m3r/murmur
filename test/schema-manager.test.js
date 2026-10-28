import test from 'node:test';
import assert from 'node:assert/strict';
import {SchemaManager} from '../src/schema-manager.js';
import {encodeKey} from '../src/keys.js';
function storage(){const values=new Map();let alarm;const api={async get(k){return structuredClone(values.get(k));},async put(k,v){values.set(k,structuredClone(v));},async delete(k){return values.delete(k);},async transaction(fn){const backup=structuredClone(values);try{return await fn(api);}catch(e){values.clear();for(const[k,v]of backup)values.set(k,v);throw e;}},async setAlarm(v){alarm=v;},async deleteAlarm(){alarm=null;}};return api;}
function fixture({count=130,duplicate=false,lostInstall=false,installPhantom=false,lostUnpin=false,batchConflict=false,currentUpdate=false,lostTopology=false}={}){
 let step=1,actor;const history=new Map(),pins=new Map(),closed=new Set(),calls=[],receipts=new Map(),metas=new Map();let lost=lostInstall,unpinLost=lostUnpin,conflict=batchConflict,topologyLost=lostTopology;
 const schema={primaryKey:'id',primaryKeys:['id'],columns:[{name:'id',type:'INTEGER',primaryKey:true,notNull:true},{name:'tag',type:'TEXT'}],epoch:1,tablets:[{id:'t0',start:null,end:null}],indexes:[]};
 function put(name,key,row,at=step){if(!history.has(name))history.set(name,new Map());const map=history.get(name),k=encodeKey(key);if(!map.has(k))map.set(k,{key,versions:[]});map.get(k).versions.push({step:at,row:structuredClone(row)});}
 function rows(name,snapshot){return [...(history.get(name)?.values()||[])].map(v=>({key:v.key,row:v.versions.filter(x=>x.step<=snapshot).at(-1)?.row})).filter(v=>v.row!=null);}
 put('catalog/a','schema',schema);for(let id=0;id<count;id++)put('base/a/t0',id,{id,tag:duplicate?'same':`v${id}`});
 const namespace=fn=>({idFromName:n=>n,get:name=>({fetch:async(url,init)=>fn(name,JSON.parse(init.body))})});
 const env={ALLOCATOR:namespace(async(name,input)=>{
  calls.push({kind:'mediator',input});if(input.op==='epoch')return Response.json({rejectBeforeEpoch:1});
  if(input.op==='pin'){if(closed.has(input.id))return Response.json({error:'closed'},{status:409});const pin=pins.get(input.id)||{...input,snapshot:input.snapshot??step};pins.set(input.id,pin);return Response.json(pin);}
  if(input.op==='unpin'){pins.delete(input.id);closed.add(input.id);if(unpinLost){unpinLost=false;return Response.json({error:'lost unpin response'},{status:503});}return Response.json({unpinned:true});}
  throw Error(input.op);
 }),SHARDS:namespace(async(name,input)=>{
  calls.push({kind:'shard',name,input});if(input.op==='stats'){const current=rows(name,step);return Response.json({rows:current.length,bytes:Buffer.byteLength(JSON.stringify(current.map(v=>v.row)))});}let values=rows(name,input.snapshot);
  if(input.op==='getMany')values=values.filter(v=>input.keys.some(k=>encodeKey(k)===encodeKey(v.key)));
  else{const r=input.range;values=values.filter(v=>{const k=encodeKey(v.key);return(!input.cursor||k>input.cursor)&&(r.lower===null||k>r.lower||(k===r.lower&&r.lowerInclusive))&&(r.upper===null||k<r.upper||(k===r.upper&&r.upperInclusive));}).sort((a,b)=>encodeKey(a.key)<encodeKey(b.key)?-1:1);}
  const page=values.slice(0,input.pageSize||256),nextCursor=page.length<values.length?encodeKey(page.at(-1).key):null;
  return Response.json(input.op==='scanKeys'?{keys:page.map(v=>v.key),nextCursor}:{rows:page.map(v=>v.row),nextCursor});
 }),COORDINATOR:namespace(async(name,input)=>{
  calls.push({kind:'raw',name,input});if(receipts.has(input.idempotencyKey))return Response.json(receipts.get(input.idempotencyKey));
  const installing=input.targets.some(t=>t.name==='catalog/a'&&t.changes.some(c=>c.row.indexes?.some(i=>i.state==='building')));
  if(conflict&&!input.targets.some(t=>t.name==='catalog/a'&&t.changes.length)){conflict=false;return Response.json({error:'injected known abort',retryable:true},{status:409});}
  if(installing&&installPhantom)put('base/a/t0',999,{id:999,tag:'phantom'},++step);
  step++;for(const target of input.targets){for(const change of target.changes)put(target.name,change.key,change.row);if(target.meta)metas.set(target.name,target.meta);}
  const result={...input.response,step,version:step};if(installing&&currentUpdate)put('base/a/t0',0,{id:0,tag:'current-not-historical'},++step);receipts.set(input.idempotencyKey,result);
  if(topologyLost&&input.targets.some(t=>t.meta?.retired)){topologyLost=false;return Response.json({error:'lost topology receipt'},{status:503});}
  if(installing&&lost){lost=false;return Response.json({error:'lost commit response'},{status:503});}return Response.json(result);
 })};
 let state={storage:storage()};actor=new SchemaManager(state,env);
 return{env,calls,pins,metas,alarm:()=>actor.alarm(),snapshot:()=>step,historicalSchema:snapshot=>rows('catalog/a',snapshot)[0].row,baseRows:(name='a',snapshot=step)=>rows(`base/${name}/t0`,snapshot).map(v=>v.row),replaceTable(next,data=[]){put('catalog/a','schema',next,++step);for(const row of data)put(`base/${next.storageName||'a'}/t0`,row.id,row);},freshJob(){state={storage:storage()};actor=new SchemaManager(state,env);},loseNextRetirement(){topologyLost=true;},replaceIndex(index){const schema=rows('catalog/a',step)[0].row;put('catalog/a','schema',{...schema,indexes:[index]},++step);},setTag(id,tag){put('base/a/t0',id,{id,tag},++step);},latest:()=>rows('catalog/a',step)[0].row,entries(){const schema=rows('catalog/a',step)[0].row;return(schema.indexes||[]).flatMap(index=>index.tablets.flatMap(t=>rows(`index/${schema.storageName||'a'}/${index.storageName||index.name}/${t.id}`,step).map(v=>v.row)));},restart(){actor=new SchemaManager(state,env);},async request(op='resume',extra={}){const response=await actor.fetch(new Request('https://schema',{method:'POST',body:JSON.stringify({op,...extra})}));return{status:response.status,body:await response.json()};},start(extra={}){return this.request('start',{id:'job1',epoch:0,table:'a',index:'tag_idx',column:'tag',unique:duplicate,...extra});},async finish(){let r;for(let n=0;n<100;n++){r=await this.request();if(r.status===200)return r;assert.equal(r.status,202,JSON.stringify(r));}throw Error('did not finish');}};
}
test('online build resumes bounded pages and pins install commit cut, not preinstall snapshot',async()=>{
 const f=fixture({installPhantom:true});assert.equal((await f.start()).status,202);f.restart();const done=await f.finish();
 assert.equal(done.body.job.state,'ready');assert.equal(done.body.job.keys,131);assert.equal(f.entries().length,131);
 const scans=f.calls.filter(c=>c.kind==='shard'&&c.input.op==='scanKeys');assert.ok(scans.length>=3);assert.ok(scans.every(c=>c.input.pageSize<=64&&c.input.snapshot===done.body.job.snapshot));
 assert.ok(done.body.job.snapshot>1);assert.ok(f.entries().some(r=>r.pk===999));assert.equal(f.pins.size,0);assert.equal(f.latest().indexes[0].state,'ready');
});
test('unknown install outcome repeats identical persisted stage after actor restart',async()=>{
 const f=fixture({count:3,lostInstall:true});assert.equal((await f.start()).status,503);f.restart();const done=await f.finish();assert.equal(done.body.job.state,'ready');
 const raw=f.calls.filter(c=>c.kind==='raw');assert.deepEqual(raw[0].input,raw[1].input);assert.equal(f.entries().length,3);assert.equal(f.pins.size,0);
});
test('unique duplicate build fails with exact generation removed and retired, base untouched',async()=>{
 const f=fixture({count:3,duplicate:true});await f.start();const done=await f.finish();assert.equal(done.body.job.state,'failed');assert.match(done.body.job.error,/Unique index/);assert.deepEqual(f.latest().indexes,[]);assert.equal(f.pins.size,0);assert.ok([...f.metas.values()].some(meta=>meta.retired));
 const changes=f.calls.filter(c=>c.kind==='raw').flatMap(c=>c.input.targets).filter(t=>t.name.startsWith('base/')).flatMap(t=>t.changes);assert.deepEqual(changes,[]);
});
test('cancellation resolves active stage then cleans up without base deletion',async()=>{
 const f=fixture({count:130});await f.start();await f.request('cancel');const done=await f.finish();assert.equal(done.body.job.state,'cancelled');assert.deepEqual(f.latest().indexes,[]);assert.equal(f.pins.size,0);
});
test('invalid schema fails without installed generation, and completed jobs collect only retired epoch',async()=>{
 const f=fixture({count:1});await f.start({column:'missing'});const done=await f.finish();assert.equal(done.body.job.state,'failed');assert.equal(f.pins.size,0);
 assert.equal((await f.request('collect',{epoch:0})).status,200);assert.equal((await f.request('status')).status,404);
});

test('lost pin release response survives restart without reusing closed planning pin',async()=>{
 const f=fixture({count:3,lostUnpin:true});const first=await f.start();assert.equal(first.status,503);f.restart();const done=await f.finish();assert.equal(done.body.job.state,'ready');assert.equal(f.pins.size,0);
});
test('known aborted batch rebuilds under new durable identity without advancing cursor',async()=>{
 const f=fixture({count:3,batchConflict:true});await f.start();const done=await f.finish();assert.equal(done.body.job.state,'ready');assert.equal(done.body.job.keys,3);assert.equal(f.entries().length,3);
 const batches=f.calls.filter(c=>c.kind==='raw'&&!c.input.targets.some(t=>t.name==='catalog/a'&&t.changes.length));assert.ok(batches.length>=2);assert.notEqual(batches[0].input.idempotencyKey,batches[1].input.idempotencyKey);
});
test('backfill enumerates old cut but indexes current values, never historical values',async()=>{
 const f=fixture({count:3,currentUpdate:true});await f.start();const done=await f.finish();assert.equal(done.body.job.state,'ready');assert.equal(f.entries().find(r=>r.pk===0).value,'current-not-historical');assert.ok(!f.entries().some(r=>r.value==='v0'));
});

test('backfill splits index tablets before READY and preserves exact entry cardinality',async()=>{
 const f=fixture({count:130});f.env.TABLET_SPLIT_BYTES='400';f.env.TABLET_MERGE_BYTES='100';await f.start();const done=await f.finish();
 assert.equal(done.body.job.state,'ready');const index=f.latest().indexes[0];assert.ok(index.tablets.length>4);assert.ok(index.tablets.every(t=>Number.isSafeInteger(t.epoch)));
 const entries=f.entries();assert.equal(entries.length,130);assert.equal(new Set(entries.map(e=>e.pk)).size,130);
 const topology=f.calls.filter(c=>c.kind==='raw'&&c.input.targets.some(t=>t.meta?.retired));assert.ok(topology.length>1);
 for(const stage of topology)assert.ok(!stage.input.targets.some(t=>t.meta&&!t.meta.retired&&!t.changes.length),'No untouched metadata fanout');
});
test('unknown committed topology copy replays exact stage after restart',async()=>{
 const f=fixture({count:130,lostTopology:true});f.env.TABLET_SPLIT_BYTES='400';f.env.TABLET_MERGE_BYTES='100';await f.start();let r;
 for(let n=0;n<20;n++){r=await f.request();if(r.status===503)break;}assert.equal(r.status,503);f.restart();const done=await f.finish();assert.equal(done.body.job.state,'ready');assert.equal(f.entries().length,130);
 const copies=f.calls.filter(c=>c.kind==='raw'&&c.input.targets.some(t=>t.meta?.retired));assert.deepEqual(copies[0].input,copies[1].input);
});
test('tablet capacity preserves bounded writes and exposes pressure rather than scanning oversized source',async()=>{
 const f=fixture({count:130});f.env.TABLET_SPLIT_BYTES='100';f.env.TABLET_MERGE_BYTES='20';f.env.TABLET_MAX_COUNT='1';await f.start();const done=await f.finish();assert.equal(done.body.job.state,'ready');assert.equal(f.entries().length,130);assert.ok(done.body.job.maintenancePressure.some(p=>p.reason==='tablet-capacity'));assert.ok(!f.calls.some(c=>c.kind==='shard'&&c.input.op==='scan'&&c.name.startsWith('index/')));
});
test('online build stores bounded index sizing without rejecting large base-table thresholds',async()=>{
 const f=fixture({count:1});f.env.TABLET_SPLIT_BYTES=String(1024*1024);await f.start();const done=await f.finish();assert.equal(done.body.job.state,'ready');assert.equal(f.latest().indexes[0].sizing.splitBytes,256*1024);
});

test('large build stays within participant budget and cleanup retires generation in bounded stages',async()=>{
 const f=fixture({count:130});f.env.TABLET_SPLIT_BYTES='40';f.env.TABLET_MERGE_BYTES='10';await f.start();
 for(let n=0;n<100&&f.latest().indexes[0].tablets.length<65;n++){const r=await f.request();assert.ok([200,202].includes(r.status),JSON.stringify(r));}
 assert.ok(f.latest().indexes[0].tablets.length>64);await f.request('cancel');const done=await f.finish();assert.equal(done.body.job.state,'cancelled');assert.deepEqual(f.latest().indexes,[]);assert.equal(f.pins.size,0);
 const raw=f.calls.filter(c=>c.kind==='raw');assert.ok(raw.every(c=>c.input.targets.length<=64));
 const retired=raw.filter(c=>c.input.targets.some(t=>t.meta?.retired)&&c.input.targets.some(t=>t.name==='catalog/a'));
 assert.ok(retired.length>2);
});

test('UNIQUE validation scans complete frozen index across tablet boundaries and fails late duplicates',async()=>{
 const f=fixture({count:130});f.env.TABLET_SPLIT_BYTES='400';f.env.TABLET_MERGE_BYTES='100';f.setTag(0,'dupe');f.setTag(129,'dupe');
 await f.start({unique:true});const done=await f.finish();assert.equal(done.body.job.state,'failed');assert.match(done.body.job.error,/validation snapshot/);assert.equal(f.latest().indexes.length,0);assert.equal(f.pins.size,0);
 const validation=f.calls.filter(c=>c.kind==='raw'&&c.input.targets.some(t=>t.name==='catalog/a'&&t.changes.some(c=>c.row.indexes?.some(i=>i.state==='validating'))));assert.equal(validation.length,1);
 assert.ok(done.body.job.validationSnapshot>done.body.job.snapshot);
 const pinV=f.calls.findIndex(c=>c.kind==='mediator'&&c.input.op==='pin'&&c.input.id.endsWith('-V'));
 const unpinB=f.calls.findIndex(c=>c.kind==='mediator'&&c.input.op==='unpin'&&c.input.id.endsWith('-B'));
 assert.ok(pinV>=0&&unpinB>pinV);
});
test('READY follows durable VALIDATING transition and paged scan at its exact cut',async()=>{
 const f=fixture({count:130});f.env.TABLET_SPLIT_BYTES='400';f.env.TABLET_MERGE_BYTES='100';await f.start({unique:true});const done=await f.finish();assert.equal(done.body.job.state,'ready');
 const scans=f.calls.filter(c=>c.kind==='shard'&&c.input.op==='scan'&&c.input.snapshot===done.body.job.validationSnapshot);assert.ok(scans.length>2);assert.equal(f.pins.size,0);
});
test('resumable DROP retires large captured generation and never removes same-name replacement',async()=>{
 const f=fixture({count:130});f.env.TABLET_SPLIT_BYTES='40';f.env.TABLET_MERGE_BYTES='10';await f.start();await f.finish();const original=f.latest().indexes[0];assert.ok(original.tablets.length>96);
 f.freshJob();const started=await f.request('start',{kind:'dropIndex',id:'drop1',epoch:0,table:'a',index:'tag_idx'});assert.equal(started.status,202);assert.equal(f.latest().indexes.length,0);
 const replacement={...original,storageName:'replacement',jobId:'replacement-job',tablets:[{id:'r0',start:null,end:null,epoch:1}],epoch:1};f.replaceIndex(replacement);f.restart();const done=await f.finish();assert.equal(done.body.job.state,'dropped');assert.equal(f.latest().indexes[0].storageName,'replacement');assert.equal(f.pins.size,0);
 assert.ok(!f.calls.filter(c=>c.kind==='raw').flatMap(c=>c.input.targets).some(t=>t.name==='index/a/replacement/r0'));
});
test('DROP lost first retirement receipt replays exact manifest before advancing cursor',async()=>{
 const f=fixture({count:10});await f.start();await f.finish();f.freshJob();f.loseNextRetirement();const start=await f.request('start',{kind:'dropIndex',id:'drop-lost',epoch:0,table:'a',index:'tag_idx'});assert.equal(start.status,503);f.restart();const done=await f.finish();assert.equal(done.body.job.state,'dropped');assert.equal(f.pins.size,0);
 const raw=f.calls.filter(c=>c.kind==='raw'&&c.input.targets.some(t=>t.meta?.retired));assert.deepEqual(raw.at(-2).input,raw.at(-1).input);
});

test('DROP TABLE retains tombstone generation and historical schema while retiring metadata only',async()=>{
 const f=fixture({count:3}),old=f.latest(),snapshot=f.snapshot();const start=await f.request('start',{kind:'dropTable',id:'table-drop',epoch:0,table:'a'});assert.ok([200,202].includes(start.status));const done=await f.finish();
 assert.equal(done.body.job.state,'dropped');assert.equal(f.latest().state,'dropped');assert.equal(f.latest().generation,0);assert.equal(f.latest().storageName,'a');assert.deepEqual(f.latest().columns,old.columns);
 assert.deepEqual(f.historicalSchema(snapshot),old);assert.equal(f.baseRows().length,3);assert.equal(f.pins.size,0);
 assert.ok(!f.calls.filter(c=>c.kind==='raw').flatMap(c=>c.input.targets).filter(t=>t.name.startsWith('base/')).some(t=>t.changes.length));
});
test('large DROP TABLE cleanup survives recreation and cannot retire fresh base/index namespace',async()=>{
 const f=fixture({count:130});f.env.TABLET_SPLIT_BYTES='40';f.env.TABLET_MERGE_BYTES='10';await f.start();await f.finish();const original=f.latest();assert.ok(original.indexes[0].tablets.length>96);f.freshJob();
 const started=await f.request('start',{kind:'dropTable',id:'wide-table-drop',epoch:0,table:'a'});assert.equal(started.status,202);assert.equal(f.latest().state,'dropped');
 const fresh={...original,generation:1,storageName:'a~g1',epoch:1,tablets:[{id:'t0',start:null,end:null,epoch:1}],indexes:[]};delete fresh.state;f.replaceTable(fresh,[{id:0,tag:'new-generation'}]);f.restart();const done=await f.finish();
 assert.equal(done.body.job.state,'dropped');assert.equal(f.latest().storageName,'a~g1');assert.deepEqual(f.baseRows('a~g1'),[{id:0,tag:'new-generation'}]);assert.equal(f.pins.size,0);
 const targets=f.calls.filter(c=>c.kind==='raw').flatMap(c=>c.input.targets);assert.ok(!targets.some(t=>t.name.startsWith('base/a~g1/')||t.name.startsWith('index/a~g1/')));
});
test('index build on recreated table exclusively uses its generation namespace',async()=>{
 const f=fixture({count:2}),fresh={...f.latest(),generation:1,storageName:'a~g1'};f.replaceTable(fresh,[{id:5,tag:'fresh'}]);await f.start();const done=await f.finish();assert.equal(done.body.job.state,'ready');assert.deepEqual(f.entries(),[{value:'fresh',pk:5}]);
 const scans=f.calls.filter(c=>c.kind==='shard'&&c.input.op==='scanKeys');assert.ok(scans.length);assert.ok(scans.every(c=>c.name.startsWith('base/a~g1/')));
 assert.ok(f.calls.filter(c=>c.kind==='raw').flatMap(c=>c.input.targets).filter(t=>t.name.startsWith('index/')).every(t=>t.name.startsWith('index/a~g1/')));
});
test('old index job fails generation fence after DROP/recreate without mutating replacement',async()=>{
 const f=fixture({count:3});await f.start();const fresh={...f.latest(),generation:1,storageName:'a~g1',indexes:[]};f.replaceTable(fresh,[{id:1,tag:'new'}]);const done=await f.finish();assert.equal(done.body.job.state,'failed');assert.match(done.body.job.error,/generation changed/);assert.equal(f.latest().generation,1);assert.deepEqual(f.latest().indexes,[]);assert.equal(f.pins.size,0);
 assert.ok(!f.calls.filter(c=>c.kind==='raw').flatMap(c=>c.input.targets).some(t=>t.name.startsWith('base/a~g1/')||t.name.startsWith('index/a~g1/')));
});

test('terminal failed admission self-collects after epoch retirement even without registered owner',async()=>{
 const f=fixture({count:1}),get=f.env.ALLOCATOR.get;let expired=false;
 f.env.ALLOCATOR.get=name=>{const stub=get(name);return{async fetch(url,init){const input=JSON.parse(init.body);if(input.op==='pin'&&!expired){expired=true;return Response.json({error:'epoch retired before first pin',code:'REQUEST_EXPIRED',retryable:false},{status:410});}return stub.fetch(url,init);}};};
 const result=await f.start();assert.equal(result.status,200,JSON.stringify(result));assert.equal(result.body.job.state,'failed');assert.equal(f.pins.size,0);f.restart();await f.alarm();
 assert.equal((await f.request('status')).status,404);
 const replay=await f.start();assert.equal(replay.status,410);assert.equal(replay.body.code,'REQUEST_EXPIRED');
});

test('cleanup commits at most one four-retiree stage per call with exact total stage counts',async()=>{
 for(const kind of ['dropIndex','dropTable']){
  const f=fixture({count:1}),index={name:'tag_idx',column:'tag',storageName:'captured',state:'ready',epoch:1,tablets:Array.from({length:13},(_,n)=>({id:`p${n}`,epoch:1,start:null,end:null}))};f.replaceIndex(index);
  let before=0,r=await f.request('start',{kind,id:`bounded-${kind}`,epoch:0,table:'a',index:'tag_idx'});
  const stages=()=>f.calls.filter(c=>c.kind==='raw');
  assert.equal(stages().length,1);assert.equal(kind==='dropTable'?f.latest().state:f.latest().indexes.length,kind==='dropTable'?'dropped':0);
  for(let n=0;r.body.job.state!=='dropped'&&n<20;n++){
   before=stages().length;f.restart();r=await f.request();assert.ok(stages().length-before<=1,'one cleanup transaction per resume');
  }
  assert.equal(r.body.job.state,'dropped');
  const raw=stages(),names=raw.flatMap(c=>c.input.targets.filter(t=>t.meta?.retired).map(t=>t.name));
  assert.equal(raw.length,Math.ceil((13+(kind==='dropTable'?1:0))/4));assert.equal(new Set(names).size,names.length);
  assert.equal(names.length,kind==='dropTable'?14:13);assert.ok(raw.every(c=>c.input.targets.length<=5));
  assert.equal(raw.filter(c=>c.input.targets.some(t=>t.name==='catalog/a'&&t.changes.length)).length,1);assert.equal(f.pins.size,0);
 }
});

test('cleanup byte cap bounds retirement manifest independently of atomic catalog mutation',async()=>{
 const f=fixture({count:1});f.env.SCHEMA_CLEANUP_TABLETS='16';f.env.SCHEMA_CLEANUP_BYTES='1024';
 f.replaceIndex({name:'tag_idx',column:'tag',storageName:'captured',state:'ready',epoch:1,tablets:Array.from({length:7},(_,n)=>({id:`${'p'.repeat(500)}${n}`,epoch:1,start:null,end:null}))});
 await f.request('start',{kind:'dropIndex',id:'byte-page',epoch:0,table:'a',index:'tag_idx'});assert.equal((await f.finish()).body.job.state,'dropped');
 const raw=f.calls.filter(c=>c.kind==='raw');assert.equal(raw.length,7);
 for(const stage of raw)assert.ok(Buffer.byteLength(JSON.stringify(stage.input.targets.filter(t=>t.meta?.retired)))<=1024);
});

test('cleanup policy validates configuration before persisting or admitting work',async()=>{
 for(const [name,value] of [['SCHEMA_CLEANUP_TABLETS','0'],['SCHEMA_CLEANUP_TABLETS','17'],['SCHEMA_CLEANUP_TABLETS','1.5'],['SCHEMA_CLEANUP_BYTES','1023'],['SCHEMA_CLEANUP_BYTES','65537'],['SCHEMA_CLEANUP_BYTES','NaN']]){
  const f=fixture({count:1});f.env[name]=value;const r=await f.request('start',{kind:'dropTable',id:'invalid-policy',epoch:0,table:'a'});assert.equal(r.status,400);assert.equal((await f.request('status')).status,404);assert.equal(f.calls.length,0);
 }
});
