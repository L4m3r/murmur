import test from 'node:test';
import assert from 'node:assert/strict';
import {QueryCursor} from '../src/query-cursor.js';
import {DataShard} from '../src/datashard.js';
import {PlanAllocator} from '../src/mediator.js';
import {routeTablet} from '../src/tablets.js';
import {encodeKey} from '../src/keys.js';
import {readRecord,writeRecord} from '../src/records.js';
function storage(){const values=new Map();const api={values,async get(k){return structuredClone(values.get(k));},async put(k,v){values.set(k,structuredClone(v));},async delete(k){return values.delete(k);},async list({prefix='',start,end,reverse=false,limit}={}){let a=[...values].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:1);if(reverse)a.reverse();return new Map((limit?a.slice(0,limit):a).map(([k,v])=>[k,structuredClone(v)]));},async transaction(fn){const backup=structuredClone(values);try{return await fn(api);}catch(e){values.clear();for(const [k,v]of backup)values.set(k,v);throw e;}},async setAlarm(at){api.alarm=at;},async deleteAlarm(){api.alarm=null;}};return api;}
async function fixture({count=40,index=false,physical='a',cachePages=8}={}){
 let now=10000,lostUnpin=false,lostRenew=null,gate=null;const calls=[],shards=new Map(),control=storage(),cursorStore=storage();
 const env={NOW:()=>now,CURSOR_MAX_CACHE_PAGES:cachePages};const allocator=new PlanAllocator({storage:control},env);allocator.now=()=>now;
 await control.put('allocated',1);await control.put('frontier',1);
 const namespace=fn=>({idFromName:x=>x,get:name=>({fetch:async(url,init)=>fn(name,JSON.parse(init.body))})});
 env.ALLOCATOR=namespace(async(name,input)=>{calls.push({kind:'control',input});if(input.op==='renewLease'&&lostRenew==='before'){lostRenew=null;return Response.json({error:'renewal not delivered'},{status:503});}const result=await allocator.fetch(new Request('https://allocator',{method:'POST',body:JSON.stringify(input)}));if(input.op==='renewLease'&&lostRenew==='after'){lostRenew=null;return Response.json({error:'renewal reply lost'},{status:503});}if(input.op==='unpin'&&lostUnpin){lostUnpin=false;return Response.json({error:'lost release response'},{status:503});}return result;});
 env.SHARDS=namespace(async(name,input)=>{calls.push({kind:'shard',name,input});if(gate&&name===gate.name&&input.op==='scan'){const wait=gate;gate=null;wait.entered();await wait.promise;}if(!shards.has(name))shards.set(name,new DataShard({storage:storage(),id:name},env));return shards.get(name).fetch(new Request('https://shard',{method:'POST',body:JSON.stringify(input)}));});
 const schema={primaryKey:'id',primaryKeys:['id'],storageName:physical,columns:[{name:'id',type:'INTEGER',primaryKey:true,notNull:true},{name:'tag',type:'TEXT'},{name:'keep',type:'BOOLEAN'},{name:'payload',type:'TEXT'}],epoch:1,tablets:[{id:'t0',start:null,end:20,epoch:1},{id:'t1',start:20,end:null,epoch:1}],indexes:index?[{name:'tag_idx',column:'tag',keyColumns:['value','pk'],epoch:1,state:'ready',tablets:[{id:'i0',start:null,end:['b',20],epoch:1},{id:'i1',start:['b',20],end:null,epoch:1}]}]:[]};
 async function seed(name,changes,step=1,meta){if(!shards.has(name))shards.set(name,new DataShard({storage:storage(),id:name},env));const actor=shards.get(name);await actor.storage.transaction(txn=>actor.appendVersions(txn,{changes:changes.map(c=>({...c,encoded:encodeKey(c.key)})),meta},step));}
 await seed('catalog/a',[{key:'schema',row:schema}]);
 for(const t of schema.tablets)await seed(`base/${physical}/${t.id}`,[],1,{epoch:1,retired:false});
 for(const t of schema.indexes[0]?.tablets||[])await seed(`index/${physical}/tag_idx/${t.id}`,[],1,{epoch:1,retired:false});
 const rows=Array.from({length:count},(_,id)=>({id,tag:['a','b','c'][id%3],keep:id%2===0,payload:`p${id}`}));
 for(const row of rows){await seed(`base/${physical}/${routeTablet(schema,row.id)}`,[{key:row.id,row}]);if(index){const entry={value:row.tag,pk:row.id};await seed(`index/${physical}/tag_idx/${routeTablet(schema.indexes[0],[row.tag,row.id])}`,[{key:[row.tag,row.id],row:entry}]);}}
 let cursor=new QueryCursor({storage:cursorStore},env);
 const request=async body=>{const r=await cursor.fetch(new Request('https://cursor',{method:'POST',body:JSON.stringify(body)}));return {status:r.status,body:await r.json()};};
 return {env,rows,calls,control,cursorStore,shards,seed,schema,allocator,request,start:extra=>request({op:'start',id:'q1',epoch:0,sql:'SELECT id FROM a ORDER BY id',pageSize:4,ttlMs:1000,...extra}),restart(){cursor=new QueryCursor({storage:cursorStore},env);},tick(n){now+=n;},loseUnpin(){lostUnpin=true;},loseRenew(where='after'){lostRenew=where;},alarm:()=>cursor.alarm(),async update(row){await control.put('allocated',2);await control.put('frontier',2);await seed(`base/${physical}/${routeTablet(schema,row.id)}`,[{key:row.id,row}],2);},hold(name){let release,entered;const promise=new Promise(r=>release=r),waiting=new Promise(r=>entered=r);gate={name,promise,entered};return{release,waiting};}};
}

test('leased cursor pages primary ranges at one snapshot; retries survive restart and later writes',async()=>{
 const f=await fixture({physical:'a~g1'}),start=await f.start({sql:'SELECT id FROM a WHERE id >= 10 AND id < 30 AND keep = true ORDER BY id DESC',pageSize:3});assert.equal(start.status,200);assert.equal(await f.control.get('pinCount'),0);
 const first=await f.request({op:'page',token:start.body.token});assert.equal(first.status,200);assert.deepEqual(first.body.rows,[{id:28},{id:26},{id:24}]);
 await f.update({id:28,tag:'z',keep:false,payload:'changed'});f.restart();assert.deepEqual((await f.request({op:'page',token:start.body.token})).body,first.body);
 let token=first.body.nextToken,rows=[...first.body.rows],pages=0;
 while(token){const page=await f.request({op:'page',token});assert.equal(page.status,200);assert.ok(page.body.plan.shardCalls<=8);rows.push(...page.body.rows);token=page.body.nextToken;if(++pages>20)throw Error('no progress');}
 assert.deepEqual(rows.map(r=>r.id),[28,26,24,22,20,18,16,14,12,10]);assert.equal(await f.control.get('pinCount'),0);
 assert.ok(f.calls.filter(c=>c.kind==='shard'&&c.name.startsWith('base/')).every(c=>c.name.startsWith('base/a~g1/')&&c.input.snapshot===1));
 assert.equal((await f.request({op:'page',token:start.body.token+'x'})).status,400);
 const [payload,signature]=start.body.token.split('.'),body=JSON.parse(atob(payload.replace(/-/g,'+').replace(/_/g,'/'))),reordered=Object.fromEntries(Object.entries(body).reverse());
 const alternate=btoa(JSON.stringify(reordered)).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')+'.'+signature;assert.equal((await f.request({op:'page',token:alternate})).status,400);
});

test('index cursor preserves tuple order and residual LIMIT across bounded lookup pages',async()=>{
 const f=await fixture({index:true}),start=await f.start({sql:"SELECT id, tag FROM a WHERE tag >= 'a' AND keep = true ORDER BY tag DESC LIMIT 13",pageSize:5});assert.equal(start.status,200);
 let token=start.body.token,rows=[],count=0,last;
 while(token){last=await f.request({op:'page',token});assert.equal(last.status,200);assert.ok(last.body.plan.shardCalls<=8);rows.push(...last.body.rows);await f.request({op:'ack',throughSeq:last.body.page});token=last.body.nextToken;if(++count>30)throw Error('no progress');}
 const expected=f.rows.filter(r=>r.keep).sort((a,b)=>encodeKey([a.tag,a.id])<encodeKey([b.tag,b.id])?1:-1).slice(0,13).map(({id,tag})=>({id,tag}));assert.deepEqual(rows,expected);assert.equal(last.body.done,true);
});

test('ACK watermark frees bounded receipt window, survives restart, and permanently expires old tokens',async()=>{
 const f=await fixture({count:20,cachePages:2}),s=await f.start({pageSize:2});const p0=await f.request({op:'page',token:s.body.token}),p1=await f.request({op:'page',token:p0.body.nextToken});
 assert.equal((await f.request({op:'page',token:p1.body.nextToken})).status,429);
 assert.equal((await f.request({op:'ack',throughSeq:99})).status,400);
 assert.equal((await f.request({op:'ack',throughSeq:0})).status,200);f.restart();assert.equal((await f.request({op:'ack',throughSeq:0})).status,200);
 const expired=await f.request({op:'page',token:s.body.token});assert.equal(expired.status,410);assert.equal(expired.body.code,'PAGE_EXPIRED');
 assert.equal((await f.request({op:'page',token:p1.body.nextToken})).status,200);assert.equal(await f.control.get('pinCount'),0);
});

test('response receipt and release intent repair lost unpin reply without changing page',async()=>{
 const f=await fixture(),s=await f.start();f.loseUnpin();assert.equal((await f.request({op:'page',token:s.body.token})).status,503);
 const job=await readRecord(f.cursorStore,'job');assert.ok(job.releasingPin);assert.equal(job.cachePages,1);const cached=await readRecord(f.cursorStore,[...(await f.cursorStore.list({prefix:'page:'})).keys()][0]);
 f.restart();const retry=await f.request({op:'page',token:s.body.token});assert.equal(retry.status,200);assert.deepEqual(retry.body,cached);assert.equal(await f.control.get('pinCount'),0);
});

test('lease expires between pages, but admitted page pin protects history until completion',async()=>{
 const f=await fixture(),s=await f.start(),held=f.hold('base/a/t0');const pending=f.request({op:'page',token:s.body.token});await held.waiting;
 f.tick(2000);await f.update({id:0,tag:'a',keep:true,payload:'new'});
 const compact=await f.allocator.fetch(new Request('https://allocator',{method:'POST',body:JSON.stringify({op:'compact',retainSteps:0})}));assert.equal((await compact.json()).retentionFloor,1);
 held.release();const page=await pending;assert.equal(page.status,200);assert.equal(await f.control.get('pinCount'),0);
 assert.equal((await f.request({op:'page',token:s.body.token})).status,410);await f.alarm();assert.equal((await f.cursorStore.list({prefix:'page:'})).size,0);
 const next=await f.allocator.fetch(new Request('https://allocator',{method:'POST',body:JSON.stringify({op:'compact',retainSteps:0})}));assert.equal((await next.json()).retentionFloor,2);
});

test('unsupported ordering rejects cleanly; close expires even cached EOF and epoch collection fences restart',async()=>{
 const invalid=await fixture();assert.equal((await invalid.start({sql:'SELECT id FROM a ORDER BY payload'})).status,400);assert.equal(await invalid.control.get('pinCount'),0);assert.equal((await invalid.control.list({prefix:'lease:'})).size,0);
 const f=await fixture({count:1}),s=await f.start(),page=await f.request({op:'page',token:s.body.token});assert.equal(page.body.done,true);f.restart();assert.deepEqual((await f.request({op:'page',token:s.body.token})).body,page.body);
 assert.equal((await f.request({op:'close'})).status,200);assert.equal((await f.request({op:'page',token:s.body.token})).status,410);
 await f.control.put('rejectBeforeEpoch',1);assert.equal((await f.request({op:'collect',epoch:0})).body.collected,true);assert.equal((await f.start()).status,410);
});

test('byte-limited page stops at consumed key, chunked receipts ACK-clean without skipping rows',async()=>{
 const f=await fixture({count:5});for(let id=0;id<5;id++)await f.update({id,tag:'a',keep:true,payload:'x'.repeat(180000)});
 const s=await f.start({sql:'SELECT id, payload FROM a ORDER BY id',pageSize:5});let token=s.body.token,ids=[],pages=0;
 while(token){const page=await f.request({op:'page',token});assert.equal(page.status,200);assert.ok(new TextEncoder().encode(JSON.stringify(page.body.rows)).length<=512*1024);ids.push(...page.body.rows.map(r=>r.id));assert.ok((await f.cursorStore.list({prefix:'__records_chunk_v1/'})).size>0);await f.request({op:'ack',throughSeq:page.body.page});assert.equal((await f.cursorStore.list({prefix:'page:'})).size,0);token=page.body.nextToken;pages++;}
 assert.deepEqual(ids,[0,1,2,3,4]);assert.equal(pages,3);assert.equal(await f.control.get('pinCount'),0);
});

test('durable abandoned admission is alarm-released after restart and initial unknown lease closes safely',async()=>{
 const f=await fixture(),s=await f.start(),job=await readRecord(f.cursorStore,'job');
 job.active={id:'abandoned-page',state:'admitting'};
 await f.cursorStore.transaction(txn=>writeRecord(txn,'job',job));
 const pin=await f.allocator.fetch(new Request('https://allocator',{method:'POST',body:JSON.stringify({op:'pin',id:'abandoned-page',epoch:0,owner:'cursor-job/q1',leaseId:job.leaseId,snapshot:job.snapshot})}));assert.equal(pin.status,200);
 f.restart();await f.alarm();assert.equal(await f.control.get('pinCount'),0);assert.equal((await readRecord(f.cursorStore,'job')).active,undefined);
 assert.equal((await f.request({op:'page',token:s.body.token})).status,200);
 const unknown=await fixture();const saved={...job,id:'initializing',leaseId:'cursor-initializing',state:'starting',snapshot:undefined,expiresAt:undefined,active:undefined,closed:false,releasingPin:undefined};
 await unknown.cursorStore.transaction(txn=>writeRecord(txn,'job',saved));
 await unknown.allocator.fetch(new Request('https://allocator',{method:'POST',body:JSON.stringify({op:'lease',id:saved.leaseId,epoch:0,ttlMs:1000})}));
 unknown.restart();await unknown.alarm();assert.equal((await readRecord(unknown.cursorStore,'job')).closed,true);assert.equal(await unknown.control.get('lease:'+saved.leaseId),undefined);
});

test('never-admitted initializing cursor self-collects only after epoch retirement and fences stale start',async()=>{
 const f=await fixture(),s=await f.start(),job=await readRecord(f.cursorStore,'job');
 await f.request({op:'close'});
 const unknown={...job,id:'unknown',leaseId:'cursor-unknown',state:'starting',expiresAt:undefined,snapshot:undefined,active:undefined,closed:false,cachePages:0,cacheBytes:0};
 await f.cursorStore.transaction(txn=>writeRecord(txn,'job',unknown));f.restart();await f.alarm();assert.equal((await readRecord(f.cursorStore,'job')).closed,true);assert.equal(f.cursorStore.alarm,70000);
 await f.control.put('rejectBeforeEpoch',1);await f.alarm();assert.equal(await readRecord(f.cursorStore,'job'),undefined);assert.equal(await f.cursorStore.get('rejectBeforeEpoch'),1);assert.equal(f.cursorStore.alarm,null);assert.equal((await f.request({op:'collect',epoch:0})).body.collected,true);
 assert.equal((await f.start({id:'unknown'})).status,410);assert.equal(await readRecord(f.cursorStore,'job'),undefined);
});

test('explicit renewal preserves fixed snapshot and tokens past original expiry then expires when client stops',async()=>{
 const f=await fixture(),s=await f.start(),first=await f.request({op:'page',token:s.body.token});f.tick(800);
 const renewed=await f.request({op:'renew',ttlMs:2000});assert.equal(renewed.status,200);assert.equal(renewed.body.snapshot,1);assert.equal(renewed.body.expiresAt,12800);
 f.tick(400);await f.update({id:0,tag:'a',keep:false,payload:'later'});f.restart();
 const replay=await f.request({op:'page',token:s.body.token});assert.equal(replay.status,200);assert.deepEqual(replay.body,first.body);assert.equal(replay.body.expiresAt,11000,'receipt remains exact; its original expiry is only a hint');
 assert.equal((await f.request({op:'status'})).body.expiresAt,12800);assert.equal((await f.request({op:'page',token:first.body.nextToken})).body.snapshot,1);
 f.tick(2000);await f.alarm();assert.equal((await f.request({op:'renew',ttlMs:2000})).status,410);assert.equal((await f.request({op:'page',token:s.body.token})).status,410);assert.equal(await f.control.get('pinCount'),0);
});

test('lost renewal reply resolves before old-expiry cleanup without extending intent again',async()=>{
 const f=await fixture(),s=await f.start();f.tick(800);f.loseRenew();assert.equal((await f.request({op:'renew',ttlMs:2000})).status,503);
 assert.ok((await readRecord(f.cursorStore,'job')).renewing);assert.equal((await f.control.get('lease:cursor-q1')).expiresAt,12800);
 f.tick(400);f.restart();await f.alarm();const status=await f.request({op:'status'});assert.equal(status.body.closed,false);assert.equal(status.body.expiresAt,12800);assert.equal((await readRecord(f.cursorStore,'job')).renewing,undefined);
 assert.equal(f.calls.filter(c=>c.kind==='control'&&c.input.op==='renewLease').length,1);assert.equal((await f.request({op:'page',token:s.body.token})).status,200);
});

test('undelivered renewal cannot revive lease that expired before recovery',async()=>{
 const f=await fixture(),s=await f.start();f.tick(800);f.loseRenew('before');assert.equal((await f.request({op:'renew',ttlMs:2000})).status,503);
 f.tick(400);f.restart();assert.equal((await f.request({op:'page',token:s.body.token})).status,410);assert.equal((await f.request({op:'renew',ttlMs:2000})).status,410);
 assert.equal(f.calls.filter(c=>c.kind==='control'&&c.input.op==='renewLease').length,1);assert.equal(await f.control.get('lease:cursor-q1'),undefined);assert.equal(await f.control.get('pinCount'),0);
});
