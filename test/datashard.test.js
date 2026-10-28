import test from 'node:test';
import assert from 'node:assert/strict';
import { DataShard } from '../src/datashard.js';
import { encodeKey, rangeForFilters } from '../src/keys.js';
const clone = structuredClone;
function storage() {
  const map = new Map();
  const io = [];
  const store = {io, get:async key=>{io.push({op:'get',key});return clone(map.get(key));},put:async(key,value)=>map.set(key,clone(value)),delete:async key=>map.delete(key),
    list:async(options={})=>{
      const {prefix='',start,end,reverse=false,limit}=options;
      let selected=[...map].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);
      if(reverse)selected.reverse();if(limit!==undefined)selected=selected.slice(0,limit);
      io.push({op:'list',options,keys:selected.map(([k])=>k)});
      return new Map(selected.map(([k,v])=>[k,clone(v)]));
    }};
  store.transaction=async fn=>{const backup=clone(map);try{return await fn(store);}catch(e){map.clear();for(const [k,v]of backup)map.set(k,v);throw e;}};
  return store;
}
function cluster() {
  const plans=new Map(),decisions=new Map(),aborted=new Set(),acknowledgments=[];let retentionFloor=0,rejectBeforeEpoch=0,epochHook=null;const state={storage:storage(),id:'a'};
  async function association(id){const plan=plans.get(id);if(!plan)return null;if(plan.manifestHash)return plan;const p=await state.storage.get(`proposal:${id}`);const {manifestHash}=await import('../src/protocol.js');return {...plan,id,epoch:Number(/^e(\d+)_/.exec(id)?.[1]||0),owner:p.owner,snapshot:p.snapshot,bucket:'m0',participants:[{name:'a',bodyHash:p.bodyHash,publicKey:p.publicKey}],manifestHash:await manifestHash([{name:'a',bodyHash:p.bodyHash,publicKey:p.publicKey}])};}
  const authority=async r=>{if(r.op==='lookup'||r.op==='abortIfUnassigned'){const a=await association(r.id);if(a)return {state:'assigned',association:a};if(r.op==='abortIfUnassigned')aborted.add(r.id);return {state:aborted.has(r.id)?'aborted':'absent'};}if(r.op==='status')return {retentionFloor,rejectBeforeEpoch};if(r.op==='epoch'){if(epochHook)await epochHook();return {currentEpoch:rejectBeforeEpoch,rejectBeforeEpoch};}throw new Error('Unknown allocator op');};
  const env={SHARDS:{idFromName:x=>x},ALLOCATOR:{idFromName:x=>x,get:()=>({fetch:async(_url,init)=>{const r=JSON.parse(init.body);if(r.op==='epoch'&&r.requestEpoch<rejectBeforeEpoch)return Response.json({error:'expired',code:'REQUEST_EXPIRED'},{status:410});return Response.json(await authority(r));}})},MEDIATORS:{idFromName:x=>x,get:()=>({fetch:async(_url,init)=>{const r=JSON.parse(init.body),a=await association(r.id);if(r.op==='get')return Response.json({association:a,decision:decisions.get(r.id)||null});if(r.op==='decide'){const {validateCertificate}=await import('../src/protocol.js');decisions.set(r.id,await validateCertificate(a,r.certificate));return Response.json({ok:true});}acknowledgments.push(r);return Response.json({ok:true});}})}};
  let actor=new DataShard(state,env);
  return {plans,acknowledgments,store:state.storage,setFloor(value){retentionFloor=value;},setEpochFloor(value){rejectBeforeEpoch=value;},setEpochHook(hook){epochHook=hook;},restart(){actor=new DataShard(state,env);},async call(body){const r=await actor.fetch(new Request('https://actor',{method:'POST',body:JSON.stringify(body)}));return {status:r.status,...await r.json()};}};
}
const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false};
async function ready(db,id,snapshot,changes,reads=changes.map(c=>point(c.key)),extra={}){const result=await db.call({op:'propose',id,name:'a',owner:'c0',epoch:Number(/^e(\d+)_/.exec(id)?.[1]||0),participants:['a'],snapshot,reads,changes,...extra});return {...result,vote:result.bodyHash};}
async function commit(db,id,step,vote){if(!db.plans.has(id))db.plans.set(id,{step,participants:[{name:'a',vote}]});return db.call({op:'drive',id});}

test('MVCC disjoint same-table proposals coexist and older snapshots retain rows/tombstones',async()=>{
 const db=cluster();const seed=await ready(db,'seed',0,[{key:1,row:{id:1,n:1}},{key:2,row:{id:2,n:2}}],[],{expectedEpoch:0,meta:{epoch:1,retired:false}});assert.equal(seed.status,200);assert.equal((await commit(db,'seed',1,seed.vote)).status,200);
 const a=await ready(db,'a',1,[{key:1,row:{id:1,n:11}}],[point(1)],{expectedEpoch:1});
 const b=await ready(db,'b',1,[{key:2,row:null}],[point(2)],{expectedEpoch:1});assert.equal(a.status,200);assert.equal(b.status,200);
 assert.equal((await commit(db,'b',3,b.vote)).status,200);assert.equal((await commit(db,'a',2,a.vote)).status,200);
 assert.deepEqual((await db.call({op:'scan',snapshot:1,epoch:1,range:all})).rows,[{id:1,n:1},{id:2,n:2}]);
 assert.deepEqual((await db.call({op:'scan',snapshot:2,epoch:1,range:all})).rows,[{id:1,n:11},{id:2,n:2}]);
 assert.deepEqual((await db.call({op:'scan',snapshot:3,epoch:1,range:all})).rows,[{id:1,n:11}]);
 db.restart();assert.equal((await commit(db,'a',2,a.vote)).status,200);assert.equal((await db.call({op:'scan',snapshot:3,range:all})).rows.length,1);
});



test('retirement retains historical routing snapshots while rejecting current retired epoch',async()=>{
 const db=cluster();const seed=await ready(db,'seed',0,[{key:1,row:{id:1}}],[],{expectedEpoch:0,meta:{epoch:1,retired:false}});await commit(db,'seed',1,seed.vote);
 const reader=await ready(db,'reader',1,[],[point(1)],{expectedEpoch:1});assert.equal(reader.status,200);
 // Unplanned readers do not freeze whole tablets; validation happens after assignment.
 await db.call({op:'abort',id:'reader'});
 const retire=await ready(db,'retire',1,[],[],{expectedEpoch:1,meta:{epoch:2,retired:true}});await commit(db,'retire',2,retire.vote);
 db.restart();assert.deepEqual((await db.call({op:'getMany',snapshot:1,epoch:1,keys:[1]})).rows,[{id:1}]);
 assert.equal((await db.call({op:'scan',snapshot:2,epoch:2,range:all})).code,'STALE_EPOCH');
 const late=await ready(db,'late',1,[{key:2,row:{id:2}}],[],{expectedEpoch:1});assert.equal((await commit(db,'late',3,late.vote)).outcome,'ABORT');
});

test('ordered MVCC physical range returns exact versions for inclusive/exclusive boundaries',async()=>{
 const db=cluster();const seed=await ready(db,'seed',0,[-10,2,10].map(key=>({key,row:{id:key}})));await commit(db,'seed',1,seed.vote);
 const update=await ready(db,'update',1,[{key:2,row:{id:2,n:1}}]);await commit(db,'update',2,update.vote);
 const r=await db.call({op:'scan',snapshot:1,range:rangeForFilters([{column:'id',op:'>=',value:2},{column:'id',op:'<',value:10}],'id')});
 assert.deepEqual(r.rows,[{id:2}]);assert.equal(r.versionsRead,1);
 assert.deepEqual((await db.call({op:'scan',snapshot:2,range:rangeForFilters([{column:'id',op:'>',value:2}],'id')})).rows,[{id:10}]);
});

test('point and metadata snapshot lookups read one version despite long retained histories',async()=>{
 const db=cluster();
 for(let step=1;step<=120;step++) {
   const p=await ready(db,`history-${step}`,step-1,[{key:7,row:{id:7,n:step}}],[],{expectedEpoch:step-1,meta:{epoch:step,retired:false}});
   assert.equal(p.status,200);assert.equal((await commit(db,`history-${step}`,step,p.vote)).status,200);
 }
 db.store.io.length=0;
 const old=await db.call({op:'getMany',snapshot:40,epoch:40,keys:[7]});
 assert.deepEqual(old.rows,[{id:7,n:40}]);assert.equal(old.versionsRead,1);
 const lists=db.store.io.filter(op=>op.op==='list'&&!['pending:','foot:'].includes(op.options.prefix));
 assert.equal(lists.length,2);assert.ok(lists.every(op=>op.options.reverse===true&&op.options.limit===1&&op.keys.length===1));
 db.store.io.length=0;
 const stale=await ready(db,'stale-history',40,[{key:7,row:{id:7,n:999}}]);assert.equal((await commit(db,'stale-history',121,stale.vote)).outcome,'ABORT');
 assert.ok(db.store.io.some(op=>op.op==='get'&&op.key===`head:${encodeKey(7)}`));
 assert.ok(!db.store.io.some(op=>op.op==='list'&&op.options.prefix==='row:'));
});

test('scan cursors page logical heads and preserve old snapshots across deletes and invisible inserts',async()=>{
 const db=cluster();const seedRows=Array.from({length:301},(_,id)=>({key:id,row:{id}}));
 const seed=await ready(db,'pages-seed',0,seedRows,[]);assert.equal((await commit(db,'pages-seed',1,seed.vote)).status,200);
 const later=await ready(db,'pages-later',1,[{key:0,row:null},{key:302,row:{id:302}}]);await commit(db,'pages-later',2,later.vote);
 let cursor,seen=[],pages=0;
 do {
   db.store.io.length=0;
   const page=await db.call({op:'scan',snapshot:1,range:all,pageSize:73,cursor});
   assert.equal(page.status,200);assert.ok(page.keysRead<=73);assert.ok(page.versionsRead<=73);
   assert.ok(db.store.io.filter(x=>x.op==='list'&&x.options.prefix.startsWith('row:')).every(x=>x.options.reverse===true&&x.options.limit===1));
   seen.push(...page.rows);cursor=page.nextCursor;pages++;
 } while(cursor!==null);
 assert.equal(pages,5);assert.deepEqual(seen,seedRows.map(x=>x.row));
 const first=await db.call({op:'scan',snapshot:2,range:all});assert.equal(first.keysRead,256);assert.equal(first.done,false);assert.ok(first.nextCursor);
 assert.equal((await db.call({op:'scan',snapshot:2,pageSize:257})).status,400);
 const stats=await db.call({op:'stats'});assert.equal(stats.rows,301);
 const expected=[...seedRows.slice(1).map(x=>x.row),{id:302}].reduce((n,row)=>n+new TextEncoder().encode(JSON.stringify(row)).length,0);assert.equal(stats.bytes,expected+302);
 await commit(db,'pages-later',2,later.vote);assert.equal((await db.call({op:'stats'})).bytes,expected+302);
});

test('planned vote range validation pages current heads and detects invisible insertion/deletion phantoms',async()=>{
 const db=cluster();const changes=Array.from({length:300},(_,id)=>({key:id,row:{id}}));
 const seed=await ready(db,'range-seed',0,changes,[]);await commit(db,'range-seed',1,seed.vote);
 db.store.io.length=0;const read=await ready(db,'stable-range',1,[],[all]);assert.equal((await commit(db,'stable-range',2,read.vote)).outcome,'COMMIT');
 const lists=db.store.io.filter(x=>x.op==='list'&&x.options.prefix==='head:');assert.equal(lists.length,2);assert.ok(lists.every(x=>x.options.limit===256));
 assert.ok(!db.store.io.some(x=>x.op==='list'&&x.options.prefix==='row:'));

 const deleted=await ready(db,'phantom-delete',1,[{key:299,row:null}]);await commit(db,'phantom-delete',5,deleted.vote);
 const stale=await ready(db,'stale-range',1,[],[all]);assert.equal((await commit(db,'stale-range',6,stale.vote)).outcome,'ABORT');
 assert.equal((await db.call({op:'stats'})).rows,299);
});

test('chunked participant intents and large row versions survive restart and clean abort/apply chunks',async()=>{
 const db=cluster();const row={id:1,payload:'界'.repeat(60000)};
 const aborted=await ready(db,'large-abort',0,[{key:1,row}],[]);assert.equal(aborted.status,200);
 assert.equal((await db.store.get('pending:large-abort')).encoding,'chunks');
 assert.ok((await db.store.list({prefix:'__records_chunk_v1/'})).size>0);
 db.restart();assert.equal((await db.call({op:'abort',id:'large-abort'})).status,200);
 assert.equal((await db.store.list({prefix:'__records_chunk_v1/'})).size,0);
 const p=await ready(db,'large-commit',0,[{key:1,row}],[]);assert.equal(p.status,200);db.restart();
 assert.equal((await commit(db,'large-commit',1,p.vote)).status,200);
 assert.equal(await db.store.get('pending:large-commit'),undefined);
 const result=await db.call({op:'getMany',snapshot:1,keys:[1]});assert.deepEqual(result.rows,[row]);assert.equal(result.versionsRead,1);
 assert.equal((await db.call({op:'stats'})).bytes,new TextEncoder().encode(JSON.stringify([row])).length);
 assert.equal((await db.call({op:'getMany',snapshot:0,keys:[1]})).rows.length,0);
});

test('participant deltas beyond former 2000 row cap are chunked and scanned with continuation',async()=>{
 const db=cluster();const changes=Array.from({length:2100},(_,id)=>({key:id,row:{id}}));
 const p=await ready(db,'large-count',0,changes,[]);assert.equal(p.status,200);assert.equal((await commit(db,'large-count',1,p.vote)).status,200);
 let cursor,count=0,pages=0;
 do {const page=await db.call({op:'scan',snapshot:1,cursor});count+=page.rows.length;cursor=page.nextCursor;pages++;}while(cursor!==null);
 assert.equal(count,2100);assert.equal(pages,9);assert.equal((await db.call({op:'stats'})).rows,2100);
});

test('bounded compaction expires old snapshots and retains row/tombstone/metadata anchors across restart',async()=>{
 const db=cluster();const k=encodeKey(1);
 for(let step=1;step<=1000;step++) {
   await db.store.put(`row:${k}:${String(step).padStart(16,'0')}`,{key:1,row:step===500?null:{id:1,n:step},step});
   await db.store.put(`meta:${String(step).padStart(16,'0')}`,{step,epoch:step,retired:false});
 }
 await db.store.put(`head:${k}`,{key:1,encoded:k,step:1000,present:true,bytes:18});
 db.setFloor(500);let cursor,deleted=0,calls=0;
 do {
   db.store.io.length=0;
   const r=await db.call({op:'compact',floor:999999,limit:37,cursor});assert.equal(r.status,200);assert.equal(r.collectionFloor,500);
   assert.ok(r.deletedVersions+r.deletedMetadata<=37);deleted+=r.deletedVersions+r.deletedMetadata;cursor=r.nextCursor;calls++;
   assert.ok(db.store.io.filter(op=>op.op==='list'&&['row:', 'meta:'].some(prefix=>op.options.prefix.startsWith(prefix))).every(op=>op.options.limit<=37));
   if(calls===3)db.restart();
 }while(cursor!==null);
 assert.equal(deleted,998);assert.ok(calls>20);
 assert.equal((await db.store.list({prefix:`row:${k}:`})).size,501);
 assert.equal((await db.store.list({prefix:'meta:'})).size,501);
 const expiredRead=await db.call({op:'getMany',snapshot:499,keys:[1]});assert.equal(expiredRead.status,410);assert.equal(expiredRead.code,'SNAPSHOT_EXPIRED');assert.equal(expiredRead.retryable,false);
 assert.equal((await ready(db,'late-below-floor',499,[],[])).status,410);
 assert.deepEqual((await db.call({op:'getMany',snapshot:500,epoch:500,keys:[1]})).rows,[]);
 assert.deepEqual((await db.call({op:'getMany',snapshot:501,epoch:501,keys:[1]})).rows,[{id:1,n:501}]);
});

test('compaction is locally clamped by prepared snapshots and deletes obsolete chunk payloads',async()=>{
 const db=cluster();const big={id:1,payload:'x'.repeat(180000)};
 const one=await ready(db,'old-large',0,[{key:1,row:big}],[]);await commit(db,'old-large',1,one.vote);
 const read=await ready(db,'pinned-prepared',1,[],[point(2)]);assert.equal(read.status,200);
 const two=await ready(db,'new-small',1,[{key:1,row:{id:1,payload:'small'}}]);await commit(db,'new-small',2,two.vote);
 assert.ok((await db.store.list({prefix:'__records_chunk_v1/'})).size>0);
 db.setFloor(2);let r=await db.call({op:'compact',limit:5});assert.equal(r.collectionFloor,1);
 while(!r.done)r=await db.call({op:'compact',limit:5,cursor:r.nextCursor});
 assert.deepEqual((await db.call({op:'getMany',snapshot:1,keys:[1]})).rows,[big]);
 await db.call({op:'abort',id:'pinned-prepared'});
 r=await db.call({op:'compact',limit:5});while(!r.done)r=await db.call({op:'compact',limit:5,cursor:r.nextCursor});
 assert.equal(r.collectionFloor,2);assert.equal((await db.store.list({prefix:'__records_chunk_v1/'})).size,0);
 assert.deepEqual((await db.call({op:'getMany',snapshot:2,keys:[1]})).rows,[{id:1,payload:'small'}]);
 assert.equal((await db.call({op:'getMany',snapshot:1,keys:[1]})).status,410);
});

test('scan response byte budget resumes before deferred large row without dropping or duplicating keys',async()=>{
 const db=cluster();const payload='q'.repeat(700000);
 for(let id=0;id<4;id++){const p=await ready(db,`large-page-${id}`,id,[{key:id,row:{id,payload}}],[]);assert.equal(p.status,200);await commit(db,`large-page-${id}`,id+1,p.vote);}
 const first=await db.call({op:'scan',snapshot:4});assert.equal(first.status,200);assert.equal(first.rows.length,2);assert.equal(first.processedKeys,2);assert.equal(first.keysRead,3);assert.equal(first.nextCursor,encodeKey(1));assert.equal(first.done,false);
 db.restart();const next=await db.call({op:'scan',snapshot:4,cursor:first.nextCursor});assert.deepEqual(next.rows.map(r=>r.id),[2,3]);assert.equal(next.done,true);
 assert.deepEqual([...first.rows,...next.rows].map(r=>r.id),[0,1,2,3]);
 const tooLarge=await db.call({op:'getMany',snapshot:4,keys:[0,1,2,3]});assert.equal(tooLarge.status,400);assert.equal(tooLarge.code,'RESPONSE_TOO_LARGE');
 assert.equal((await db.call({op:'getMany',snapshot:4,keys:[0,1]})).rows.length,2);
 // Tombstones and future-only heads advance even when no row consumes budget.
 const absent=await db.call({op:'scan',snapshot:0,pageSize:2});assert.equal(absent.rows.length,0);assert.equal(absent.nextCursor,encodeKey(1));
});

test('allocated snapshot read waits only for relevant planned changes on the same shard',async()=>{
 const db=cluster();const seed=await ready(db,'visibility-seed',0,[{key:1,row:{id:1,n:1}},{key:2,row:{id:2,n:2}}],[]);await commit(db,'visibility-seed',1,seed.vote);
 const held=await ready(db,'visibility-held',1,[{key:1,row:{id:1,n:10}}]);assert.equal(held.status,200);
 db.plans.set('visibility-held',{step:2,owner:'c3',participants:[{name:'a',vote:held.vote}]});
 const blocked=await db.call({op:'getMany',snapshot:2,keys:[1]});assert.equal(blocked.status,409);assert.equal(blocked.code,'VISIBILITY_PENDING');assert.deepEqual(blocked.owners,[]);assert.deepEqual(blocked.dependencies,[{id:'visibility-held',bucket:'m0'}]);
 assert.deepEqual((await db.call({op:'getMany',snapshot:2,keys:[2]})).rows,[{id:2,n:2}]);
 assert.deepEqual((await db.call({op:'getMany',snapshot:1,keys:[1]})).rows,[{id:1,n:1}]);
 // A later plan and a read-only reservation do not affect this timestamp.
 assert.deepEqual((await db.call({op:'scan',snapshot:1,range:all})).rows,[{id:1,n:1},{id:2,n:2}]);
 await commit(db,'visibility-held',2,held.vote);
 assert.deepEqual((await db.call({op:'getMany',snapshot:2,keys:[1]})).rows,[{id:1,n:10}]);
});

test('visibility guards full scan predicates, empty catalog keys, and metadata before epoch validation',async()=>{
 const db=cluster();const pending=await ready(db,'new-schema',0,[{key:'schema',row:{name:'new-table'}}],[]);
 assert.deepEqual((await db.call({op:'getMany',snapshot:0,keys:['schema']})).rows,[]); // no plan yet
 db.plans.set('new-schema',{step:1,owner:'catalog-owner',participants:[{name:'a',vote:pending.vote}]});
 assert.equal((await db.call({op:'getMany',snapshot:1,keys:['schema']})).code,'VISIBILITY_PENDING');
 assert.equal((await db.call({op:'scan',snapshot:1,range:all,cursor:encodeKey('zzzz')})).code,'VISIBILITY_PENDING','page cursor must not narrow requested logical footprint');
 await commit(db,'new-schema',1,pending.vote);
 const meta=await ready(db,'future-route',1,[],[],{expectedEpoch:0,meta:{epoch:1,retired:false}});
 db.plans.set('future-route',{step:2,owner:'router',participants:[{name:'a',vote:meta.vote}]});
 const empty={lower:null,upper:null,empty:true};
 const blocked=await db.call({op:'scan',snapshot:2,epoch:1,range:empty});assert.equal(blocked.code,'VISIBILITY_PENDING');assert.deepEqual(blocked.owners,[]);assert.deepEqual(blocked.dependencies,[{id:'future-route',bucket:'m0'}]);
 assert.equal((await db.call({op:'scan',snapshot:1,epoch:0,range:empty})).status,200);
 await commit(db,'future-route',2,meta.vote);
 assert.equal((await db.call({op:'scan',snapshot:2,epoch:1,range:empty})).status,200);
 const readOnly=await ready(db,'read-only-reservation',2,[],[all],{expectedEpoch:1});
 db.plans.set('read-only-reservation',{step:3,owner:'reader',participants:[{name:'a',vote:readOnly.vote}]});
 assert.equal((await db.call({op:'scan',snapshot:3,epoch:1,range:all})).status,200);
});

test('retired request epochs fence proposals/replay before bounded abort/applied marker cleanup',async()=>{
 const db=cluster();const old=await ready(db,'e0_committed',0,[{key:1,row:{id:1}}],[]);await commit(db,'e0_committed',1,old.vote);
 await db.call({op:'abort',id:'e0_aborted'});await db.call({op:'abort',id:'e1_active_epoch_marker'});
 db.setEpochFloor(1);let cursor,markers=0,calls=0;
 do{const r=await db.call({op:'compact',limit:1,cursor});assert.equal(r.status,200);assert.equal(r.rejectBeforeEpoch,1);assert.ok(r.markersVisited+r.deletedVersions+r.deletedMetadata<=1);markers+=r.deletedMarkers;cursor=r.nextCursor;calls++;if(calls===1)db.restart();}while(cursor!==null);
 assert.equal(markers,8);assert.equal(await db.store.get('applied:e0_committed'),undefined);assert.equal(await db.store.get('aborted:e0_aborted'),undefined);assert.equal(await db.store.get('aborted:e1_active_epoch_marker'),true);
 const oldPrepare=await ready(db,'e0_late',1,[{key:2,row:{id:2}}],[]);assert.equal(oldPrepare.status,410);assert.equal(oldPrepare.code,'REQUEST_EXPIRED');
 assert.equal((await db.call({op:'proposal',id:'e0_committed'})).status,410);
 assert.equal((await db.call({op:'abort',id:'e0_late_abort'})).retired,true);assert.equal(await db.store.get('aborted:e0_late_abort'),undefined);
 assert.equal((await ready(db,'e1_new',1,[{key:2,row:{id:2}}],[])).status,200);
});

test('local request epoch fence catches a prepare whose global admission raced retirement; pending epochs clamp collection',async()=>{
 const db=cluster();db.setEpochHook(()=>db.store.put('rejectBeforeEpoch',1));
 const delayed=await ready(db,'e0_delayed',0,[],[]);assert.equal(delayed.status,410);assert.equal(delayed.code,'REQUEST_EXPIRED');
 const second=cluster();assert.equal((await ready(second,'e0_pending',0,[],[])).status,200);second.setEpochFloor(1);
 let compacted=await second.call({op:'compact'});assert.equal(compacted.rejectBeforeEpoch,0);
 await second.call({op:'abort',id:'e0_pending'});
 compacted=await second.call({op:'compact'});while(!compacted.done)compacted=await second.call({op:'compact',cursor:compacted.nextCursor});
 assert.equal(compacted.rejectBeforeEpoch,1);assert.equal(await second.store.get('aborted:e0_pending'),undefined);
});

test('floor-visible terminal tombstones collect their head and anchor without resurrecting old snapshots',async()=>{
 const db=cluster();const first=await ready(db,'e0_live',0,[{key:7,row:{id:7,n:1}}],[]);await commit(db,'e0_live',1,first.vote);
 const deleted=await ready(db,'e0_delete',1,[{key:7,row:null}]);await commit(db,'e0_delete',2,deleted.vote);
 db.setFloor(1);let r=await db.call({op:'compact',limit:1});while(!r.done)r=await db.call({op:'compact',limit:1,cursor:r.nextCursor});
 assert.ok(await db.store.get(`head:${encodeKey(7)}`));assert.deepEqual((await db.call({op:'getMany',snapshot:1,keys:[7]})).rows,[{id:7,n:1}]);
 db.setFloor(2);r=await db.call({op:'compact',limit:1});let heads=r.deletedHeads;while(!r.done){db.restart();r=await db.call({op:'compact',limit:1,cursor:r.nextCursor});heads+=r.deletedHeads;}
 assert.equal(heads,1);assert.equal(await db.store.get(`head:${encodeKey(7)}`),undefined);assert.equal((await db.store.list({prefix:`row:${encodeKey(7)}:`})).size,0);
 assert.deepEqual((await db.call({op:'scan',snapshot:2})).rows,[]);assert.equal((await db.call({op:'getMany',snapshot:1,keys:[7]})).status,410);
 const resurrect=await ready(db,'e0_reinsert',2,[{key:7,row:{id:7,n:3}}]);assert.equal(resurrect.status,200);await commit(db,'e0_reinsert',3,resurrect.vote);
 assert.deepEqual((await db.call({op:'getMany',snapshot:2,keys:[7]})).rows,[]);assert.deepEqual((await db.call({op:'getMany',snapshot:3,keys:[7]})).rows,[{id:7,n:3}]);
});

test('retired tablet collection removes live anchors and chunks incrementally while identity stays fenced',async()=>{
 const db=cluster();const payload='r'.repeat(70000);
 const changes=Array.from({length:5},(_,id)=>({key:id,row:{id,payload}}));
 const seed=await ready(db,'e0_retired-seed',0,changes,[],{expectedEpoch:0,meta:{epoch:1,retired:false}});await commit(db,'e0_retired-seed',1,seed.vote);
 const retire=await ready(db,'e0_retired-final',1,[],[],{expectedEpoch:1,meta:{epoch:2,retired:true}});await commit(db,'e0_retired-final',2,retire.vote);
 db.setFloor(1);let r=await db.call({op:'compact',limit:2});while(!r.done)r=await db.call({op:'compact',limit:2,cursor:r.nextCursor});
 assert.equal((await db.call({op:'scan',snapshot:1,epoch:1})).rows.length,5);
 assert.ok((await db.store.list({prefix:'__records_chunk_v1/'})).size>0);
 db.setFloor(2);let cursor=null,deleted=0,heads=0;
 do{r=await db.call({op:'compact',limit:2,cursor});assert.equal(r.status,200);deleted+=r.deletedVersions;heads+=r.deletedHeads;cursor=r.nextCursor;db.restart();}while(cursor!==null);
 assert.equal(deleted,5);assert.equal(heads,5);assert.equal((await db.store.list({prefix:'head:'})).size,0);assert.equal((await db.store.list({prefix:'row:'})).size,0);assert.equal((await db.store.list({prefix:'__records_chunk_v1/'})).size,0);
 assert.equal((await db.store.list({prefix:'meta:'})).size,1);assert.equal((await db.call({op:'stats'})).rows,0);
 assert.equal((await db.call({op:'scan',snapshot:1,epoch:1})).status,410);
 assert.equal((await db.call({op:'scan',snapshot:2,epoch:2})).code,'STALE_EPOCH');
 assert.equal((await db.call({op:'scan',snapshot:2})).code,'STALE_EPOCH');
 const unretire=await ready(db,'e0_try-unretire',2,[{key:8,row:{id:8}}],[],{meta:{epoch:3,retired:false}});assert.equal((await commit(db,'e0_try-unretire',3,unretire.vote)).outcome,'ABORT');
 const newrow=await ready(db,'e0_try-new-row',2,[{key:8,row:{id:8}}],[]);assert.equal((await commit(db,'e0_try-new-row',4,newrow.vote)).outcome,'ABORT');
});

test('scanKeys enumerates historical presence without loading large row chunks',async()=>{
 const db=cluster(),big='z'.repeat(180000);
 const seed=await ready(db,'keys-seed',0,[{key:1,row:{id:1,big}},{key:2,row:{id:2,big}}]);await commit(db,'keys-seed',1,seed.vote);
 const del=await ready(db,'keys-delete',1,[{key:1,row:null},{key:3,row:{id:3,big}}]);await commit(db,'keys-delete',2,del.vote);
 const re=await ready(db,'keys-reinsert',2,[{key:1,row:{id:1,big}}]);await commit(db,'keys-reinsert',3,re.vote);
 db.restart();db.store.io.length=0;
 for(const [snapshot,expected]of [[0,[]],[1,[1,2]],[2,[2,3]],[3,[1,2,3]]]){
   let cursor=null,keys=[],page;
   do{page=await db.call({op:'scanKeys',snapshot,pageSize:1,cursor});assert.equal(page.status,200);keys.push(...page.keys);cursor=page.nextCursor;}while(!page.done);
   assert.deepEqual(keys,expected);
 }
 assert.equal(db.store.io.filter(io=>io.op==='get'&&io.key.startsWith('__records_chunk_v1/')).length,0);
 const pending=await ready(db,'keys-pending',3,[{key:4,row:{id:4}}]);db.plans.set('keys-pending',{step:4,owner:'c0',participants:[{name:'a',vote:pending.vote}]});
 assert.equal((await db.call({op:'scanKeys',snapshot:4})).code,'VISIBILITY_PENDING');
 assert.deepEqual((await db.call({op:'scanKeys',snapshot:4,range:point(2)})).keys,[2]);
});

test('scanKeys key byte budget resumes after consumed head without loss',async()=>{
 const db=cluster(),keys=Array.from({length:140},(_,i)=>String(i).padStart(3,'0')+'a'.repeat(510));
 const seed=await ready(db,'key-budget',0,keys.map(key=>({key,row:{ok:true}})));assert.equal(seed.status,200);await commit(db,'key-budget',1,seed.vote);
 let cursor=null,found=[],pages=0,page;
 do{page=await db.call({op:'scanKeys',snapshot:1,cursor});assert.equal(page.status,200);assert.ok(page.keysBytes<=65536);found.push(...page.keys);cursor=page.nextCursor;pages++;}while(!page.done);
 assert.equal(pages,2);assert.deepEqual(found,keys);
});

async function peerCluster(){
 const {PlanAllocator}=await import('../src/mediator.js'),{PlanBucket}=await import('../src/plan-bucket.js');
 const env={PLAN_BUCKETS:'4'},actors=new Map();
 for(const [binding,Class]of [['ALLOCATOR',PlanAllocator],['MEDIATORS',PlanBucket],['SHARDS',DataShard]])env[binding]={idFromName:x=>x,get:name=>({fetch:async(url,init)=>{const k=binding+':'+name;if(!actors.has(k))actors.set(k,new Class({storage:storage(),id:name},env));return actors.get(k).fetch(new Request(url,init));}})};
 const call=async(binding,name,body)=>{const r=await env[binding].get(name).fetch('https://actor',{method:'POST',body:JSON.stringify(body)});const value=await r.json();if(!r.ok)throw Object.assign(new Error(JSON.stringify(value)),{status:r.status});return value;};
 return {env,actors,call,async plan(id,snapshot,bodies){
   const {manifestHash}=await import('../src/protocol.js');
   await call('ALLOCATOR','global',{op:'pin',id,owner:'c0',snapshot});
   const names=Object.keys(bodies),participants=[];
   for(const name of names){const result=await call('SHARDS',name,{op:'propose',id,name,owner:'c0',epoch:0,snapshot,participants:names,reads:[],changes:[],...bodies[name]});participants.push({name,bodyHash:result.bodyHash,publicKey:result.publicKey});}
   const association=await call('ALLOCATOR','global',{op:'assign',id,epoch:0,owner:'c0',snapshot,participants,manifestHash:await manifestHash(participants)});
   await call('MEDIATORS',association.bucket,{op:'register',association});
   for(const name of names)await call('SHARDS',name,{op:'acceptPlan',association});
   return association;
 },async finish(a){for(let i=0;i<8;i++){for(const p of a.participants)await call('SHARDS',p.name,{op:'drive',id:a.id});const state=await call('MEDIATORS',a.bucket,{op:'get',id:a.id});if(state.applied.length===a.participants.length)return state;}throw new Error('No completion');}};
}

test('peer protocol signs durable shard votes, commits across shards and repairs finalization replay',async()=>{
 const c=await peerCluster(),a=await c.plan('peer-seed',0,{a:{changes:[{key:1,row:{id:1,n:10}}]},b:{changes:[{key:2,row:{id:2,n:20}}]}});
 const result=await c.finish(a);assert.equal(result.decision.outcome,'COMMIT');assert.equal(result.decision.certificate.length,2);
 assert.deepEqual((await c.call('SHARDS','a',{op:'scanKeys',snapshot:a.step})).keys,[1]);
 assert.deepEqual((await c.call('SHARDS','b',{op:'scan',snapshot:a.step})).rows,[{id:2,n:20}]);
 assert.equal((await c.call('SHARDS','a',{op:'drive',id:a.id})).outcome,'COMMIT');
});

test('peer protocol earlier planned invisible write aborts conflicting later proposal, disjoint progresses',async()=>{
 const c=await peerCluster();
 const first=await c.plan('peer-hole',0,{a:{changes:[{key:1,row:{id:1,n:1}}]}});
 const disjoint=await c.plan('peer-disjoint',0,{a:{reads:[point(2)],changes:[{key:2,row:{id:2,n:2}}]}});
 const conflictPlan=await c.plan('peer-conflict',0,{a:{reads:[point(1)],changes:[{key:3,row:{id:3,n:3}}]}});
 assert.equal((await c.finish(disjoint)).decision.outcome,'COMMIT');
 assert.equal((await c.finish(conflictPlan)).decision.outcome,'ABORT');
 await assert.rejects(c.call('SHARDS','a',{op:'scan',snapshot:disjoint.step}),e=>e.message.includes('VISIBILITY_PENDING'));
 assert.deepEqual((await c.call('SHARDS','a',{op:'scan',snapshot:disjoint.step,range:point(2)})).rows,[{id:2,n:2}]);
 assert.equal((await c.finish(first)).decision.outcome,'COMMIT');
});

test('peer protocol test-only finalization hold retains visibility barrier until explicit recovery release',async()=>{
 const c=await peerCluster();c.env.ALLOW_FAULTS='true';
 const a=await c.plan('peer-held',0,{a:{changes:[{key:1,row:{id:1}}],holdFinalization:true},b:{changes:[{key:2,row:{id:2}}],holdFinalization:true}});
 await c.call('SHARDS','a',{op:'drive',id:a.id});const result=await c.call('SHARDS','b',{op:'drive',id:a.id});
 assert.equal(result.held,true);assert.equal(result.applied,false);assert.equal(result.decision.outcome,'COMMIT');
 await c.call('SHARDS','a',{op:'drive',id:a.id,release:true});
 await assert.rejects(c.call('SHARDS','b',{op:'scanKeys',snapshot:a.step}),e=>e.message.includes('VISIBILITY_PENDING'));
 assert.equal((await c.call('SHARDS','b',{op:'drive',id:a.id,release:true})).applied,true);
 assert.deepEqual((await c.call('SHARDS','b',{op:'scanKeys',snapshot:a.step})).keys,[2]);
});

test('peer protocol actor identity, immutable proposal and signed peer vote are anchored',async()=>{
 const c=await peerCluster();
 await assert.rejects(c.call('SHARDS','a',{op:'propose',id:'fake',name:'b',owner:'c0',epoch:0,snapshot:0,participants:['b'],reads:[],changes:[]}),e=>e.message.includes('name mismatch'));
 const a=await c.plan('peer-anchor',0,{a:{changes:[{key:1,row:{id:1}}]},b:{changes:[]}});
 await c.call('SHARDS','a',{op:'drive',id:a.id});
 const vote=await c.actors.get('SHARDS:a').storage.get(`readset:${a.id}`);
 await assert.rejects(c.call('SHARDS','b',{op:'readSet',association:a,readSet:{...vote,outcome:'ABORT'}}),e=>e.message.includes('Invalid peer ReadSet'));
 await assert.rejects(c.call('SHARDS','a',{op:'propose',id:a.id,name:'a',owner:'c0',epoch:0,snapshot:0,participants:['a','b'],reads:[],changes:[{key:2,row:{id:2}}]}),e=>e.message.includes('identity mismatch'));
});

test('reverse MVCC pages skip future-only and tombstone heads with exclusive descending cursors',async()=>{
 const db=cluster(),seed=await ready(db,'reverse-seed',0,[-10,-0.5,0,2,10,20].map(key=>({key,row:{id:key}})),[]);await commit(db,'reverse-seed',1,seed.vote);
 const later=await ready(db,'reverse-later',1,[{key:20,row:null},{key:30,row:{id:30}},{key:40,row:{id:40}}],[]);await commit(db,'reverse-later',2,later.vote);
 for(const [snapshot,expected]of [[1,[20,10,2,0,-0.5,-10]],[2,[40,30,10,2,0,-0.5,-10]]]){
   db.store.io.length=0;let cursor=null,keys=[],pages=0,empty=0,page;
   do{page=await db.call({op:'scanKeys',snapshot,reverse:true,pageSize:1,cursor});assert.equal(page.status,200);keys.push(...page.keys);if(!page.keys.length)empty++;if(cursor&&page.nextCursor)assert.ok(page.nextCursor<cursor);cursor=page.nextCursor;pages++;assert.ok(pages<20);}while(!page.done);
   assert.deepEqual(keys,expected);assert.ok(empty>=1);
   const heads=db.store.io.filter(io=>io.op==='list'&&io.options.prefix==='head:');assert.ok(heads.every(io=>io.options.reverse===true&&io.options.limit===2&&io.keys.length<=2));
 }
 const range=rangeForFilters([{column:'id',op:'>=',value:-0.5},{column:'id',op:'<',value:10}],'id');
 assert.deepEqual((await db.call({op:'scan',snapshot:2,range,reverse:true})).rows.map(r=>r.id),[2,0,-0.5]);
 assert.deepEqual((await db.call({op:'scan',snapshot:2,range})).rows.map(r=>r.id),[-0.5,0,2]);
 assert.equal((await db.call({op:'scan',snapshot:2,reverse:'yes'})).status,400);
});

test('reverse scan byte overflow resumes before deferred row and composite key pages stay typed ordered',async()=>{
 const db=cluster(),payload='b'.repeat(700000);
 for(let key=0;key<4;key++){const p=await ready(db,`reverse-big-${key}`,key,[{key,row:{id:key,payload}}],[]);await commit(db,`reverse-big-${key}`,key+1,p.vote);}
 let cursor=null,found=[],page,pages=0;
 do{page=await db.call({op:'scan',snapshot:4,reverse:true,cursor});assert.equal(page.status,200);found.push(...page.rows.map(r=>r.id));cursor=page.nextCursor;pages++;}while(!page.done);
 assert.deepEqual(found,[3,2,1,0]);assert.equal(pages,2);
 const tuples=cluster(),keys=[[null,2],['',1],['a',-10],['a',2],['a',10],['\ue000',1],['\u{10000}',1]];
 const seed=await ready(tuples,'reverse-tuples',0,keys.map(key=>({key,row:{key}})),[]);await commit(tuples,'reverse-tuples',1,seed.vote);
 cursor=null;found=[];
 do{page=await tuples.call({op:'scanKeys',snapshot:1,reverse:true,pageSize:2,cursor});found.push(...page.keys);cursor=page.nextCursor;}while(!page.done);
 assert.deepEqual(found,[...keys].sort((a,b)=>encodeKey(a)<encodeKey(b)?1:-1));
});

test('reverse scanKeys 64 KiB key budget resumes exactly with long typed keys',async()=>{
 const db=cluster(),keys=Array.from({length:140},(_,i)=>String(i).padStart(3,'0')+'x'.repeat(510));
 const seed=await ready(db,'reverse-key-budget',0,keys.map(key=>({key,row:{ok:true}})),[]);await commit(db,'reverse-key-budget',1,seed.vote);
 let cursor=null,found=[],page,pages=0;
 do{page=await db.call({op:'scanKeys',snapshot:1,reverse:true,cursor});assert.ok(page.keysBytes<=65536);found.push(...page.keys);cursor=page.nextCursor;pages++;}while(!page.done);
 assert.equal(pages,2);assert.deepEqual(found,[...keys].reverse());
});
