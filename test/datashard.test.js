import test from 'node:test';
import assert from 'node:assert/strict';
import { DataShard } from '../src/datashard.js';
import { encodeKey, rangeForFilters } from '../src/keys.js';
const clone = structuredClone;
function storage() {
  const map = new Map();
  const store = {get:async key=>clone(map.get(key)),put:async(key,value)=>map.set(key,clone(value)),delete:async key=>map.delete(key),
    list:async({prefix='',start,end}={})=>new Map([...map].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:1).map(([k,v])=>[k,clone(v)]))};
  store.transaction=async fn=>{const backup=clone(map);try{return await fn(store);}catch(e){map.clear();for(const [k,v]of backup)map.set(k,v);throw e;}};
  return store;
}
function cluster() {
  const plans=new Map(), acknowledgments=[];const state={storage:storage()};
  const env={MEDIATOR:{idFromName:x=>x,get:()=>({fetch:async(_url,init)=>{const r=JSON.parse(init.body);if(r.op==='get')return Response.json({plan:plans.get(r.id)||null});acknowledgments.push(r);return Response.json({ok:true});}})}};
  let actor=new DataShard(state,env);
  return {plans,acknowledgments,restart(){actor=new DataShard(state,env);},async call(body){const r=await actor.fetch(new Request('https://actor',{method:'POST',body:JSON.stringify(body)}));return {status:r.status,...await r.json()};}};
}
const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false};
async function ready(db,id,snapshot,changes,reads=changes.map(c=>point(c.key)),extra={}){return db.call({op:'prepare',id,owner:'c0',snapshot,reads,changes,...extra});}
async function commit(db,id,step,vote){db.plans.set(id,{step,participants:[{name:'a',vote}]});return db.call({op:'apply',id,step,vote,shard:'a'});}

test('MVCC disjoint same-table reservations coexist and older snapshots retain rows/tombstones',async()=>{
 const db=cluster();const seed=await ready(db,'seed',0,[{key:1,row:{id:1,n:1}},{key:2,row:{id:2,n:2}}],[],{expectedEpoch:0,meta:{epoch:1,retired:false}});assert.equal(seed.status,200);assert.equal((await commit(db,'seed',1,seed.vote)).status,200);
 const a=await ready(db,'a',1,[{key:1,row:{id:1,n:11}}],[point(1)],{expectedEpoch:1});
 const b=await ready(db,'b',1,[{key:2,row:null}],[point(2)],{expectedEpoch:1});assert.equal(a.status,200);assert.equal(b.status,200);
 assert.equal((await commit(db,'b',3,b.vote)).status,200);assert.equal((await commit(db,'a',2,a.vote)).status,200);
 assert.deepEqual((await db.call({op:'scan',snapshot:1,epoch:1,range:all})).rows,[{id:1,n:1},{id:2,n:2}]);
 assert.deepEqual((await db.call({op:'scan',snapshot:2,epoch:1,range:all})).rows,[{id:1,n:11},{id:2,n:2}]);
 assert.deepEqual((await db.call({op:'scan',snapshot:3,epoch:1,range:all})).rows,[{id:1,n:11}]);
 db.restart();assert.equal((await commit(db,'a',2,a.vote)).status,200);assert.equal((await db.call({op:'scan',snapshot:3,range:all})).rows.length,1);
});

test('shared range reservations prevent phantom and write-skew, invisible applied versions reject stale writes',async()=>{
 const db=cluster();const range=rangeForFilters([{column:'id',op:'>=',value:1},{column:'id',op:'<',value:10}],'id');
 assert.equal((await ready(db,'reader',0,[{key:20,row:{id:20}}],[range])).status,200);
 const blocked=await ready(db,'phantom',0,[{key:5,row:{id:5}}]);assert.equal(blocked.status,409);assert.deepEqual(blocked.owners,['c0']);
 assert.equal((await ready(db,'skew',0,[{key:2,row:{id:2}}],[point(20)])).status,409);
 await db.call({op:'abort',id:'reader'});
 const next=await ready(db,'writer',0,[{key:5,row:{id:5}}]);await commit(db,'writer',5,next.vote);
 assert.equal((await ready(db,'stale',0,[{key:5,row:{id:5,n:1}}])).status,409);
 assert.equal((await ready(db,'stale-reader',0,[],[range])).status,409);
});

test('durable abort fences late prepare and vote replay rejects changed payload',async()=>{
 const db=cluster();await db.call({op:'abort',id:'old'});db.restart();assert.equal((await ready(db,'old',0,[])).status,409);
 const vote=await ready(db,'new',0,[{key:1,row:{id:1}}]);assert.equal((await ready(db,'new',0,[{key:1,row:{id:1}}])).vote,vote.vote);
 assert.equal((await ready(db,'new',0,[{key:2,row:{id:2}}])).status,409);
 db.plans.set('new',{step:1,participants:[{name:'a',vote:vote.vote}]});assert.equal((await db.call({op:'abort',id:'new'})).status,503);
 assert.equal((await db.call({op:'apply',id:'new',step:1,vote:'wrong',shard:'a'})).status,503);
});

test('retirement retains historical routing snapshots while rejecting current retired epoch',async()=>{
 const db=cluster();const seed=await ready(db,'seed',0,[{key:1,row:{id:1}}],[],{expectedEpoch:0,meta:{epoch:1,retired:false}});await commit(db,'seed',1,seed.vote);
 const reader=await ready(db,'reader',1,[],[point(1)],{expectedEpoch:1});assert.equal(reader.status,200);
 const blocked=await ready(db,'blocked-retire',1,[],[],{expectedEpoch:1,meta:{epoch:2,retired:true}});assert.equal(blocked.status,409);assert.deepEqual(blocked.owners,['c0']);
 await db.call({op:'abort',id:'reader'});
 const retire=await ready(db,'retire',1,[],[],{expectedEpoch:1,meta:{epoch:2,retired:true}});await commit(db,'retire',2,retire.vote);
 db.restart();assert.deepEqual((await db.call({op:'getMany',snapshot:1,epoch:1,keys:[1]})).rows,[{id:1}]);
 assert.equal((await db.call({op:'scan',snapshot:2,epoch:2,range:all})).code,'STALE_EPOCH');
 assert.equal((await ready(db,'late',1,[{key:2,row:{id:2}}],[],{expectedEpoch:1})).status,409);
});

test('ordered MVCC physical range returns exact versions for inclusive/exclusive boundaries',async()=>{
 const db=cluster();const seed=await ready(db,'seed',0,[-10,2,10].map(key=>({key,row:{id:key}})));await commit(db,'seed',1,seed.vote);
 const update=await ready(db,'update',1,[{key:2,row:{id:2,n:1}}]);await commit(db,'update',2,update.vote);
 const r=await db.call({op:'scan',snapshot:1,range:rangeForFilters([{column:'id',op:'>=',value:2},{column:'id',op:'<',value:10}],'id')});
 assert.deepEqual(r.rows,[{id:2}]);assert.equal(r.versionsRead,2);
 assert.deepEqual((await db.call({op:'scan',snapshot:2,range:rangeForFilters([{column:'id',op:'>',value:2}],'id')})).rows,[{id:10}]);
});
