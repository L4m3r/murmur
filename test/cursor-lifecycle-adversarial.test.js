import test from 'node:test';
import assert from 'node:assert/strict';
import {QueryCursor} from '../src/query-cursor.js';
import {DataShard} from '../src/datashard.js';
import {PlanAllocator} from '../src/mediator.js';
import {readRecord} from '../src/records.js';
import {encodeKey} from '../src/keys.js';
function storage(){const data=new Map();let fail=null;const check=(op,key,value)=>{if(fail&&fail(op,key,value)){fail=null;throw new Error('Injected cursor storage rollback');}};const api={data,failOnce(fn){fail=fn;},async get(k){return structuredClone(data.get(k));},async put(k,v){check('put',k,v);data.set(k,structuredClone(v));},async delete(k){check('delete',k);return data.delete(k);},async list({prefix='',start,end,limit,reverse=false}={}){let rows=[...data].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);if(reverse)rows.reverse();if(limit!==undefined)rows=rows.slice(0,limit);return new Map(structuredClone(rows));},async transaction(fn){const old=structuredClone(data);try{return await fn(api);}catch(e){data.clear();for(const[k,v]of old)data.set(k,v);throw e;}},async setAlarm(at){api.alarm=at;},async deleteAlarm(){api.alarm=null;}};return api;}
async function model({payload='small',maxPages=8,count=6}={}){
 const control=storage(),cursorStore=storage(),env={NOW:()=>1000,CURSOR_MAX_CACHE_PAGES:maxPages},shards=new Map();
 const allocator=new PlanAllocator({storage:control},env);allocator.now=()=>1000;await control.put('allocated',1);await control.put('frontier',1);
 env.ALLOCATOR={idFromName:x=>x,get:()=>({fetch:(url,init)=>allocator.fetch(new Request(url,init))})};
 env.SHARDS={idFromName:x=>x,get:name=>({fetch:async(url,init)=>{if(!shards.has(name))shards.set(name,new DataShard({storage:storage(),id:name},env));return shards.get(name).fetch(new Request(url,init));}})};
 const schema={primaryKey:'id',primaryKeys:['id'],columns:[{name:'id',type:'INTEGER',primaryKey:true,notNull:true},{name:'payload',type:'TEXT'}],epoch:1,tablets:[{id:'t0',start:null,end:null,epoch:1}],indexes:[]};
 const seed=async(name,changes,meta)=>{const actor=new DataShard({storage:storage(),id:name},env);shards.set(name,actor);await actor.storage.transaction(tx=>actor.appendVersions(tx,{changes:changes.map(c=>({...c,encoded:encodeKey(c.key)})),meta},1));};
 await seed('catalog/a',[{key:'schema',row:schema}]);await seed('base/a/t0',Array.from({length:count},(_,id)=>({key:id,row:{id,payload}})),{epoch:1,retired:false});
 let cursor=new QueryCursor({storage:cursorStore},env);
 const call=async input=>{const r=await cursor.fetch(new Request('https://cursor',{method:'POST',body:JSON.stringify(input)}));return{status:r.status,...await r.json()};};
 env.CURSORS={idFromName:x=>x,get:()=>({fetch:(url,init)=>cursor.fetch(new Request(url,init))})};
 return{env,allocator,control,cursorStore,call,alarm:()=>cursor.alarm(),restart(){cursor=new QueryCursor({storage:cursorStore},env);},start:()=>call({op:'start',id:'cursor1',epoch:0,sql:'SELECT id,payload FROM a ORDER BY id',pageSize:2,ttlMs:10000}),job:()=>readRecord(cursorStore,'job')};
}

test('cursor lifecycle independent: rolled-back chunked page receipt cannot persist phantom accounting or pin',async()=>{
 const db=await model({payload:'界'.repeat(60000),maxPages:1}),start=await db.start();assert.equal(start.status,200);
 db.cursorStore.failOnce((op,key)=>op==='put'&&key.startsWith('receipt:'));
 const failed=await db.call({op:'page',token:start.token});assert.equal(failed.status,503);
 const job=await db.job();assert.equal(job.cachePages,0);assert.equal(job.cacheBytes,0);assert.equal(job.highestIssued,-1);assert.equal(job.active,undefined);assert.equal(job.releasingPin,undefined);
 assert.equal([...db.cursorStore.data.keys()].filter(k=>k.startsWith('page:')||k.startsWith('receipt:')||k.startsWith('__records_chunk_v1/')).length,0);
 assert.equal(await db.control.get('pinCount'),0);
 db.restart();const page=await db.call({op:'page',token:start.token});assert.equal(page.status,200);assert.deepEqual(page.rows.map(r=>r.id),[0,1]);assert.equal((await db.job()).cachePages,1);
 assert.deepEqual(await db.call({op:'page',token:start.token}),page);
 assert.equal((await db.call({op:'ack',throughSeq:0})).status,200);assert.equal((await db.job()).cachePages,0);
 assert.equal((await db.call({op:'page',token:page.nextToken})).status,200,'rolled-back accounting must not consume the single-page window');
});

test('cursor lifecycle independent: failed acknowledged-page cleanup cannot undercount retained receipts through finally-save',async()=>{
 const db=await model(),start=await db.start(),p0=await db.call({op:'page',token:start.token}),p1=await db.call({op:'page',token:p0.nextToken});assert.equal(p1.status,200);
 const original=await db.job(),second='receipt:0000000000000001';
 db.cursorStore.failOnce((op,key)=>op==='delete'&&key===second);
 assert.equal((await db.call({op:'ack',throughSeq:1})).status,503);assert.equal((await db.job()).rejectBeforePage,2,'ACK fence survives cleanup rollback');
 db.cursorStore.failOnce((op,key)=>op==='delete'&&key===second);
 assert.equal((await db.call({op:'page',token:p1.nextToken})).status,503);
 const after=await db.job();assert.equal(after.cachePages,2);assert.equal(after.cacheBytes,original.cacheBytes);assert.equal([...db.cursorStore.data.keys()].filter(k=>k.startsWith('receipt:')).length,2);assert.equal(await db.control.get('pinCount'),0);
 db.restart();assert.equal((await db.call({op:'page',token:start.token})).code,'PAGE_EXPIRED');
 assert.equal((await db.call({op:'ack',throughSeq:1})).status,200);assert.equal((await db.job()).cachePages,0);assert.equal((await db.job()).cacheBytes,0);
 const next=await db.call({op:'page',token:p1.nextToken});assert.equal(next.status,200);assert.deepEqual(next.rows.map(r=>r.id),[4,5]);
});

test('cursor lifecycle independent: ACK cleanup beyond one batch keeps an early alarm instead of waiting for lease expiry',async()=>{
 const db=await model({maxPages:12,count:24}),start=await db.start();let token=start.token;
 for(let seq=0;seq<12;seq++){const page=await db.call({op:'page',token});assert.equal(page.status,200);assert.equal(page.page,seq);token=page.nextToken;}
 assert.equal((await db.job()).cachePages,12);
 const ack=await db.call({op:'ack',throughSeq:11});assert.equal(ack.status,200);assert.equal(ack.done,false);assert.equal((await db.job()).cachePages,4);
 assert.equal(db.cursorStore.alarm,1100,'remaining ACK cleanup must run before the 11000 lease deadline');
 db.restart();await db.alarm();assert.equal((await db.job()).cachePages,0);assert.equal([...db.cursorStore.data.keys()].filter(k=>k.startsWith('receipt:')).length,0);
 assert.equal((await db.call({op:'page',token:start.token})).code,'PAGE_EXPIRED');
});

test('cursor lifecycle independent: epoch maintenance retains owner discovery until every bounded receipt batch is collected',async()=>{
 const {default:Worker,PlanBucket}=await import('../src/index.js');const db=await model({maxPages:24,count:48}),start=await db.start();let token=start.token;
 for(let seq=0;seq<24;seq++){const page=await db.call({op:'page',token});assert.equal(page.status,200);token=page.nextToken;}
 assert.equal((await db.call({op:'close'})).done,false);assert.equal((await db.job()).cachePages,16);
 const buckets=new Map();db.env.MEDIATORS={idFromName:x=>x,get:name=>({fetch:(url,init)=>{if(!buckets.has(name))buckets.set(name,new PlanBucket({storage:storage()},db.env));return buckets.get(name).fetch(new Request(url,init));}})};
 const med=async input=>{const r=await db.allocator.fetch(new Request('https://allocator',{method:'POST',body:JSON.stringify(input)}));return{status:r.status,...await r.json()};};
 await med({op:'rotateEpoch'});assert.equal((await med({op:'retireEpochs',before:1})).status,200);
 const maintenance=async()=>{const r=await Worker.fetch(new Request('http://127.0.0.1/maintenance',{method:'POST',body:JSON.stringify({action:'collectEpochs'})}),db.env);return{status:r.status,...await r.json()};};
 const first=await maintenance();assert.equal(first.status,200);assert.equal((await db.job()).cachePages,8);
 assert.ok((await med({op:'retiredOwners'})).owners.some(o=>o.owner==='cursor-job/cursor1'),'partially collected owner must remain discoverable');assert.equal(first.done,false);
 const second=await maintenance();assert.equal(second.status,200);assert.equal(await db.job(),undefined);assert.ok(!(await med({op:'retiredOwners'})).owners.some(o=>o.owner==='cursor-job/cursor1'));
 assert.equal([...db.cursorStore.data.keys()].filter(k=>k.startsWith('receipt:')||k.startsWith('page:')).length,0);
});
