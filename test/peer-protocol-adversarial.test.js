import test from 'node:test';
import assert from 'node:assert/strict';
import { DataShard } from '../src/datashard.js';
import { PlanAllocator } from '../src/mediator.js';
import { PlanBucket } from '../src/plan-bucket.js';
import { manifestHash, signReadSet } from '../src/protocol.js';
import { encodeKey } from '../src/keys.js';
function storage() {
 const data=new Map(); const api={
  get:async key=>structuredClone(data.get(key)),
  put:async(key,value)=>{data.set(key,structuredClone(value));},
  delete:async key=>data.delete(key),
  list:async({prefix='',start,end,limit,reverse=false}={})=>{let items=[...data].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);if(reverse)items.reverse();if(limit!==undefined)items=items.slice(0,limit);return new Map(structuredClone(items));},
  transaction:async fn=>{const before=structuredClone(data);try{return await fn(api);}catch(error){data.clear();for(const[k,v]of before)data.set(k,v);throw error;}}
 };return api;
}
const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false};
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
async function deadline(promise){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Visibility progress deadline exceeded')),2500);})]);}finally{clearTimeout(timer);}}
function cluster(options={}){
 const env={PLAN_BUCKETS:'1',...options},actors=new Map(),states=new Map();let hook=async()=>{};
 for(const [binding,Type]of [['ALLOCATOR',PlanAllocator],['MEDIATORS',PlanBucket],['SHARDS',DataShard]])env[binding]={idFromName:x=>x,get:name=>({fetch:async(url,init)=>{const key=binding+':'+name;if(!states.has(key))states.set(key,{storage:storage(),id:name});if(!actors.has(key))actors.set(key,new Type(states.get(key),env));const input=JSON.parse(init.body);await hook('before',binding,name,input);const response=await actors.get(key).fetch(new Request(url,init));await hook('after',binding,name,input,response);return response;}})};
 const raw=async(binding,name,input)=>{const response=await env[binding].get(name).fetch('https://internal',{method:'POST',body:JSON.stringify(input)});return {status:response.status,...await response.json()};};
 const call=async(...args)=>{const result=await raw(...args);assert.equal(result.status,200,JSON.stringify(result));return result;};
 return {env,raw,call,actors,states,hook(fn){hook=fn;},reset(){actors.clear();},
  async propose(id,snapshot,bodies){await call('ALLOCATOR','global',{op:'pin',id,owner:'owner',snapshot});const names=Object.keys(bodies),participants=[];for(const name of names){const p=await call('SHARDS',name,{op:'propose',id,name,owner:'owner',epoch:0,snapshot,participants:names,reads:[],changes:[],...bodies[name]});participants.push({name,bodyHash:p.bodyHash,publicKey:p.publicKey});}return {op:'assign',id,owner:'owner',epoch:0,snapshot,participants,manifestHash:await manifestHash(participants)};},
  async plan(id,snapshot,bodies,{register=true}={}){const request=await this.propose(id,snapshot,bodies);const {status,...association}=await call('ALLOCATOR','global',request);if(register){await call('MEDIATORS',association.bucket,{op:'register',association});for(const p of association.participants)await call('SHARDS',p.name,{op:'acceptPlan',association});}return association;},
  async finish(a){for(let i=0;i<8;i++){await Promise.all(a.participants.map(p=>call('SHARDS',p.name,{op:'drive',id:a.id})));const result=await call('MEDIATORS',a.bucket,{op:'get',id:a.id});if(result.applied.length===a.participants.length)return result;}assert.fail('Participants did not finish');}
 };
}

test('peer independent: absent owning bucket cannot hide an earlier allocated conflicting footprint',async()=>{
 const db=cluster({PLAN_BUCKETS:'4'});
 const earlier=await db.plan('e0_earlier',0,{a:{changes:[{key:1,row:{id:1}}]}},{register:false});
 const later=await db.plan('e0_later',0,{a:{reads:[point(1)],changes:[{key:2,row:{id:2}}]}});
 assert.notEqual(earlier.bucket,later.bucket);const result=await db.finish(later);assert.equal(result.decision.outcome,'ABORT');
 assert.deepEqual((await db.call('SHARDS','a',{op:'scan',snapshot:later.step,range:point(2)})).rows,[]);
 assert.equal((await db.raw('SHARDS','a',{op:'scan',snapshot:later.step,range:point(1)})).status,409);
});

test('peer independent: root abort-if-unassigned permanently fences delayed allocation and proposal replay',async()=>{
 const db=cluster(),request=await db.propose('e0_dead',0,{a:{changes:[{key:1,row:{id:1}}]}});
 assert.equal((await db.call('ALLOCATOR','global',{op:'abortIfUnassigned',id:'e0_dead'})).state,'aborted');
 assert.equal((await db.raw('ALLOCATOR','global',request)).status,409);
 await db.call('SHARDS','a',{op:'abort',id:'e0_dead'});
 assert.equal((await db.raw('SHARDS','a',{op:'propose',id:'e0_dead',name:'a',owner:'owner',epoch:0,snapshot:0,participants:['a'],reads:[],changes:[{key:1,row:{id:1}}]})).status,409);
});

test('peer independent: simultaneous peer exchange finishes without origin coordinator or callback deadlock',async()=>{
 const db=cluster(),a=await db.plan('e0_cycle',0,{a:{changes:[{key:1,row:{id:1}}]},b:{changes:[{key:2,row:{id:2}}]}});
 db.reset();const result=await deadline(db.finish(a));assert.equal(result.decision.outcome,'COMMIT');assert.equal(result.decision.certificate.length,2);
 for(const [name,key]of [['a',1],['b',2]])assert.deepEqual((await db.call('SHARDS',name,{op:'get',key,snapshot:a.step})).rows,[{id:key}]);
});

test('peer independent: unavailable earliest plan cannot starve healthy later plan in same bucket',async()=>{
 const db=cluster(),bad=await db.plan('e0_a_bad',0,{bad:{changes:[{key:1,row:{id:1}}]}}),good=await db.plan('e0_z_good',0,{good:{changes:[{key:2,row:{id:2}}]}});
 assert.equal(bad.bucket,good.bucket);db.hook(async(phase,binding,name)=>{if(phase==='before'&&binding==='SHARDS'&&name==='bad')throw new Error('Peer unavailable');});
 let result;for(let i=0;i<8;i++){await db.raw('MEDIATORS',good.bucket,{op:'drive'});result=await db.call('MEDIATORS',good.bucket,{op:'get',id:good.id});if(result.applied.length===1)break;}
 assert.equal(result.decision?.outcome,'COMMIT');assert.equal(result.applied.length,1);
});

test('peer independent: last applied ACK racing allocator notification is not lost',async()=>{
 const db=cluster(),a=await db.plan('e0_notify',0,{a:{changes:[{key:1,row:{id:1}}]}}),atAck=deferred(),releaseAck=deferred();let held=false,inCompletion=false,raced=false,driver;
 db.hook(async(phase,binding,name,input)=>{
  if(!held&&phase==='before'&&binding==='MEDIATORS'&&input.op==='applied'){held=true;atAck.resolve();await releaseAck.promise;}
  if(phase==='before'&&binding==='ALLOCATOR'&&input.op==='completion')inCompletion=true;
  if(inCompletion&&!raced&&phase==='after'&&binding==='MEDIATORS'&&input.op==='get'){raced=true;releaseAck.resolve();await driver;}
 });
 driver=db.call('SHARDS','a',{op:'drive',id:a.id});await deadline(atAck.promise);
 try{await deadline(db.call('MEDIATORS',a.bucket,{op:'drive',id:'not-a-plan'}));}finally{releaseAck.resolve();}
 await driver;assert.equal(raced,true);
 for(let i=0;i<3;i++)await db.call('MEDIATORS',a.bucket,{op:'drive'});
 assert.equal((await db.call('ALLOCATOR','global',{op:'snapshot'})).frontier,a.step);
});

test('peer independent: hanging relevant visibility lookup does not block unrelated same-shard read',async()=>{
 const db=cluster(),a=await db.plan('e0_readhang',0,{a:{changes:[{key:1,row:{id:1}}]}}),entered=deferred(),release=deferred();let once=false;
 db.hook(async(phase,binding,name,input)=>{if(!once&&phase==='before'&&binding==='ALLOCATOR'&&input.op==='lookup'&&input.id===a.id){once=true;entered.resolve();await release.promise;}});
 const affected=db.raw('SHARDS','a',{op:'get',key:1,snapshot:a.step});await deadline(entered.promise);
 try{const unrelated=await deadline(db.call('SHARDS','a',{op:'get',key:2,snapshot:a.step}));assert.deepEqual(unrelated.rows,[]);}finally{release.resolve();await affected;}
});

test('peer independent: hanging finalize authority lookup does not block unrelated same-shard read',async()=>{
 const db=cluster(),a=await db.plan('e0_finalizehang',0,{a:{changes:[{key:1,row:{id:1}}]}}),state=await db.finish(a),entered=deferred(),release=deferred();let once=false;
 db.hook(async(phase,binding,name,input)=>{if(!once&&phase==='before'&&binding==='MEDIATORS'&&input.op==='get'&&input.id===a.id){once=true;entered.resolve();await release.promise;}});
 const replay=db.call('SHARDS','a',{op:'finalize',association:a,decision:state.decision});await deadline(entered.promise);
 try{const unrelated=await deadline(db.call('SHARDS','a',{op:'get',key:2,snapshot:a.step}));assert.deepEqual(unrelated.rows,[]);}finally{release.resolve();await replay;}
});

test('peer independent: current epoch cannot relabel a retired transaction to bypass bucket admission',async()=>{
 const db=cluster(),a=await db.plan('e0_retired',0,{a:{changes:[{key:1,row:{id:1}}]}});await db.finish(a);
 await db.call('ALLOCATOR','global',{op:'completion',id:a.id});await db.call('ALLOCATOR','global',{op:'unpin',id:a.id});
 await db.call('ALLOCATOR','global',{op:'rotateEpoch'});await db.call('ALLOCATOR','global',{op:'retireEpochs',before:1});
 assert.notEqual((await db.raw('ALLOCATOR','global',{op:'lookup',id:a.id,epoch:1})).status,200);
 assert.notEqual((await db.raw('MEDIATORS',a.bucket,{op:'get',id:a.id,epoch:1})).status,200);
});

test('peer independent: signed inbox replay is idempotent, equivocation and incomplete certificates are rejected',async()=>{
 const db=cluster(),a=await db.plan('e0_cert',0,{a:{changes:[{key:1,row:{id:1}}]},b:{changes:[{key:2,row:{id:2}}]}});
 await db.call('SHARDS','a',{op:'drive',id:a.id});const vote=await db.states.get('SHARDS:a').storage.get(`readset:${a.id}`);
 await db.call('SHARDS','b',{op:'readSet',association:a,readSet:vote});await db.call('SHARDS','b',{op:'readSet',association:a,readSet:vote});
 assert.notEqual((await db.raw('MEDIATORS',a.bucket,{op:'decide',id:a.id,certificate:[vote]})).status,200);
 assert.notEqual((await db.raw('MEDIATORS',a.bucket,{op:'decide',id:a.id,certificate:[vote,vote]})).status,200);
 const identity=await db.states.get('SHARDS:a').storage.get('identity'),equivocation=await signReadSet(identity.privateKey,a,'a','ABORT');
 assert.notEqual((await db.raw('SHARDS','b',{op:'readSet',association:a,readSet:equivocation})).status,200);
 assert.notEqual((await db.raw('SHARDS','b',{op:'readSet',association:a,readSet:{...vote,manifestHash:'0'.repeat(64)}})).status,200);
 const state=await db.finish(a);assert.equal(state.decision.outcome,'COMMIT');
 assert.notEqual((await db.raw('MEDIATORS',a.bucket,{op:'decide',id:a.id,certificate:[equivocation]})).status,200);
});
