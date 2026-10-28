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
export function cluster(options={}){
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

