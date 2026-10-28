import {coordinatorName,COORDINATOR_POOL_SIZE} from './coordinator-routing.js';
const json=(body,status=200)=>Response.json(body,{status});
const bad=message=>Object.assign(new Error(message),{status:400,code:'INVALID_SCOPE'});
const busy=()=>Object.assign(new Error('Coordinator service queue capacity reached; retry same request'),{status:503,code:'COORDINATOR_BACKPRESSURE'});
const encoded=scope=>btoa(String.fromCharCode(...new TextEncoder().encode(scope))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const alarmPrefix='_pool/due/';
const dueKey=(deadline,scope)=>`${alarmPrefix}${String(deadline).padStart(16,'0')}/${encoded(scope)}`;
const alarmKey=scope=>'_pool/alarm/'+encoded(scope);
const keyBytes=value=>new TextEncoder().encode(value).length;

export function scopedStorage(raw,scope,alarmDriver={}){
 coordinatorName(scope);const transactional=!!alarmDriver.transactional;
  const service={scopedStorage:(scope,txn,transactional)=>scopedStorage(txn,scope,{...alarmDriver,transactional}),changeAlarm:alarmDriver?.changeAlarm||(()=>{throw new Error('Virtual alarm driver required');})},prefix=`scope:${encoded(scope)}:`,upper=prefix.slice(0,-1)+';';
  const key=value=>{if(typeof value!=='string')throw bad('Storage key must be a string');const result=prefix+value;if(keyBytes(result)>2048)throw bad('Scoped physical storage key exceeds 2048 bytes');return result;};
  const strip=value=>{if(!value.startsWith(prefix))throw new Error('Storage namespace escaped');return value.slice(prefix.length);};
  const perform=fn=>transactional||!alarmDriver.perform?fn():alarmDriver.perform(fn);
  const transact=fn=>alarmDriver.transact?alarmDriver.transact(fn):raw.transaction(fn);
  const api={
   async get(value,options){if(Array.isArray(value)){const found=await perform(()=>raw.get(value.map(key),options));return new Map([...found].map(([k,v])=>[strip(k),v]));}return perform(()=>raw.get(key(value),options));},
   async put(value,data,options){if(typeof value==='string')return perform(()=>raw.put(key(value),data,options));if(!value||typeof value!=='object')throw bad('Invalid storage entries');const entries=Object.create(null);for(const [k,v]of value instanceof Map?value:Object.entries(value))entries[key(k)]=v;return perform(()=>raw.put(entries,data));},
   async delete(value,options){return perform(()=>raw.delete(Array.isArray(value)?value.map(key):key(value),options));},
   async list(options={}){
    const mapped={...options,prefix:key(options.prefix??''),end:options.end===undefined?upper:key(options.end)};
    if(options.start!==undefined)mapped.start=key(options.start);else if(options.startAfter===undefined)mapped.start=prefix;
    if(options.startAfter!==undefined)mapped.startAfter=key(options.startAfter);
    const found=await perform(()=>raw.list(mapped));return new Map([...found].map(([k,v])=>[strip(k),v]));
   },
   async transaction(fn){if(transactional)return fn(api);return transact(txn=>fn(service.scopedStorage(scope,txn,true))); },
   async getAlarm(){return (await perform(()=>raw.get(alarmKey(scope))))?.deadline??null;},
   async setAlarm(deadline){const timestamp=deadline instanceof Date?deadline.getTime():deadline;return transactional?service.changeAlarm(raw,scope,timestamp):transact(txn=>service.changeAlarm(txn,scope,timestamp));},
   async deleteAlarm(){return transactional?service.changeAlarm(raw,scope,null):transact(txn=>service.changeAlarm(txn,scope,null));},
  };
  return api;

}

/** Fixed physical coordinator pool, with isolated durable logical owners.
 * Only short local storage transactions are shared. Coordinator network work
 * retains per-scope queues, so a blocked owner cannot serialize other owners.
 */
export class CoordinatorService {
 constructor(state,env,TransactionCoordinatorClass){
  if(env.COORDINATOR_POOL_SIZE!==undefined&&Number(env.COORDINATOR_POOL_SIZE)!==COORDINATOR_POOL_SIZE)throw new Error('Coordinator pool v1 has exactly16 services; changing it requires a routing version');
  this.state=state;this.storage=state.storage;this.env=env;this.Class=TransactionCoordinatorClass;this.contexts=new Map();this.totalRefs=0;this.storageTail=Promise.resolve();
  this.maxContexts=this.limit('COORDINATOR_MAX_CONTEXTS',4,8);
  this.maxTotal=this.limit('COORDINATOR_MAX_QUEUED',16,32);
  this.totalBytes=0;this.maxBytes=this.limit('COORDINATOR_MAX_QUEUED_BYTES',8*1024*1024,8*1024*1024);
  this.maxScope=this.limit('COORDINATOR_MAX_SCOPE_QUEUED',4,8);
  this.alarmBatch=this.limit('COORDINATOR_ALARM_BATCH',8,32);
  this.alarmSlice=this.limit('COORDINATOR_ALARM_SLICE_MS',50,1000);
 }
 limit(name,value,max){const n=Number(this.env[name]??value);if(!Number.isSafeInteger(n)||n<1||n>max)throw new Error(`Invalid ${name}`);return n;}
 now(){return typeof this.env.NOW==='function'?this.env.NOW():Date.now();}
 checkScope(scope,coordinatorId){
  let route;try{route=coordinatorName(scope);}catch(e){throw bad(e.message);}
  if(coordinatorId!==undefined&&coordinatorId!==scope)throw bad('Scope and logical coordinatorId differ');
  if(this.state.id&&this.env.COORDINATOR&&String(this.state.id)!==String(this.env.COORDINATOR.idFromName(route)))throw bad('Scope routed to wrong physical coordinator service');
  return route;
 }
 // Never enter overlapping physical storage transactions from different logical
 // contexts. Only local KV/crypto callbacks use this queue, never actor RPCs.
 localStorage(fn){const task=this.storageTail.then(fn);this.storageTail=task.catch(()=>{});return task;}
 localTransaction(fn){return this.localStorage(()=>this.storage.transaction(fn));}
 scopedStorage(scope,raw=this.storage,transactional=false){return scopedStorage(raw,scope,{transactional,perform:fn=>this.localStorage(fn),transact:fn=>this.localTransaction(fn),changeAlarm:(txn,scope,deadline)=>this.changeAlarm(txn,scope,deadline)});}
 acquire(scope,alarm=false,payloadBytes=0){
  let context=this.contexts.get(scope);
  if(alarm&&context?.refs)return null;
  if(this.totalBytes+payloadBytes>this.maxBytes||this.totalRefs>=this.maxTotal||(context?.refs||0)>=this.maxScope||(!context&&this.contexts.size>=this.maxContexts))throw busy();
  if(!context){
   const state={storage:this.scopedStorage(scope),id:this.state.id};
   if(this.state.waitUntil)state.waitUntil=this.state.waitUntil.bind(this.state);
   context={actor:new this.Class(state,this.env),refs:0,alarmRunning:false};this.contexts.set(scope,context);
  }
  context.refs++;this.totalRefs++;this.totalBytes+=payloadBytes;if(alarm)context.alarmRunning=true;
  return context;
 }
 release(scope,context,alarm=false,payloadBytes=0){if(alarm)context.alarmRunning=false;context.refs--;this.totalRefs--;this.totalBytes-=payloadBytes;if(context.refs===0&&this.contexts.get(scope)===context)this.contexts.delete(scope);}
 async fetch(request){
  let context,scope,payloadBytes=0;
  try{
   const input=await request.json();scope=input.scope;this.checkScope(scope,input.coordinatorId);
   if(!['recover','collect'].includes(input.op)&&input.coordinatorId!==scope)throw bad('Mutation envelope requires matching coordinatorId');
   const body=JSON.stringify(input);payloadBytes=keyBytes(body);context=this.acquire(scope,false,payloadBytes);
   return await context.actor.fetch(new Request(request.url,{method:'POST',headers:{'content-type':'application/json'},body}));
  }catch(e){return json({error:e.message,code:e.code||'COORDINATOR_SERVICE_ERROR',retryable:(e.status||503)>=500},e.status||503);}
  finally{if(context)this.release(scope,context,false,payloadBytes);}
 }
 async schedule(txn){
  const first=(await txn.list({prefix:alarmPrefix,limit:1})).values().next().value;
  if(first)await txn.setAlarm(first.deadline>this.now()?first.deadline:this.now()+1000);else await txn.deleteAlarm();
 }
 async changeAlarm(txn,scope,deadline){
  if(deadline!==null&&(!Number.isSafeInteger(deadline)||deadline<0))throw bad('Invalid virtual alarm deadline');
  const old=await txn.get(alarmKey(scope));if(old)await txn.delete(dueKey(old.deadline,scope));
  // Unique incarnation avoids ABA after delete/recreate without retaining one
  // permanent counter record for every logical request owner ever observed.
  const revision=crypto.randomUUID();
  if(deadline===null)await txn.delete(alarmKey(scope));
  else {const record={scope,revision,deadline};await txn.put(alarmKey(scope),record);await txn.put(dueKey(deadline,scope),record);}
  await this.schedule(txn);
 }
 async alarm(){
  const batch=await this.localTransaction(async txn=>{
   const cursor=await txn.get('_pool/dispatchCursor');
   const end=alarmPrefix+String(this.now()+1).padStart(16,'0');
   let page=await txn.list({prefix:alarmPrefix,...(cursor?{start:cursor+'\u0000'}:{}),end,limit:this.alarmBatch});
   if(!page.size&&cursor)page=await txn.list({prefix:alarmPrefix,end,limit:this.alarmBatch});
   if(page.size)await txn.put('_pool/dispatchCursor',[...page.keys()].at(-1));
   // Leave every due record durable and schedule rediscovery BEFORE any RPC.
   await this.schedule(txn);return [...page.values()];
  });
  const tasks=[];
  for(const record of batch){
   let context;try{this.checkScope(record.scope);context=this.acquire(record.scope,true);}catch{continue;}
   if(!context)continue;
   tasks.push((async()=>{
    try{
     await context.actor.alarm();
     await this.localTransaction(async txn=>{
      const current=await txn.get(alarmKey(record.scope));
      if(current?.revision===record.revision){await txn.delete(alarmKey(record.scope));await txn.delete(dueKey(record.deadline,record.scope));}
      // A concurrent scoped set/delete wins by revision; never erase it.
      await this.schedule(txn);
     });
    }catch{
     // Failure retains the exact durable due entry for fair later retry.
     await this.localTransaction(txn=>this.schedule(txn));
    }finally{this.release(record.scope,context,true);}
   })());
  }
  const completion=Promise.allSettled(tasks);
  // Returning the alarm handler must not depend on an unavailable remote peer.
  // Background work keeps its scope refs until its actual settlement; the next
  // durable wake can skip those busy scopes and dispatch other due owners.
  this.state.waitUntil?.(completion);
  let timer;
  try{await Promise.race([completion,new Promise(resolve=>{timer=setTimeout(resolve,this.alarmSlice);})]);}
  finally{if(timer!==undefined)clearTimeout(timer);}
 }
}
