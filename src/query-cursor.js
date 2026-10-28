import { decodeRpcResponse } from './rpc-response.js';
import { physicalTableName } from './schema.js';
import { parse, execute } from './sql.js';
import { encodeKey, keyOfRow, primaryColumns, primaryRangeForFilters, rangeForFilters } from './keys.js';
import { routeTablet, tabletEpoch, selectTablets } from './tablets.js';
import { canonical, hash, rpc } from './protocol.js';
import { readRecord, writeRecord, deleteRecord } from './records.js';

const json = (value,status=200)=>Response.json(value,{status});
const bytes=value=>new TextEncoder().encode(JSON.stringify(value)).length;
const error=(message,status=400,code='INVALID_CURSOR')=>Object.assign(new Error(message),{status,code});
const closed=()=>error('Query cursor lease is closed or expired',410,'CURSOR_EXPIRED');
const ROOT_OPTIONS={maxBytes:4*1024*1024}, PAGE_OPTIONS={maxBytes:1024*1024};
const RESPONSE_BYTES=512*1024, RPC_BUDGET=8;
const receiptKey=seq=>'receipt:'+String(seq).padStart(16,'0');
const baseName=(table,id,schema)=>`base/${physicalTableName(table,schema)}/${id}`;
const indexName=(table,index,id,schema)=>`index/${physicalTableName(table,schema)}/${index.storageName||index.name}/${id}`;
const to64=text=>btoa(text).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const from64=text=>atob(text.replace(/-/g,'+').replace(/_/g,'/'));

/** Fixed-query snapshot leases with independently admitted operation pins.
 * Immutable signed positions and per-token receipts preserve exact retries even
 * when future-only physical heads change bounded work-page boundaries.
 */
export class QueryCursor {
 constructor(state,env){this.storage=state.storage;this.env=env;this.tail=Promise.resolve();}
 run(fn){const task=this.tail.then(fn);this.tail=task.catch(()=>{});return task;}
 fetch(request){return this.run(async()=>this.handle(await request.json())).catch(e=>json({error:e.message,code:e.code||'CURSOR_RETRY',rpc:e.rpc,retryAfterMs:e.retryAfterMs,retryable:(e.status||503)>=500,dependencies:e.dependencies||[],owners:e.owners||[]},e.status||503));}
 now(){return typeof this.env.NOW==='function'?this.env.NOW():Date.now();}
 med(input){return rpc(this.env.ALLOCATOR,'global',input);}
 async shard(name,input){const response=await this.env.SHARDS.get(this.env.SHARDS.idFromName(name)).fetch('https://shard/',{method:'POST',body:JSON.stringify(input)});const body=await decodeRpcResponse(response,`${name}:${input.op}`);if(!response.ok)throw Object.assign(new Error(body.error||'Shard unavailable'),{...body,status:response.status});return body;}
 load(){return readRecord(this.storage,'job',ROOT_OPTIONS);}
 save(job){return this.storage.transaction(txn=>writeRecord(txn,'job',job,ROOT_OPTIONS));}
 budget(name,fallback,max){const n=Number(this.env[name]??fallback);if(!Number.isSafeInteger(n)||n<1||n>max)throw error(`Invalid ${name} configuration`);return n;}
 async arm(job){const expiry=job.expiresAt||this.now()+1000;await this.storage.setAlarm?.(job.closed?this.now()+100:job.cleanupPending?Math.min(expiry,this.now()+100):expiry);}
 async resolveRenewal(job){
  if(!job.renewing)return;
  const intent=job.renewing;
  let lease;
  try{
   // Existing lease reads are idempotent and do not extend its TTL. A closed
   // identity is fenced by the allocator; expired leases cannot be recreated.
   lease=await this.med({op:'lease',id:job.leaseId,epoch:job.epoch,snapshot:job.snapshot});
   if(lease.expiresAt<intent.targetExpiresAt&&lease.expiresAt<=intent.previousExpiresAt){
    const remaining=intent.targetExpiresAt-this.now();
    if(remaining<=0)throw closed();
    lease=await this.med({op:'renewLease',id:job.leaseId,epoch:job.epoch,snapshot:job.snapshot,ttlMs:remaining});
   }
  }catch(e){
   if(e.status!==410&&e.code!=='LEASE_CLOSED')throw e;
   const staged=structuredClone(job);delete staged.renewing;staged.expiresAt=Math.min(staged.expiresAt,this.now());
   await this.save(staged);delete job.renewing;job.expiresAt=staged.expiresAt;
   return;
  }
  if(lease.snapshot!==job.snapshot)throw error('Renewal changed the fixed snapshot',409,'CURSOR_MISMATCH');
  const staged=structuredClone(job);delete staged.renewing;staged.expiresAt=lease.expiresAt;
  await this.save(staged);delete job.renewing;job.expiresAt=lease.expiresAt;
  await this.arm(job);
 }
 async renew(input,job){
  if(job.closed||job.state!=='ready'||job.expiresAt<=this.now())throw closed();
  const ttlMs=input.ttlMs??job.ttlMs;
  if(!Number.isSafeInteger(ttlMs)||ttlMs<1||ttlMs>300000)throw error('Renewal ttlMs must be1..300000');
  const targetExpiresAt=Math.max(job.expiresAt,this.now()+ttlMs);
  job.renewing={previousExpiresAt:job.expiresAt,targetExpiresAt};await this.save(job);
  await this.storage.setAlarm?.(this.now()+1000);
  await this.resolveRenewal(job);
  if(job.expiresAt<=this.now())throw closed();
  return json({id:job.id,snapshot:job.snapshot,expiresAt:job.expiresAt,renewed:true});
 }
 async releasePin(job){
  if(job.active){job.releasingPin=job.active.id;delete job.active;await this.save(job);}
  if(job.releasingPin){await this.med({op:'unpin',id:job.releasingPin,epoch:job.epoch});delete job.releasingPin;await this.save(job);}
 }
 async admit(job){
  if(job.closed||job.expiresAt<=this.now())throw closed();
  await this.releasePin(job);
  job.active={id:`qc-${crypto.randomUUID()}`,state:'admitting'};await this.save(job);await this.storage.setAlarm?.(this.now()+1000);
  await this.med({op:'pin',id:job.active.id,epoch:job.epoch,owner:`cursor-job/${job.id}`,leaseId:job.leaseId,snapshot:job.snapshot});
  job.active.state='admitted';await this.save(job);
 }
 async closeJob(job){
  if(!job.closed){job.closed=true;job.releasingLease=true;await this.save(job);}
  await this.releasePin(job);
  if(job.releasingLease){await this.med({op:'releaseLease',id:job.leaseId,epoch:job.epoch});delete job.releasingLease;await this.save(job);}
 }
 async cleanup(job,ackOnly=false){
  const entries=await this.storage.list({prefix:'receipt:',...(ackOnly?{end:receiptKey(job.rejectBeforePage||0)}:{}),limit:8});
  const staged=structuredClone(job);
  await this.storage.transaction(async txn=>{
   for(const [key,receipt]of entries){await deleteRecord(txn,receipt.cacheKey,PAGE_OPTIONS);await txn.delete(key);staged.cachePages--;staged.cacheBytes-=receipt.bytes;}
   staged.cleanupPending=(await txn.list({prefix:'receipt:',...(ackOnly?{end:receiptKey(job.rejectBeforePage||0)}:{}),limit:1})).size>0;
   await writeRecord(txn,'job',staged,ROOT_OPTIONS);
  });
  job.cachePages=staged.cachePages;job.cacheBytes=staged.cacheBytes;job.cleanupPending=staged.cleanupPending;
  const more=job.cleanupPending;
  if(more)await this.storage.setAlarm?.(this.now()+100);else if(job.closed)await this.collectClosed(job);
  return !more;
 }
 async collectClosed(job){
  const state=await this.med({op:'epoch'});
  if(job.epoch<state.rejectBeforeEpoch){
   await this.storage.transaction(async txn=>{await txn.put('rejectBeforeEpoch',Math.max(await txn.get('rejectBeforeEpoch')||0,state.rejectBeforeEpoch));await deleteRecord(txn,'job',ROOT_OPTIONS);});
   await this.storage.deleteAlarm?.();return true;
  }
  // Even initialization interrupted before its first pin has no owner registry
  // entry. Its own alarm retains the closed replay identity until retirement,
  // then removes secrets/metadata behind a permanent local epoch fence.
  await this.storage.setAlarm?.(this.now()+60000);return false;
 }
 async alarm(){return this.run(async()=>{const job=await this.load();if(!job)return;try{await this.releasePin(job);await this.resolveRenewal(job);if(job.closed||job.expiresAt===undefined||job.expiresAt<=this.now()){await this.closeJob(job);await this.cleanup(job);}else {await this.cleanup(job,true);await this.arm(job);}}catch{await this.storage.setAlarm?.(this.now()+1000);}});}
 async signature(job,body){const key=await crypto.subtle.importKey('raw',Uint8Array.from(job.secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);return [...new Uint8Array(await crypto.subtle.sign('HMAC',key,new TextEncoder().encode(canonical(body))))].map(x=>x.toString(16).padStart(2,'0')).join('');}

 async verifySignature(job,body,signature){
  if(typeof signature!=='string'||!/^([0-9a-f]{2}){32}$/.test(signature))return false;
  const key=await crypto.subtle.importKey('raw',Uint8Array.from(job.secret),{name:'HMAC',hash:'SHA-256'},false,['verify']);
  return crypto.subtle.verify('HMAC',key,Uint8Array.from(signature.match(/../g),byte=>parseInt(byte,16)),new TextEncoder().encode(canonical(body)));
 }
 async token(job,position){const body={v:1,context:job.context,...position};return to64(canonical(body))+'.'+await this.signature(job,body);}
 async position(job,token){
  if(typeof token!=='string'||token.length>8192)throw error('Invalid continuation token');
  const [payload,signature,...extra]=token.split('.');let body;try{body=JSON.parse(from64(payload));}catch{throw error('Malformed continuation token');}
  if(extra.length||payload!==to64(canonical(body))||!await this.verifySignature(job,body,signature)||body.v!==1||body.context!==job.context||!Number.isSafeInteger(body.tablet)||body.tablet<0||body.tablet>job.tablets.length||!Number.isSafeInteger(body.seq)||body.seq<0||!Number.isSafeInteger(body.emitted)||body.emitted<0||!(body.cursor===null||typeof body.cursor==='string'&&/^[0-9a-f]{1,1800}$/.test(body.cursor)))throw error('Continuation token does not match query context');
  return body;
 }
 async start(input,job){
  if(typeof input.id!=='string'||!/^[A-Za-z0-9_-]{1,100}$/.test(input.id)||!Number.isSafeInteger(input.epoch)||input.epoch<0)throw error('Invalid cursor identity');
  const pageSize=input.pageSize??64,ttlMs=input.ttlMs??60000;
  if(!Number.isSafeInteger(pageSize)||pageSize<1||pageSize>128||!Number.isSafeInteger(ttlMs)||ttlMs<1||ttlMs>300000)throw error('pageSize1..128 and ttlMs1..300000 required');
  let statements;try{statements=parse(input.sql,input.params===undefined?{}:input.params);}catch(e){throw error(e.message);}
  if(statements.length!==1||statements[0].kind!=='select')throw error('Cursor requires one SELECT');
  const identity=await hash({id:input.id,epoch:input.epoch,sql:input.sql,params:input.params||{},pageSize,ttlMs,snapshot:input.snapshot});
  await this.med({op:'epoch',requestEpoch:input.epoch});
  if(input.epoch<(await this.storage.get('rejectBeforeEpoch')||0))throw closed();
  if(job&&job.identity!==identity)throw error('Cursor identity reused with another query',409,'CURSOR_MISMATCH');
  if(!job){job={id:input.id,epoch:input.epoch,identity,statement:statements[0],pageSize,ttlMs,leaseId:`cursor-${input.id}`,requestedSnapshot:input.snapshot,cacheBytes:0,cachePages:0,rejectBeforePage:0,highestIssued:-1,secret:[...crypto.getRandomValues(new Uint8Array(32))],state:'starting'};await this.save(job);await this.storage.setAlarm?.(this.now()+1000);}
  await this.resolveRenewal(job);
  if(job.closed)throw closed();
  if(job.state==='ready'){if(job.expiresAt<=this.now())throw closed();return this.reply(job);}
  await this.releasePin(job);
  const lease=await this.med({op:'lease',id:job.leaseId,epoch:job.epoch,ttlMs:job.ttlMs,...(job.requestedSnapshot===undefined?{}:{snapshot:job.requestedSnapshot})});
  job.snapshot=lease.snapshot;job.expiresAt=lease.expiresAt;await this.save(job);
  try{
   await this.admit(job);
   const found=await this.shard(`catalog/${job.statement.table}`,{op:'getMany',keys:['schema'],snapshot:job.snapshot});
   if(!found.rows[0])throw error('Unknown table');
   job.schema=found.rows[0];if(job.schema.state==='dropped')throw error('Unknown table');const statement=job.statement,schema=job.schema;
   try{execute([statement],{[statement.table]:schema},{[statement.table]:[]});}catch(e){throw error(e.message);}
   const primary=primaryColumns(schema)[0],order=statement.order?.column;
   const eligible=(schema.indexes||[]).filter(i=>!['building','validating'].includes(i.state));
   let index=null;
   if(order&&order!==primary){index=eligible.find(i=>i.column===order);if(!index)throw error('Cursor ORDER BY must use primary key or a ready index column');}
   else if(!order){const bounded=column=>statement.filter.some(f=>f.column===column&&['=','<','<=','>','>='].includes(f.op));if(!bounded(primary))index=eligible.find(i=>bounded(i.column))||null;}
   job.index=index;job.reverse=!!statement.order?.desc;
   job.range=index?rangeForFilters(statement.filter,index.column,{compositePrefix:true}):primaryRangeForFilters(schema,statement.filter);
   job.tablets=selectTablets(index||schema,job.range);if(job.reverse)job.tablets.reverse();
   job.context=await hash({identity:job.identity,snapshot:job.snapshot,schema:job.schema,index:job.index,range:job.range,reverse:job.reverse});
   job.initialToken=await this.token(job,{tablet:0,cursor:null,emitted:0,seq:0});job.state='ready';await this.save(job);
  }catch(e){if(e.status&&e.status<500&&e.code!=='VISIBILITY_PENDING')await this.closeJob(job);throw e;}finally{await this.releasePin(job);}
  await this.arm(job);return this.reply(job);
 }
 reply(job){return json({id:job.id,snapshot:job.snapshot,expiresAt:job.expiresAt,pageSize:job.pageSize,token:job.initialToken,closed:!!job.closed,order:job.index?job.index.column:primaryColumns(job.schema)[0],reverse:job.reverse});}
 async compute(job,position){
  const p={tablet:position.tablet,cursor:position.cursor,emitted:position.emitted,seq:position.seq+1};
  const rows=[];let usedBytes=2,calls=0,keysRead=0,versionsRead=0;
  const limit=job.statement.limit??Infinity;
  let stopped=false;
  while(p.tablet<job.tablets.length&&rows.length<job.pageSize&&p.emitted<limit&&calls<RPC_BUDGET&&!stopped){
   const tablet=job.tablets[p.tablet],map=job.index||job.schema;
   const name=job.index?indexName(job.statement.table,job.index,tablet.id,job.schema):baseName(job.statement.table,tablet.id,job.schema);
   const page=await this.shard(name,{op:'scan',snapshot:job.snapshot,epoch:tabletEpoch(map,tablet.id),range:job.range,reverse:job.reverse,cursor:p.cursor,pageSize:Math.min(32,job.pageSize-rows.length)});calls++;keysRead+=page.keysRead||0;versionsRead+=page.versionsRead||0;
   let consumedAll=true;
   for(const entry of page.rows){
    if(rows.length>=job.pageSize||p.emitted>=limit){consumedAll=false;stopped=true;break;}
    const physicalKey=job.index?job.index.keyColumns.map(c=>entry[c]):keyOfRow(job.schema,entry);
    let row=entry;
    if(job.index){
     if(calls>=RPC_BUDGET){consumedAll=false;stopped=true;break;}
     const id=routeTablet(job.schema,entry.pk),found=await this.shard(baseName(job.statement.table,id,job.schema),{op:'getMany',keys:[entry.pk],snapshot:job.snapshot,epoch:tabletEpoch(job.schema,id)});calls++;versionsRead+=found.versionsRead||0;row=found.rows[0];
    }
    const selected=row?execute([{...job.statement,order:null,limit:null}],{[job.statement.table]:job.schema},{[job.statement.table]:[row]}).results[0].rows:[];
    if(selected.length){
     const next=usedBytes+bytes(selected[0])+(rows.length?1:0);
     if(next>RESPONSE_BYTES){if(!rows.length)throw error('Projected row exceeds cursor response byte budget',413,'CURSOR_ROW_TOO_LARGE');consumedAll=false;stopped=true;break;}
     rows.push(selected[0]);usedBytes=next;p.emitted++;
    }
    p.cursor=encodeKey(physicalKey);
   }
   if(consumedAll){if(page.done){p.tablet++;p.cursor=null;}else p.cursor=page.nextCursor;}
  }
  const done=p.tablet>=job.tablets.length||p.emitted>=limit;
  const nextToken=done?null:await this.token(job,p);
  return {id:job.id,page:position.seq,snapshot:job.snapshot,rows,nextToken,done,emitted:p.emitted,expiresAt:job.expiresAt,plan:{index:job.index?.name||null,reverse:job.reverse,shardCalls:calls,keysRead,versionsRead}};
 }
 async page(input,job){
  if(job.state!=='ready')throw error('Cursor is not initialized',409);
  if(job.closed||job.expiresAt<=this.now()){await this.closeJob(job);throw closed();}
  const token=input.token??job.initialToken,position=await this.position(job,token),cacheKey='page:'+await hash(token);
  if(position.seq<(job.rejectBeforePage||0))throw error('Cursor page acknowledged and expired',410,'PAGE_EXPIRED');
  try{
   await this.admit(job);
   const cached=await readRecord(this.storage,cacheKey,PAGE_OPTIONS);if(cached)return json(cached);
   await this.cleanup(job,true);
   const maxPages=this.budget('CURSOR_MAX_CACHE_PAGES',8,10000),maxBytes=this.budget('CURSOR_MAX_CACHE_BYTES',4*1024*1024,64*1024*1024);
   if(job.cachePages>=maxPages)throw error('Cursor receipt page budget exhausted; ACK consumed pages or close cursor',429,'CURSOR_BACKPRESSURE');
   const response=await this.compute(job,position),size=bytes(response);
   if(job.cacheBytes+size>maxBytes)throw error('Cursor receipt byte budget exhausted; ACK consumed pages or close cursor',429,'CURSOR_BACKPRESSURE');
   const staged=structuredClone(job);staged.cachePages++;staged.cacheBytes+=size;staged.highestIssued=Math.max(staged.highestIssued,position.seq);staged.releasingPin=staged.active.id;delete staged.active;
   await this.storage.transaction(async txn=>{await writeRecord(txn,cacheKey,response,PAGE_OPTIONS);await txn.put(receiptKey(position.seq),{cacheKey,bytes:size});await writeRecord(txn,'job',staged,ROOT_OPTIONS);});
   job=staged;
   return json(response);
  }finally{await this.releasePin(job);await this.arm(job);}
 }
 async acknowledge(input,job){
  if(job.closed||job.expiresAt<=this.now())throw closed();
  const seq=input.token===undefined?input.throughSeq:(await this.position(job,input.token)).seq;
  if(!Number.isSafeInteger(seq)||seq<0||seq>job.highestIssued)throw error('ACK must name an issued page');
  if(seq>=(job.rejectBeforePage||0)&&!await this.storage.get(receiptKey(seq)))throw error('ACK page has no durable receipt');
  job.rejectBeforePage=Math.max(job.rejectBeforePage||0,seq+1);await this.save(job);
  const done=await this.cleanup(job,true);await this.arm(job);
  return json({acknowledgedThrough:job.rejectBeforePage-1,rejectBeforePage:job.rejectBeforePage,done});
 }
 async handle(input){
  let job=await this.load();
  if(input.op==='start')return this.start(input,job);
  if(!job&&input.op==='collect'){const epoch=await this.med({op:'epoch'});if(Number.isSafeInteger(input.epoch)&&input.epoch>=0&&input.epoch<epoch.rejectBeforeEpoch)return json({collected:true,done:true});}
  if(!job)throw error('Unknown query cursor',404,'CURSOR_NOT_FOUND');
  if(input.op==='collect'){
   const epoch=await this.med({op:'epoch'});if(input.epoch!==job.epoch||job.epoch>=epoch.rejectBeforeEpoch)throw error('Cursor epoch is not retired',409);
   await this.closeJob(job);const done=await this.cleanup(job);
   return json({collected:done,done});
  }
  await this.med({op:'epoch',requestEpoch:job.epoch});
  await this.resolveRenewal(job);
  if(input.op==='renew')return this.renew(input,job);
  if(input.op==='page')return this.page(input,job);
  if(input.op==='ack')return this.acknowledge(input,job);
  if(input.op==='close'){await this.closeJob(job);return json({closed:true,done:await this.cleanup(job)});}
  if(input.op==='recover'){
   await this.releasePin(job);
   if(job.closed||job.expiresAt<=this.now()){await this.closeJob(job);return json({closed:true,done:await this.cleanup(job)});}
   await this.cleanup(job,true);await this.arm(job);return json({recovered:true,snapshot:job.snapshot,expiresAt:job.expiresAt});
  }
  if(input.op==='status')return json({id:job.id,state:job.state,closed:!!job.closed,snapshot:job.snapshot,expiresAt:job.expiresAt,cachePages:job.cachePages,cacheBytes:job.cacheBytes});
  throw error('Unknown cursor operation');
 }
}
