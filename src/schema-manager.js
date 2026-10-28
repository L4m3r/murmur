import { decodeRpcResponse } from './rpc-response.js';
import { coordinatorName } from './coordinator-routing.js';
import { readRecord, writeRecord, deleteRecord } from './records.js';
import { encodeKey, keyOfRow, rangeForFilters } from './keys.js';
import { indexEntry, indexKeyColumns, physicalTableName } from './schema.js';
import { routeTablet, selectTablets, tabletEpoch, planAffectedTablets } from './tablets.js';
const json=(body,status=200)=>Response.json(body,{status});
const all={lower:null,upper:null,lowerInclusive:true,upperInclusive:false,empty:false};
const point=key=>({lower:encodeKey(key),upper:encodeKey(key),lowerInclusive:true,upperInclusive:true,empty:false});
const bytes=value=>new TextEncoder().encode(JSON.stringify(value)).byteLength;
// Retirement manifests are deliberately small: peer ReadSets are all-to-all.
// This byte cap excludes the indivisible catalog schema mutation; raw admission
// still enforces its overall transaction budget before any stage is admitted.
function cleanupPolicy(env){
 const count=Number(env.SCHEMA_CLEANUP_TABLETS??4),maxBytes=Number(env.SCHEMA_CLEANUP_BYTES??32768);
 if(!Number.isSafeInteger(count)||count<1||count>16||!Number.isSafeInteger(maxBytes)||maxBytes<1024||maxBytes>65536)throw Object.assign(new Error('Cleanup requires SCHEMA_CLEANUP_TABLETS 1..16 and SCHEMA_CLEANUP_BYTES 1024..65536'),{status:400});
 return{count,maxBytes};
}
function cleanupPage(env,actors,offset){
 const {count,maxBytes}=cleanupPolicy(env),targets=[];let size=2;
 for(const actor of actors.slice(offset,offset+count)){
  const target={name:actor.name,expectedEpoch:actor.epoch,reads:[],changes:[],meta:{epoch:actor.retireEpoch,retired:true}},added=bytes(target)+(targets.length?1:0);
  if(size+added>maxBytes){if(!targets.length)throw Object.assign(new Error('Retirement target exceeds cleanup byte budget'),{status:400});break;}
  targets.push(target);size+=added;
 }
 return{targets,next:offset+targets.length};
}
const terminal=job=>['ready','dropped','failed','cancelled'].includes(job.state);
const digest=async value=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),b=>b.toString(16).padStart(2,'0')).join('');
const catName=table=>`catalog/${table}`;
const baseName=(table,id)=>`base/${table}/${id}`;
const idxName=(table,index,id)=>`index/${table}/${index.storageName||index.name}/${id}`;
async function call(namespace,name,input){
 const r=await namespace.get(namespace.idFromName(name)).fetch('https://actor/internal',{method:'POST',body:JSON.stringify(input)});
 const body=await decodeRpcResponse(r,`${name}:${input.op}`);if(!r.ok)throw Object.assign(new Error(body.error||'Actor call failed'),{status:r.status,code:body.code,rpc:body.rpc,retryAfterMs:body.retryAfterMs,body});return body;
}

/** Durable online index builder. No stage is recreated while its outcome is
 * unknown, and no enumeration cursor advances before a durable commit receipt.
 */
export class SchemaManager {
 constructor(state,env){this.storage=state.storage;this.env=env;this.tail=Promise.resolve();}
 run(fn){const next=this.tail.then(fn);this.tail=next.catch(()=>{});return next;}
 fetch(request){return this.run(async()=>this.handle(await request.json())).catch(e=>json({error:e.message,code:e.code||'SCHEMA_RETRY',rpc:e.rpc,retryAfterMs:e.retryAfterMs,retryable:e.body?.retryable??(e.status===undefined||e.status>=500)},e.status||503));}
 async alarm(){return this.run(async()=>{
  const job=await this.load();if(!job){await this.storage.deleteAlarm?.();return;}
  try{if(terminal(job))await this.collectTerminal(job);else{await this.work(job,4);await this.arm(job);}}
  catch{await this.storage.setAlarm?.(Date.now()+1000);}
 });}
 async collectTerminal(job,authority){
  if(!terminal(job))return false;
  const state=authority||await this.med({op:'epoch'});
  if(job.epoch<state.rejectBeforeEpoch){
   await this.storage.transaction(async txn=>{await txn.put('rejectBeforeEpoch',Math.max(await txn.get('rejectBeforeEpoch')||0,state.rejectBeforeEpoch));await deleteRecord(txn,'job');});
   await this.storage.deleteAlarm?.();return true;
  }
  // Failed admission before the first pin may have no global owner registry.
  // Preserve its replay identity locally until epoch retirement, then collect.
  await this.storage.setAlarm?.(Date.now()+60000);return false;
 }
 load(){return readRecord(this.storage,'job');}
 save(job){return this.storage.transaction(txn=>writeRecord(txn,'job',job));}
 async arm(job){if(!terminal(job))await this.storage.setAlarm?.(Date.now()+1000);else await this.storage.setAlarm?.(Date.now()+60000);}
 med(input){return call(this.env.ALLOCATOR,'global',input);}
 shard(name,input){return call(this.env.SHARDS,name,input);}
 async handle(input){
  let job=await this.load();
  if(input.op==='start'){
   const kind=input.kind||'createIndex';if(!['createIndex','dropIndex','dropTable'].includes(kind))return json({error:'Invalid schema job kind'},400);
   for(const field of (kind==='dropTable'?['id','table']:kind==='dropIndex'?['id','table','index']:['id','table','index','column']))if(typeof input[field]!=='string'||!(field==='id'?/^[a-zA-Z0-9_-]{1,100}$/:/^[a-z_][a-z0-9_]*$/).test(input[field]))return json({error:`Invalid ${field}`},400);
   if(input.unique!==undefined&&typeof input.unique!=='boolean')return json({error:'unique must be boolean'},400);
   if(!Number.isSafeInteger(input.epoch)||input.epoch<0)return json({error:'Invalid request epoch'},400);
   if(input.epoch<(await this.storage.get('rejectBeforeEpoch')||0))return json({error:'Request epoch permanently expired',code:'REQUEST_EXPIRED',retryable:false},410);
   const split=Number(this.env.TABLET_SPLIT_BYTES||4096),merge=Number(this.env.TABLET_MERGE_BYTES||1024),max=Number(this.env.TABLET_MAX_COUNT||256);
   if(!Number.isSafeInteger(split)||split<4||!Number.isSafeInteger(merge)||merge<2||merge>=split||!Number.isSafeInteger(max)||max<1||max>4096)return json({error:'Online index sizing requires split >=4 bytes, merge below split, max tablets 1..4096'},400);
   cleanupPolicy(this.env);
   await this.med({op:'epoch',requestEpoch:input.epoch});
   const identity=JSON.stringify({kind,id:input.id,epoch:input.epoch,table:input.table,index:input.index,column:input.column,unique:!!input.unique});
   if(job&&job.identity!==identity)return json({error:'Schema job identity reused'},409);
   if(!job){const hash=(await digest(identity)).slice(0,24);job={kind,id:input.id,epoch:input.epoch,table:input.table,index:input.index,column:input.column,unique:!!input.unique,identity,hash,storageName:`${input.index}~${hash}`,state:kind==='createIndex'?'installing':'dropping',sequence:0,attempt:0,pages:0,keys:0,tablet:0,cursor:null};await this.save(job);}
  }else if(!job)return json({error:'Unknown schema job'},404);
  if(input.op==='collect'){
   if(!terminal(job))return json({error:'Schema job still active'},409);
   if(input.epoch!==job.epoch)return json({error:'Schema job epoch mismatch'},409);
   const state=await this.med({op:'epoch'});if(job.epoch>=state.rejectBeforeEpoch)return json({error:'Schema job epoch not retired'},409);
   return json({collected:await this.collectTerminal(job,state)});
  }
  if(!['start','resume','status','cancel'].includes(input.op))return json({error:'Unknown schema operation'},400);
  if(input.op==='cancel'&&!terminal(job)){job.cancelRequested=true;await this.save(job);}
  if(input.op!=='status'&&!terminal(job)){await this.work(job,4);await this.arm(job);}
  return this.reply(job);
 }
 reply(job){const visible={id:job.id,state:job.state,table:job.table,index:job.index,pages:job.pages,keys:job.keys,snapshot:job.B,validationSnapshot:job.V,...(job.error?{error:job.error}:{}),...(job.maintenancePressure?.length?{maintenancePressure:job.maintenancePressure}:{})};return json(['ready','dropped'].includes(job.state)?{...job.result,job:visible}:{job:visible},terminal(job)?200:202);}
 async pin(job,id,snapshot){try{return await this.med({op:'pin',id,owner:`schema-job/${job.id}`,epoch:job.epoch,...(snapshot===undefined?{}:{snapshot})});}catch(e){if(e.code==='REQUEST_EXPIRED')e.fatal=true;throw e;}}
 async unpin(job,id){if(id)await this.med({op:'unpin',id,epoch:job.epoch});}
 async planning(job){
  if(!job.planning){job.planning={id:`sj-${job.hash}-${job.sequence}-${job.attempt}`,sequence:job.sequence,attempt:job.attempt};await this.save(job);}
  if(job.planning.snapshot===undefined){const pin=await this.pin(job,job.planning.id);job.planning.snapshot=pin.snapshot;await this.save(job);}
  return job.planning.snapshot;
 }
 async releasePlanning(job){
  if(job.planning){job.releasingPlanning=job.planning.id;delete job.planning;await this.save(job);}
  if(job.releasingPlanning){await this.unpin(job,job.releasingPlanning);delete job.releasingPlanning;await this.save(job);}
 }
 async catalog(job,snapshot,{allowChanged=false,allowDropped=false}={}){
  const found=await this.shard(catName(job.table),{op:'getMany',keys:['schema'],snapshot}),schema=found.rows[0];
  if(!schema||(!allowDropped&&schema.state==='dropped'))throw Object.assign(new Error('Unknown table'),{fatal:true});
  if(!allowChanged&&job.tableGeneration!==undefined&&!this.sameTable(job,schema))throw Object.assign(new Error('Table generation changed'),{fatal:true});
  return schema;
 }
 sameTable(job,schema){return(job.tableGeneration===undefined||(schema.generation??0)===job.tableGeneration)&&(!job.tableStorageName||physicalTableName(job.table,schema)===job.tableStorageName);}
 async captureTable(job,schema){if(job.tableGeneration===undefined){job.tableGeneration=schema.generation??0;job.tableStorageName=physicalTableName(job.table,schema);await this.save(job);}}
 find(job,schema){const index=(schema.indexes||[]).find(i=>i.name===job.index&&i.storageName===job.storageName&&i.jobId===job.id);if(!index)throw Object.assign(new Error('Building index generation disappeared'),{fatal:true});return index;}
 async stage(job,kind,snapshot,targets,extra={}){
  const token=await digest(`${job.hash}/${job.sequence}/${job.attempt}`),owner=`schema/${token}`;
  job.active={kind,body:{op:'applyTargets',idempotencyKey:`schema_${token}`,coordinatorId:owner,requestEpoch:job.epoch,snapshot,targets,response:{results:[{rowsAffected:0}],schemaJob:job.id}},...extra};
  await this.save(job);
 }
 async settle(job){
  const active=job.active;let receipt;
  try{receipt=await call(this.env.COORDINATOR,coordinatorName(active.body.coordinatorId),{...active.body,scope:active.body.coordinatorId});}
  catch(e){
   if(e.status===409&&e.body?.retryable===true){delete job.active;job.attempt++;await this.save(job);await this.releasePlanning(job);return false;}
   if(e.status===400){delete job.active;job.error=e.message;job.state='cleaning';job.attempt++;await this.save(job);await this.releasePlanning(job);return false;}
   // Includes a lost successful response: retain the exact durable request.
   throw e;
  }
  // Retain receipt/transition before releasing any pin or advancing work.
  if(active.kind==='install') {job.B=receipt.step;job.enumeration=active.schema;job.state='cutover';}
  if(active.kind==='batch'){job.tablet=active.next.tablet;job.cursor=active.next.cursor;job.pages++;job.keys+=active.count;job.pageLimit=64;if(active.pressure?.length)job.maintenancePressure=active.pressure;}
  if(active.kind==='validate'){job.V=receipt.step;job.validationIndex=active.index;job.validationTablet=0;job.validationCursor=null;job.previous=null;job.state='validation-cutover';}
  if(active.kind==='ready'){job.state='finishing';job.result={...receipt,routing:[{table:job.table,...active.routing}]};}
  if(active.kind==='cleanup'){if(active.cleanupIndex)job.cleanupIndex=active.cleanupIndex;if(active.cleanupTable)job.cleanupTable=active.cleanupTable;if(['dropIndex','dropTable'].includes(job.kind))job.result={...receipt,routing:active.routing||[]};job.cleanupPosition=active.nextCleanup||0;job.state=active.cleanupDone?'releasing':'cleaning';if(['dropIndex','dropTable'].includes(job.kind)&&!job.dropPin){job.dropStep=receipt.step;job.dropResumeState=job.state;job.state='drop-cutover';}}
  delete job.active;job.sequence++;job.attempt=0;await this.save(job);
  if(!['cutover','validation-cutover','drop-cutover'].includes(job.state))await this.releasePlanning(job);
  return true;
 }
 async install(job){
  const snapshot=await this.planning(job),schema=await this.catalog(job,snapshot);await this.captureTable(job,schema);
  if(!(schema.columns||[]).some(c=>c.name===job.column))throw Object.assign(new Error('Unknown index column'),{fatal:true});
  if((schema.indexes||[]).some(i=>i.name===job.index))throw Object.assign(new Error('Index already exists'),{fatal:true});
  const splitBytes=Math.min(Number(this.env.TABLET_SPLIT_BYTES||4096),256*1024);
  const sizing={splitBytes,mergeBytes:Math.max(2,Math.min(Number(this.env.TABLET_MERGE_BYTES||1024),Math.floor(splitBytes/4))),maxTablets:Number(this.env.TABLET_MAX_COUNT||256)};
  const index={sizing,name:job.index,column:job.column,unique:job.unique,state:'building',jobId:job.id,storageName:job.storageName,epoch:1,tablets:[{id:'t0',start:null,end:null,epoch:1}]};index.keyColumns=indexKeyColumns(schema,index);
  const next={...schema,indexes:[...(schema.indexes||[]),index]};
  await this.stage(job,'install',snapshot,[{name:catName(job.table),reads:[point('schema')],changes:[{key:'schema',row:next}]},{name:idxName(job.tableStorageName||job.table,index,'t0'),expectedEpoch:0,reads:[],changes:[],meta:{epoch:1,retired:false}}],{schema:next});
 }
 async enumerationPage(job){
  const tablets=job.enumeration.tablets;
  if(job.tablet>=tablets.length)return null;
  const result=await this.shard(baseName(job.tableStorageName||job.table,tablets[job.tablet].id),{op:'scanKeys',snapshot:job.B,epoch:tabletEpoch(job.enumeration,tablets[job.tablet].id),range:all,pageSize:job.pageLimit||64,...(job.cursor?{cursor:job.cursor}:{})});
  const keys=[];let keyBytes=0,trimmed=false;
  for(const key of result.keys){const n=bytes(key);if(keys.length&&keyBytes+n>48*1024){trimmed=true;break;}keys.push(key);keyBytes+=n;}
  const cursor=trimmed?encodeKey(keys.at(-1)):result.nextCursor;
  return{keys,next:{tablet:cursor?job.tablet:job.tablet+1,cursor:cursor||null}};
 }
 async batch(job){
  const page=await this.enumerationPage(job);if(!page){job.state='validation-start';await this.save(job);return;}
  const snapshot=await this.planning(job),schema=await this.catalog(job,snapshot),index=this.find(job,schema),targets=new Map();
  const target=(name,epoch)=>{if(!targets.has(name))targets.set(name,{name,...(epoch===undefined?{}:{expectedEpoch:epoch}),reads:[],changes:[]});return targets.get(name);};
  target(catName(job.table)).reads.push(point('schema'));
  const entries=[];
  for(const key of page.keys){const id=routeTablet(schema,key),name=baseName(job.tableStorageName||job.table,id),epoch=tabletEpoch(schema,id);target(name,epoch).reads.push(point(key));const found=await this.shard(name,{op:'getMany',keys:[key],snapshot,epoch});entries.push(...found.rows.map(row=>indexEntry(schema,index,row)));}
  for(const entry of entries){const key=index.keyColumns.map(c=>entry[c]),id=routeTablet(index,key),name=idxName(job.tableStorageName||job.table,index,id);target(name,tabletEpoch(index,id)).changes.push({key,row:entry});}
  const sizing=index.sizing||{splitBytes:Number(this.env.TABLET_SPLIT_BYTES||4096),mergeBytes:Number(this.env.TABLET_MERGE_BYTES||1024),maxTablets:Number(this.env.TABLET_MAX_COUNT||256)};
  const loaded=new Map(),pressure=[];let materializedBytes=0;
  for(const tablet of index.tablets){
    const name=idxName(job.tableStorageName||job.table,index,tablet.id),pending=targets.get(name);
    if(!pending?.changes.length)continue;
    const stats=await this.shard(name,{op:'stats'}),projected=stats.bytes+pending.changes.reduce((n,c)=>n+bytes(c.row)+1,0);
    if(projected<=sizing.splitBytes||stats.rows+pending.changes.length<2)continue;
    if(index.tablets.length>=sizing.maxTablets){pressure.push({tablet:tablet.id,reason:'tablet-capacity',maxTablets:sizing.maxTablets});continue;}
    // Old oversized generations are not silently materialized without bounds.
    if(stats.bytes>1024*1024){pressure.push({tablet:tablet.id,reason:'oversized-source',bytes:stats.bytes});continue;}
    let cursor,readBytes=0;const working=new Map();
    do{
      const found=await this.shard(name,{op:'scan',range:all,snapshot,epoch:tabletEpoch(index,tablet.id),pageSize:64,...(cursor?{cursor}:{})});
      for(const row of found.rows){const rowBytes=bytes(row);readBytes+=rowBytes;materializedBytes+=rowBytes;if(materializedBytes>1024*1024&&(job.pageLimit||64)>1){job.pageLimit=Math.max(1,Math.floor((job.pageLimit||64)/2));job.attempt++;await this.save(job);await this.releasePlanning(job);return;}if(readBytes>1024*1024)throw Object.assign(new Error('Online index topology source exceeds 1 MiB maintenance budget'),{fatal:true});working.set(encodeKey(index.keyColumns.map(c=>row[c])),row);}
      cursor=found.nextCursor;
    }while(cursor);
    pending.reads.push(all);
    for(const change of pending.changes){if(change.row===null)working.delete(encodeKey(change.key));else working.set(encodeKey(change.key),change.row);}
    loaded.set(tablet.id,[...working.values()]);
  }
  let topology=false;
  if(loaded.size){
    const planned=planAffectedTablets(index,loaded,sizing);topology=planned.changed;
    if(planned.schema.tablets.length>=sizing.maxTablets)for(const part of planned.partitions)if(part.rows.length>1&&part.bytes>sizing.splitBytes)pressure.push({tablet:part.id,reason:'tablet-capacity',maxTablets:sizing.maxTablets});
    if(topology){
      const remaining=new Set(planned.schema.tablets.map(t=>t.id));
      for(const tablet of index.tablets)if(!remaining.has(tablet.id)){
        const t=target(idxName(job.tableStorageName||job.table,index,tablet.id),tabletEpoch(index,tablet.id));t.reads.push(all);t.changes=[];t.meta={epoch:planned.schema.epoch,retired:true};
      }
      for(const part of planned.partitions)if(!index.tablets.some(t=>t.id===part.id)){
        const t=target(idxName(job.tableStorageName||job.table,index,part.id),0);t.meta={epoch:tabletEpoch(planned.schema,part.id),retired:false};t.changes=part.rows.map(row=>({key:index.keyColumns.map(c=>row[c]),row}));
      }
      const next={...schema,indexes:schema.indexes.map(i=>i===index?planned.schema:i)};
      target(catName(job.table)).changes=[{key:'schema',row:next}];
    }
  }
  if(targets.size>64){
    if((job.pageLimit||64)<=1)throw Object.assign(new Error('Index topology participant budget exceeded'),{fatal:true});
    job.pageLimit=Math.max(1,Math.floor((job.pageLimit||64)/2));job.attempt++;await this.save(job);await this.releasePlanning(job);return;
  }
  const budget=topology?2*1024*1024:56*1024;
  if(bytes([...targets.values()])>budget){
    if(topology&&(job.pageLimit||64)<=1)throw Object.assign(new Error('Online index topology stage exceeds 2 MiB budget'),{fatal:true});
    if((job.pageLimit||64)<=1)throw Object.assign(new Error('Index batch exceeds 64 KiB budget'),{fatal:true});
    job.pageLimit=Math.max(1,Math.floor((job.pageLimit||64)/2));job.attempt++;await this.save(job);await this.releasePlanning(job);return;
  }
  await this.stage(job,'batch',snapshot,[...targets.values()],{next:page.next,count:page.keys.length,pressure});
 }
 async startValidation(job){
  const snapshot=await this.planning(job),schema=await this.catalog(job,snapshot),index=this.find(job,schema);
  const validating={...index,state:'validating'},next={...schema,indexes:schema.indexes.map(i=>i===index?validating:i)};
  await this.stage(job,'validate',snapshot,[{name:catName(job.table),reads:[point('schema')],changes:[{key:'schema',row:next}]}],{index:validating});
 }
 async validatePage(job){
  const index=job.validationIndex;
  if(!index.unique||job.validationTablet>=index.tablets.length){job.state='publishing';await this.save(job);return;}
  const tablet=index.tablets[job.validationTablet];
  const found=await this.shard(idxName(job.tableStorageName||job.table,index,tablet.id),{op:'scan',snapshot:job.V,epoch:tabletEpoch(index,tablet.id),range:all,pageSize:64,...(job.validationCursor?{cursor:job.validationCursor}:{})});
  let previous=job.previous;
  for(const entry of found.rows)if(entry.value!==null){
    if(previous&&encodeKey(previous.value)===encodeKey(entry.value)&&encodeKey(previous.pk)!==encodeKey(entry.pk))throw Object.assign(new Error(`Unique index ${index.name} violation at validation snapshot`),{fatal:true});
    previous={value:entry.value,pk:entry.pk};
  }
  job.previous=previous;job.validationCursor=found.nextCursor||null;if(!found.nextCursor)job.validationTablet++;await this.save(job);
 }
 async publish(job){const snapshot=await this.planning(job),schema=await this.catalog(job,snapshot),index=this.find(job,schema);const next={...schema,indexes:schema.indexes.map(i=>i===index?{...i,state:'ready'}:i)};
  await this.stage(job,'ready',snapshot,[{name:catName(job.table),reads:[point('schema')],changes:[{key:'schema',row:next}]}],{routing:{epoch:next.epoch,tablets:next.tablets,indexes:next.indexes}});
 }
 async cleanupTable(job){
  const snapshot=await this.planning(job),schema=await this.catalog(job,snapshot,{allowChanged:true,allowDropped:true});
  if(job.tableGeneration===undefined){if(schema.state==='dropped')throw Object.assign(new Error('Unknown table'),{fatal:true});await this.captureTable(job,schema);}
  const current=this.sameTable(job,schema)&&schema.state!=='dropped'?schema:null;
  const source=job.cleanupTable||current,offset=job.cleanupPosition||0,targets=[{name:catName(job.table),reads:[point('schema')],changes:[]}];
  let next=schema;
  if(current){next={...current,state:'dropped',generation:current.generation??0,storageName:physicalTableName(job.table,current)};targets[0].changes=[{key:'schema',row:next}];}
  const actors=source?[
    ...source.tablets.map(t=>({name:baseName(job.tableStorageName,t.id),epoch:tabletEpoch(source,t.id),retireEpoch:source.epoch+1})),
    ...(source.indexes||[]).flatMap(index=>index.tablets.map(t=>({name:idxName(job.tableStorageName,index,t.id),epoch:tabletEpoch(index,t.id),retireEpoch:index.epoch+1}))),
  ]:[];
  const page=cleanupPage(this.env,actors,offset);targets.push(...page.targets);
  const nextCleanup=page.next;
  await this.stage(job,'cleanup',snapshot,targets,{cleanupTable:source,nextCleanup,cleanupDone:nextCleanup>=actors.length,routing:next.state==='dropped'?[]:[{table:job.table,epoch:next.epoch,tablets:next.tablets,indexes:next.indexes}]});
 }
 async cleanup(job){
  if(job.kind==='dropTable')return this.cleanupTable(job);
  const snapshot=await this.planning(job),schema=await this.catalog(job,snapshot,{allowChanged:true,allowDropped:true});
  if(job.tableGeneration===undefined){if(schema.state==='dropped')throw Object.assign(new Error('Unknown table'),{fatal:true});await this.captureTable(job,schema);}
  const sameTable=this.sameTable(job,schema)&&schema.state!=='dropped';
  let current=sameTable&&(schema.indexes||[]).find(i=>i.name===job.index&&i.storageName===job.storageName&&i.jobId===job.id);
  if(job.kind==='dropIndex'){
    const candidate=sameTable&&(schema.indexes||[]).find(i=>i.name===job.index);
    if(!job.dropCapture){
      if(!candidate)throw Object.assign(new Error('Unknown index'),{fatal:true});
      if(candidate.state&&candidate.state!=='ready')throw Object.assign(new Error('Only READY indexes may be dropped by a DROP job'),{fatal:true});
      job.dropCapture={storageName:candidate.storageName||candidate.name,jobId:candidate.jobId??null};await this.save(job);
    }
    current=candidate&&(candidate.storageName||candidate.name)===job.dropCapture.storageName&&(candidate.jobId??null)===job.dropCapture.jobId?candidate:null;
  }
  const index=job.cleanupIndex||current,offset=job.cleanupPosition||0;
  const targets=[{name:catName(job.table),reads:[point('schema')],changes:[]}];
  let next=schema;
  if(current){next={...schema,indexes:schema.indexes.filter(i=>i!==current),indexGenerations:{...(schema.indexGenerations||{}),[job.index]:(schema.indexGenerations?.[job.index]||0)+1}};targets[0].changes=[{key:'schema',row:next}];}
  const actors=index?index.tablets.map(tablet=>({name:idxName(job.tableStorageName||job.table,index,tablet.id),epoch:tabletEpoch(index,tablet.id),retireEpoch:index.epoch+1})):[];
  const page=cleanupPage(this.env,actors,offset);targets.push(...page.targets);
  const nextCleanup=page.next;
  await this.stage(job,'cleanup',snapshot,targets,{cleanupIndex:index,nextCleanup,cleanupDone:!index||nextCleanup>=index.tablets.length,routing:[{table:job.table,epoch:next.epoch,tablets:next.tablets,indexes:next.indexes}]});
 }
 async work(job,budget){
  await this.arm(job);
  for(let n=0;n<budget&&!terminal(job);n++){
   try{
    if(job.active){const cleanup=job.active.kind==='cleanup';await this.settle(job);if(cleanup)return;continue;}
    if(job.releasingPlanning){await this.releasePlanning(job);continue;}
    if(job.planning&&!['cutover','validation-cutover','drop-cutover'].includes(job.state)&&(job.planning.sequence!==job.sequence||job.planning.attempt!==job.attempt)){await this.releasePlanning(job);continue;}
    if(job.state==='cutover'){
     const id=`sj-${job.hash}-B`;await this.pin(job,id,job.B);job.enumerationPin=id;job.state='building';await this.save(job);await this.releasePlanning(job);continue;
    }
    if(job.state==='drop-cutover'){
     const id=`sj-${job.hash}-D`;await this.pin(job,id,job.dropStep);job.dropPin=id;job.state=job.dropResumeState;await this.save(job);await this.releasePlanning(job);continue;
    }
    if(job.state==='validation-cutover'){
     const id=`sj-${job.hash}-V`;await this.pin(job,id,job.V);job.validationPin=id;await this.save(job);
     await this.releasePlanning(job);await this.unpin(job,job.enumerationPin);delete job.enumerationPin;job.state='validating';await this.save(job);continue;
    }
    if(job.state==='finishing'||job.state==='releasing'){
     await this.releasePlanning(job);await this.unpin(job,job.enumerationPin);delete job.enumerationPin;await this.unpin(job,job.validationPin);delete job.validationPin;await this.unpin(job,job.dropPin);delete job.dropPin;
     job.state=['dropIndex','dropTable'].includes(job.kind)?'dropped':job.state==='finishing'?'ready':job.cancelRequested?'cancelled':'failed';await this.save(job);continue;
    }
    if((job.cancelRequested||job.error)&&job.B===undefined&&!job.cleanupIndex&&!job.cleanupTable){await this.releasePlanning(job);job.state=job.cancelRequested?'cancelled':'failed';await this.save(job);continue;}
    if(job.cancelRequested||job.error){job.state='cleaning';await this.save(job);await this.cleanup(job);continue;}
    if(job.state==='installing')await this.install(job);
    else if(job.state==='building')await this.batch(job);
    else if(job.state==='validation-start')await this.startValidation(job);
    else if(job.state==='validating')await this.validatePage(job);
    else if(job.state==='dropping')await this.cleanup(job);
    else if(job.state==='publishing')await this.publish(job);
    else if(job.state==='cleaning')await this.cleanup(job);
   }catch(e){if(e.fatal){job.error=e.message;job.state='cleaning';job.attempt++;await this.save(job);await this.releasePlanning(job);}else throw e;}
  }
 }
}
