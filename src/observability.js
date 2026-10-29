import { decodeRpcResponse } from './rpc-response.js';

const states = new WeakMap();
const paths = new Set(['/query','/cursor','/schema','/snapshot','/recover','/maintenance']);
const codes = new Set(['PLAN_PENDING','CONFLICT','COORDINATOR_BACKPRESSURE','RPC_BACKPRESSURE','RPC_UNAVAILABLE','RPC_PROTOCOL_ERROR','RECOVERY_REQUIRED','VISIBILITY_PENDING','REQUEST_EXPIRED','SNAPSHOT_EXPIRED','CURSOR_EXPIRED','FAULT_INJECTED','IDEMPOTENCY_CONFLICT']);
const identifier = value => typeof value === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]{0,127}$/.test(value) && !['__proto__','constructor','prototype'].includes(value);
const json = (value,status=200) => Response.json(value,{status,headers:{'cache-control':'no-store','x-content-type-options':'nosniff'}});
function state(env) {
  let value=states.get(env);
  if(!value){value={startedAt:Date.now(),observedRequests:0,samples:[],tables:[],active:0,budget:24,refilledAt:Date.now()};states.set(env,value);}
  return value;
}
export function observeRequest(env,path,status,durationMs,result) {
  if(!paths.has(path))return;
  const s=state(env);s.observedRequests=Math.min(Number.MAX_SAFE_INTEGER,s.observedRequests+1);
  const code=typeof result?.code==='string'?(codes.has(result.code)?result.code:'OTHER'):status>=400?'HTTP_ERROR':'OK';
  const sample={at:Date.now(),path,status,code,durationMs:Math.max(0,Math.round(durationMs))};
  if(typeof result?.transactionId==='string'&&/^e\d+_[a-f0-9-]{36}$/.test(result.transactionId))sample.transactionId=result.transactionId;
  s.samples.push(sample);if(s.samples.length>128)s.samples.shift();
  for(const item of (Array.isArray(result?.routing)?result.routing.slice(0,8):[]))if(identifier(item?.table)){s.tables=s.tables.filter(t=>t!==item.table);s.tables.push(item.table);if(s.tables.length>32)s.tables.shift();}
}
export function metricsSnapshot(env) {
  const s=state(env),latencies=s.samples.map(x=>x.durationMs).sort((a,b)=>a-b),statusCounts={},codeCounts={};
  for(const sample of s.samples){const group=`${Math.floor(sample.status/100)}xx`;statusCounts[group]=(statusCounts[group]||0)+1;codeCounts[sample.code]=(codeCounts[sample.code]||0)+1;}
  const percentile=p=>latencies.length?latencies[Math.min(latencies.length-1,Math.ceil(latencies.length*p)-1)]:null;
  return {scope:'This Worker isolate only; resets on eviction. Counts and latency use the latest 128 observed API requests, not global totals.',startedAt:s.startedAt,observedRequests:s.observedRequests,windowSize:128,sampleCount:s.samples.length,statusCounts,codeCounts,latencyMs:{p50:percentile(.5),p95:percentile(.95),max:latencies.at(-1)??null},recent:s.samples.slice(-32).reverse(),tables:[...s.tables].reverse()};
}
function timeoutError(){return Object.assign(new Error('Diagnostic read timed out; remote reads may still be completing'),{status:504,code:'DIAGNOSTICS_TIMEOUT'});}
async function withDeadline(work,ms){let timer;try{return await Promise.race([work,new Promise((_,reject)=>{timer=setTimeout(()=>reject(timeoutError()),ms);})]);}finally{clearTimeout(timer);}}
async function rpc(env,binding,name,input,deadline){
  if(Date.now()>=deadline)throw timeoutError();
  const response=await env[binding].get(env[binding].idFromName(name)).fetch('https://diagnostics/internal',{method:'POST',body:JSON.stringify(input)});
  if(Date.now()>=deadline)throw timeoutError();
  const value=await decodeRpcResponse(response,`${binding}:diagnostics`);
  if(!response.ok)throw Object.assign(new Error('Diagnostic read unavailable'),{status:response.status,code:value.code});
  return value;
}
async function readInput(request){
  if(!(request.headers.get('content-type')||'').toLowerCase().startsWith('application/json'))throw Object.assign(new Error('JSON content type required'),{status:415});
  const reader=request.body?.getReader();if(!reader)throw Object.assign(new Error('JSON body required'),{status:400});
  let bytes=0;const chunks=[];
  const reading=(async()=>{while(true){const {value,done}=await reader.read();if(done)break;bytes+=value.byteLength;if(bytes>4096)throw Object.assign(new Error('Diagnostic body exceeds 4096 bytes'),{status:413});chunks.push(value);}})();
  try{await withDeadline(reading,5000);}catch(error){void reader.cancel().catch(()=>{});throw error;}
  const data=new Uint8Array(bytes);let offset=0;for(const part of chunks){data.set(part,offset);offset+=part.byteLength;}
  let input;try{input=JSON.parse(new TextDecoder().decode(data));}catch{throw Object.assign(new Error('Invalid JSON'),{status:400});}
  if(!input||typeof input!=='object'||Array.isArray(input))throw Object.assign(new Error('JSON object required'),{status:400});return input;
}
function allowed(input,keys){if(Object.keys(input).some(key=>!keys.includes(key)))throw Object.assign(new Error('Unknown diagnostic input field'),{status:400});}
function admission(env,cost){const s=state(env),now=Date.now();s.budget=Math.min(24,s.budget+(now-s.refilledAt)*.0024);s.refilledAt=now;if(s.active>=4||s.budget<cost)return false;s.budget-=cost;s.active++;return true;}

/** Authenticated read-only local-storage snapshots. No plan drive, recovery,
 * maintenance, row scan, or per-request durable metrics write is permitted. */
export async function diagnosticsResponse(request,env){
  if(typeof env.API_TOKEN!=='string'||!env.API_TOKEN)return json({error:'Diagnostics require configured API_TOKEN',code:'DIAGNOSTICS_DISABLED'},403);
  if(request.headers.get('authorization')!==`Bearer ${env.API_TOKEN}`)return json({error:'Unauthorized',code:'UNAUTHORIZED'},401);
  const origin=request.headers.get('origin');if(origin&&origin!==new URL(request.url).origin)return json({error:'Cross-origin diagnostics denied',code:'ORIGIN_DENIED'},403);
  if(request.method!=='POST')return json({error:'POST required'},405);
  let admitted=false;
  try{
    const input=await readInput(request);let cost=1;
    if(input.action==='overview'){allowed(input,['action','cursor']);if(input.cursor!==undefined&&(!Number.isSafeInteger(input.cursor)||input.cursor<0))return json({error:'Invalid pending cursor'},400);}
    else if(input.action==='table'){
      allowed(input,['action','table','index','offset','limit']);
      if(!identifier(input.table)||(input.index!==undefined&&!identifier(input.index)))return json({error:'Valid table/index identifier required'},400);
      if(input.offset!==undefined&&(!Number.isSafeInteger(input.offset)||input.offset<0||input.offset>4096))return json({error:'Invalid tablet offset'},400);
      if(input.limit!==undefined&&(!Number.isSafeInteger(input.limit)||input.limit<1||input.limit>8))return json({error:'Tablet limit must be 1 through 8'},400);
      cost=2+(input.limit??8);
    }else if(input.action==='transaction'){
      allowed(input,['action','transactionId']);if(typeof input.transactionId!=='string'||!/^e\d{1,16}_[a-f0-9-]{36}$/.test(input.transactionId))return json({error:'Valid transaction ID required'},400);cost=2;
    }else return json({error:'Unknown read-only diagnostics action'},400);
    admitted=admission(env,cost);if(!admitted)return json({error:'Diagnostic read budget exhausted; retry later',code:'DIAGNOSTICS_BACKPRESSURE',retryAfterMs:5000},429);
    const timeoutMs=Math.max(100,Math.min(5000,Number(env.DIAGNOSTICS_TIMEOUT_MS)||5000)),deadline=Date.now()+timeoutMs;
    const call=(binding,name,input)=>rpc(env,binding,name,input,deadline);
    const work=(async()=>{let result;
    if(input.action==='overview'){
      const runtime=await call('ALLOCATOR','global',{op:'diagnostics',cursor:input.cursor??0,limit:16});
      const buildId=typeof env.BUILD_ID==='string'&&/^[a-zA-Z0-9._-]{1,80}$/.test(env.BUILD_ID)?env.BUILD_ID:'unversioned';
      result={identity:{engine:'murmur-services-v7',buildId,coordinatorServices:16,placement:'Physical Durable Object placement and provider storage bytes are not exposed.'},runtime,metrics:metricsSnapshot(env)};
    }else if(input.action==='transaction'){
      const {transaction}=await call('ALLOCATOR','global',{op:'diagnostics',id:input.transactionId});
      const progress=transaction?.state==='assigned'?(await call('MEDIATORS',transaction.bucket,{op:'diagnostics',id:input.transactionId})).transaction:null;
      result={transaction,progress,scope:'Durable protocol records sampled separately. Absent means unknown or already collected; this endpoint never drives recovery.'};
    }else{
      const runtime=await call('ALLOCATOR','global',{op:'diagnostics',limit:1});
      const catalog=await call('SHARDS',`catalog/${input.table}`,{op:'diagnostics',catalog:true,snapshot:runtime.frontier,offset:input.offset??0,limit:input.limit??8,...(input.index?{index:input.index}:{})});
      const selected=catalog.selection,tablets=[];
      for(const tablet of selected.tablets){
        const name=selected.index?`index/${catalog.schema.storageName}/${selected.storageName}/${tablet.id}`:`base/${catalog.schema.storageName}/${tablet.id}`;
        try{const info=await call('SHARDS',name,{op:'diagnostics'});tablets.push({...tablet,...info});}
        catch{tablets.push({...tablet,errorCode:'TABLET_UNAVAILABLE'});}
      }
      result={table:input.table,snapshot:catalog.snapshot,schema:catalog.schema,topology:{kind:selected.index?'index':'base',index:selected.index,totalTablets:selected.totalTablets,offset:input.offset??0,nextOffset:selected.nextOffset,tablets},scope:'Catalog at the fully applied frontier; current per-tablet logical row/byte estimates sampled separately. Not physical storage size or an atomic cross-tablet measurement. Key boundary values are redacted.'};
    }
    if(new TextEncoder().encode(JSON.stringify(result)).byteLength>65536)return json({error:'Diagnostic response exceeds bounded budget',code:'DIAGNOSTICS_BUDGET'},503);
    return json(result);
    })().finally(()=>{state(env).active--;});
    // A timed-out HTTP reply does not imply cancellation of an actor RPC. Keep
    // its admission slot until the actual work settles; never fan out again
    // after the deadline. This bounds detached reads during provider outages.
    admitted=false;
    return await withDeadline(work,timeoutMs);
  }catch(error){return json({error:error.status===504?'Diagnostic read timed out; remote reads may still be completing':error.status&&error.status<500?error.message:'Diagnostic read unavailable',code:error.status===504?'DIAGNOSTICS_TIMEOUT':error.status===404?'NOT_FOUND':error.status===410?'SNAPSHOT_EXPIRED':'DIAGNOSTICS_UNAVAILABLE',retryable:!error.status||error.status>=500},error.status||503);}
  finally{if(admitted)state(env).active--;}
}
