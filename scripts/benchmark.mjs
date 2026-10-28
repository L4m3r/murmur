import {queryRequest} from './http-query.mjs';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {performance} from 'node:perf_hooks';

const options={base:process.env.BASE_URL||'http://127.0.0.1:9876',iterations:40,concurrency:4,retries:20,keepTable:false};
const args=process.argv.slice(2);
for(let i=0;i<args.length;i++){
 const arg=args[i];
 if(arg==='--help'){
  console.log('node scripts/benchmark.mjs [--base-url URL] [--iterations 40] [--concurrency 4] [--max-retries 20] [--keep-table]\nRuns disjoint then contended point increments against one synthetic table. Reports observed HTTP latency, not a throughput guarantee. API_TOKEN is optional.');process.exit(0);
 }
 if(arg==='--keep-table'){options.keepTable=true;continue;}
 const key={'--base-url':'base','--iterations':'iterations','--concurrency':'concurrency','--max-retries':'retries'}[arg];
 if(!key||args[i+1]===undefined)throw Error(`Unknown or missing option ${arg}`);
 options[key]=key==='base'?args[++i]:Number(args[++i]);
}
for(const[key,max]of [['iterations',100000],['concurrency',64],['retries',100]])if(!Number.isSafeInteger(options[key])||options[key]<1||options[key]>max)throw Error(`${key} must be 1..${max}`);
const url=new URL(options.base);if(!['http:','https:'].includes(url.protocol))throw Error('HTTP(S) base URL required');
options.base=options.base.replace(/\/$/,'');
const table=`bench_${Date.now()}_${randomUUID().replaceAll('-','').slice(0,8)}`;
const headers={'content-type':'application/json',...(process.env.API_TOKEN?{authorization:`Bearer ${process.env.API_TOKEN}`}:{})};
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const percentile=(values,q)=>{const sorted=[...values].sort((a,b)=>a-b);return sorted[Math.max(0,Math.ceil(sorted.length*q)-1)]||0;};
let created=false,uncertain=false;
async function request(path,input,context={},counters){
 if(path==='/query'){
  let r;try{r=await queryRequest(options.base,input,{headers,maxUnexpectedRetries:options.retries});}
  catch(error){uncertain=true;throw Object.assign(error,{operation:input,context});}
  if(counters){counters.httpAttempts+=r.attempts;counters.pendingResponses=(counters.pendingResponses||0)+r.pendingResponses;counters.retryable503s+=r.unexpectedRetries;}
  if(r.status!==200)throw Object.assign(new Error(JSON.stringify({status:r.status,...r.body})),{definite:r.status<500});
  return r.body;
 }
 // All attempts reuse the exact serialized body, including idempotency key.
 const body=JSON.stringify(input),started=Date.now(),deadline=started+30000;let last;
 for(let attempt=0;attempt<=options.retries;attempt++){
  const remaining=deadline-Date.now();if(remaining<=0)break;let retryAfterMs=0;
  counters&&(counters.httpAttempts++);
  try{
   const response=await fetch(options.base+path,{method:'POST',headers,body,signal:AbortSignal.timeout(Math.min(10000,remaining))});
   const result=await response.json();
   if(response.status===200||response.status===202)return result;
   if(response.status===503&&result.retryable){
    if(counters)counters.retryable503s++;
    last=Error(JSON.stringify({status:response.status,...result}));
    retryAfterMs=Number.isFinite(result.retryAfterMs)&&result.retryAfterMs>0?result.retryAfterMs:0;
    console.error(JSON.stringify({event:result.code==='PLAN_PENDING'?'PLAN_PENDING':'RETRY',...context,idempotencyKey:input.idempotencyKey,attempt,code:result.code,rpc:result.rpc,retryAfterMs,elapsedMs:Date.now()-started}));
   }else throw Object.assign(new Error(JSON.stringify({status:response.status,...result})),{definite:true});
  }catch(error){
   if(error.definite)throw error;last=error;
   if(!String(error.message).startsWith('{"status":503')){if(counters)counters.transportRetries++;console.error(JSON.stringify({event:'transport_retry',...context,idempotencyKey:input.idempotencyKey,attempt,error:error.message}));}
  }
  if(attempt<options.retries)await delay(Math.min(Math.max(retryAfterMs,Math.min(1000,25*(attempt+1))),Math.max(0,deadline-Date.now())));
 }
 uncertain=true;throw Object.assign(new Error(`Retry budget exhausted; preserve table and retry this exact operation: ${last?.message}`),{operation:input,context});
}
const query=async(sql,params,context,counters)=>request('/query',{sql,params,idempotencyKey:randomUUID()},context,counters);
async function scenario(name){
 const counters={httpAttempts:0,retryable503s:0,transportRetries:0};
 const latencies=[],expected=Array(options.concurrency).fill(0);let next=0,completed=0,firstError;
 const began=performance.now();
 await Promise.all(Array.from({length:options.concurrency},async(_,worker)=>{
  while(!firstError){const operation=next++;if(operation>=options.iterations)return;const id=name==='disjoint'?worker:0,started=performance.now();
   try{await query(`UPDATE ${table} SET n=n+1 WHERE id=:id`,{id},{scenario:name,operation,worker},counters);latencies.push(performance.now()-started);expected[id]++;completed++;}
   catch(error){firstError??=error;return;}
  }
 }));
 if(firstError)throw firstError;
 const elapsedMs=performance.now()-began,result=await query(`SELECT id,n FROM ${table} ORDER BY id`);
 assert.deepEqual(result.results[0].rows,expected.map((n,id)=>({id,n})),`${name}: final increments must equal acknowledged logical operations`);
 assert.equal(expected.reduce((a,b)=>a+b,0),options.iterations);
 return {scenario:name,completed,concurrency:options.concurrency,elapsedMs:Number(elapsedMs.toFixed(2)),p50Ms:Number(percentile(latencies,.5).toFixed(2)),p95Ms:Number(percentile(latencies,.95).toFixed(2)),maxMs:Number(latencies.reduce((max,value)=>Math.max(max,value),0).toFixed(2)),...counters,verifiedFinalSum:options.iterations,invariantsVerified:true};
}
try{
 await query(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY,n INTEGER)`);created=true;
 await query(`INSERT INTO ${table}(id,n) VALUES ${Array.from({length:options.concurrency},(_,id)=>`(${id},0)`).join(',')}`);
 const disjoint=await scenario('disjoint');console.log(JSON.stringify(disjoint));
 await query(`UPDATE ${table} SET n=0`);
 const contended=await scenario('contended');console.log(JSON.stringify(contended));
 let cleanup='retained by --keep-table';
 if(!options.keepTable){
  const start=await request('/schema',{action:'start',sql:`DROP TABLE ${table}`,idempotencyKey:randomUUID()},{scenario:'cleanup'});let result=start;
  for(let attempt=0;attempt<1000&&result.job.state!=='dropped';attempt++){
   if(['failed','cancelled'].includes(result.job.state))throw Error(`Cleanup failed: ${JSON.stringify(result)}`);
   result=await request('/schema',{action:'resume',id:start.job.id},{scenario:'cleanup'});
  }
  assert.equal(result.job.state,'dropped','Cleanup must reach durable terminal state');cleanup='dropped synthetic table';
 }
 console.log(JSON.stringify({benchmark:'murmur-http-point-writes',table,iterationsPerScenario:options.iterations,concurrency:options.concurrency,cleanup,invariantsVerified:true,note:'Observed latency for this runtime and workload only; no throughput or production-performance guarantee.'}));
}catch(error){
 console.error(JSON.stringify({benchmark:'murmur-http-point-writes',table,created,uncertain,error:error.message,operation:error.operation,context:error.context,invariantsVerified:false,note:'Synthetic table preserved for diagnosis; no automatic cleanup after an uncertain or failed workload.'}));process.exitCode=1;
}
