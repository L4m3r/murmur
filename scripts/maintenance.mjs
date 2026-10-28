#!/usr/bin/env node
/** Operator-owned bounded maintenance loop. Never enabled by importing this file.
 * Epoch rotation is opt-in: clients must send current requestEpoch before old
 * epochs are retired. Use one rotation-policy owner per database. */
import { readFile, open, rename, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

function integer(value,fallback,min,name){const n=value===undefined?fallback:Number(value);if(!Number.isSafeInteger(n)||n<min)throw new Error(`${name} must be an integer >= ${min}`);return n;}
export function optionsFromEnv(env=process.env){return {
 baseUrl:env.BASE_URL||'http://127.0.0.1:9876',apiToken:env.API_TOKEN||'',
 retainSteps:integer(env.RETAIN_STEPS,1000,0,'RETAIN_STEPS'),intervalMs:integer(env.INTERVAL_MS,30000,1,'INTERVAL_MS'),
 workBatches:integer(env.WORK_BATCHES,8,1,'WORK_BATCHES'),requestTimeoutMs:integer(env.REQUEST_TIMEOUT_MS,30000,1,'REQUEST_TIMEOUT_MS'),
 checkpointFile:env.CHECKPOINT_FILE||null,rotateIntervalMs:integer(env.ROTATE_INTERVAL_MS,0,0,'ROTATE_INTERVAL_MS'),keepEpochs:integer(env.KEEP_EPOCHS,2,1,'KEEP_EPOCHS')
};}
export class MaintenanceRunner {
 constructor(options={},dependencies={}){
  this.options={...optionsFromEnv({}),...options};this.fetch=dependencies.fetch||globalThis.fetch;this.now=dependencies.now||Date.now;this.log=dependencies.log||(()=>{});
  this.options.baseUrl=new URL(this.options.baseUrl).href.replace(/\/$/,'');
  if(!['http:','https:'].includes(new URL(this.options.baseUrl).protocol))throw new Error('BASE_URL must be HTTP(S)');
  this.state={version:1,baseUrl:this.options.baseUrl,turn:0,historyContinuation:null,epochCursor:null,ownerCursor:null,bucket:0,bucketEpochFloor:null,policy:{phase:'idle',nextAt:this.now()+this.options.rotateIntervalMs}};
 }
 async load(){if(!this.options.checkpointFile)return;try{const state=JSON.parse(await readFile(this.options.checkpointFile,'utf8'));if(state.version!==1||state.baseUrl!==this.options.baseUrl||!Number.isSafeInteger(state.turn)||!state.policy)throw new Error('Checkpoint version/database mismatch');this.state=state;}catch(e){if(e.code!=='ENOENT')throw e;}}
 async save(){if(!this.options.checkpointFile)return;const path=resolve(this.options.checkpointFile);await mkdir(dirname(path),{recursive:true});const temp=`${path}.${process.pid}.tmp`;const file=await open(temp,'w',0o600);try{await file.writeFile(JSON.stringify(this.state)+'\n');await file.sync();}finally{await file.close();}await rename(temp,path);const directory=await open(dirname(path),'r');try{await directory.sync();}finally{await directory.close();}}
 async post(path,body,signal){
  const timed=AbortSignal.timeout(this.options.requestTimeoutMs),combined=signal?AbortSignal.any([signal,timed]):timed;
  const response=await this.fetch(this.options.baseUrl+path,{method:'POST',headers:{'content-type':'application/json',...(this.options.apiToken?{authorization:`Bearer ${this.options.apiToken}`}:{})},body:JSON.stringify(body),signal:combined});
  const result=await response.json();if(!response.ok)throw Object.assign(new Error(result.error||`HTTP ${response.status}`),{status:response.status,code:result.code});return result;
 }
 async history(signal){const result=await this.post('/maintenance',{retainSteps:this.options.retainSteps,...(this.state.historyContinuation?{continuation:this.state.historyContinuation}:{})},signal);this.state.historyContinuation=result.done?null:result.continuation;return {kind:'history',...result};}
 async epochs(signal){const result=await this.post('/maintenance',{action:'collectEpochs',cursor:this.state.epochCursor,ownerCursor:this.state.ownerCursor,bucket:this.state.bucket,...(this.state.bucketEpochFloor===null?{}:{bucketEpochFloor:this.state.bucketEpochFloor})},signal);this.state.epochCursor=result.nextCursor??null;this.state.ownerCursor=result.ownerCursor??null;this.state.bucket=result.nextBucket??0;this.state.bucketEpochFloor=result.nextBucketEpochFloor??null;return {kind:'epochs',...result};}
 async policy(signal){
  const p=this.state.policy;if(!this.options.rotateIntervalMs)return {kind:'policy',idle:true};
  if(p.phase==='idle'){if(this.now()<p.nextAt)return {kind:'policy',idle:true};p.phase='inspect';}
  if(p.phase==='inspect'||p.phase==='reconcile'){
   const status=await this.post('/snapshot',{},signal);p.currentEpoch=status.currentEpoch;
   if(p.phase==='inspect'){p.target=status.currentEpoch+1;p.phase='rotate';}
   else p.phase=status.currentEpoch>=p.target?'retire':'rotate';
   return {kind:'policy',phase:p.phase,currentEpoch:status.currentEpoch};
  }
  if(p.phase==='rotate'){
   // Persist uncertain-send state BEFORE a non-idempotent rotation RPC. After
   // a lost reply/restart, inspect the allocated epoch before issuing another.
   p.phase='reconcile';await this.save();
   const result=await this.post('/snapshot',{action:'rotateEpoch'},signal);p.currentEpoch=result.currentEpoch;p.phase='retire';return {kind:'policy',rotated:result.currentEpoch};
  }
  if(p.phase==='retire'){
   const before=Math.max(0,p.currentEpoch-this.options.keepEpochs+1);
   try{const result=await this.post('/maintenance',{action:'retireEpochs',before},signal);p.phase='idle';p.nextAt=this.now()+this.options.rotateIntervalMs;return {kind:'policy',retiredBefore:result.rejectBeforeEpoch};}
   catch(e){if(e.code==='EPOCH_BUSY')return {kind:'policy',busy:true,before};throw e;}
  }
  throw new Error('Invalid checkpoint policy phase');
 }
 async cycle({signal}={}){
  const results=[];
  for(let i=0;i<this.options.workBatches&&!signal?.aborted;i++){
   const slots=this.options.rotateIntervalMs?3:2,slot=this.state.turn%slots;
   // Persist fair scheduling even when one operation fails; failed batch cursors
   // remain unchanged and are replayed on the next matching slot.
   this.state.turn++;await this.save();
   try{const result=await (slot===0?this.history(signal):slot===1?this.epochs(signal):this.policy(signal));results.push(result);this.log(result);}
   catch(e){if(signal?.aborted)break;const result={kind:'error',slot,error:e.message,code:e.code,status:e.status};results.push(result);this.log(result);}
   await this.save();
  }
  await this.save();return results;
 }
 async run({once=false,signal}={}){await this.load();do{await this.cycle({signal});if(once||signal?.aborted)break;await new Promise(resolve=>{const done=()=>{clearTimeout(timer);signal?.removeEventListener('abort',done);resolve();},timer=setTimeout(done,this.options.intervalMs);signal?.addEventListener('abort',done,{once:true});if(signal?.aborted)done();});}while(!signal?.aborted);await this.save();}
}
export async function main(args=process.argv.slice(2),env=process.env){if(args.some(arg=>arg!=='--once'))throw new Error('Usage: node scripts/maintenance.mjs [--once]');const controller=new AbortController(),stop=()=>controller.abort();process.once('SIGINT',stop);process.once('SIGTERM',stop);try{const runner=new MaintenanceRunner(optionsFromEnv(env),{log:value=>process.stdout.write(JSON.stringify(value)+'\n')});await runner.run({once:args.includes('--once'),signal:controller.signal});}finally{process.removeListener('SIGINT',stop);process.removeListener('SIGTERM',stop);}}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)main().catch(error=>{console.error(error.message);process.exitCode=1;});
