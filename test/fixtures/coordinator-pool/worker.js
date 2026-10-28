import {readRecord,writeRecord} from './src/records.js';
// Synthetic runtime fixture only: deterministic held RPCs are not production APIs.
import {CoordinatorService,scopedStorage} from './src/coordinator-service.js';
import {coordinatorName} from './src/coordinator-routing.js';
const owners=[];const route=coordinatorName('tx/pool-probe');
for(let i=0;owners.length<2;i++){const owner=`tx/pool-probe-${i}`;if(coordinatorName(owner)===route)owners.push(owner);}
async function gate(env,op,label){return env.GATES.get(env.GATES.idFromName('gate')).fetch('https://gate/',{method:'POST',body:JSON.stringify({op,label})});}
class LogicalCoordinator {
 constructor(state,env){this.storage=state.storage;this.env=env;this.tail=Promise.resolve();}
 run(fn){const p=this.tail.then(fn);this.tail=p.catch(()=>{});return p;}
 fetch(request){return this.run(async()=>{
  const input=await request.json();await this.storage.put('scope',input.scope);
  if(input.op==='arm'){await this.storage.put('kind',input.kind);await this.storage.setAlarm(input.deadline);return Response.json({armed:true});}
  const previous=await this.storage.get(`request:${input.key}`);if(previous)return Response.json(previous);
  if(input.hold)await gate(this.env,'hold',input.barrier||'work');
  if(input.transactions){for(let i=0;i<input.transactions;i++)await this.storage.transaction(async txn=>{const old=await readRecord(txn,'local-record')||{n:0};await writeRecord(txn,'local-record',{n:old.n+1,scope:input.scope,payload:'x'.repeat(4096)});await txn.put('txCount',old.n+1);});const result={n:await this.storage.get('txCount'),scope:input.scope};await this.storage.put(`request:${input.key}`,result);return Response.json(result);}
  const n=(await this.storage.get('counter')||0)+1;await this.storage.put('counter',n);const result={n,scope:input.scope};await this.storage.put(`request:${input.key}`,result);return Response.json(result);
 });}
 alarm(){return this.run(async()=>{if(await this.storage.get('kind')==='hang')await gate(this.env,'hold','alarm');await this.storage.put('firedAt',Date.now());});}
}
export class Pool extends CoordinatorService {
 constructor(state,env){super(state,env,LogicalCoordinator);}
 async fetch(request){const input=await request.clone().json();if(input.inspect){const results=[];for(const scope of owners){const s=scopedStorage(this.storage,scope);results.push({scope,firedAt:await s.get('firedAt')||null,counter:await s.get('counter')||0,txCount:await s.get('txCount')||0,alarm:await s.getAlarm(),refs:this.contexts.get(scope)?.refs||0});}return Response.json({route,owners:results});}return super.fetch(request);}
}
export class Gate {
 constructor(state){this.state=state;this.pending=new Map();}
 async fetch(request){const input=await request.json();if(input.op==='hold'){let item=this.pending.get(input.label);if(!item){let resolve;const promise=new Promise(r=>resolve=r);item={promise,resolve,count:0};this.pending.set(input.label,item);}item.count++;await item.promise;return Response.json({released:true});}if(input.op==='release'){const item=this.pending.get(input.label);this.pending.delete(input.label);item?.resolve();return Response.json({released:!!item});}return Response.json({pending:[...this.pending.keys()],pendingCounts:Object.fromEntries([...this.pending].map(([key,value])=>[key,value.count]))});}
}
export default {async fetch(request,env){const url=new URL(request.url);if(url.pathname==='/health')return Response.json({ok:true});const pool=env.COORDINATOR.get(env.COORDINATOR.idFromName(route));if(url.pathname==='/setup'){const now=Date.now();for(const [i,scope]of owners.entries())await pool.fetch('https://pool/',{method:'POST',body:JSON.stringify({scope,coordinatorId:scope,op:'arm',kind:i===0?'hang':'healthy',deadline:now+(i===0?100:1800)})});return Response.json({route,owners});}if(url.pathname==='/inspect'){const response=await pool.fetch('https://pool/',{method:'POST',body:JSON.stringify({inspect:true})});const body=await response.json(),status=await (await gate(env,'status')).json();return Response.json({...body,...status});}if(url.pathname==='/release'){const input=await request.json();return gate(env,'release',input.label);}if(url.pathname==='/work'){const input=await request.json(),scope=owners[input.owner??0];return pool.fetch('https://pool/',{method:'POST',body:JSON.stringify({...input,scope,coordinatorId:scope,op:'work'})});}return Response.json({error:'not found'},{status:404});}};
