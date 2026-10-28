import assert from 'node:assert/strict';
import Worker from '../src/index.js';

// No automatic alarm delivery in these model fixtures. Each PLAN_PENDING reply
// explicitly permits one genuine bucket background-drive turn, then retries the
// exact same serialized request. Fault probes deliberately remain single-shot.
export async function queryWithBackgroundTurns(env,input){
 const body=JSON.stringify(input);
 for(let attempt=0;attempt<128;attempt++){
  const response=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body}),env);
  const result={status:response.status,...await response.json()};
  if(input.fault||result.status!==503||result.code!=='PLAN_PENDING')return result;
  const rpc=async(namespace,name,message)=>{
   const r=await namespace.get(namespace.idFromName(name)).fetch('https://background',{method:'POST',body:JSON.stringify(message)});
   const value=await r.json();assert.equal(r.status,200,JSON.stringify(value));return value;
  };
  const found=await rpc(env.ALLOCATOR,'global',{op:'lookup',id:result.transactionId});
  assert.equal(found.state,'assigned',JSON.stringify(found));
  await rpc(env.MEDIATORS,found.association.bucket,{op:'drive',id:result.transactionId});
 }
 assert.fail('Transaction still PLAN_PENDING after128 explicit background turns');
}
