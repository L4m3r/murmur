/** Test/demo HTTP continuation contract. One serialized request and identity;
 * <=10s per attempt, <=30s total, including response-body reads. No transport
 * retry by default; PLAN_PENDING is not counted as an unexpected retry. */
export async function queryRequest(base,input,{headers={},expected=200,totalMs=30000,attemptMs=10000,pollMs=100,retryTransport=false,maxUnexpectedRetries=Infinity,fetchImpl=fetch,log=message=>console.error(JSON.stringify(message))}={}){
 const encoded=JSON.stringify(input),started=Date.now(),deadline=started+totalMs;
 let attempts=0,pendingResponses=0,unexpectedRetries=0;
 const context={path:'/query',idempotencyKey:input.idempotencyKey,sql:input.sql?.slice(0,160)};
 const elapsed=()=>Date.now()-started;
 while(true){
  const remaining=deadline-Date.now();
  if(remaining<=0)throw new Error(`Query operation deadline exceeded after ${elapsed()}ms; retry exact identity ${input.idempotencyKey}`);
  attempts++;let response,body,retryAfterMs=0;
  try{
   response=await fetchImpl(base.replace(/\/$/,'')+'/query',{method:'POST',headers:{'content-type':'application/json',...headers},body:encoded,signal:AbortSignal.timeout(Math.min(attemptMs,remaining))});
   body=await response.json();
  }catch(error){
   log({event:retryTransport?'RETRY':'request_timeout_or_transport',...context,attempt:attempts,elapsedMs:elapsed(),error:String(error)});
   if(!retryTransport)throw error;
   unexpectedRetries++;
  }
  if(Date.now()>deadline)throw new Error(`Query response exceeded ${totalMs}ms operation budget; retry exact identity ${input.idempotencyKey}`);
  if(response&&body!==undefined){
   const pending=expected===200&&response.status===503&&body.code==='PLAN_PENDING'&&body.outcome==='PENDING'&&body.retryable===true&&body.faultReached!==true;
   const retry=expected===200&&response.status===503&&body.retryable===true&&body.faultReached!==true;
   if(!pending&&!retry){
    log({event:'operation_complete',...context,attempts,pendingResponses,unexpectedRetries,elapsedMs:elapsed(),status:response.status});
    return {status:response.status,body,attempts,pendingResponses,unexpectedRetries,elapsedMs:elapsed()};
   }
   if(pending)pendingResponses++;else unexpectedRetries++;
   retryAfterMs=Number.isFinite(body.retryAfterMs)&&body.retryAfterMs>0?body.retryAfterMs:0;
   log({event:pending?'PLAN_PENDING':'RETRY',...context,attempt:attempts,elapsedMs:elapsed(),code:body.code,error:body.error,retryAfterMs,rpc:body.rpc});
  }
  if(unexpectedRetries>maxUnexpectedRetries)throw new Error(`Unexpected retry budget exceeded; retry exact identity ${input.idempotencyKey}`);
  const delay=Math.min(Math.max(pollMs,retryAfterMs),Math.max(0,deadline-Date.now()));
  if(delay)await new Promise(resolve=>setTimeout(resolve,delay));
 }
}
