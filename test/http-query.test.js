import test from'node:test';import assert from'node:assert/strict';
import{queryRequest}from'../scripts/http-query.mjs';
const pending={code:'PLAN_PENDING',outcome:'PENDING',retryable:true,faultReached:false};
const reply=(status,body)=>({status,json:async()=>body});
test('HTTP continuation preserves exact serialized mutation and distinguishes pending from retry',async()=>{
 const calls=[],events=[],input={sql:'UPDATE t SET n=n+1',idempotencyKey:'same',params:{n:1}};
 const responses=[reply(503,pending),reply(503,{code:'BUSY',retryable:true}),reply(200,{ok:true})];
 const r=await queryRequest('http://localhost',input,{pollMs:0,fetchImpl:async(url,options)=>{calls.push(options.body);return responses.shift()},log:e=>events.push(e)});
 assert.equal(new Set(calls).size,1);assert.equal(calls[0],JSON.stringify(input));assert.equal(r.attempts,3);assert.equal(r.pendingResponses,1);assert.equal(r.unexpectedRetries,1);
 assert.deepEqual(events.map(e=>e.event),['PLAN_PENDING','RETRY','operation_complete']);
});
test('expected injected 503 and terminal validation errors are not continued',async()=>{
 for(const [expected,status,body]of [[503,503,pending],[200,400,{error:'bad'}],[200,503,{...pending,faultReached:true}]]){
  let calls=0;const r=await queryRequest('http://localhost',{sql:'x',idempotencyKey:'key'},{expected,pollMs:0,log:()=>{},fetchImpl:async()=>{calls++;return reply(status,body)}});assert.equal(calls,1);assert.equal(r.status,status);
 }
});
test('transport timeout is not hidden by a fresh mutation or retry',async()=>{
 let calls=0;await assert.rejects(queryRequest('http://localhost',{sql:'x',idempotencyKey:'key'},{log:()=>{},fetchImpl:async()=>{calls++;throw new Error('transport timeout')}}),/transport timeout/);assert.equal(calls,1);
});
test('pending operations have finite total budget including attempt body',async()=>{
 const bodies=[];await assert.rejects(queryRequest('http://localhost',{sql:'x',idempotencyKey:'key'},{totalMs:15,attemptMs:5,pollMs:2,log:()=>{},fetchImpl:async(url,options)=>{bodies.push(options.body);return reply(503,pending)}}),/budget|deadline/);assert.equal(new Set(bodies).size,1);assert.ok(bodies.length>0);
});
test('upstream Retry-After is observed but cannot extend total operation deadline',async()=>{
 const events=[],started=Date.now();let calls=0;
 await assert.rejects(queryRequest('http://localhost',{sql:'x',idempotencyKey:'key'},{totalMs:20,attemptMs:10,pollMs:0,log:e=>events.push(e),fetchImpl:async()=>{calls++;return reply(503,{code:'RPC_BACKPRESSURE',retryable:true,retryAfterMs:60000,rpc:{status:503}})}}),/deadline|budget/);
 assert.equal(calls,1);assert.ok(Date.now()-started<1000);assert.equal(events[0].event,'RETRY');assert.equal(events[0].rpc.status,503);
});
