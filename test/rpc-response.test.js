import test from 'node:test';
import assert from 'node:assert/strict';
import {decodeRpcResponse} from '../src/rpc-response.js';

test('RPC decoder preserves successful JSON and complete application error metadata', async () => {
  const body = {error:'conflict',code:'CONFLICT',owners:['x'],dependencies:[{id:'a',bucket:'m0'}],details:'x'.repeat(5000)};
  assert.deepEqual(await decodeRpcResponse(Response.json(body,{status:409}),'s1'),body);
  assert.deepEqual(await decodeRpcResponse(Response.json({rows:[1,2]}),'s1'),{rows:[1,2]});
  assert.deepEqual(await decodeRpcResponse(new Response('{"code":"CONFLICT"}',{status:409}),'s1'),{code:'CONFLICT'});
});

test('RPC decoder exposes overload and Retry-After without disguising it as plan progress', async () => {
  await assert.rejects(decodeRpcResponse(new Response('cell request limit reached',{status:503,headers:{'x-celld-overload':'1','retry-after':'2'}}),'pool/1'), e => {
    assert.equal(e.code,'RPC_BACKPRESSURE'); assert.equal(e.status,503); assert.equal(e.responseStatus,503);
    assert.equal(e.actor,'pool/1'); assert.equal(e.retryAfter,'2'); assert.equal(e.retryAfterMs,2000); assert.match(e.message,/cell request limit reached/); return true;
  });
});

test('RPC decoder labels unmarked plaintext failure unavailable and retains HTTP status', async () => {
  await assert.rejects(decodeRpcResponse(new Response('upstream unavailable',{status:502}),'shard'), e => {
    assert.equal(e.code,'RPC_UNAVAILABLE'); assert.equal(e.status,502); assert.equal(e.retryAfter,null); return true;
  });
});

test('RPC decoder bounds unknown error stream consumption and diagnostics', async () => {
  let pulls = 0, cancelled = false;
  const stream = new ReadableStream({pull(c) { pulls++; c.enqueue(new TextEncoder().encode('x'.repeat(1024))); },cancel() { cancelled=true; }},{highWaterMark:0});
  await assert.rejects(decodeRpcResponse(new Response(stream,{status:503}),'shard'), e => {
    assert.equal(e.code,'RPC_UNAVAILABLE'); assert.ok(e.message.length < 700); assert.match(e.message,/truncated/); return true;
  });
  assert.equal(pulls,4); assert.equal(cancelled,true);
});

test('RPC decoder rejects malformed successful JSON as protocol error, never success', async () => {
  for (const headers of [{},{'content-type':'application/json'}]) {
    await assert.rejects(decodeRpcResponse(new Response('not json',{headers}),'shard'),e => {
      assert.equal(e.code,'RPC_PROTOCOL_ERROR'); assert.equal(e.status,502); assert.equal(e.responseStatus,200); return true;
    });
  }
});

test('RPC decoder preserves transport classification even for mislabeled malformed JSON', async () => {
  await assert.rejects(decodeRpcResponse(new Response('cell request limit reached',{status:503,headers:{'content-type':'application/json','x-celld-overload':'1','retry-after':'Wed, 28 Oct 2026 12:00:00 GMT'}}),'s'),e => {
    assert.equal(e.code,'RPC_BACKPRESSURE'); assert.equal(e.retryAfter,'Wed, 28 Oct 2026 12:00:00 GMT'); return true;
  });
});

 test('RPC Retry-After backoff is finite and capped at sixty seconds', async () => {
  for (const [raw,expected] of [['99999999999999999999999999999999999999999999999999999999999',60000],['nonsense',0],['0',0]]) {
    await assert.rejects(decodeRpcResponse(new Response('busy',{status:503,headers:{'retry-after':raw}}),'s'),e => {
      assert.equal(e.retryAfterMs,expected); return true;
    });
  }
});

test('RPC evidence survives JSON forwarding and every diagnostic field is bounded', async () => {
  const response = new Response('secret-free overload detail '.repeat(400),{status:503,headers:{
    'x-celld-overload':'x'.repeat(100), 'retry-after':'9'.repeat(200),
  }});
  await assert.rejects(decodeRpcResponse(response,'actor/'.repeat(100)),error => {
    const forwarded = JSON.parse(JSON.stringify({rpc:error.rpc})).rpc;
    assert.equal(forwarded.actor.length,128);
    assert.equal(forwarded.responseStatus,503);
    assert.equal(forwarded.overload.length,64);
    assert.equal(forwarded.retryAfter.length,128);
    assert.equal(forwarded.bodyExcerpt.length,512);
    assert.equal(forwarded.truncated,true);
    assert.equal(error.retryAfterMs,60000);
    return true;
  });
});
