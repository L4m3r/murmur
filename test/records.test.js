import test from 'node:test';
import assert from 'node:assert/strict';
import { readRecord, writeRecord, deleteRecord, RECORD_CHUNK_BYTES } from '../src/records.js';

// The mock enforces the stricter legacy-KV value ceiling for portability. Its rollback models
// caller-owned storage.transaction, not process/runtime crash durability.
function storage() {
  const data = new Map(); let failPut = 0, puts = 0, failDelete = false;
  const api = {
    data,
    get: async key => structuredClone(data.get(key)),
    put: async (key,value) => {
      puts++;
      if (failPut && puts === failPut) throw new Error('injected storage interruption');
      const size = value instanceof Uint8Array ? value.byteLength : new TextEncoder().encode(JSON.stringify(value)).byteLength;
      assert.ok(size <= 128 * 1024, `persisted value exceeds 128 KiB: ${size}`);
      data.set(key,structuredClone(value));
    },
    delete: async key => { if (failDelete) throw new Error('injected cleanup interruption'); return data.delete(key); },
    list: async ({prefix=''}={}) => new Map([...data].filter(([k])=>k.startsWith(prefix)).sort().map(([k,v])=>[k,structuredClone(v)])),
    transaction: async fn => { const before = structuredClone(data); try { return await fn(api); } catch(error) { data.clear(); for(const [k,v] of before)data.set(k,v); throw error; } },
    failOnPut(n) { puts=0; failPut=n; },
    failOnDelete(value) { failDelete=value; },
  };
  return api;
}
const put = (s,k,v,options) => s.transaction(tx=>writeRecord(tx,k,v,options));
const del = (s,k) => s.transaction(tx=>deleteRecord(tx,k));
test('large Unicode JSON crosses arbitrary UTF-8 chunk boundaries and round-trips exactly',async()=>{
  const s=storage(), value={text:'😀界\u0000'.repeat(240000), nested:[true,null,-0.25,{astral:'𐀀'}]};
  const info=await put(s,'active',value);
  assert.ok(info.bytes>2*1024*1024); assert.ok(info.chunks>3); assert.match(info.hash,/^[a-f0-9]{64}$/);
  assert.deepEqual(await readRecord(s,'active'),value);
  assert.equal([...s.data.values()].filter(v=>v instanceof Uint8Array).length,info.chunks);
  assert.ok([...s.data.values()].filter(v=>v instanceof Uint8Array).every(v=>v.byteLength<=RECORD_CHUNK_BYTES));
});
test('overwrite atomically replaces generation and removes only its old chunks; deletion leaves no garbage',async()=>{
  const s=storage();const first=await put(s,'a',{text:'a'.repeat(200000)});
  await put(s,'unrelated',{value:'keep'});const unrelated=structuredClone(await readRecord(s,'unrelated'));
  const second=await put(s,'a',{value:'short'});assert.notEqual(first.generation,second.generation);
  assert.ok([...s.data.keys()].every(k=>!k.includes(first.generation)));
  assert.deepEqual(await readRecord(s,'a'),{value:'short'});
  assert.equal(await del(s,'a'),true);assert.equal(await del(s,'a'),false);assert.equal(await readRecord(s,'a'),undefined);
  assert.deepEqual(await readRecord(s,'unrelated'),unrelated);
  await del(s,'unrelated');assert.equal(s.data.size,0);
});
test('interrupted chunk writes roll back publication and all newly allocated chunks',async()=>{
  const s=storage();await put(s,'record',{before:'old'});const prior=structuredClone(s.data);
  s.failOnPut(3);
  await assert.rejects(put(s,'record',{large:'🌍'.repeat(70000)}),/interruption/);
  assert.deepEqual(s.data,prior);assert.deepEqual(await readRecord(s,'record'),{before:'old'});
});
test('interruption after new pointer publication during old generation cleanup rolls back both generations',async()=>{
  const s=storage();await put(s,'record',{before:'x'.repeat(100000)});const prior=structuredClone(s.data);
  s.failOnDelete(true);
  await assert.rejects(put(s,'record',{after:'new'}),/cleanup/);
  assert.deepEqual(s.data,prior);assert.deepEqual(await readRecord(s,'record'),{before:'x'.repeat(100000)});
  await assert.rejects(del(s,'record'),/cleanup/);assert.deepEqual(s.data,prior);
});
test('hash, missing chunks, malformed envelopes, and cross-key manifests fail closed',async()=>{
  const s=storage();await put(s,'a',{text:'hello'},{inlineBytes:0});const pristine=structuredClone(s.data);
  const chunkKey=[...s.data.keys()].find(k=>k.startsWith('__records_chunk_v1/'));
  const corrupt=s.data.get(chunkKey);corrupt[0]^=1;
  await assert.rejects(readRecord(s,'a'),{code:'RECORD_CORRUPT'});
  s.data.clear();for(const[k,v]of pristine)s.data.set(k,structuredClone(v));s.data.delete(chunkKey);
  await assert.rejects(readRecord(s,'a'),{code:'RECORD_CORRUPT'});
  s.data.clear();for(const[k,v]of pristine)s.data.set(k,structuredClone(v));s.data.set('b',s.data.get('a'));
  await assert.rejects(readRecord(s,'b'),{code:'RECORD_CORRUPT'});
  s.data.set('a',{...s.data.get('a'),version:2});await assert.rejects(readRecord(s,'a'),{code:'RECORD_CORRUPT'});
});
test('configurable byte backpressure checks before mutation and applies to reads',async()=>{
  const s=storage();await put(s,'small',{a:1});const prior=structuredClone(s.data);
  await assert.rejects(put(s,'small',{text:'界'.repeat(100)},{maxBytes:100}),{code:'RECORD_TOO_LARGE'});
  assert.deepEqual(s.data,prior);
  await assert.rejects(readRecord(s,'small',{maxBytes:1}),{code:'RECORD_TOO_LARGE'});
  await assert.rejects(put(s,'x',undefined),TypeError);
  await assert.rejects(put(s,'__records_chunk_v1/reserved',{}),TypeError);
  await assert.rejects(put(s,'x',{}, {maxBytes:Infinity}),TypeError);
  assert.deepEqual(s.data,prior);
});
test('each logical key has an isolated generation namespace, including similar names',async()=>{
  const s=storage();await put(s,'a',{n:1});await put(s,'a/chunk/0',{n:2});
  await del(s,'a');assert.deepEqual(await readRecord(s,'a/chunk/0'),{n:2});
});

test('small records inline one value, configurable chunk forcing, and transitions clean old generations',async()=>{
  const s=storage();const inline=await put(s,'small',{text:'hello'});
  assert.equal(inline.chunks,0);assert.equal(s.data.size,1);assert.deepEqual(await readRecord(s,'small'),{text:'hello'});
  const chunks=await put(s,'small',{text:'hello'},{inlineBytes:0});assert.equal(chunks.chunks,1);assert.equal(s.data.size,2);
  await put(s,'small',{text:'new'});assert.equal(s.data.size,1);assert.deepEqual(await readRecord(s,'small'),{text:'new'});
  s.data.get('small').json='{"text":"BAD"}';await assert.rejects(readRecord(s,'small',{allowRaw:true}),{code:'RECORD_CORRUPT'});
});
test('raw fixture fallback is explicit and cannot hide tagged corruption',async()=>{
  const s=storage();await s.put('fixture',{id:'old',version:1});
  await assert.rejects(readRecord(s,'fixture'),{code:'RECORD_CORRUPT'});
  assert.deepEqual(await readRecord(s,'fixture',{allowRaw:true}),{id:'old',version:1});
  await put(s,'fixture',{id:'new'},{allowRaw:true});assert.deepEqual(await readRecord(s,'fixture'),{id:'new'});
  s.data.get('fixture').kind='wrong';await assert.rejects(readRecord(s,'fixture',{allowRaw:true}),{code:'RECORD_CORRUPT'});
});
