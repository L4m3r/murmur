import test from 'node:test';
import assert from 'node:assert/strict';
import { Mediator } from '../src/mediator.js';
function storage() {
  const data = new Map();
  const api = { get: async k => structuredClone(data.get(k)), put: async (k, v) => data.set(k, structuredClone(v)) };
  api.transaction = async fn => { const saved = structuredClone(data); try { return await fn(api); } catch (e) { data.clear(); for (const [k,v] of saved) data.set(k,v); throw e; } };
  return api;
}
async function invoke(actor, body) { const response = await actor.fetch(new Request('https://mediator', { method:'POST', body:JSON.stringify(body) })); return { status: response.status, ...await response.json() }; }
const plan = (actor,id,participants=[{name:'a',vote:'va'}]) => invoke(actor,{op:'plan',id,owner:'c0',participants});
async function done(actor,id,name='a') { await invoke(actor,{op:'delivered',id,name}); await invoke(actor,{op:'applied',id,name}); }
test('plan allocation is replay stable, rejects changed manifest, and survives actor restart', async () => {
  const state = {storage:storage()}; let actor = new Mediator(state);
  const first = await plan(actor,'t1'); assert.equal(first.step,1);
  actor = new Mediator(state);
  assert.deepEqual(await plan(actor,'t1'),first);
  assert.equal((await plan(actor,'t1',[{name:'a',vote:'different'}])).status,409);
  assert.equal((await plan(actor,'t2')).step,2);
  assert.equal((await invoke(actor,{op:'get',id:'missing'})).missing,true);
});
test('independent plans can apply behind a hole without publishing an unsafe frontier', async () => {
  const actor = new Mediator({storage:storage()});
  await plan(actor,'first'); await plan(actor,'second'); await done(actor,'second');
  let status = await invoke(actor,{op:'snapshot'});
  assert.equal(status.frontier,0); assert.equal(status.allocated,2); assert.deepEqual(status.pending,[{id:'first',step:1,owner:'c0'}]);
  assert.equal((await plan(actor,'third')).step,3);
  await invoke(actor,{op:'delivered',id:'first',name:'a'});
  status = await invoke(actor,{op:'status'}); assert.equal(status.deliveredFrontier,2); assert.equal(status.frontier,0);
  await invoke(actor,{op:'applied',id:'first',name:'a'});
  status = await invoke(actor,{op:'snapshot'}); assert.equal(status.frontier,2);
  await done(actor,'third'); assert.equal((await invoke(actor,{op:'snapshot'})).frontier,3);
});
test('every immutable participant must acknowledge delivery and apply; repeated acknowledgments are harmless', async () => {
  const actor = new Mediator({storage:storage()});
  await plan(actor,'p',[{name:'a',vote:'va'},{name:'b',vote:'vb'}]);
  assert.equal((await invoke(actor,{op:'applied',id:'p',name:'a'})).status,409);
  assert.equal((await invoke(actor,{op:'delivered',id:'p',name:'outsider'})).status,409);
  await done(actor,'p','a'); await done(actor,'p','a');
  assert.equal((await invoke(actor,{op:'snapshot'})).frontier,0);
  await done(actor,'p','b'); assert.equal((await invoke(actor,{op:'snapshot'})).frontier,1);
  assert.equal((await invoke(actor,{op:'get',id:'p'})).applied.length,2);
});
test('empty no-op plans complete and malformed or duplicate participants fail', async () => {
  const actor = new Mediator({storage:storage()});
  assert.equal((await plan(actor,'noop',[])).step,1);
  assert.equal((await invoke(actor,{op:'snapshot'})).frontier,1);
  assert.equal((await plan(actor,'bad',[{name:'a',vote:'x'},{name:'a',vote:'y'}])).status,400);
});
test('manifest byte limit includes future acknowledgment arrays before allocating a step', async () => {
  const actor = new Mediator({storage:storage()});
  const participants = Array.from({length:64}, (_,i) => ({name:`${i}-${'界'.repeat(330)}`,vote:'v'.repeat(64)}));
  assert.equal((await plan(actor,'too-large',participants)).status,400);
  assert.equal((await invoke(actor,{op:'snapshot'})).allocated,0);
});
