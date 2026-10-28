import test from 'node:test';
import assert from 'node:assert/strict';
import { PlanAllocator } from '../src/mediator.js';
function storage() {
  const data = new Map();
  const api = { get: async k => structuredClone(data.get(k)), put: async (k, v) => data.set(k, structuredClone(v)), delete: async k => data.delete(k), list: async ({prefix='',start,end,limit}={}) => new Map([...data].filter(([k])=>k.startsWith(prefix)&&(start===undefined||k>=start)&&(end===undefined||k<end)).sort(([a],[b])=>a<b?-1:a>b?1:0).slice(0,limit).map(([k,v])=>[k,structuredClone(v)])) };
  api.transaction = async fn => { const saved = structuredClone(data); try { return await fn(api); } catch (e) { data.clear(); for (const [k,v] of saved) data.set(k,v); throw e; } };
  return api;
}
async function invoke(actor, body) { const response = await actor.fetch(new Request('https://mediator', { method:'POST', body:JSON.stringify(body) })); return { status: response.status, ...await response.json() }; }
// Retention/control unit fixtures seed durable assigned records directly; signed
// allocation and ACK paths are covered by peer-plan + peer-protocol tests.
async function plan(actor,id,participants=[{name:'a',vote:'va'}]) {
 const epoch=Number(/^e([0-9]+)_/.exec(id)?.[1]||0),valid=await invoke(actor,{op:'epoch',requestEpoch:epoch});if(valid.status!==200)return valid;
 const step=(await actor.storage.get('allocated')||0)+1;
 const association={id,epoch,step,owner:'c0',snapshot:0,participants,bucket:'m0',manifestHash:'fixture'};
 const record={...association,association,delivered:[],applied:[]};
 await actor.storage.put(`plan:${id}`,record);await actor.storage.put(`step:${String(step).padStart(16,'0')}`,id);await actor.storage.put('allocated',step);
 for(const p of participants)await actor.storage.put(`shard:${p.name}`,p.name);
 if(participants.length)await actor.storage.put(`pending-plan:${String(step).padStart(16,'0')}`,{id,step,owner:'c0',epoch});
 else await actor.storage.put(`epoch-gc:${String(epoch).padStart(16,'0')}:plan:${id}`,{kind:'plan',key:`plan:${id}`,epoch});
 await actor.storage.transaction(tx=>actor.advance(tx));return {status:200,...association};
}
async function done(actor,id,name='a') {
 const p=await actor.storage.get(`plan:${id}`);if(!p.delivered.includes(name))p.delivered.push(name);if(!p.applied.includes(name))p.applied.push(name);await actor.storage.put(`plan:${id}`,p);
 if(p.applied.length===p.participants.length){await actor.storage.delete(`pending-plan:${String(p.step).padStart(16,'0')}`);await actor.storage.put(`epoch-gc:${String(p.epoch).padStart(16,'0')}:plan:${id}`,{kind:'plan',key:`plan:${id}`,epoch:p.epoch});}
 await actor.storage.transaction(tx=>actor.advance(tx));
}
async function visibleSteps(actor, count) { for (let i=0;i<count;i++) await plan(actor,`visible-${i}`,[]); }
test('pin admission and compaction are atomic, floor is monotonic, and replay preserves snapshot',async()=>{
  const actor=new PlanAllocator({storage:storage()});await visibleSteps(actor,5);
  const pinned=await invoke(actor,{op:'pin',id:'op',owner:'c0',snapshot:2});assert.equal(pinned.snapshot,2);
  assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,2);
  assert.deepEqual(await invoke(actor,{op:'pin',id:'op',owner:'c0'}),pinned);
  assert.equal((await invoke(actor,{op:'pin',id:'op',owner:'other'})).status,409);
  assert.equal((await invoke(actor,{op:'pin',id:'too-old',owner:'c1',snapshot:1})).code,'SNAPSHOT_EXPIRED');
  await invoke(actor,{op:'unpin',id:'op'});
  assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,5);
  assert.equal((await invoke(actor,{op:'compact',retainSteps:1000})).retentionFloor,5);
  assert.equal((await invoke(actor,{op:'pin',id:'latest',owner:'c1'})).snapshot,5);
  assert.equal((await invoke(actor,{op:'pin',id:'future',owner:'c1',snapshot:6})).status,400);
});
test('expired external lease cannot admit new work while its independent admitted pin remains protective',async()=>{
  const actor=new PlanAllocator({storage:storage()});let now=1000;actor.now=()=>now;await visibleSteps(actor,5);
  await invoke(actor,{op:'lease',id:'lease',snapshot:1,ttlMs:100});
  const pin=await invoke(actor,{op:'pin',id:'operation',owner:'c0',leaseId:'lease'});assert.equal(pin.snapshot,1);
  now=1101;
  assert.equal((await invoke(actor,{op:'pin',id:'new-operation',owner:'c1',leaseId:'lease'})).code,'LEASE_EXPIRED');
  assert.equal((await invoke(actor,{op:'renew',id:'lease',ttlMs:100})).code,'LEASE_EXPIRED');
  assert.deepEqual(await invoke(actor,{op:'pin',id:'operation',owner:'c0',leaseId:'lease'}),pin);
  assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,1);
  await invoke(actor,{op:'unpin',id:'operation'});
  assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,5);
  assert.equal((await invoke(actor,{op:'status'})).pins,0);
});
test('live lease holds floor through restart and renewal; release does not remove admitted pins',async()=>{
  const state={storage:storage()};let actor=new PlanAllocator(state);let now=1000;actor.now=()=>now;await visibleSteps(actor,4);
  const leased=await invoke(actor,{op:'lease',id:'lease',snapshot:2,ttlMs:1000});
  actor=new PlanAllocator(state);actor.now=()=>now;
  assert.deepEqual(await invoke(actor,{op:'lease',id:'lease'}),leased);
  now=1500;assert.equal((await invoke(actor,{op:'renewLease',id:'lease',ttlMs:2000})).expiresAt,3500);
  assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,2);
  await invoke(actor,{op:'pin',id:'op',owner:'c2',leaseId:'lease'});
  await invoke(actor,{op:'releaseLease',id:'lease'});
  assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,2);
  await invoke(actor,{op:'unpin',id:'op'});assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,4);
});
test('compaction versus admission queue ordering cannot publish a floor beyond an admitted snapshot',async()=>{
  const actor=new PlanAllocator({storage:storage()});await visibleSteps(actor,3);
  const [pin,compact]=await Promise.all([invoke(actor,{op:'pin',id:'first',owner:'c0',snapshot:1}),invoke(actor,{op:'compact',retainSteps:0})]);
  assert.equal(pin.status,200);assert.equal(compact.retentionFloor,1);
  await invoke(actor,{op:'unpin',id:'first'});
  const [next,late]=await Promise.all([invoke(actor,{op:'compact',retainSteps:0}),invoke(actor,{op:'pin',id:'late',owner:'c0',snapshot:1})]);
  assert.equal(next.retentionFloor,3);assert.equal(late.status,410);assert.equal(late.retryable,false);
});
test('lease and pin validation and configurable admission backpressure',async()=>{
  const actor=new PlanAllocator({storage:storage()},{MAX_SNAPSHOT_PINS:'1',MAX_SNAPSHOT_LEASES:'1',SNAPSHOT_LEASE_MAX_TTL_MS:'500'});
  assert.equal((await invoke(actor,{op:'lease',id:'too-long',ttlMs:501})).status,400);
  await invoke(actor,{op:'lease',id:'l',ttlMs:100});
  assert.equal((await invoke(actor,{op:'lease',id:'another',ttlMs:100})).status,429);
  assert.equal((await invoke(actor,{op:'pin',id:'bad',owner:'c0',leaseId:'l',snapshot:1})).status,400);
  await invoke(actor,{op:'pin',id:'p',owner:'c0'});
  assert.equal((await invoke(actor,{op:'pin',id:'p2',owner:'c1'})).status,429);
});

test('unpin durably fences a delayed original pin even when no pin was admitted',async()=>{
  const state={storage:storage()};let actor=new PlanAllocator(state);
  assert.equal((await invoke(actor,{op:'unpin',id:'late'})).unpinned,true);
  actor=new PlanAllocator(state);
  assert.equal((await invoke(actor,{op:'pin',id:'late',owner:'c0',snapshot:0})).code,'PIN_CLOSED');
  assert.equal((await invoke(actor,{op:'status'})).pins,0);
  await invoke(actor,{op:'pin',id:'active',owner:'c0'});await invoke(actor,{op:'unpin',id:'active'});
  assert.equal((await invoke(actor,{op:'pin',id:'active',owner:'c0'})).code,'PIN_CLOSED');
});
test('durable shard registry pages all ever-planned names including retired identifiers',async()=>{
  const state={storage:storage()};let actor=new PlanAllocator(state);
  await plan(actor,'first',[{name:'row/z',vote:'a'},{name:'row/a',vote:'b'},{name:'catalog/a',vote:'c'}]);
  await plan(actor,'second',[{name:'row/a',vote:'d'},{name:'row/retired-old',vote:'e'}]);
  actor=new PlanAllocator(state);
  const first=await invoke(actor,{op:'shards',limit:2});assert.deepEqual(first.names,['catalog/a','row/a']);assert.equal(first.nextCursor,'row/a');
  const second=await invoke(actor,{op:'shards',limit:2,cursor:first.nextCursor});assert.deepEqual(second.names,['row/retired-old','row/z']);assert.equal(second.nextCursor,null);
  assert.deepEqual((await invoke(actor,{op:'shards',cursor:'row/z'})).names,[]);
  assert.equal((await invoke(actor,{op:'shards',limit:65})).status,400);
});

test('pending recovery index is bounded and paginated independently of a blocked historical prefix',async()=>{
  const actor=new PlanAllocator({storage:storage()});
  for(let i=0;i<70;i++) await plan(actor,`blocked-${i}`);
  const status=await invoke(actor,{op:'status'});assert.equal(status.pending.length,64);assert.equal(status.pendingNextCursor,64);
  const rest=await invoke(actor,{op:'pending',cursor:status.pendingNextCursor,limit:64});assert.equal(rest.pending.length,6);assert.equal(rest.nextCursor,null);
  await done(actor,'blocked-5');
  const page=await invoke(actor,{op:'pending',cursor:5,limit:1});assert.equal(page.pending[0].step,7);
  assert.equal((await invoke(actor,{op:'status'})).frontier,0);
});
test('large complete prefix advances in bounded batches and never crosses an unresolved hole',async()=>{
  const actor=new PlanAllocator({storage:storage()},{MAX_FRONTIER_ADVANCE:'3'});
  await plan(actor,'hole');
  for(let i=0;i<12;i++) await plan(actor,`complete-${i}`,[]);
  const before=await invoke(actor,{op:'status'});assert.equal(before.frontier,0);assert.equal(before.pending.length,1);
  await done(actor,'hole');
  const first=await invoke(actor,{op:'status'});assert.ok(first.frontier>0&&first.frontier<13);
  let last=first;for(let i=0;i<8&&last.frontier<13;i++)last=await invoke(actor,{op:'status'});
  assert.equal(last.frontier,13);assert.equal(last.deliveredFrontier,13);assert.equal(last.pending.length,0);
});
test('active owner discovery is durable and paginated; final unpin removes owner entries and exact count',async()=>{
  const state={storage:storage()};let actor=new PlanAllocator(state);
  await invoke(actor,{op:'pin',id:'a',owner:'tx/one'});await invoke(actor,{op:'pin',id:'b',owner:'tx/one'});await invoke(actor,{op:'pin',id:'c',owner:'tx/two'});
  actor=new PlanAllocator(state);
  const seen=new Set();let cursor=null;
  do {const page=await invoke(actor,{op:'owners',limit:1,...(cursor?{cursor}:{})});page.owners.forEach(owner=>seen.add(owner));cursor=page.nextCursor;} while(cursor);
  assert.deepEqual([...seen].sort(),['tx/one','tx/two']);assert.equal((await invoke(actor,{op:'status'})).pins,3);
  await invoke(actor,{op:'unpin',id:'a'});assert.ok((await invoke(actor,{op:'owners'})).owners.includes('tx/one'));
  await invoke(actor,{op:'unpin',id:'b'});await invoke(actor,{op:'unpin',id:'b'});assert.deepEqual((await invoke(actor,{op:'owners'})).owners,['tx/two']);
  await invoke(actor,{op:'unpin',id:'c'});assert.deepEqual((await invoke(actor,{op:'owners'})).owners,[]);assert.equal((await invoke(actor,{op:'status'})).pins,0);
});

test('released lease identity is permanently fenced against delayed create and delayed pin rebinding',async()=>{
  const state={storage:storage()};let actor=new PlanAllocator(state);await visibleSteps(actor,3);
  await invoke(actor,{op:'lease',id:'l',snapshot:1});await invoke(actor,{op:'release',id:'l'});
  actor=new PlanAllocator(state);
  assert.equal((await invoke(actor,{op:'lease',id:'l',snapshot:2})).code,'LEASE_CLOSED');
  assert.equal((await invoke(actor,{op:'pin',id:'delayed',owner:'c0',leaseId:'l'})).code,'LEASE_EXPIRED');
  await invoke(actor,{op:'release',id:'never-created'});
  assert.equal((await invoke(actor,{op:'lease',id:'never-created'})).code,'LEASE_CLOSED');
});

test('allocated snapshots admit independent reads while safe retention remains behind unresolved plan',async()=>{
  const actor=new PlanAllocator({storage:storage()});await plan(actor,'hole');
  const status=await invoke(actor,{op:'snapshot'});assert.equal(status.snapshot,1);assert.equal(status.frontier,0);
  assert.equal((await invoke(actor,{op:'pin',id:'reader',owner:'r'})).snapshot,1);
  assert.equal((await invoke(actor,{op:'lease',id:'l',snapshot:1})).status,200);
  assert.equal((await invoke(actor,{op:'compact',retainSteps:0})).retentionFloor,0);
});
test('epoch retirement refuses old admitted pins, incomplete plans, and live external leases',async()=>{
  const actor=new PlanAllocator({storage:storage()});
  assert.deepEqual((await invoke(actor,{op:'epoch'})).currentEpoch,0);
  await invoke(actor,{op:'pin',id:'e0_op',owner:'owner'});await plan(actor,'e0_plan');await invoke(actor,{op:'lease',id:'lease',epoch:0});
  assert.equal((await invoke(actor,{op:'rotateEpoch'})).currentEpoch,1);
  let blocked=await invoke(actor,{op:'retireEpochs',before:1});assert.equal(blocked.code,'EPOCH_BUSY');assert.ok(blocked.owners.includes('owner'));assert.equal(blocked.plans,1);assert.equal(blocked.leases,1);
  await invoke(actor,{op:'unpin',id:'e0_op'});await done(actor,'e0_plan');
  assert.equal((await invoke(actor,{op:'retireEpochs',before:1})).code,'EPOCH_BUSY');
  await invoke(actor,{op:'release',id:'lease',epoch:0});assert.equal((await invoke(actor,{op:'retireEpochs',before:1})).rejectBeforeEpoch,1);
  const expired=await invoke(actor,{op:'epoch',requestEpoch:0});assert.equal(expired.status,410);assert.equal(expired.code,'REQUEST_EXPIRED');assert.equal(expired.retryable,false);
  assert.equal((await invoke(actor,{op:'pin',id:'e0_new',owner:'late'})).code,'REQUEST_EXPIRED');
  assert.equal((await invoke(actor,{op:'pin',id:'e1_new',epoch:1,owner:'new'})).status,200);
});
test('bounded epoch collection removes resolved replay records but old identities stay rejected after restart',async()=>{
  const state={storage:storage()};let actor=new PlanAllocator(state);
  await invoke(actor,{op:'pin',id:'e0_op',owner:'tx/old'});await invoke(actor,{op:'unpin',id:'e0_op'});
  await invoke(actor,{op:'lease',id:'old-lease'});await invoke(actor,{op:'release',id:'old-lease'});await plan(actor,'e0_done',[]);
  await invoke(actor,{op:'rotateEpoch'});await invoke(actor,{op:'retireEpochs',before:1});
  assert.deepEqual((await invoke(actor,{op:'retiredOwners'})).owners,[{owner:'tx/old',epoch:0}]);
  assert.equal((await invoke(actor,{op:'forgetOwner',owner:'tx/old',epoch:1})).status,409);
  await invoke(actor,{op:'forgetOwner',owner:'tx/old',epoch:0});assert.deepEqual((await invoke(actor,{op:'retiredOwners'})).owners,[]);
  let cursor,rounds=0;do{const page=await invoke(actor,{op:'collectEpochs',limit:1,...(cursor?{cursor}:{})});assert.ok(page.processed<=1);cursor=page.nextCursor;assert.ok(++rounds<10);}while(cursor);
  assert.equal(await state.storage.get('pinClosed:e0_op'),undefined);assert.equal(await state.storage.get('leaseClosed:old-lease'),undefined);assert.equal(await state.storage.get('plan:e0_done'),undefined);assert.equal(await state.storage.get('step:0000000000000001'),undefined);
  actor=new PlanAllocator(state);
  assert.equal((await invoke(actor,{op:'lookup',id:'e0_done'})).code,'REQUEST_EXPIRED');assert.equal((await plan(actor,'e0_done',[])).code,'REQUEST_EXPIRED');
  assert.equal((await invoke(actor,{op:'pin',id:'e0_op',owner:'tx/old'})).code,'REQUEST_EXPIRED');
  assert.equal((await invoke(actor,{op:'unpin',id:'e0_op'})).expired,true);assert.equal(await state.storage.get('pinClosed:e0_op'),undefined);
  assert.equal((await invoke(actor,{op:'release',id:'old-lease'})).expired,true);assert.equal(await state.storage.get('leaseClosed:old-lease'),undefined);
});
test('completed retired plan is not collected until both visibility and delivery frontiers pass its step',async()=>{
  const state={storage:storage()},actor=new PlanAllocator(state);await invoke(actor,{op:'rotateEpoch'});
  await plan(actor,'e1_hole');await plan(actor,'e0_old',[]);
  assert.equal((await invoke(actor,{op:'retireEpochs',before:1})).status,200);
  const blocked=await invoke(actor,{op:'collectEpochs'});assert.equal(blocked.blocked,1);assert.equal(blocked.done,false);assert.ok(await state.storage.get('plan:e0_old'));
  await done(actor,'e1_hole');assert.equal((await invoke(actor,{op:'collectEpochs'})).blocked,0);assert.equal(await state.storage.get('plan:e0_old'),undefined);
  assert.ok(await state.storage.get('plan:e1_hole'));
});
test('epoch identity mismatch cannot close another epoch pin or access another epoch plan',async()=>{
  const actor=new PlanAllocator({storage:storage()});await invoke(actor,{op:'rotateEpoch'});
  await invoke(actor,{op:'pin',id:'plain',epoch:0,owner:'old'});
  assert.equal((await invoke(actor,{op:'unpin',id:'plain',epoch:1})).status,409);
  assert.equal((await invoke(actor,{op:'pin',id:'plain',epoch:0,owner:'old'})).status,200);
  await plan(actor,'plain-plan',[]);assert.equal((await invoke(actor,{op:'lookup',id:'plain-plan',epoch:1})).status,409);
  assert.equal((await invoke(actor,{op:'pin',id:'e0_mismatch',epoch:1,owner:'bad'})).status,400);
});
