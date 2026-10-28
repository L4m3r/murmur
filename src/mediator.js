/** Local allocation control, retention pins and durable prefix watermarks. */
const json = (body, status = 200) => Response.json(body, { status });
const stepKey = step => `step:${String(step).padStart(16, '0')}`;
const pendingKey = step => `pending-plan:${String(step).padStart(16, '0')}`;
const ownerKey = (owner, id) => `owner-pin:${String(owner.length).padStart(3, '0')}:${owner}:${id}`;
const epochPad = epoch => String(epoch).padStart(16, '0');
const epochOfId = id => { const match = /^e([0-9]+)_/.exec(id || ''); return match ? Number(match[1]) : 0; };
const epochGcKey = (epoch, kind, id) => `epoch-gc:${epochPad(epoch)}:${kind}:${id}`;
const receiptOwnerKey = (epoch, owner) => `receipt-owner:${epochPad(epoch)}:${owner}`;
const complete = (plan, field) => plan[field].length === plan.participants.length;
class AllocationControl {
  constructor(state, env = {}) { this.storage = state.storage; this.env = env; this.tail = Promise.resolve(); }
  now() { return Date.now(); }
  configured(name, fallback) {
    const value = this.env[name] === undefined ? fallback : Number(this.env[name]);
    if (!Number.isSafeInteger(value) || value < 1) throw new Error(`Invalid ${name}`);
    return value;
  }
  configuredBucketCount() {
    const count=this.env.PLAN_BUCKETS===undefined?4:Number(this.env.PLAN_BUCKETS);
    if(!Number.isSafeInteger(count)||count<1||count>64)throw Object.assign(new Error('PLAN_BUCKETS must be an integer from 1 through 64'),{status:400,code:'INVALID_BUCKET_COUNT'});
    return count;
  }
  async authoritativeBucketCount(storage=this.storage) {return (await storage.get('planBucketCount'))??this.configuredBucketCount();}
  async advance(txn) {
    const allocated = await txn.get('allocated') || 0;
    for (const [key, field] of [['deliveredFrontier', 'delivered'], ['frontier', 'applied']]) {
      let frontier = await txn.get(key) || 0;
      let advanced = 0;
      while (frontier < allocated && advanced < this.configured('MAX_FRONTIER_ADVANCE', 256)) {
        const id = await txn.get(stepKey(frontier + 1));
        const plan = id && await txn.get(`plan:${id}`);
        if (!plan || !complete(plan, field)) break;
        frontier++; advanced++;
      }
      await txn.put(key, frontier);
    }
  }
  async epochState(storage = this.storage) { return { currentEpoch: await storage.get('currentEpoch') || 0, rejectBeforeEpoch: await storage.get('rejectBeforeEpoch') || 0 }; }
  requestEpoch(input) {
    const derived = epochOfId(input.id), epoch = input.epoch === undefined ? derived : input.epoch;
    if (!Number.isSafeInteger(epoch) || epoch < 0 || (/^e[0-9]+_/.test(input.id || '') && input.epoch !== undefined && epoch !== derived)) return null;
    return epoch;
  }
  epochError(epoch, state) {
    if (!Number.isSafeInteger(epoch) || epoch < 0 || epoch > state.currentEpoch) return { status: 400, body: { error: 'Request epoch is not allocated', code: 'INVALID_REQUEST_EPOCH', retryable: false, ...state } };
    if (epoch < state.rejectBeforeEpoch) return { status: 410, body: { error: 'Request epoch permanently expired', code: 'REQUEST_EXPIRED', retryable: false, ...state } };
    return null;
  }
  async replay(input) {
    if (input.op === 'epoch') {
      const state = await this.epochState();
      const invalid = input.requestEpoch === undefined ? null : this.epochError(input.requestEpoch, state);
      return invalid ? json(invalid.body, invalid.status) : json(state);
    }
    if (input.op === 'rotateEpoch') return json(await this.storage.transaction(async txn => {
      const state = await this.epochState(txn), currentEpoch = state.currentEpoch + 1;
      if (!Number.isSafeInteger(currentEpoch)) throw new Error('Request epoch exhausted');
      await txn.put('currentEpoch', currentEpoch); return { ...state, currentEpoch };
    }));
    if (input.op === 'retireEpochs') {
      const result = await this.storage.transaction(async txn => {
        const state = await this.epochState(txn), before = input.before;
        if (!Number.isSafeInteger(before) || before < state.rejectBeforeEpoch || before > state.currentEpoch) return { status: 400, body: { error: 'Retirement floor must advance within allocated epochs', code: 'INVALID_REQUEST_EPOCH', retryable: false, ...state } };
        const pins = [...(await txn.list({ prefix: 'pin:' })).values()].filter(pin => (pin.epoch ?? 0) < before);
        const plans = [...(await txn.list({ prefix: 'pending-plan:' })).values()].filter(plan => (plan.epoch ?? 0) < before);
        const leases = [...(await txn.list({ prefix: 'lease:' })).values()].filter(lease => (lease.epoch ?? 0) < before && lease.expiresAt > this.now());
        if (pins.length || plans.length || leases.length) return { status: 409, body: { error: 'Epoch still has admitted work or live leases', code: 'EPOCH_BUSY', retryable: true, owners: [...new Set([...pins, ...plans].map(value => value.owner))].slice(0, 64), pins: pins.length, plans: plans.length, leases: leases.length, ...state } };
        await txn.put('rejectBeforeEpoch', before);
        return { body: { ...state, rejectBeforeEpoch: before } };
      });
      return json(result.body, result.status || 200);
    }
    if (input.op === 'retiredOwners') {
      const limit = input.limit ?? 16, state = await this.epochState();
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 64 || (input.cursor != null && (typeof input.cursor !== 'string' || !input.cursor.startsWith('receipt-owner:') || input.cursor.length > 1024))) return json({ error: 'Invalid retired owner page', retryable: false }, 400);
      const entries = [...await this.storage.list({ prefix: 'receipt-owner:', start: input.cursor == null ? 'receipt-owner:' : input.cursor + '\u0000', end: `receipt-owner:${epochPad(state.rejectBeforeEpoch)}:`, limit: limit + 1 })];
      const page = entries.slice(0, limit);
      return json({ owners: page.map(([, value]) => value), nextCursor: entries.length > limit ? page.at(-1)[0] : null });
    }
    if (input.op === 'forgetOwner') {
      const state = await this.epochState();
      if (!Number.isSafeInteger(input.epoch) || input.epoch < 0 || input.epoch >= state.rejectBeforeEpoch || typeof input.owner !== 'string' || !input.owner.length || input.owner.length > 128) return json({ error: 'Only retired receipt owners may be forgotten', retryable: false }, 409);
      await this.storage.delete(receiptOwnerKey(input.epoch, input.owner)); return json({ forgotten: true });
    }
    if (input.op === 'collectEpochs') {
      const limit = input.limit ?? 128;
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 128 || (input.cursor != null && (typeof input.cursor !== 'string' || !input.cursor.startsWith('epoch-gc:') || input.cursor.length > 1024))) return json({ error: 'Invalid epoch collection page', retryable: false }, 400);
      return json(await this.storage.transaction(async txn => {
        const state = await this.epochState(txn), frontier = await txn.get('frontier') || 0, delivered = await txn.get('deliveredFrontier') || 0;
        const entries = [...await txn.list({ prefix: 'epoch-gc:', start: input.cursor == null ? 'epoch-gc:' : input.cursor + '\u0000', end: `epoch-gc:${epochPad(state.rejectBeforeEpoch)}:`, limit: limit + 1 })];
        const page = entries.slice(0, limit); let deleted = 0, blocked = 0;
        for (const [indexKey, item] of page) {
          const value = await txn.get(item.key);
          if (value !== undefined && (value.epoch ?? 0) === item.epoch) {
            if (item.kind === 'plan') {
              if (!complete(value, 'applied') || value.step > frontier || value.step > delivered) { blocked++; continue; }
              if (await txn.get(stepKey(value.step)) === value.id) { await txn.delete(stepKey(value.step)); deleted++; }
              await txn.delete(`register:${value.id}`);
            }
            await txn.delete(item.key); deleted++;
            if (item.kind === 'lease-record') await txn.put('leaseCount', Math.max(0, (await txn.get('leaseCount') || 0) - 1));
          }
          await txn.delete(indexKey);
        }
        const remaining = await txn.list({ prefix: 'epoch-gc:', end: `epoch-gc:${epochPad(state.rejectBeforeEpoch)}:`, limit: 1 });
        const done = remaining.size === 0;
        return { ...state, bucketCount:await this.authoritativeBucketCount(txn), deleted, processed: page.length, blocked, done, restartRequired: !done && entries.length <= limit, nextCursor: entries.length > limit ? page.at(-1)[0] : null };
      }));
    }
    return json({ error: 'Unknown replay operation' }, 400);
  }
  async pendingPage({ cursor = 0, limit = 64 } = {}) {
    if (!Number.isSafeInteger(cursor) || cursor < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 64) return null;
    const page = await this.storage.list({ prefix: 'pending-plan:', start: pendingKey(cursor) + '\u0000', limit: limit + 1 });
    const all = [...page.values()];
    const pending = all.slice(0, limit);
    return { pending, nextCursor: all.length > limit ? pending.at(-1).step : null };
  }
  async status() {
    // Advance only a bounded completed prefix. Callers can repeat status when a
    // large formerly blocked prefix needs several batches; never skip a hole.
    await this.storage.transaction(txn => this.advance(txn));
    const frontier = await this.storage.get('frontier') || 0;
    const allocated = await this.storage.get('allocated') || 0;
    const page = await this.pendingPage();
    return { ...await this.epochState(), retentionFloor: await this.storage.get('retentionFloor') || 0, pins: await this.storage.get('pinCount') || 0, leaseRecords: await this.storage.get('leaseCount') || 0, snapshot: allocated, frontier, deliveredFrontier: await this.storage.get('deliveredFrontier') || 0, allocated, pending: page.pending, pendingNextCursor: page.nextCursor };
  }
  snapshotError(snapshot, floor, frontier) {
    if (!Number.isSafeInteger(snapshot) || snapshot < 0 || snapshot > frontier) return { status: 400, body: { error: 'Snapshot must be a nonnegative allocated step', code: 'INVALID_SNAPSHOT', retryable: false } };
    if (snapshot < floor) return { status: 410, body: { error: 'Snapshot is below the retention floor', code: 'SNAPSHOT_EXPIRED', retryable: false, retentionFloor: floor } };
    return null;
  }
  async closePin(txn,id,epoch) {
    if(epoch<(await txn.get('rejectBeforeEpoch')||0))return {unpinned:true,expired:true};
    const prior=await txn.get(`pin:${id}`);
    if(prior&&(prior.epoch??0)!==epoch)throw Object.assign(new Error('Pin belongs to another epoch'),{status:409,code:'EPOCH_MISMATCH'});
    await txn.put(`pinClosed:${id}`,{epoch});
    await txn.put(epochGcKey(epoch,'pinClosed',id),{kind:'pinClosed',key:`pinClosed:${id}`,epoch});
    if(prior){await txn.delete(`pin:${id}`);await txn.delete(ownerKey(prior.owner,prior.id));await txn.put('pinCount',Math.max(0,(await txn.get('pinCount')||0)-1));}
    // receipt-owner survives closure for later epoch-scoped receipt collection.
    return {unpinned:true,existed:!!prior};
  }
  async retention(input) {
    if (input.op === 'compact') {
      const retainSteps = input.retainSteps ?? 1000;
      if (!Number.isSafeInteger(retainSteps) || retainSteps < 0) return json({ error: 'retainSteps must be a nonnegative safe integer', retryable: false }, 400);
      return json(await this.storage.transaction(async txn => {
        const frontier = await txn.get('frontier') || 0;
        const prior = await txn.get('retentionFloor') || 0;
        const pins = await txn.list({ prefix: 'pin:' });
        const leases = await txn.list({ prefix: 'lease:' });
        const now = this.now();
        let candidate = Math.max(0, frontier - retainSteps), liveLeases = 0;
        for (const pin of pins.values()) candidate = Math.min(candidate, pin.snapshot);
        for (const lease of leases.values()) if (lease.expiresAt > now) { candidate = Math.min(candidate, lease.snapshot); liveLeases++; }
        const retentionFloor = Math.max(prior, candidate);
        await txn.put('retentionFloor', retentionFloor);
        return { retentionFloor, frontier, snapshot: await txn.get('allocated') || 0, pins: pins.size, leases: liveLeases, retainSteps };
      }));
    }
    if (typeof input.id !== 'string' || !input.id.length || input.id.length > 128) return json({ error: 'Invalid pin/lease id', retryable: false }, 400);
    const result = await this.storage.transaction(async txn => {
      const floor = await txn.get('retentionFloor') || 0;
      const frontier = await txn.get('allocated') || 0;
      const now = this.now();
      const epoch = this.requestEpoch(input), epochState = await this.epochState(txn);
      const invalidEpoch = this.epochError(epoch, epochState);
      if (invalidEpoch) {
        if (epoch !== null && epoch < epochState.rejectBeforeEpoch && ['unpin','release','releaseLease'].includes(input.op)) return { body: { expired: true, unpinned: input.op === 'unpin', released: input.op !== 'unpin' } };
        return invalidEpoch;
      }
      const error = (status, code, message) => ({ status, body: { error: message, code, retryable: false } });
      if (input.op === 'unpin') return {body:await this.closePin(txn,input.id,epoch)};
      if (input.op === 'release' || input.op === 'releaseLease') { const prior = await txn.get(`lease:${input.id}`); if (prior && (prior.epoch ?? 0) !== epoch) return error(409, 'EPOCH_MISMATCH', 'Lease belongs to another epoch'); await txn.put(`leaseClosed:${input.id}`, { epoch }); await txn.put(epochGcKey(epoch, 'leaseClosed', input.id), { kind: 'leaseClosed', key: `leaseClosed:${input.id}`, epoch }); const existed = await txn.delete(`lease:${input.id}`); if (existed) await txn.put('leaseCount', Math.max(0, (await txn.get('leaseCount') || 0) - 1)); return { body: { released: true, existed: !!existed } }; }
      if (input.op === 'pin') {
        if (await txn.get(`pinClosed:${input.id}`)) return error(409, 'PIN_CLOSED', 'Operation pin was permanently released');
        if (typeof input.owner !== 'string' || !input.owner.length || input.owner.length > 128 || (input.leaseId !== undefined && (typeof input.leaseId !== 'string' || !input.leaseId.length || input.leaseId.length > 128))) return error(400, 'INVALID_PIN', 'Pin owner and optional leaseId must be valid identifiers');
        const existing = await txn.get(`pin:${input.id}`);
        if (existing) {
          if ((existing.epoch ?? 0) !== epoch || existing.owner !== input.owner || (input.snapshot !== undefined && existing.snapshot !== input.snapshot) || (input.leaseId ?? null) !== existing.leaseId) return error(409, 'PIN_MISMATCH', 'Pin id reused with different owner/snapshot/lease');
          return { body: existing }; // admitted operation remains pinned after external lease expiration
        }
        let lease;
        if (input.leaseId !== undefined) {
          lease = await txn.get(`lease:${input.leaseId}`);
          if (!lease || lease.expiresAt <= now) return error(410, 'LEASE_EXPIRED', 'External snapshot lease is absent or expired');
        }
        const snapshot = input.snapshot ?? lease?.snapshot ?? frontier;
        const invalid = this.snapshotError(snapshot, floor, frontier); if (invalid) return invalid;
        if (lease && snapshot !== lease.snapshot) return error(409, 'LEASE_SNAPSHOT_MISMATCH', 'Pin snapshot differs from external lease');
        const pinCount = await txn.get('pinCount') || 0;
        if (pinCount >= this.configured('MAX_SNAPSHOT_PINS', 4096)) return error(429, 'SNAPSHOT_BACKPRESSURE', 'Too many active snapshot pins');
        const pin = { id: input.id, owner: input.owner, epoch, snapshot, leaseId: input.leaseId ?? null };
        await txn.put(`pin:${input.id}`, pin);
        await txn.put(ownerKey(pin.owner, pin.id), { owner: pin.owner, id: pin.id, epoch });
        await txn.put(receiptOwnerKey(epoch, pin.owner), { owner: pin.owner, epoch });
        await txn.put('pinCount', pinCount + 1);
        return { body: pin };
      }
      if (input.op === 'lease' || input.op === 'renew' || input.op === 'renewLease') {
        if (await txn.get(`leaseClosed:${input.id}`)) return error(409, 'LEASE_CLOSED', 'Lease identity was permanently released; use a new id');
        const existing = await txn.get(`lease:${input.id}`);
        const renewing = input.op !== 'lease';
        if (renewing && (!existing || existing.expiresAt <= now)) return error(410, 'LEASE_EXPIRED', 'Cannot renew an absent or expired lease');
        if (existing && existing.expiresAt <= now) return error(410, 'LEASE_EXPIRED', 'Lease identity expired; use a new id');
        const snapshot = input.snapshot ?? existing?.snapshot ?? frontier;
        const invalid = this.snapshotError(snapshot, floor, frontier); if (invalid) return invalid;
        const ttlMs = input.ttlMs ?? existing?.ttlMs ?? 60000;
        if (!Number.isSafeInteger(ttlMs) || ttlMs < 1 || ttlMs > this.configured('SNAPSHOT_LEASE_MAX_TTL_MS', 24 * 60 * 60 * 1000)) return error(400, 'INVALID_LEASE_TTL', 'Lease ttlMs is outside the configured range');
        if (existing && ((existing.epoch ?? 0) !== epoch || existing.snapshot !== snapshot || (!renewing && existing.ttlMs !== ttlMs))) return error(409, 'LEASE_MISMATCH', 'Lease id reused with different snapshot/TTL');
        if (existing && !renewing) return { body: existing };
        if (!existing) {
          const leases = await txn.list({ prefix: 'lease:' });
          const activeCount = [...leases.values()].filter(lease => lease.expiresAt > now).length;
          if (activeCount >= this.configured('MAX_SNAPSHOT_LEASES', 4096)) return error(429, 'SNAPSHOT_BACKPRESSURE', 'Too many active external leases');
        }
        const expiresAt = now + ttlMs;
        if (!Number.isSafeInteger(expiresAt)) return error(400, 'INVALID_LEASE_TTL', 'Lease expiry overflow');
        const lease = { id: input.id, epoch, snapshot, ttlMs, expiresAt };
        await txn.put(`lease:${input.id}`, lease);
        await txn.put(epochGcKey(epoch, 'lease-record', input.id), { kind: 'lease-record', key: `lease:${input.id}`, epoch });
        if (!existing) await txn.put('leaseCount', (await txn.get('leaseCount') || 0) + 1);
        return { body: lease };
      }
      return error(400, 'UNKNOWN_OPERATION', 'Unknown retention operation');
    });
    return json(result.body, result.status || 200);
  }
  async handle(input) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) return json({ error: 'Object required' }, 400);
    if (['epoch','rotateEpoch','retireEpochs','retiredOwners','forgetOwner','collectEpochs'].includes(input.op)) return this.replay(input);
    if (input.op === 'pending') {
      const page = await this.pendingPage(input);
      return page ? json(page) : json({ error: 'Invalid pending-plan page', retryable: false }, 400);
    }
    if (input.op === 'owners') {
      const limit = input.limit ?? 16;
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 64 || (input.cursor != null && (typeof input.cursor !== 'string' || !input.cursor.startsWith('owner-pin:') || new TextEncoder().encode(input.cursor).byteLength > 1800))) return json({ error: 'Invalid owner page', retryable: false }, 400);
      const page = await this.storage.list({ prefix: 'owner-pin:', start: input.cursor == null ? 'owner-pin:' : input.cursor + '\u0000', limit: limit + 1 });
      const all = [...page];
      const selected = all.slice(0, limit);
      return json({ owners: [...new Set(selected.map(([, value]) => value.owner))], nextCursor: all.length > limit ? selected.at(-1)[0] : null });
    }
    if (input.op === 'shards') {
      const limit = input.limit ?? 16;
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 64 || (input.cursor !== undefined && input.cursor !== null && (typeof input.cursor !== 'string' || new TextEncoder().encode(input.cursor).byteLength > 1800))) return json({ error: 'Invalid shard page', retryable: false }, 400);
      const page = await this.storage.list({ prefix: 'shard:', start: input.cursor == null ? 'shard:' : `shard:${input.cursor}\u0000`, limit: limit + 1 });
      const all = [...page.values()];
      const names = all.slice(0, limit);
      return json({ names, nextCursor: all.length > limit ? names.at(-1) : null });
    }
    if (['pin', 'unpin', 'lease', 'renew', 'renewLease', 'release', 'releaseLease', 'compact'].includes(input.op)) return this.retention(input);
    if (input.op === 'snapshot' || input.op === 'status') return json(await this.status());
    return json({ error: 'Unknown mediator operation' }, 400);
  }
}

/** Allocation authority: network anchoring is deliberately outside its local queue. */
export class PlanAllocator extends AllocationControl {
  local(fn) { const task=this.tail.then(fn); this.tail=task.catch(()=>{}); return task; }
  async fetch(request) {
    try {
      const input=await request.json();
      if(input.op==='assign') return json(await this.assign(input));
      if(input.op==='drive') return json(await this.drive());
      if(input.op==='completion') return json(await this.completion(input));
      if(['plan','get','delivered','applied'].includes(input.op)) return json({error:'Obsolete protocol operation'},400);
      return await this.local(async()=> {
        if(['lookup','abortIfUnassigned'].includes(input.op)) return this.lookup(input);
        return this.handle(input);
      });
    } catch(error) { return json({error:error.message,code:error.code||'ALLOCATOR_ERROR',rpc:error.rpc,retryAfterMs:error.retryAfterMs,retryable:(error.status||503)>=500},error.status||503); }
  }
  async lookup(input) {
    if(typeof input.id!=='string'||!input.id.length||input.id.length>128) return json({error:'Invalid transaction id'},400);
    const invalid=this.epochError(this.requestEpoch(input),await this.epochState());
    if(invalid) return json(invalid.body,invalid.status);
    return json(await this.storage.transaction(async txn=> {
      const plan=await txn.get(`plan:${input.id}`);
      if(plan) {if(plan.epoch!==this.requestEpoch(input))throw Object.assign(new Error('Plan belongs to another epoch'),{status:409,code:'EPOCH_MISMATCH'});return {state:'assigned',association:plan.association};}
      if(input.op==='abortIfUnassigned') {
        const epoch=this.requestEpoch(input);
        await txn.put(`unassigned-abort:${input.id}`,{epoch});
        await txn.put(epochGcKey(epoch,'unassigned-abort',input.id),{kind:'unassigned-abort',key:`unassigned-abort:${input.id}`,epoch});
      }
      return {state:await txn.get(`unassigned-abort:${input.id}`)?'aborted':'absent'};
    }));
  }
  async assign(input) {
    const {canonical,manifestHash,rpc}=await import('./protocol.js');
    if(typeof input.id!=='string'||!input.id.length||input.id.length>128||typeof input.owner!=='string'||!input.owner.length||input.owner.length>128||!Array.isArray(input.participants)||input.participants.length<1||input.participants.length>64) throw Object.assign(new Error('Invalid assignment'),{status:400});
    const participants=[...input.participants].sort((a,b)=>a.name<b.name?-1:1);
    if(new Set(participants.map(p=>p.name)).size!==participants.length||participants.some(p=>typeof p.name!=='string'||!p.name.length||p.name.length>1024||typeof p.bodyHash!=='string'||!/^[0-9a-f]{64}$/.test(p.bodyHash)||!p.publicKey)) throw Object.assign(new Error('Invalid participant manifest'),{status:400});
    if(await manifestHash(participants)!==input.manifestHash) throw Object.assign(new Error('Manifest hash mismatch'),{status:409});
    const identity={id:input.id,epoch:this.requestEpoch(input),owner:input.owner,snapshot:input.snapshot,participants,manifestHash:input.manifestHash};
    if(new TextEncoder().encode(canonical(identity)).length>80000) throw Object.assign(new Error('Manifest budget exceeded'),{status:413});
    // Only actual named actors anchor keys; caller-supplied replacement keys cannot vote.
    for(let start=0;start<participants.length;start+=8) await Promise.all(participants.slice(start,start+8).map(async p=> {
      const proposal=await rpc(this.env.SHARDS,p.name,{op:'proposal',id:input.id,epoch:identity.epoch});
      if(proposal.id!==input.id||proposal.name!==p.name||proposal.bodyHash!==p.bodyHash||canonical(proposal.publicKey)!==canonical(p.publicKey)||proposal.owner!==identity.owner||proposal.snapshot!==identity.snapshot||proposal.epoch!==identity.epoch||canonical(proposal.participants)!==canonical(participants.map(p=>p.name))) throw Object.assign(new Error('Proposal identity mismatch'),{status:409,code:'PROPOSAL_MISMATCH'});
    }));
    return this.local(()=>this.storage.transaction(async txn=> {
      const invalid=this.epochError(identity.epoch,await this.epochState(txn)); if(invalid) throw Object.assign(new Error(invalid.body.error),{status:invalid.status,code:invalid.body.code});
      const prior=await txn.get(`plan:${input.id}`);
      if(prior) { const {step,bucket,...old}=prior.association; if(canonical(old)!==canonical(identity)) throw Object.assign(new Error('Assignment manifest changed'),{status:409}); return prior.association; }
      if(await txn.get(`unassigned-abort:${input.id}`)) throw Object.assign(new Error('Unassigned transaction permanently aborted'),{status:409,code:'ASSIGNMENT_ABORTED'});
      const pin=await txn.get(`pin:${input.id}`);
      if(!pin||pin.owner!==identity.owner||pin.snapshot!==identity.snapshot||pin.epoch!==identity.epoch) throw Object.assign(new Error('Matching root operation pin required'),{status:409,code:'PIN_REQUIRED'});
      const step=(await txn.get('allocated')||0)+1;if(!Number.isSafeInteger(step)) throw new Error('Plan step exhausted');
      const configuredCount=this.configuredBucketCount(),persistedCount=await txn.get('planBucketCount');
      if(persistedCount!==undefined&&persistedCount!==configuredCount)throw Object.assign(new Error(`PLAN_BUCKETS is immutable after first assignment (stored ${persistedCount}, configured ${configuredCount})`),{status:409,code:'PLAN_BUCKET_CONFIG_CHANGED'});
      const bucketCount=persistedCount??configuredCount;
      if(persistedCount===undefined)await txn.put('planBucketCount',bucketCount);
      const association={...identity,step,bucket:`m${(step-1)%bucketCount}`};
      const plan={...association,association,delivered:[],applied:[]};
      const names=participants.map(p=>p.name);
      const certificate=participants.map(p=>({domain:'murmur/readset',version:1,id:association.id,epoch:association.epoch,step,name:p.name,bodyHash:p.bodyHash,manifestHash:association.manifestHash,outcome:'COMMIT',signature:'f'.repeat(128)}));
      const largest=Math.max(new TextEncoder().encode(canonical({...plan,delivered:names,applied:names})).length,new TextEncoder().encode(canonical({association,delivered:names,applied:names,decision:{outcome:'COMMIT',decisionHash:'f'.repeat(64),certificate}})).length);
      if(largest>110000)throw Object.assign(new Error('Plan and certificate byte budget exceeded'),{status:413});
      await txn.put(`plan:${input.id}`,plan); await txn.put(stepKey(step),input.id); await txn.put('allocated',step);
      await txn.put(pendingKey(step),{id:input.id,step,owner:identity.owner,epoch:identity.epoch,bucket:association.bucket});
      for(const p of participants) await txn.put(`shard:${p.name}`,p.name);
      await txn.put(`register:${input.id}`,association); if(txn.setAlarm) await txn.setAlarm(Date.now()+100);
      return association;
    }));
  }
  async drive() {
    const {rpc}=await import('./protocol.js');
    const entries=await this.local(async()=>{const cursor=await this.storage.get('registerCursor');let page=await this.storage.list({prefix:'register:',start:cursor?cursor+'\u0000':'register:',limit:8});if(!page.size&&cursor)page=await this.storage.list({prefix:'register:',limit:8});if(page.size)await this.storage.put('registerCursor',[...page.keys()].at(-1));return page;});
    let registered=0,failed=0;
    if(entries.size&&this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1000);
    for(const [key,association] of entries) {try{
      await rpc(this.env.MEDIATORS,association.bucket,{op:'register',association});
      await this.local(()=>this.storage.delete(key)); registered++;
    }catch{failed++;}}
    if((await this.storage.list({prefix:'register:',limit:1})).size&&this.storage.setAlarm) await this.storage.setAlarm(Date.now()+100);
    return {registered,failed};
  }
  async alarm() { try {await this.drive();} catch { if(this.storage.setAlarm) await this.storage.setAlarm(Date.now()+1000); } }
  async completion(input) {
    const {rpc,canonical}=await import('./protocol.js');
    const plan=await this.local(()=>this.storage.get(`plan:${input.id}`));
    if(!plan) throw Object.assign(new Error('Unknown assigned plan'),{status:404});
    const state=await rpc(this.env.MEDIATORS,plan.association.bucket,{op:'get',id:input.id});
    if(canonical(state.association)!==canonical(plan.association)) throw new Error('Bucket association mismatch');
    return this.local(()=>this.storage.transaction(async txn=> {
      const current=await txn.get(`plan:${input.id}`); if(!current) throw new Error('Plan retired');
      current.delivered=current.participants.filter(p=>current.delivered.includes(p.name)||state.delivered.includes(p.name)).map(p=>p.name);
      current.applied=current.participants.filter(p=>current.applied.includes(p.name)||state.applied.includes(p.name)).map(p=>p.name);
      await txn.put(`plan:${input.id}`,current);
      if(complete(current,'applied')) {await this.closePin(txn,current.id,current.epoch);await txn.delete(pendingKey(current.step));await txn.put(epochGcKey(current.epoch,'plan',current.id),{kind:'plan',key:`plan:${current.id}`,epoch:current.epoch});}
      await this.advance(txn); return {delivered:complete(current,'delivered'),applied:complete(current,'applied')};
    }));
  }
}
