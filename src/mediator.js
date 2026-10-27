/** Durable plan ordering and delivery/apply watermarks. No conflict certification
 * or remote execution happens here; DataShards vote before a plan is requested. */
const json = (body, status = 200) => Response.json(body, { status });
const stepKey = step => `step:${String(step).padStart(16, '0')}`;
const complete = (plan, field) => plan[field].length === plan.participants.length;
export class Mediator {
  constructor(state) { this.storage = state.storage; this.tail = Promise.resolve(); }
  fetch(request) {
    const task = this.tail.then(() => request.json()).then(input => this.handle(input));
    this.tail = task.catch(() => {});
    return task.catch(error => json({ error: error.message, code: error.code || 'MEDIATOR_ERROR', retryable: true }, error.status || 503));
  }
  async advance(txn) {
    const allocated = await txn.get('allocated') || 0;
    for (const [key, field] of [['deliveredFrontier', 'delivered'], ['frontier', 'applied']]) {
      let frontier = await txn.get(key) || 0;
      while (frontier < allocated) {
        const id = await txn.get(stepKey(frontier + 1));
        const plan = id && await txn.get(`plan:${id}`);
        if (!plan || !complete(plan, field)) break;
        frontier++;
      }
      await txn.put(key, frontier);
    }
  }
  async status() {
    const frontier = await this.storage.get('frontier') || 0;
    const allocated = await this.storage.get('allocated') || 0;
    const pending = [];
    // Retained plans also form the recovery manifest. Later complete plans do
    // not need recovery; a hole before them still holds the global frontier.
    for (let step = frontier + 1; step <= allocated; step++) {
      const id = await this.storage.get(stepKey(step));
      const plan = id && await this.storage.get(`plan:${id}`);
      if (plan && !complete(plan, 'applied')) pending.push({ id, step, owner: plan.owner });
    }
    return { snapshot: frontier, frontier, deliveredFrontier: await this.storage.get('deliveredFrontier') || 0, allocated, pending };
  }
  async handle(input) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) return json({ error: 'Object required' }, 400);
    if (input.op === 'snapshot' || input.op === 'status') return json(await this.status());
    if (typeof input.id !== 'string' || !input.id.length || input.id.length > 128) return json({ error: 'Invalid transaction id' }, 400);
    if (input.op === 'get') return json(await this.storage.get(`plan:${input.id}`) || { missing: true });
    if (input.op === 'plan') {
      if (typeof input.owner !== 'string' || !input.owner.length || input.owner.length > 128 || !Array.isArray(input.participants) || input.participants.length > 64) return json({ error: 'Invalid plan manifest' }, 400);
      const participants = input.participants.map(p => ({ name: p?.name, vote: p?.vote }));
      if (participants.some(p => typeof p.name !== 'string' || !p.name.length || p.name.length > 1024 || typeof p.vote !== 'string' || !p.vote.length || p.vote.length > 256) || new Set(participants.map(p => p.name)).size !== participants.length) return json({ error: 'Invalid or duplicate participant' }, 400);
      participants.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
      const fingerprint = input.fingerprint ?? null;
      if (fingerprint !== null && (typeof fingerprint !== 'string' || fingerprint.length > 1024)) return json({ error: 'Invalid fingerprint' }, 400);
      const identity = JSON.stringify({ owner: input.owner, participants, fingerprint });
      const names = participants.map(participant => participant.name);
      const maximumRecord = { id: input.id, step: Number.MAX_SAFE_INTEGER, owner: input.owner, participants, fingerprint, delivered: names, applied: names };
      if (new TextEncoder().encode(JSON.stringify(maximumRecord)).byteLength > 110000) return json({ error: 'Manifest and acknowledgments too large' }, 400);
      const result = await this.storage.transaction(async txn => {
        const previous = await txn.get(`plan:${input.id}`);
        if (previous) return JSON.stringify({ owner: previous.owner, participants: previous.participants, fingerprint: previous.fingerprint }) === identity ? { plan: previous } : { conflict: true };
        const step = (await txn.get('allocated') || 0) + 1;
        if (!Number.isSafeInteger(step)) throw new Error('Plan step exhausted');
        const plan = { id: input.id, step, owner: input.owner, participants, fingerprint, delivered: [], applied: [] };
        await txn.put(`plan:${input.id}`, plan);
        await txn.put(stepKey(step), input.id);
        await txn.put('allocated', step);
        await this.advance(txn);
        return { plan };
      });
      return result.conflict ? json({ error: 'Transaction id reused with different manifest', code: 'PLAN_MISMATCH' }, 409) : json(result.plan);
    }
    if (input.op === 'delivered' || input.op === 'applied') {
      const result = await this.storage.transaction(async txn => {
        const plan = await txn.get(`plan:${input.id}`);
        if (!plan) return { missing: true };
        if (!plan.participants.some(p => p.name === input.name)) return { invalid: true };
        if (input.op === 'applied' && !plan.delivered.includes(input.name)) return { notDelivered: true };
        if (!plan[input.op].includes(input.name)) plan[input.op].push(input.name);
        await txn.put(`plan:${input.id}`, plan);
        await this.advance(txn);
        return { step: plan.step, delivered: complete(plan, 'delivered'), applied: complete(plan, 'applied') };
      });
      if (result.missing) return json({ error: 'Unknown plan', code: 'UNKNOWN_PLAN' }, 404);
      if (result.invalid) return json({ error: 'Participant is not in immutable manifest', code: 'NOT_PARTICIPANT' }, 409);
      if (result.notDelivered) return json({ error: 'Plan delivery must be acknowledged before apply', code: 'NOT_DELIVERED' }, 409);
      return json(result);
    }
    return json({ error: 'Unknown mediator operation' }, 400);
  }
}
