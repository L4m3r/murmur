import { encodeKey } from './keys.js';

const json = (value, status = 200) => Response.json(value, { status });
const all = { lower: null, upper: null, lowerInclusive: true, upperInclusive: false };
const conflict = (message, owners = []) => Object.assign(new Error(message), { status: 409, code: 'CONFLICT', owners: [...new Set(owners.filter(Boolean))] });
const token = step => String(step).padStart(16, '0');
const rowPrefix = key => `row:${encodeKey(key)}:`;
const contains = (range, encoded) => !range.empty &&
  (range.lower == null || encoded > range.lower || (encoded === range.lower && range.lowerInclusive)) &&
  (range.upper == null || encoded < range.upper || (encoded === range.upper && range.upperInclusive));
const touches = (reads, changes, key) => reads.some(r => contains(r, key)) || changes.some(c => c.encoded === key);
function stable(value) {
  if (Array.isArray(value)) return '[' + value.map(stable).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).filter(k => value[k] !== undefined).sort().map(k => JSON.stringify(k) + ':' + stable(value[k])).join(',') + '}';
  return JSON.stringify(value);
}
async function digest(text) {
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(hash)].map(v => v.toString(16).padStart(2, '0')).join('');
}
function bounds(range = all) {
  return { prefix: 'row:', start: range.lower == null ? 'row:' : 'row:' + range.lower + (range.lowerInclusive ? '' : '~'),
    end: range.upper == null ? 'row:~' : 'row:' + range.upper + (range.upperInclusive ? '~' : '') };
}
function validSnapshot(value) { if (!Number.isSafeInteger(value) || value < 0) throw new Error('Invalid MVCC snapshot'); }

/** One actor implementation for catalog records, base rows and index entries.
 * Reservation lifetimes span prepare->apply/abort; only overlapping footprints
 * conflict, not all work on the tablet. Historical versions are intentionally retained.
 */
export class DataShard {
  constructor(state, env) { this.storage = state.storage; this.env = env; this.tail = Promise.resolve(); }
  fetch(request) {
    const next = this.tail.then(() => request.json()).then(input => this.handle(input));
    this.tail = next.catch(() => {});
    return next.catch(e => json({ error: e.message, code: e.code || 'SHARD_ERROR', retryable: true, owners: e.owners || [] }, e.status || 503));
  }
  async mediator(input) {
    const response = await this.env.MEDIATOR.get(this.env.MEDIATOR.idFromName('global')).fetch('https://mediator/internal', { method: 'POST', body: JSON.stringify(input) });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || 'Mediator unavailable');
    return body;
  }
  async meta(storage, snapshot = Number.MAX_SAFE_INTEGER) {
    const history = await storage.list({ prefix: 'meta:' });
    let selected = { step: 0, epoch: 0, retired: false };
    for (const entry of history.values()) if (entry.step <= snapshot && entry.step >= selected.step) selected = entry;
    return selected;
  }
  async read(input) {
    validSnapshot(input.snapshot);
    const meta = await this.meta(this.storage, input.snapshot);
    if (input.epoch !== undefined && (input.epoch !== meta.epoch || meta.retired)) return json({ error: 'Stale routing epoch at snapshot', code: 'STALE_EPOCH' }, 409);
    let versions = [];
    if (input.op === 'getMany' || input.op === 'get') {
      const keys = input.op === 'get' ? [input.key] : input.keys;
      if (!Array.isArray(keys) || keys.length > 2000) return json({ error: 'At most 2000 point keys' }, 400);
      for (const key of keys) versions.push(...(await this.storage.list({ prefix: rowPrefix(key) })).values());
    } else if (!input.range?.empty) versions = [...(await this.storage.list(bounds(input.range))).values()];
    const latest = new Map();
    for (const version of versions) {
      const key = encodeKey(version.key);
      if (version.step <= input.snapshot && (!latest.has(key) || latest.get(key).step < version.step)) latest.set(key, version);
    }
    const entries = [...latest].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([, value]) => value).filter(v => v.row !== null);
    return json({ rows: entries.map(v => v.row), rowsRead: entries.length, versionsRead: versions.length, epoch: meta.epoch, retired: meta.retired, ...(input.op === 'get' ? { row: entries[0]?.row || null } : {}) });
  }
  async prepare(input) {
    validSnapshot(input.snapshot);
    if (!Array.isArray(input.reads) || !Array.isArray(input.changes) || input.changes.length > 2000 || input.reads.length > 4000) throw new Error('Invalid or oversized participant footprint');
    const changes = input.changes.map(c => ({ key: c.key, encoded: encodeKey(c.key), row: c.row }));
    if (new Set(changes.map(c => c.encoded)).size !== changes.length) throw new Error('Duplicate delta key');
    if (changes.some(c => c.row === undefined)) throw new Error('Delta row must be explicit (null deletes)');
    const body = { id: input.id, owner: input.owner, snapshot: input.snapshot, expectedEpoch: input.expectedEpoch, reads: input.reads, changes, meta: input.meta };
    const fingerprint = stable(body), vote = await digest(fingerprint);
    const pending = { ...body, vote };
    if (new TextEncoder().encode(JSON.stringify(pending)).length > 110000) throw new Error('Prepared participant exceeds 110 KB');
    await this.storage.transaction(async txn => {
      if (await txn.get(`aborted:${input.id}`)) throw conflict('Transaction already aborted');
      const existing = await txn.get(`pending:${input.id}`);
      const applied = await txn.get(`applied:${input.id}`);
      if (existing || applied) {
        if ((existing || applied).vote !== vote) throw conflict('Transaction ID payload mismatch');
        return;
      }
      const latestMeta = await this.meta(txn);
      const snapshotMeta = await this.meta(txn, input.snapshot);
      if (input.expectedEpoch !== undefined && (snapshotMeta.epoch !== input.expectedEpoch || snapshotMeta.retired || latestMeta.step > input.snapshot)) throw conflict('Routing metadata changed since snapshot');
      if (input.meta && latestMeta.step > input.snapshot) throw conflict('Concurrent metadata change');
      // Includes applied versions above the global visible frontier: otherwise a
      // delayed unrelated plan could make stale writes overwrite newer data.
      const versions = await txn.list({ prefix: 'row:' });
      for (const version of versions.values()) if (version.step > input.snapshot && touches(input.reads, changes, encodeKey(version.key))) throw conflict('Read/write version changed since snapshot');
      const reservations = await txn.list({ prefix: 'pending:' });
      if (reservations.size >= 64) throw conflict('Participant reservation capacity reached', [...reservations.values()].map(other => other.owner));
      for (const other of reservations.values()) {
        const metaConflict = (input.meta && (other.meta || other.expectedEpoch !== undefined)) || (other.meta && (input.meta || input.expectedEpoch !== undefined));
        if (metaConflict) throw conflict('Routing metadata reservation conflict', [other.owner]);
        if (changes.some(c => touches(other.reads, other.changes, c.encoded)) || other.changes.some(c => input.reads.some(r => contains(r, c.encoded)))) throw conflict('Overlapping read/write reservation', [other.owner]);
      }
      await txn.put(`pending:${input.id}`, pending);
    });
    return json({ prepared: true, vote });
  }
  async apply(input) {
    if (!Number.isSafeInteger(input.step) || input.step <= 0) throw new Error('Invalid plan step');
    const response = await this.mediator({ op: 'get', id: input.id });
    const plan = response.plan === undefined ? response : response.plan;
    if (!plan || plan.step !== input.step || !plan.participants?.some(p => p.name === input.shard && p.vote === input.vote)) throw new Error('Missing or mismatched durable plan certificate');
    await this.mediator({ op: 'delivered', id: input.id, name: input.shard });
    await this.storage.transaction(async txn => {
      const applied = await txn.get(`applied:${input.id}`);
      if (applied) { if (applied.step !== input.step || applied.vote !== input.vote) throw new Error('Applied transaction identity mismatch'); return; }
      if (await txn.get(`aborted:${input.id}`)) throw new Error('Cannot apply aborted transaction');
      const pending = await txn.get(`pending:${input.id}`);
      if (!pending || pending.vote !== input.vote) throw new Error('Missing prepared vote');
      for (const delta of pending.changes) await txn.put(`row:${delta.encoded}:${token(input.step)}`, { key: delta.key, row: delta.row, step: input.step });
      if (pending.meta) await txn.put(`meta:${token(input.step)}`, { ...pending.meta, step: input.step });
      await txn.put(`applied:${input.id}`, { step: input.step, vote: input.vote });
      await txn.delete(`pending:${input.id}`);
    });
    await this.mediator({ op: 'applied', id: input.id, name: input.shard });
    return json({ applied: true, step: input.step });
  }
  async abort(input) {
    // The coordinator never aborts after its durable COMMIT_READY boundary;
    // a durable plan is an additional local fence against accidental late abort.
    const response = await this.mediator({ op: 'get', id: input.id });
    const plan = response.plan === undefined ? (response.step ? response : null) : response.plan;
    if (plan) throw new Error('Planned transaction cannot be aborted');
    await this.storage.transaction(async txn => {
      if (await txn.get(`applied:${input.id}`)) throw new Error('Applied transaction cannot be aborted');
      await txn.put(`aborted:${input.id}`, true);
      await txn.delete(`pending:${input.id}`);
    });
    return json({ aborted: true });
  }
  async handle(input) {
    if (['scan', 'read', 'getMany', 'get'].includes(input.op)) return this.read(input);
    if (input.op === 'prepare') return this.prepare(input);
    if (['apply', 'commit'].includes(input.op)) return this.apply(input);
    if (input.op === 'abort') return this.abort(input);
    if (input.op === 'inspect') return json({ pending: [...(await this.storage.list({ prefix: 'pending:' })).values()] });
    return json({ error: 'Unknown DataShard operation' }, 400);
  }
}
