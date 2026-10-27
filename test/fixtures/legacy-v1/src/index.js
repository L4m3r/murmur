import { parse, execute } from './sql.js';

const MAX_BODY = 65536;
const MAX_ROWS = 2000;
const json = (body, status = 200) => Response.json(body, { status });
const copy = value => structuredClone(value);
export function shardFor(key, count = 4) {
  const text = JSON.stringify(key);
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 16777619);
  return (hash >>> 0) % count;
}
async function call(namespace, name, body) {
  const response = await namespace.get(namespace.idFromName(name)).fetch('https://actor/internal', {
    method: 'POST', body: JSON.stringify(body), headers: { 'content-type': 'application/json' }
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || `Actor ${name} failed`);
  return result;
}

// Participants only expose internal DO fetch: the public Worker never routes to them.
class Participant {
  constructor(state) { this.storage = state.storage; this.tail = Promise.resolve(); }
  fetch(request) {
    const task = this.tail.then(() => this.handle(request));
    this.tail = task.catch(() => {});
    return task.catch(error => json({ error: error.message }, 500));
  }
  async encode(value) { return value; }
  async read() { return await this.storage.get('data') || { version: 0, value: null }; }
  async write(txn, pending) { await txn.put('data', { version: pending.version, value: pending.value }); }
  async handle(request) {
    const input = await request.json();
    if (input.op === 'read') return json(await this.read());
    if (input.op === 'prepare') {
      const encoded = await this.encode(input.value, input.primaryKey);
      if (new TextEncoder().encode(JSON.stringify(encoded)).byteLength > 110000) throw new Error('Prepared shard exceeds 110 KB');
      await this.storage.transaction(async txn => {
        if (await txn.get(`aborted:${input.id}`)) throw new Error('Transaction already aborted');
        const data = await txn.get('data');
        const pending = await txn.get('pending');
        if (pending && pending.id !== input.id) throw new Error('Participant already prepared');
        if (data && data.version >= input.version) throw new Error('Stale prepare');
        await txn.put('pending', { id: input.id, version: input.version, value: encoded });
      });
      return json({ prepared: true });
    }
    if (input.op === 'commit') {
      await this.storage.transaction(async txn => {
        const data = await txn.get('data');
        if (data && data.version >= input.version) return; // replay is harmless
        const pending = await txn.get('pending');
        if (!pending || pending.id !== input.id) throw new Error('Missing prepared transaction');
        await this.write(txn, pending);
        await txn.delete('pending');
      });
      return json({ committed: true });
    }
    if (input.op === 'abort') {
      await this.storage.transaction(async txn => {
        await txn.put(`aborted:${input.id}`, true); // fence delayed PREPARE after recovery
        const pending = await txn.get('pending');
        if (pending?.id === input.id) await txn.delete('pending');
      });
      return json({ aborted: true });
    }
    return json({ error: 'Unknown participant operation' }, 400);
  }
}
export class Catalog extends Participant {}
export class RowShard extends Participant {
  async encode(rows, primaryKey) {
    return Promise.all(rows.map(async row => {
      const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(row[primaryKey])));
      const key = 'row:' + Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('');
      return { key, row };
    }));
  }
  async read() {
    const meta = await this.storage.get('data');
    const rows = await this.storage.list({ prefix: 'row:' });
    return { version: meta?.version || 0, value: [...rows.values()] };
  }
  async write(txn, pending) {
    const existing = await txn.list({ prefix: 'row:' });
    const nextKeys = new Set(pending.value.map(entry => entry.key));
    for (const key of existing.keys()) if (!nextKeys.has(key)) await txn.delete(key);
    for (const { key, row } of pending.value) await txn.put(key, row);
    await txn.put('data', { version: pending.version });
  }
}

export class Coordinator {
  constructor(state, env) {
    this.storage = state.storage; this.env = env; this.tail = Promise.resolve();
  }
  fetch(request) {
    // DO requests interleave at awaits; explicit queue holds the global read/write gate.
    const task = this.tail.then(() => this.handle(request));
    this.tail = task.catch(() => {});
    return task.catch(error => json({ error: error.message, retryable: true }, 503));
  }
  participant(target, message) {
    return call(target.kind === 'catalog' ? this.env.CATALOG : this.env.ROWS, target.name, message);
  }
  async recover() {
    const active = await this.storage.get('active');
    if (!active) return;
    const op = active.decision === 'commit' ? 'commit' : 'abort';
    for (const target of active.targets) await this.participant(target, { op, id: active.id, version: active.version });
    await this.storage.transaction(async txn => {
      if (op === 'commit') {
        await txn.put('version', active.version);
        await txn.put(`request:${active.key}`, { sql: active.sql, response: active.response });
      }
      await txn.delete('active');
    });
  }
  async handle(request) {
    await this.recover(); // no database observation until a prior decision is fully resolved
    const input = await request.json();
    const key = input.idempotencyKey;
    if (typeof input.sql !== 'string' || input.sql.length > 32768) return json({ error: 'sql must be a string of at most 32768 characters' }, 400);
    if (typeof key !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(key)) return json({ error: 'idempotencyKey required (1-100 letters, digits, _ or -)' }, 400);
    const previous = await this.storage.get(`request:${key}`);
    if (previous) return previous.sql === input.sql ? json(previous.response) : json({ error: 'Idempotency key reused for different SQL' }, 409);
    let statements;
    try { statements = parse(input.sql); } catch (error) { return json({ error: error.message }, 400); }
    const catalogData = await call(this.env.CATALOG, 'catalog', { op: 'read' });
    const catalog = catalogData.value || {};
    const tables = {};
    for (const [name, schema] of Object.entries(catalog)) {
      const partitions = await Promise.all(Array.from({ length: schema.shards || 4 }, (_, shard) => call(this.env.ROWS, `${name}:${shard}`, { op: 'read' })));
      tables[name] = partitions.flatMap(part => part.value || []);
    }
    let result;
    try {
      result = execute(statements, copy(catalog), copy(tables));
      if (Object.keys(result.catalog).length > 16 || Object.values(result.tables).reduce((n, rows) => n + rows.length, 0) > MAX_ROWS) throw new Error('POC limit: 16 tables and 2000 total rows');
    } catch (error) { return json({ error: error.message }, 400); }
    const version = (await this.storage.get('version') || 0) + 1;
    const id = crypto.randomUUID();
    const targets = [];
    if (JSON.stringify(catalog) !== JSON.stringify(result.catalog)) targets.push({ kind: 'catalog', name: 'catalog', value: result.catalog });
    for (const [name, rows] of Object.entries(result.tables)) {
      if (JSON.stringify(rows) === JSON.stringify(tables[name])) continue;
      const schema = result.catalog[name];
      const partitions = Array.from({ length: schema.shards || 4 }, () => []);
      for (const row of rows) partitions[shardFor(row[schema.primaryKey], partitions.length)].push(row);
      for (let shard = 0; shard < partitions.length; shard++) {
        const value = partitions[shard];
        if (JSON.stringify(value) !== JSON.stringify((tables[name] || []).filter(row => shardFor(row[schema.primaryKey], partitions.length) === shard))) targets.push({ kind: 'row', name: `${name}:${shard}`, value, primaryKey: schema.primaryKey });
      }
    }
    const response = { transactionId: id, version, results: result.results, participants: targets.map(({ kind, name }) => ({ kind, name })) };
    // Bound serialized durable object values (CF storage value limit is 128 KiB).
    const active = { id, key, sql: input.sql, version, targets, response, decision: 'preparing' };
    if (new TextEncoder().encode(JSON.stringify(active)).byteLength > 100000) return json({ error: 'POC transaction snapshot/result limit exceeded (100 KB)' }, 400);
    await this.storage.put('active', active);
    for (const target of targets) await this.participant(target, { op: 'prepare', id, version, value: target.value, primaryKey: target.primaryKey });
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-prepare') throw new Error('Injected failure after prepare; retry will abort');
    active.decision = 'commit';
    await this.storage.put('active', active); // durable point of no return
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-decision') throw new Error('Injected failure after durable commit decision; retry will commit');
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-first-commit' && targets.length) {
      await this.participant(targets[0], { op: 'commit', id, version });
      throw new Error('Injected failure during participant commit; retry will finish');
    }
    await this.recover();
    return json(response);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health' && request.method === 'GET') return json({ ok: true, engine: 'actor-row-db', shardsPerTable: 4 });
    if (url.pathname !== '/query' || request.method !== 'POST') return json({ error: 'POST /query or GET /health' }, 404);
    if (env.API_TOKEN && request.headers.get('authorization') !== `Bearer ${env.API_TOKEN}`) return json({ error: 'Unauthorized' }, 401);
    if (!env.API_TOKEN && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) return json({ error: 'Set API_TOKEN for non-loopback access' }, 403);
    if (Number(request.headers.get('content-length')) > MAX_BODY) return json({ error: 'Body too large' }, 413);
    const reader = request.body?.getReader();
    if (!reader) return json({ error: 'JSON body required' }, 400);
    let size = 0; const chunks = [];
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_BODY) { await reader.cancel(); return json({ error: 'Body too large' }, 413); }
      chunks.push(value);
    }
    const bytes = new Uint8Array(size); let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
    let input;
    try { input = JSON.parse(new TextDecoder().decode(bytes)); } catch { return json({ error: 'Invalid JSON' }, 400); }
    if (!input || typeof input !== 'object' || Array.isArray(input)) return json({ error: 'JSON object required' }, 400);
    return env.COORDINATOR.get(env.COORDINATOR.idFromName('global')).fetch('https://coordinator/query', { method: 'POST', body: JSON.stringify(input) });
  }
};
