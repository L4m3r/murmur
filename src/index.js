import { parse, execute } from './sql.js';
import { keyHash, routeTablet, planTablets } from './tablets.js';

const json = (body, status = 200) => Response.json(body, { status });
const bytes = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;
const copy = value => structuredClone(value);
export const shardFor = (key, count = 4) => keyHash(key) % count;
async function rowKey(key) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(key)));
  return 'row:' + Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, '0')).join('');
}
export async function actorCall(namespace, name, body) {
  const response = await namespace.get(namespace.idFromName(name)).fetch('https://actor/internal', {
    method: 'POST', body: JSON.stringify(body), headers: { 'content-type': 'application/json' }
  });
  const result = await response.json();
  if (!response.ok) { const error = new Error(result.error || `Actor ${name} failed`); error.code = result.code; throw error; }
  return result;
}
class SerialActor {
  constructor(state, env) { this.storage = state.storage; this.env = env; this.tail = Promise.resolve(); }
  fetch(request) {
    const task = this.tail.then(() => request.json()).then(input => this.handle(input));
    this.tail = task.catch(() => {});
    return task.catch(error => json({ error: error.message, code: error.code || 'RECOVERY_REQUIRED', retryable: true }, 503));
  }
}

// Durable table-level locks, not a transaction executor: disjoint table sets overlap.
export class LockManager extends SerialActor {
  async handle(input) {
    if (input.op === 'acquire') {
      return json(await this.storage.transaction(async txn => {
        if (await txn.get(`released:${input.id}`)) return { released: true };
        const prior = await txn.get(`grant:${input.id}`);
        if (prior) return prior;
        const tables = [...new Set(input.tables)].sort();
        const owners = [];
        for (const table of tables) {
          const lock = await txn.get(`lock:${table}`);
          if (lock && lock.id !== input.id) owners.push(lock.owner);
        }
        if (owners.length) return { busy: true, owners: [...new Set(owners)] };
        const version = (await txn.get('counter') || 0) + 1;
        const grant = { id: input.id, owner: input.owner, tables, version };
        await txn.put('counter', version);
        await txn.put(`grant:${input.id}`, grant);
        for (const table of tables) await txn.put(`lock:${table}`, { id: input.id, owner: input.owner });
        return grant;
      }));
    }
    if (input.op === 'release') {
      await this.storage.transaction(async txn => {
        await txn.put(`released:${input.id}`, true); // late acquire cannot resurrect an orphan
        const grant = await txn.get(`grant:${input.id}`);
        for (const table of grant?.tables || []) {
          if ((await txn.get(`lock:${table}`))?.id === input.id) await txn.delete(`lock:${table}`);
        }
        await txn.delete(`grant:${input.id}`);
      });
      return json({ released: true });
    }
    if (input.op === 'inspect') {
      const locks = await this.storage.list({ prefix: 'lock:' });
      return json({ locks: Object.fromEntries(locks) });
    }
    return json({ error: 'Unknown lock operation' }, 400);
  }
}

class Participant extends SerialActor {
  async encode(value) { return value; }
  epoch(data) { return data?.epoch ?? data?.value?.epoch ?? 0; }
  async read(input) {
    const data = await this.storage.get('data');
    if (input.epoch !== undefined && (input.epoch !== this.epoch(data) || data?.retired)) return json({ error: 'Stale tablet routing epoch', code: 'STALE_EPOCH' }, 409);
    return json(data || { version: 0, epoch: 0, value: null });
  }
  async write(txn, pending) {
    await txn.put('data', { id: pending.id, version: pending.version, epoch: pending.epoch, retired: pending.retired, value: pending.value });
  }
  async handle(input) {
    if (input.op === 'read' || input.op === 'get') return this.read(input);
    if (input.op === 'prepare') {
      const value = await this.encode(input.value, input.primaryKey);
      const pending = { id: input.id, version: input.version, value, epoch: input.epoch, retired: !!input.retired };
      if (bytes(pending) > 110000) throw new Error('Prepared participant exceeds 110 KB');
      await this.storage.transaction(async txn => {
        if (await txn.get(`aborted:${input.id}`)) throw new Error('Transaction already aborted');
        const data = await txn.get('data');
        if (data?.retired || this.epoch(data) !== input.expectedEpoch) { const e = new Error('Stale prepare epoch'); e.code = 'STALE_EPOCH'; throw e; }
        const old = await txn.get('pending');
        if (old && (old.id !== input.id || JSON.stringify(old) !== JSON.stringify(pending))) throw new Error('Conflicting prepared transaction');
        if (data && data.version >= input.version) throw new Error('Stale prepare fence');
        await txn.put('pending', pending);
      });
      return json({ prepared: true });
    }
    if (input.op === 'commit') {
      await this.storage.transaction(async txn => {
        const data = await txn.get('data');
        if (data && data.version >= input.version) {
          if (data.version === input.version && data.id !== input.id) throw new Error('Commit identity/fence mismatch');
          return;
        }
        const pending = await txn.get('pending');
        if (!pending || pending.id !== input.id || pending.version !== input.version) throw new Error('Missing prepared transaction');
        await this.write(txn, pending);
        await txn.delete('pending');
      });
      return json({ committed: true });
    }
    if (input.op === 'abort') {
      await this.storage.transaction(async txn => {
        await txn.put(`aborted:${input.id}`, true);
        if ((await txn.get('pending'))?.id === input.id) await txn.delete('pending');
      });
      return json({ aborted: true });
    }
    return json({ error: 'Unknown participant operation' }, 400);
  }
}
export class Catalog extends Participant {}
export class RowShard extends Participant {
  async encode(rows, primaryKey) { return Promise.all(rows.map(async row => ({ key: await rowKey(row[primaryKey]), row }))); }
  async read(input) {
    const data = await this.storage.get('data');
    if (input.epoch !== undefined && (input.epoch !== this.epoch(data) || data?.retired)) return json({ error: 'Stale tablet routing epoch', code: 'STALE_EPOCH' }, 409);
    if (input.op === 'get') return json({ row: await this.storage.get(await rowKey(input.key)) || null });
    const rows = await this.storage.list({ prefix: 'row:' });
    return json({ version: data?.version || 0, epoch: this.epoch(data), retired: !!data?.retired, value: [...rows.values()] });
  }
  async write(txn, pending) {
    const existing = await txn.list({ prefix: 'row:' });
    const next = new Set(pending.value.map(entry => entry.key));
    for (const key of existing.keys()) if (!next.has(key)) await txn.delete(key);
    for (const { key, row } of pending.value) await txn.put(key, row);
    await txn.put('data', { id: pending.id, version: pending.version, epoch: pending.epoch, retired: pending.retired });
  }
}
export class IndexShard extends RowShard {}

const rowName = (table, tablet) => `${table}/${tablet}`;
const indexName = (table, index, shard) => `${table}/${index}/${shard}`;
function indexRows(rows, schema, index) {
  const buckets = new Map();
  for (const row of rows) {
    const value = row[index.column];
    if (value === null) continue;
    const key = JSON.stringify(value);
    if (!buckets.has(key)) buckets.set(key, { id: value, pks: [] });
    buckets.get(key).pks.push(row[schema.primaryKey]);
  }
  return [...buckets.values()];
}

export class Coordinator extends SerialActor {
  participant(target, input) {
    const namespace = target.kind === 'catalog' ? this.env.CATALOG : target.kind === 'index' ? this.env.INDEXES : this.env.ROWS;
    return actorCall(namespace, target.name, input);
  }
  locks(input) { return actorCall(this.env.LOCKS, 'locks', input); }
  async recover() {
    const active = await this.storage.get('active');
    if (!active) return;
    const committed = active.decision === 'commit';
    for (const target of active.targets) await this.participant(target, { op: committed ? 'commit' : 'abort', id: active.id, version: active.version });
    if (committed) await this.storage.put(`request:${active.key}`, { sql: active.sql, response: active.response });
    await this.locks({ op: 'release', id: active.id });
    await this.storage.delete('active');
  }
  async handle(input) {
    if (input.op === 'legacyInspect') {
      const active = await this.storage.get('active');
      const version = await this.storage.get('version');
      const receipts = await this.storage.list({ prefix: 'request:', limit: 1 });
      return json({ legacy: !!active || !!version || receipts.size > 0 });
    }
    await this.recover();
    if (input.op === 'recover') return json({ recovered: true });
    const { sql, idempotencyKey: key, coordinatorId } = input;
    const previous = await this.storage.get(`request:${key}`);
    if (previous) return previous.sql === sql ? json(previous.response) : json({ error: 'Idempotency key reused for different SQL' }, 409);
    let statements;
    try { statements = parse(sql); } catch (e) { return json({ error: e.message }, 400); }
    // V1's singleton catalog is never silently ignored. Upgrade must use a fresh
    // state directory or an explicit future export/import; no destructive migration.
    const legacy = await actorCall(this.env.CATALOG, 'catalog', { op: 'read' });
    const legacyCoordinator = await actorCall(this.env.COORDINATOR, 'global', { op: 'legacyInspect' });
    if ((legacy.value && Object.keys(legacy.value).length) || legacyCoordinator.legacy) return json({ error: 'V1 database detected: export/import into a new V2 state directory is required; existing data is untouched', code: 'LEGACY_STATE' }, 409);
    const tableNames = [...new Set(statements.map(s => s.table))].sort();
    if (tableNames.length > 8) return json({ error: 'At most 8 tables per transaction' }, 400);
    const active = { id: crypto.randomUUID(), key, sql, owner: coordinatorId, targets: [], decision: 'acquiring' };
    await this.storage.put('active', active); // precedes grant, including lost grant response
    const grant = await this.locks({ op: 'acquire', id: active.id, owner: coordinatorId, tables: tableNames });
    if (grant.busy || grant.released) {
      await this.recover();
      return json({ error: 'Tables busy', code: 'BUSY', owners: grant.owners || [], retryable: true }, 409);
    }
    active.version = grant.version;
    await this.storage.put('active', active);
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-lock') throw new Error('Injected failure after lock grant');
    if (this.env.ALLOW_FAULTS === 'true' && Number.isInteger(input.pauseAfterLockMs)) await new Promise(resolve => setTimeout(resolve, Math.min(1000, Math.max(0, input.pauseAfterLockMs))));
    try {
      return await this.execute(input, statements, tableNames, active);
    } catch (error) {
      // Logical validation failures occur before PREPARING; release immediately.
      if (active.decision === 'acquiring') { await this.recover(); return json({ error: error.message }, 400); }
      throw error; // uncertain prepare/commit must be recovered from its durable record
    }
  }
  async execute(input, statements, tableNames, active) {
    const catalog = {}, tables = {};
    for (const table of tableNames) {
      const data = await actorCall(this.env.CATALOG, `table:${table}`, { op: 'read' });
      if (data.value) catalog[table] = data.value;
    }
    let plan = { type: 'scan' };
    const select = statements.length === 1 && statements[0].kind === 'select' ? statements[0] : null;
    const schema = select && catalog[select.table];
    const index = schema?.indexes?.find(index => select.filter.some(f => f.column === index.column && f.op === '=' && f.value !== null));
    if (index) {
      const value = select.filter.find(f => f.column === index.column && f.op === '=').value;
      const hit = await actorCall(this.env.INDEXES, indexName(select.table, index.name, shardFor(value)), { op: 'get', key: value, epoch: schema.epoch });
      const pks = hit.row?.pks || [];
      tables[select.table] = (await Promise.all(pks.map(pk => actorCall(this.env.ROWS, rowName(select.table, routeTablet(schema, pk)), { op: 'get', key: pk, epoch: schema.epoch })))).map(r => r.row).filter(Boolean);
      plan = { type: 'index', index: index.name, rowsFetched: tables[select.table].length, indexShard: shardFor(value) };
    } else {
      for (const [table, schema] of Object.entries(catalog)) {
        const parts = await Promise.all(schema.tablets.map(tablet => actorCall(this.env.ROWS, rowName(table, tablet.id), { op: 'read', epoch: schema.epoch })));
        tables[table] = parts.flatMap(part => part.value || []);
      }
    }
    const result = execute(statements, copy(catalog), copy(tables));
    if (Object.values(result.tables).reduce((n, rows) => n + rows.length, 0) > 2000) throw new Error('POC limit: 2000 rows per transaction');
    const targets = [], events = [];
    const writes = new Set(statements.filter(s => s.kind !== 'select').map(s => s.table));
    for (const table of writes) {
      const old = catalog[table];
      const current = result.catalog[table];
      const planned = planTablets(current, result.tables[table], { splitBytes: Number(this.env.TABLET_SPLIT_BYTES || 4096), mergeBytes: Number(this.env.TABLET_MERGE_BYTES || 1024) });
      result.catalog[table] = planned.schema;
      const next = planned.schema;
      events.push(...planned.events.map(event => ({ table, ...event })));
      if (JSON.stringify(old) !== JSON.stringify(next)) targets.push({ kind: 'catalog', name: `table:${table}`, value: next, expectedEpoch: old?.epoch || 0, epoch: next.epoch });
      for (const part of planned.partitions) {
        const existing = old?.tablets?.some(t => t.id === part.id);
        targets.push({ kind: 'row', name: rowName(table, part.id), value: part.rows, primaryKey: next.primaryKey, epoch: next.epoch, expectedEpoch: existing ? old.epoch : 0 });
      }
      for (const retired of old?.tablets || []) if (!next.tablets.some(t => t.id === retired.id)) targets.push({ kind: 'row', name: rowName(table, retired.id), value: [], primaryKey: next.primaryKey, epoch: next.epoch, expectedEpoch: old.epoch, retired: true });
      for (const index of next.indexes || []) {
        const buckets = indexRows(result.tables[table], next, index);
        const exists = old?.indexes?.some(i => i.name === index.name);
        for (let shard = 0; shard < 4; shard++) targets.push({ kind: 'index', name: indexName(table, index.name, shard), value: buckets.filter(bucket => shardFor(bucket.id) === shard), primaryKey: 'id', epoch: next.epoch, expectedEpoch: exists ? old.epoch : 0 });
      }
    }
    if (targets.length > 64) throw new Error('At most 64 participants per transaction');
    const response = { transactionId: active.id, version: active.version, coordinatorId: active.owner, results: result.results, participants: targets.map(({ kind, name }) => ({ kind, name })), routing: Object.entries(result.catalog).map(([table, schema]) => ({ table, epoch: schema.epoch, tablets: schema.tablets })), events, plan };
    Object.assign(active, { targets, response, decision: 'preparing' });
    if (bytes(active) > 100000) { active.decision = 'acquiring'; throw new Error('POC transaction snapshot/result limit exceeded (100 KB)'); }
    await this.storage.put('active', active);
    for (const target of targets) await this.participant(target, { op: 'prepare', id: active.id, version: active.version, ...target });
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-prepare') throw new Error('Injected failure after prepare');
    active.decision = 'commit';
    await this.storage.put('active', active);
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-decision') throw new Error('Injected failure after durable decision');
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-first-commit' && targets.length) {
      await this.participant(targets[0], { op: 'commit', id: active.id, version: active.version });
      throw new Error('Injected failure after first participant commit');
    }
    await this.recover();
    return json(response);
  }
}

async function readBody(request) {
  if (Number(request.headers.get('content-length')) > 65536) throw new Error('Body too large');
  const reader = request.body?.getReader();
  if (!reader) throw new Error('JSON body required');
  let size = 0; const chunks = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > 65536) { await reader.cancel(); throw new Error('Body too large'); }
    chunks.push(value);
  }
  const data = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { data.set(chunk, offset); offset += chunk.byteLength; }
  return JSON.parse(new TextDecoder().decode(data));
}
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health' && request.method === 'GET') return json({ ok: true, engine: 'actor-row-db-v2', coordinators: 4 });
    if (request.method !== 'POST' || !['/query', '/debug/tablet', '/debug/locks'].includes(url.pathname)) return json({ error: 'POST /query or GET /health' }, 404);
    if (env.API_TOKEN && request.headers.get('authorization') !== `Bearer ${env.API_TOKEN}`) return json({ error: 'Unauthorized' }, 401);
    if (!env.API_TOKEN && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) return json({ error: 'Set API_TOKEN for non-loopback access' }, 403);
    let input;
    try { input = await readBody(request); } catch (e) { return json({ error: e.message }, e.message === 'Body too large' ? 413 : 400); }
    if (!input || typeof input !== 'object' || Array.isArray(input)) return json({ error: 'JSON object required' }, 400);
    if (url.pathname.startsWith('/debug/')) {
      if (env.ALLOW_FAULTS !== 'true') return json({ error: 'Not found' }, 404);
      if (url.pathname === '/debug/locks') return env.LOCKS.get(env.LOCKS.idFromName('locks')).fetch('https://actor/internal', { method: 'POST', body: JSON.stringify({ op: 'inspect' }) });
      return env.ROWS.get(env.ROWS.idFromName(rowName(input.table, input.tablet))).fetch('https://actor/internal', { method: 'POST', body: JSON.stringify({ op: 'read', epoch: input.epoch }) });
    }
    if (typeof input.sql !== 'string' || input.sql.length > 32768) return json({ error: 'sql must be a string of at most 32768 characters' }, 400);
    if (typeof input.idempotencyKey !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(input.idempotencyKey)) return json({ error: 'idempotencyKey required (1-100 letters, digits, _ or -)' }, 400);
    delete input.op; // internal recovery/inspection operations are not client API
    const coordinatorId = `c${shardFor(input.idempotencyKey)}`;
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(coordinatorId));
    for (let attempt = 0; attempt < 100; attempt++) {
      const response = await coordinator.fetch('https://coordinator/query', { method: 'POST', body: JSON.stringify({ ...input, coordinatorId }) });
      if (response.status !== 409) return response;
      const body = await response.clone().json();
      if (body.code !== 'BUSY') return response;
      // Current handler has ended. Recover owners through their own queues;
      // recovery never acquires locks or recursively calls another coordinator.
      try { await Promise.all(body.owners.map(owner => actorCall(env.COORDINATOR, owner, { op: 'recover' }))); }
      catch (error) { return json({ error: error.message, code: 'RECOVERY_REQUIRED', retryable: true }, 503); }
      await new Promise(resolve => setTimeout(resolve, 5 + attempt));
    }
    return json({ error: 'Table lock contention; retry same idempotency key', code: 'BUSY', retryable: true }, 503);
  }
};
