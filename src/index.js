import { parse, execute } from './sql.js';
import { keyHash, routeTablet, planTablets, selectTablets } from './tablets.js';
import { encodeKey, rangeForFilters } from './keys.js';
export { DataShard } from './datashard.js';
export { Mediator } from './mediator.js';

const json = (body, status = 200) => Response.json(body, { status });
const copy = structuredClone;
const size = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;
export const shardFor = (key, count = 4) => keyHash(key) % count;
const catalogName = table => `catalog/${table}`;
const baseName = (table, tablet) => `base/${table}/${tablet}`;
const indexName = (table, index, tablet) => `index/${table}/${index}/${tablet}`;
const keyOf = (schema, row) => schema.keyColumns ? schema.keyColumns.map(c => row[c]) : row[schema.primaryKey];
const allRange = () => ({ lower: null, upper: null, lowerInclusive: true, upperInclusive: false, empty: false });
const point = key => ({ lower: encodeKey(key), upper: encodeKey(key), lowerInclusive: true, upperInclusive: true, empty: false });
const inRange = (key, range) => {
  if (range.empty) return false;
  const encoded = encodeKey(key);
  return (range.lower === null || encoded > range.lower || (encoded === range.lower && range.lowerInclusive)) && (range.upper === null || encoded < range.upper || (encoded === range.upper && range.upperInclusive));
};
const indexRows = (rows, schema, index) => rows.map(row => ({ value: row[index.column], pk: row[schema.primaryKey] }));
function diffRows(before, after, schema) {
  const old = new Map(before.map(row => [encodeKey(keyOf(schema, row)), row]));
  const next = new Map(after.map(row => [encodeKey(keyOf(schema, row)), row]));
  const changes = [];
  for (const [key, row] of next) if (JSON.stringify(row) !== JSON.stringify(old.get(key))) changes.push({ key: keyOf(schema, row), row });
  for (const [key, row] of old) if (!next.has(key)) changes.push({ key: keyOf(schema, row), row: null });
  return changes;
}
export async function actorCall(namespace, name, input) {
  const response = await namespace.get(namespace.idFromName(name)).fetch('https://actor/internal', { method: 'POST', body: JSON.stringify(input) });
  const result = await response.json();
  if (!response.ok) { const error = new Error(result.error || 'Actor request failed'); error.code = result.code; error.owners = result.owners; throw error; }
  return result;
}
const mediator = (env, input) => actorCall(env.MEDIATOR, 'global', input);
const shard = (env, name, input) => actorCall(env.SHARDS, name, input);

export class Coordinator {
  constructor(state, env) { this.storage = state.storage; this.env = env; this.tail = Promise.resolve(); }
  run(fn) { const task = this.tail.then(fn); this.tail = task.catch(() => {}); return task; }
  fetch(request) {
    return this.run(async () => this.handle(await request.json())).catch(error => json({ error: error.message, code: error.code || 'RECOVERY_REQUIRED', retryable: true }, 503));
  }
  alarm() { return this.run(() => this.recover()); }
  async arm() { if (this.storage.setAlarm) await this.storage.setAlarm(Date.now() + 2000); }
  async recover() {
    const active = await this.storage.get('active');
    if (!active) return;
    if (active.phase === 'preparing') {
      for (const target of active.targets) await shard(this.env, target.name, { op: 'abort', id: active.id });
    } else {
      // COMMIT_READY is durable and irrevocable. Lost plan responses are retried
      // under the same identity; no recovery path can change this into an abort.
      const planned = await mediator(this.env, { op: 'plan', id: active.id, owner: active.owner, participants: active.votes });
      const step = planned.step;
      for (const target of active.targets) {
        const vote = active.votes.find(v => v.name === target.name).vote;
        await shard(this.env, target.name, { op: 'apply', id: active.id, step, vote, shard: target.name });
        await mediator(this.env, { op: 'applied', id: active.id, name: target.name });
      }
      const response = { ...active.response, step, version: step };
      await this.storage.put(`request:${active.key}`, { fingerprint: active.fingerprint, response });
    }
    await this.storage.delete('active');
    if (this.storage.deleteAlarm) await this.storage.deleteAlarm();
  }
  async handle(input) {
    await this.recover();
    if (input.op === 'recover') return json({ recovered: true });
    const { sql, idempotencyKey: key, coordinatorId: owner } = input;
    const fingerprint = JSON.stringify({ sql, snapshot: input.snapshot ?? null });
    const previous = await this.storage.get(`request:${key}`);
    if (previous) return previous.fingerprint === fingerprint ? json(previous.response) : json({ error: 'Idempotency key reused for different query/snapshot' }, 409);
    let statements;
    try { statements = parse(sql); } catch (error) { return json({ error: error.message }, 400); }
    const readOnly = statements.every(s => s.kind === 'select');
    const status = await mediator(this.env, { op: 'snapshot' });
    const frontier = status.snapshot ?? status.frontier;
    const snapshot = input.snapshot ?? frontier;
    if (!Number.isSafeInteger(snapshot) || snapshot < 0 || snapshot > frontier || (input.snapshot !== undefined && !readOnly)) return json({ error: 'Explicit snapshot must be a visible nonnegative step for a read-only request' }, 400);
    const names = [...new Set(statements.map(s => s.table))].sort();
    if (names.length > 8) return json({ error: 'At most 8 tables per transaction' }, 400);
    const id = crypto.randomUUID();
    let built;
    try { built = await this.build(statements, names, snapshot); }
    catch (error) { return json({ error: error.message, code: error.code }, 400); }
    const response = { transactionId: id, coordinatorId: owner, snapshot, version: snapshot, ...built.response };
    if (readOnly) {
      if (size({ fingerprint, response }) > 100000) return json({ error: 'POC result limit exceeded' }, 400);
      await this.storage.put(`request:${key}`, { fingerprint, response });
      return json(response);
    }
    if (this.env.ALLOW_FAULTS === 'true' && input.pauseAfterReadMs) await this.pause(input.pauseAfterReadMs);
    const active = { id, key, owner, snapshot, fingerprint, phase: 'preparing', targets: built.targets, votes: [], response };
    if (size({ ...active, votes: active.targets.map(t => ({ name: t.name, vote: 'x'.repeat(64) })) }) > 100000) return json({ error: 'POC durable transaction limit exceeded (100 KB)' }, 400);
    await this.storage.put('active', active);
    await this.arm();
    try {
      for (const target of active.targets) {
        const prepared = await shard(this.env, target.name, { op: 'prepare', id, owner, snapshot, ...target });
        active.votes.push({ name: target.name, vote: prepared.vote });
      }
    } catch (error) {
      await this.recover();
      return json({ error: error.message, code: error.code || 'CONFLICT', owners: error.owners || [], retryable: true }, 409);
    }
    if (this.env.ALLOW_FAULTS === 'true' && input.pauseAfterPrepareMs) await this.pause(input.pauseAfterPrepareMs);
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-prepare') throw new Error('Injected failure before COMMIT_READY');
    active.phase = 'commit-ready';
    await this.storage.put('active', active);
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-commit-ready') throw new Error('Injected failure after irrevocable COMMIT_READY');
    const planned = await mediator(this.env, { op: 'plan', id, owner, participants: active.votes });
    if (this.env.ALLOW_FAULTS === 'true' && input.pauseAfterPlanMs) await this.pause(input.pauseAfterPlanMs);
    if (this.env.ALLOW_FAULTS === 'true' && ['after-plan', 'after-decision'].includes(input.fault)) throw new Error('Injected failure after durable plan');
    if (this.env.ALLOW_FAULTS === 'true' && input.fault === 'after-first-commit' && active.targets.length) {
      const target = active.targets[0];
      await shard(this.env, target.name, { op: 'apply', id, step: planned.step, vote: active.votes[0].vote, shard: target.name });
      throw new Error('Injected failure after first MVCC participant apply');
    }
    await this.recover();
    return json((await this.storage.get(`request:${key}`)).response);
  }
  pause(ms) { return new Promise(resolve => setTimeout(resolve, Math.min(1000, Math.max(0, Number(ms) || 0)))); }

  async build(statements, names, snapshot) {
    const catalog = {}, original = {}, targets = new Map();
    const target = (name, expectedEpoch) => {
      if (!targets.has(name)) targets.set(name, { name, reads: [], changes: [], ...(expectedEpoch === undefined ? {} : { expectedEpoch }) });
      return targets.get(name);
    };
    const addRead = (name, range, epoch) => {
      if (range.empty) return;
      const t = target(name, epoch);
      if (!t.reads.some(r => JSON.stringify(r) === JSON.stringify(range))) t.reads.push(range);
    };
    for (const table of names) {
      const found = await shard(this.env, catalogName(table), { op: 'getMany', keys: ['schema'], snapshot });
      if (found.rows[0]) catalog[table] = found.rows[0];
      addRead(catalogName(table), point('schema'));
    }
    const select = statements.length === 1 && statements[0].kind === 'select' ? statements[0] : null;
    let plan = { type: 'scan', tabletsVisited: 0, rowsRead: 0, versionsRead: 0 };
    if (select && catalog[select.table]) {
      const schema = catalog[select.table];
      execute(statements, copy(catalog), { [select.table]: [] });
      const bounded = column => select.filter.some(f => f.column === column && ['=', '<', '<=', '>', '>='].includes(f.op));
      const index = !bounded(schema.primaryKey) && schema.indexes?.find(i => bounded(i.column));
      if (index) {
        const range = rangeForFilters(select.filter, index.column, { compositePrefix: true });
        const selected = selectTablets(index, range);
        const parts = await Promise.all(selected.map(t => shard(this.env, indexName(select.table, index.name, t.id), { op: 'scan', range, snapshot, epoch: index.epoch })));
        const entries = parts.flatMap(p => p.rows), byTablet = new Map();
        for (const entry of entries) { const id = routeTablet(schema, entry.pk); if (!byTablet.has(id)) byTablet.set(id, []); byTablet.get(id).push(entry.pk); }
        const bases = await Promise.all([...byTablet].map(([id, keys]) => shard(this.env, baseName(select.table, id), { op: 'getMany', keys, snapshot, epoch: schema.epoch })));
        original[select.table] = bases.flatMap(p => p.rows);
        plan = { type: 'index-range', index: index.name, indexTabletsVisited: selected.length, indexTabletsTotal: index.tablets.length, tabletsVisited: selected.length, tabletsTotal: index.tablets.length, baseTabletsVisited: byTablet.size, rowsRead: entries.length, rowsFetched: original[select.table].length, versionsRead: [...parts, ...bases].reduce((n,p) => n + p.versionsRead, 0) };
      } else {
        const range = rangeForFilters(select.filter, schema.primaryKey), selected = selectTablets(schema, range);
        const parts = await Promise.all(selected.map(t => shard(this.env, baseName(select.table, t.id), { op: 'scan', range, snapshot, epoch: schema.epoch })));
        original[select.table] = parts.flatMap(p => p.rows);
        plan = { type: bounded(schema.primaryKey) ? 'primary-range' : 'scan', tabletsVisited: selected.length, tabletsTotal: schema.tablets.length, rowsRead: original[select.table].length, rowsFetched: original[select.table].length, versionsRead: parts.reduce((n,p)=>n+p.versionsRead,0) };
      }
    } else {
      for (const [table, schema] of Object.entries(catalog)) {
        const parts = await Promise.all(schema.tablets.map(t => shard(this.env, baseName(table, t.id), { op: 'scan', range: allRange(), snapshot, epoch: schema.epoch })));
        original[table] = parts.flatMap(p => p.rows); plan.tabletsVisited += parts.length; plan.rowsRead += original[table].length;
      }
    }
    const currentCatalog = copy(catalog), tables = copy(original), results = [];
    const baseRead = (table, range) => {
      const schema = catalog[table]; if (!schema) return;
      for (const t of selectTablets(schema, range)) addRead(baseName(table, t.id), range, schema.epoch);
    };
    const indexRead = (table, index, range) => {
      const existing = catalog[table]?.indexes?.find(i => i.name === index.name); if (!existing) return;
      for (const t of selectTablets(existing, range)) addRead(indexName(table, index.name, t.id), range, existing.epoch);
    };
    for (const statement of statements) {
      const table = statement.table, schema = currentCatalog[table], before = copy(tables[table] || []);
      if (schema && ['select', 'update', 'delete'].includes(statement.kind)) {
        const filter = statement.filter || [], pkBound = filter.some(f => f.column === schema.primaryKey && ['=', '<', '<=', '>', '>='].includes(f.op));
        const index = !pkBound && schema.indexes?.find(i => filter.some(f => f.column === i.column && ['=', '<', '<=', '>', '>='].includes(f.op)));
        if (index) {
          const range = rangeForFilters(filter, index.column, { compositePrefix: true }); indexRead(table, index, range);
          for (const row of before) if (inRange([row[index.column]], range)) baseRead(table, point(row[schema.primaryKey]));
        } else baseRead(table, rangeForFilters(filter, schema.primaryKey));
      }
      if (schema && ['insert', 'upsert'].includes(statement.kind)) {
        const pkColumn = statement.columns.indexOf(schema.primaryKey);
        if (pkColumn >= 0) for (const values of statement.values) baseRead(table, point(values[pkColumn]));
      }
      if (statement.kind === 'createIndex') baseRead(table, allRange());
      const part = execute([statement], currentCatalog, tables); results.push(...part.results);
      const afterSchema = currentCatalog[table];
      if (afterSchema && statement.kind !== 'select') {
        const changes = diffRows(before, tables[table], afterSchema);
        for (const index of afterSchema.indexes || []) if (index.unique) for (const change of changes) if (change.row && change.row[index.column] !== null) indexRead(table, index, rangeForFilters([{ column: index.column, op: '=', value: change.row[index.column] }], index.column, { compositePrefix: true }));
      }
    }
    if (Object.values(tables).reduce((n,rows)=>n+rows.length,0) > 2000) throw new Error('At most 2000 touched rows');
    const writes = new Set(statements.filter(s => s.kind !== 'select').map(s => s.table)), events = [];
    let baseChanges = 0, indexChanges = 0;
    const sizing = { splitBytes: Number(this.env.TABLET_SPLIT_BYTES || 4096), mergeBytes: Number(this.env.TABLET_MERGE_BYTES || 1024) };
    const materialize = (old, next, partitions, changes, nameOf) => {
      const topology = !old || JSON.stringify(old.tablets) !== JSON.stringify(next.tablets);
      for (const part of partitions) {
        const existed = old?.tablets?.some(t => t.id === part.id);
        const delta = existed ? changes.filter(c => routeTablet(next, c.key) === part.id) : part.rows.map(row => ({ key: keyOf(next, row), row }));
        if (delta.length || topology) {
          const t = target(nameOf(part.id), existed ? old.epoch : 0); t.changes = delta;
          if (topology) t.meta = { epoch: next.epoch, retired: false };
        }
      }
      if (topology) for (const oldPart of old?.tablets || []) {
        addRead(nameOf(oldPart.id), allRange(), old.epoch);
        if (!next.tablets.some(t => t.id === oldPart.id)) target(nameOf(oldPart.id), old.epoch).meta = { epoch: next.epoch, retired: true };
      }
    };
    for (const table of writes) {
      const old = catalog[table], current = currentCatalog[table];
      const changed = diffRows(original[table] || [], tables[table], current); baseChanges += changed.length;
      const planned = planTablets(current, tables[table], sizing), next = planned.schema;
      events.push(...planned.events.map(e=>({table,...e})));
      materialize(old, next, planned.partitions, changed, id => baseName(table,id));
      const indexes = [];
      for (const index of next.indexes || []) {
        const existing = old?.indexes?.find(i=>i.name===index.name);
        const idxSchema = {...index,keyColumns:index.column===next.primaryKey?['value']:['value','pk']};
        const entries = indexRows(tables[table],next,index), previous = existing ? indexRows(original[table],old,existing) : [];
        const deltas = diffRows(previous,entries,idxSchema); indexChanges += deltas.length;
        const indexed = planTablets(idxSchema,entries,sizing); indexes.push(indexed.schema);
        events.push(...indexed.events.map(e=>({table,index:index.name,...e})));
        materialize(existing,indexed.schema,indexed.partitions,deltas,id=>indexName(table,index.name,id));
      }
      next.indexes=indexes; currentCatalog[table]=next;
      if (JSON.stringify(old)!==JSON.stringify(next)) {
        target(catalogName(table)).changes=[{key:'schema',row:next}];
        // Topology/DDL is the rare schema-exclusive operation; ordinary row
        // changes carry only shared schema reads and narrow key/range predicates.
        baseRead(table,allRange());
      }
    }
    const list=[...targets.values()].sort((a,b)=>a.name.localeCompare(b.name));
    if(list.length>64)throw new Error('At most 64 participants');
    for (const t of list) {
      if (t.name.length > 512) throw new Error('Participant identifier exceeds 512 characters');
      const pending = { ...t, id: 'x'.repeat(36), owner: 'c0', snapshot, vote: 'x'.repeat(64), changes: t.changes.map(c => ({ ...c, encoded: encodeKey(c.key) })) };
      if (size(pending) > 105000) throw new Error('Prepared participant envelope exceeds 105 KB');
    }
    return {targets:list,response:{results,plan,changes:{baseRows:baseChanges,indexRows:indexChanges},participants:list.map(t=>({name:t.name})),routing:Object.entries(currentCatalog).map(([table,s])=>({table,epoch:s.epoch,tablets:s.tablets,indexes:(s.indexes||[]).map(i=>({name:i.name,keyColumns:i.keyColumns,epoch:i.epoch,tablets:i.tablets}))})),events}};
  }
}

async function body(request) {
  if (Number(request.headers.get('content-length')) > 65536) throw new Error('Body too large');
  const reader=request.body?.getReader();if(!reader)throw new Error('JSON body required');
  let size=0;const chunks=[];
  while(true){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>65536){await reader.cancel();throw new Error('Body too large');}chunks.push(value);}
  const data=new Uint8Array(size);let offset=0;for(const chunk of chunks){data.set(chunk,offset);offset+=chunk.length;}
  return JSON.parse(new TextDecoder().decode(data));
}
async function recoverOwners(env,owners){await Promise.all([...new Set(owners)].map(owner=>actorCall(env.COORDINATOR,owner,{op:'recover'})));}
export default {
  async fetch(request,env){
    const url=new URL(request.url);
    if(url.pathname==='/health'&&request.method==='GET')return json({ok:true,engine:'murmur-mvcc-v4',coordinators:4});
    if(request.method!=='POST'||!['/query','/snapshot','/recover','/debug/tablet'].includes(url.pathname))return json({error:'POST /query, /snapshot or /recover'},404);
    if(env.API_TOKEN&&request.headers.get('authorization')!==`Bearer ${env.API_TOKEN}`)return json({error:'Unauthorized'},401);
    if(!env.API_TOKEN&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname))return json({error:'Set API_TOKEN for public access'},403);
    let input;try{input=await body(request);}catch(e){return json({error:e.message},e.message==='Body too large'?413:400);}
    if(!input||typeof input!=='object'||Array.isArray(input))return json({error:'JSON object required'},400);
    try{
      if(url.pathname==='/snapshot')return json(await mediator(env,{op:'snapshot'}));
      if(url.pathname==='/recover'){await recoverOwners(env,['c0','c1','c2','c3']);return json(await mediator(env,{op:'snapshot'}));}
      if(url.pathname==='/debug/tablet'){
        if(env.ALLOW_FAULTS!=='true')return json({error:'Not found'},404);
        const status=await mediator(env,{op:'snapshot'});
        const name=input.name||baseName(input.table,input.tablet);
        const stub=env.SHARDS.get(env.SHARDS.idFromName(name));
        return stub.fetch('https://actor',{method:'POST',body:JSON.stringify({op:'scan',snapshot:input.snapshot??status.snapshot,epoch:input.epoch,range:allRange()})});
      }
      if(typeof input.sql!=='string'||input.sql.length>32768)return json({error:'SQL string required, max32768 characters'},400);
      if(typeof input.idempotencyKey!=='string'||!/^[a-zA-Z0-9_-]{1,100}$/.test(input.idempotencyKey))return json({error:'Valid idempotencyKey required'},400);
      delete input.op;
      const owner=`c${shardFor(input.idempotencyKey)}`,coordinator=env.COORDINATOR.get(env.COORDINATOR.idFromName(owner));
      for(let attempt=0;attempt<40;attempt++){
        const response=await coordinator.fetch('https://coordinator',{method:'POST',body:JSON.stringify({...input,coordinatorId:owner})});
        const result=await response.clone().json();
        if(response.status===409&&result.retryable){
          const status=await mediator(env,{op:'snapshot'});
          const owners=[...(result.owners||[]),...(status.pending||[]).map(p=>p.owner)].filter(Boolean);
          if(owners.length)await recoverOwners(env,owners);
          await new Promise(resolve=>setTimeout(resolve,5+attempt*2));continue;
        }
        if(response.ok&&result.step){
          for(let wait=0;wait<40;wait++){
            const status=await mediator(env,{op:'snapshot'});
            if((status.snapshot??status.frontier)>=result.step)return response;
            await recoverOwners(env,(status.pending||[]).filter(p=>p.step<=result.step).map(p=>p.owner));
            await new Promise(resolve=>setTimeout(resolve,5));
          }
          return json({error:'Commit durable but visibility frontier awaits recovery; retry same key',code:'VISIBILITY_PENDING',retryable:true},503);
        }
        return response;
      }
      return json({error:'Serialization conflict; retry same key',code:'CONFLICT',retryable:true},503);
    }catch(error){return json({error:error.message,code:error.code||'RECOVERY_REQUIRED',retryable:true},503);}
  }
};
