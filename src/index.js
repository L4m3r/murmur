import { decodeRpcResponse } from './rpc-response.js';
import { CoordinatorService } from './coordinator-service.js';
import { coordinatorName, COORDINATOR_POOL_SIZE } from './coordinator-routing.js';
import { physicalTableName, indexEntry, indexKeyColumns, insertKeys } from './schema.js';
import { tryBuildSelective } from './planner.js';
import { readRecord, writeRecord, deleteRecord } from './records.js';
import { parse, execute } from './sql.js';
import { keyHash, routeTablet, planTablets, selectTablets, tabletEpoch } from './tablets.js';
import { encodeKey, rangeForFilters, keyOfRow, primaryRangeForFilters } from './keys.js';
export { DataShard } from './datashard.js';
export { PlanAllocator } from './mediator.js';
export { PlanBucket } from './plan-bucket.js';
import { manifestHash } from './protocol.js';
export { SchemaManager } from './schema-manager.js';
export { QueryCursor } from './query-cursor.js';

const json = (body, status = 200) => Response.json(body, { status });
const copy = structuredClone;
const size = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;
export const shardFor = (key, count = 4) => keyHash(key) % count;
async function coordinatorFor(key) {
  const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(key)));
  return 'tx/' + [...bytes].map(b=>b.toString(16).padStart(2,'0')).join('');
}
const catalogName = table => `catalog/${table}`;
const baseName = (table, tablet, schema) => `base/${physicalTableName(table,schema)}/${tablet}`;
const indexName = (table, index, tablet, schema) => `index/${physicalTableName(table,schema)}/${index}/${tablet}`;
const keyOf = keyOfRow;
const allRange = () => ({ lower: null, upper: null, lowerInclusive: true, upperInclusive: false, empty: false });
const point = key => ({ lower: encodeKey(key), upper: encodeKey(key), lowerInclusive: true, upperInclusive: true, empty: false });
const inRange = (key, range) => {
  if (range.empty) return false;
  const encoded = encodeKey(key);
  return (range.lower === null || encoded > range.lower || (encoded === range.lower && range.lowerInclusive)) && (range.upper === null || encoded < range.upper || (encoded === range.upper && range.upperInclusive));
};
const indexRows = (rows, schema, index) => rows.map(row => indexEntry(schema,index,row));
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
  const result = await decodeRpcResponse(response, `${name}:${input.op||'query'}`);
  if (!response.ok) { const error = new Error(result.error || 'Actor request failed'); error.code = result.code; error.status = response.status; error.owners = result.owners; error.dependencies = result.dependencies; error.rpc=result.rpc; error.retryAfterMs=result.retryAfterMs; throw error; }
  return result;
}
const mediator = (env, input) => actorCall(env.ALLOCATOR, 'global', input);
const shard = async (env, name, input) => {
  const result = await actorCall(env.SHARDS, name, input);
  if (input.op !== 'scan' || !result.nextCursor) return result;
  // Pages bound each storage operation; the transaction still has explicit
  // memory/result backpressure rather than an unbounded actor response.
  const rows = [...result.rows]; let cursor = result.nextCursor, readBytes = size(result.rows);
  while (cursor) {
    const page = await actorCall(env.SHARDS, name, { ...input, cursor });
    readBytes += size(page.rows);
    if (readBytes > 8 * 1024 * 1024) throw new Error('Scan exceeds 8 MiB work budget; narrow the predicate');
    rows.push(...page.rows);
    if (rows.length > 20000) throw new Error('Scan result exceeds 20000 rows; narrow the predicate');
    result.versionsRead += page.versionsRead || 0;
    result.keysRead = (result.keysRead || 0) + (page.keysRead || 0);
    cursor = page.nextCursor;
  }
  return { ...result, rows, rowsRead: rows.length, nextCursor: null, done: true };
};

function transactionFingerprint(input) {
  if(input.op==='applyTargets')return JSON.stringify({snapshot:input.snapshot,targets:input.targets,response:input.response||{}});
  const params=input.params===undefined?{}:input.params;
  const canonicalParams=params&&typeof params==='object'&&!Array.isArray(params)?Object.fromEntries(Object.entries(params).sort(([a],[b])=>a<b?-1:a>b?1:0)):params;
  return JSON.stringify({sql:input.sql,params:canonicalParams,snapshot:input.snapshot??null,epoch:input.requestEpoch??0});
}

class TransactionCoordinator {
  constructor(state, env) { this.storage = state.storage; this.env = env; this.tail = Promise.resolve(); }
  run(fn) { const task = this.tail.then(fn); this.tail = task.catch(() => {}); return task; }
  fetch(request) {
    return this.run(async () => this.handle(await request.json())).catch(error => json({ error: error.message, code: error.code || 'RECOVERY_REQUIRED', retryable: true, rpc:error.rpc,retryAfterMs:error.retryAfterMs, ...(error.code==='PLAN_PENDING'?{outcome:'PENDING',transactionId:error.transactionId,faultReached:false}:{}), ...(error.code==='FAULT_INJECTED'?{faultReached:true,faultState:error.faultState}: {}) }, 503));
  }
  alarm() { return this.run(async()=>{
    const active=await this.record('active');
    if(active?.faultAfterFirst&&active.faultTriggered&&!active.faultReleased){
      // A test milestone is not a client receipt. Stop this virtual alarm until
      // an explicit no-fault retry or /recover releases the intentional fence.
      if(this.storage.deleteAlarm)await this.storage.deleteAlarm();return;
    }
    return this.recover();
  }); }
  async record(key) { return readRecord(this.storage, key, { maxBytes: 2 * 1024 * 1024 }); }
  async save(key, value) { return this.storage.transaction(txn => writeRecord(txn, key, value, { maxBytes: 2 * 1024 * 1024 })); }
  async remove(key) { return this.storage.transaction(txn => deleteRecord(txn, key, { allowRaw: true })); }
  async arm() { if (this.storage.setAlarm) await this.storage.setAlarm(Date.now() + 2000); }
  async releaseOperation() {
    const operation = await this.record('operation');
    if (operation) {
      try { await mediator(this.env, { op: 'unpin', id: operation.id, epoch: operation.epoch || 0 }); }
      catch(error) { if(error.code !== 'REQUEST_EXPIRED') throw error; }
      await this.remove('operation');
    }
  }
  async firstApplyFault(active,bucket) {
    const record=await actorCall(this.env.MEDIATORS,bucket,{op:'get',id:active.id});
    active.faultTriggered=true;active.faultBucket=bucket;
    active.faultProof={phase:'planning',transactionId:active.id,step:record.association.step,appliedCount:record.applied.length,participantCount:record.association.participants.length,decision:record.decision?.outcome??null};
    await this.save('active',active);await this.arm();
    return Object.assign(new Error('Injected failure after first MVCC participant apply'),{code:'FAULT_INJECTED',faultState:active.faultProof});
  }
  async finishPlanned(active, {release = true,releaseFault = false} = {}) {
    if(active.faultAfterFirst&&!active.faultReleased){
      if(releaseFault){active.faultReleased=true;await this.save('active',active);}
      else if(active.faultTriggered)throw await this.firstApplyFault(active,active.faultBucket);
    }
    const association = await mediator(this.env, {op:'assign', id:active.id, owner:active.owner, epoch:active.epoch, snapshot:active.snapshot, participants:active.votes, manifestHash:active.manifestHash});
    const call = input => actorCall(this.env.MEDIATORS,association.bucket,input);
    await call({op:'register',association});
    // One bounded peer-drive slice per foreground request or recovery/alarm.
    // A pending response is not a commit receipt; durable active state and alarms
    // retain the same immutable assignment for the next same-key retry.
    const wide=association.participants.length>8;
    await call({op:'drive',id:active.id,...(wide?{participantLimit:1,readSetLimit:1,finalizeLimit:0,notifyLimit:1,deferFinalize:true}:{})});
    let record=await call({op:'get',id:active.id});
    if(active.faultAfterFirst&&!active.faultTriggered&&!active.faultReleased&&record.applied.length>0){
      // Independent helpers may have finalized before this owner resumed. The
      // durable fault milestone is still consumed exactly once, never skipped.
      throw await this.firstApplyFault(active,association.bucket);
    }
    if(record.decision && release) {
      const faultPending=active.faultAfterFirst&&!active.faultTriggered&&!active.faultReleased;
      let targets=association.participants.filter(p=>record.delivered.includes(p.name)&&!record.applied.includes(p.name)).slice(0,wide?1:8);
      if(faultPending) {
        if(!active.faultFirstParticipant&&targets.length){active.faultFirstParticipant=targets[0].name;await this.save('active',active);}
        targets=active.faultFirstParticipant?association.participants.filter(p=>p.name===active.faultFirstParticipant):[];
      }
      for(const participant of targets) {
        const result=await shard(this.env,participant.name,{op:'finalize',association,decision:record.decision,release:true});
        if(result.ack)await call({op:'applied',id:active.id,ack:result.ack});
        if(faultPending&&result.ack)throw await this.firstApplyFault(active,association.bucket);
      }
      record=await call({op:'get',id:active.id});
    }
    if(record.applied.length===association.participants.length) {
      await mediator(this.env,{op:'completion',id:active.id});
      if(record.decision.outcome==='COMMIT')await this.save(`request:${active.key}`,{fingerprint:active.fingerprint,response:{...active.response,step:association.step,version:association.step}});
      await this.remove('active');await this.releaseOperation();
      if(this.storage.deleteAlarm)await this.storage.deleteAlarm();
      return record.decision.outcome;
    }
    await this.arm();
    throw Object.assign(new Error('Peer transaction is still resolving; retry the same idempotency key and payload'),{code:'PLAN_PENDING',transactionId:active.id});
  }
  async recover({releaseFault=false}={}) {
    const active=await this.record('active');
    if(!active){await this.releaseOperation();return;}
    const authority=await mediator(this.env,{op:'epoch'});
    if((active.epoch??0)<authority.rejectBeforeEpoch){
      await this.remove('active');await this.releaseOperation();
      if(this.storage.deleteAlarm)await this.storage.deleteAlarm();
      return 'EXPIRED';
    }
    if(active.phase==='preparing') {
      const assigned=await mediator(this.env,{op:'abortIfUnassigned',id:active.id,epoch:active.epoch});
      if(assigned.state==='assigned')throw new Error('Preparing transaction unexpectedly assigned');
      for(const target of active.targets)await shard(this.env,target.name,{op:'abort',id:active.id});
      await this.remove('active');await this.releaseOperation();
      if(this.storage.deleteAlarm)await this.storage.deleteAlarm();
      return 'ABORT';
    }
    return this.finishPlanned(active,{releaseFault});
  }
  async applyTargets(input) {
    const { idempotencyKey:key, coordinatorId:owner, snapshot, targets } = input;
    const epoch=input.requestEpoch??0;
    await mediator(this.env,{op:'epoch',requestEpoch:epoch});
    if(typeof key!=='string'||typeof owner!=='string'||!Array.isArray(targets)||targets.length>64||!Number.isSafeInteger(snapshot))return json({error:'Invalid internal transaction'},400);
    if(targets.some(t=>typeof t.name!=='string'||t.name.length>512||!Array.isArray(t.reads)||!Array.isArray(t.changes))||new Set(targets.map(t=>t.name)).size!==targets.length)return json({error:'Invalid participant manifest'},400);
    const fingerprint=transactionFingerprint(input);
    const previous=await this.record(`request:${key}`);
    if(previous)return previous.fingerprint===fingerprint?json(previous.response):json({error:'Internal transaction identity mismatch'},409);
    const id=`e${epoch}_${crypto.randomUUID()}`;
    await this.save('operation',{id,owner,epoch});await this.arm();
    try { await mediator(this.env,{op:'pin',id,owner,epoch,snapshot}); }
    catch(error){await this.releaseOperation();return json({error:error.message,code:error.code,owners:error.owners||[],retryable:(error.status||503)>=500,rpc:error.rpc,retryAfterMs:error.retryAfterMs},error.status||503);}
    const built={targets,response:input.response||{results:[]}};
    const response={transactionId:id,coordinatorId:owner,snapshot,version:snapshot,...built.response};
    return this.commit(input,{id,key,owner,epoch,snapshot,fingerprint,built,response});
  }
  async handle(input) {
    if (input.op === 'collect') {
      const authority = await mediator(this.env, {op:'epoch'});
      if (!Number.isSafeInteger(input.epoch) || input.epoch >= authority.rejectBeforeEpoch) return json({error:'Epoch is not retired'},409);
      const active=await this.record('active'),operation=await this.record('operation');
      if([active,operation].some(record=>record && (record.epoch??0)>=authority.rejectBeforeEpoch))return json({error:'Actor still owns admitted work'},409);
      // Epoch retirement proves no unresolved admitted work remains. Do not
      // replay a terminal active record through protocol state already collected.
      await this.remove('active');await this.remove('operation');
      const receipts=await this.storage.list({prefix:'request:',limit:16});
      for(const key of receipts.keys())await this.remove(key);
      if(this.storage.deleteAlarm)await this.storage.deleteAlarm();
      return json({collected:true,receipts:receipts.size});
    }
    if(input.op!=='recover') {
      const active=await this.record('active');
      if(active&&(active.key!==input.idempotencyKey||active.epoch!==(input.requestEpoch??0)||active.fingerprint!==transactionFingerprint(input)))return json({error:'Idempotency key reused with a different pending transaction payload',code:'IDEMPOTENCY_CONFLICT',retryable:false},409);
    }
    await this.recover({releaseFault:input.op==='recover'||input.fault!=='after-first-commit'});
    if (input.op === 'recover') return json({ recovered: true });
    if (input.op === 'applyTargets') return this.applyTargets(input);
    const { sql, idempotencyKey: key, coordinatorId: owner } = input;
    const epoch = input.requestEpoch ?? 0;
    const params = input.params === undefined ? {} : input.params;
    const fingerprint = transactionFingerprint(input);
    const previous = await this.record(`request:${key}`);
    if (previous) return previous.fingerprint === fingerprint ? json(previous.response) : json({ error: 'Idempotency key reused for different query/snapshot' }, 409);
    let statements;
    try { statements = parse(sql, params); } catch (error) { return json({ error: error.message }, 400); }
    const readOnly = statements.every(s => s.kind === 'select');
    const status = await mediator(this.env, { op: 'snapshot' });
    const frontier = status.snapshot ?? status.frontier;
    const snapshot = input.snapshot ?? frontier;
    if (!Number.isSafeInteger(snapshot) || snapshot < 0 || snapshot > frontier || (input.snapshot !== undefined && !readOnly)) return json({ error: 'Explicit snapshot must be a visible nonnegative step for a read-only request' }, 400);
    const names = [...new Set(statements.map(s => s.table))].sort();
    if (names.length > 8) return json({ error: 'At most 8 tables per transaction' }, 400);
    const id = `e${epoch}_${crypto.randomUUID()}`;
    await this.save('operation', { id, owner, epoch });
    await this.arm();
    try {
      await mediator(this.env, { op: 'pin', id, owner, epoch, snapshot, leaseId: input.snapshotLease });
    } catch (error) {
      await this.releaseOperation();
      const expired = error.code === 'SNAPSHOT_EXPIRED' && input.snapshot === undefined;
      return json({ error: error.message, code: error.code, retryable:expired||(error.status||0)>=500,rpc:error.rpc,retryAfterMs:error.retryAfterMs }, expired ? 409 : (error.status || 410));
    }
    let built;
    try { built = await this.build(statements, names, snapshot); }
    catch (error) {
      await this.releaseOperation();
      const dependency = error.code === 'VISIBILITY_PENDING',retryable=dependency||(error.status||0)>=500;
      return json({ error: error.message, code: error.code, owners: error.owners || [], dependencies: error.dependencies || [], retryable,rpc:error.rpc,retryAfterMs:error.retryAfterMs }, dependency ? 409 : (error.status || 400));
    }
    const response = { transactionId: id, coordinatorId: owner, snapshot, version: snapshot, ...built.response };
    if (readOnly) {
      if (size({ fingerprint, response }) > 2 * 1024 * 1024) { await this.releaseOperation(); return json({ error: 'Result exceeds 2 MiB response budget' }, 400); }
      await this.save(`request:${key}`, { fingerprint, response });
      await this.releaseOperation();
      return json(response);
    }
    return this.commit(input, { id, key, owner, epoch, snapshot, fingerprint, built, response });
  }
  async commit(input, context) {
    const {id,key,owner,epoch,snapshot,fingerprint,built,response}=context;
    if(this.env.ALLOW_FAULTS==='true'&&input.pauseAfterReadMs)await this.pause(input.pauseAfterReadMs);
    const active={id,key,owner,epoch,snapshot,fingerprint,phase:'preparing',targets:built.targets,votes:[],response,...(this.env.ALLOW_FAULTS==='true'&&input.fault==='after-first-commit'?{faultAfterFirst:true,faultTriggered:false}:{})};
    if(size(active)>1900000){await this.releaseOperation();return json({error:'Transaction exceeds durable payload budget'},400);}
    if(!active.targets.length){await this.save(`request:${key}`,{fingerprint,response});await this.releaseOperation();return json(response);}
    await this.save('active',active);await this.arm();
    const participants=active.targets.map(t=>t.name).sort();
    try {
      for(const target of active.targets){
        const proposal=await shard(this.env,target.name,{op:'propose',id,owner,epoch,snapshot,...target,participants,...(this.env.ALLOW_FAULTS==='true'&&['after-first-commit','after-plan-held'].includes(input.fault)?{holdFinalization:true}:{})});
        active.votes.push({name:target.name,bodyHash:proposal.bodyHash,publicKey:proposal.publicKey});
      }
    }catch(error){await this.recover();return json({error:error.message,code:error.code||'CONFLICT',owners:error.owners||[],retryable:true,rpc:error.rpc,retryAfterMs:error.retryAfterMs},(error.status||0)>=500?error.status:409);}
    if(this.env.ALLOW_FAULTS==='true'&&input.pauseAfterPrepareMs)await this.pause(input.pauseAfterPrepareMs);
    if(this.env.ALLOW_FAULTS==='true'&&input.fault==='after-prepare'){const allocation=await mediator(this.env,{op:'lookup',id,epoch});throw Object.assign(new Error('Injected failure before durable planning obligation'),{code:'FAULT_INJECTED',faultState:{transactionId:id,phase:active.phase,allocationState:allocation.state}});}
    active.manifestHash=await manifestHash(active.votes);active.phase='planning';await this.save('active',active);
    if(this.env.ALLOW_FAULTS==='true'&&input.fault==='after-commit-ready'){const allocation=await mediator(this.env,{op:'lookup',id,epoch});throw Object.assign(new Error('Injected failure after durable planning obligation'),{code:'FAULT_INJECTED',faultState:{transactionId:id,phase:active.phase,allocationState:allocation.state}});}
    if(this.env.ALLOW_FAULTS==='true'&&(input.pauseAfterPlanMs||['after-plan','after-plan-held','after-decision'].includes(input.fault))){
      await mediator(this.env,{op:'assign',id,owner,epoch,snapshot,participants:active.votes,manifestHash:active.manifestHash});
      if(input.pauseAfterPlanMs)await this.pause(input.pauseAfterPlanMs);
      if(['after-plan','after-plan-held','after-decision'].includes(input.fault))throw new Error('Injected failure after durable plan');
    }
    const outcome=await this.finishPlanned(active);
    if(outcome==='ABORT')return json({error:'Serializable validation aborted transaction',code:'CONFLICT',retryable:true},409);
    return json((await this.record(`request:${key}`)).response);
  }
  pause(ms) { return new Promise(resolve => setTimeout(resolve, Math.min(1000, Math.max(0, Number(ms) || 0)))); }

  async build(statements, names, snapshot) {
    const selective = await tryBuildSelective(this.env, statements, names, snapshot);
    if (selective) return selective;
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
    if (select && catalog[select.table] && catalog[select.table].state!=='dropped') {
      const schema = catalog[select.table];
      execute(statements, copy(catalog), { [select.table]: [] });
      const bounded = column => select.filter.some(f => f.column === column && ['=', '<', '<=', '>', '>='].includes(f.op));
      const index = !bounded(schema.primaryKey) && schema.indexes?.find(i => !['building','validating'].includes(i.state) && bounded(i.column));
      if (index) {
        const range = rangeForFilters(select.filter, index.column, { compositePrefix: true });
        const selected = selectTablets(index, range);
        const parts = await Promise.all(selected.map(t => shard(this.env, indexName(select.table, index.storageName || index.name, t.id,schema), { op: 'scan', range, snapshot, epoch: tabletEpoch(index,t.id) })));
        const entries = parts.flatMap(p => p.rows), byTablet = new Map();
        for (const entry of entries) { const id = routeTablet(schema, entry.pk); if (!byTablet.has(id)) byTablet.set(id, []); byTablet.get(id).push(entry.pk); }
        const bases = await Promise.all([...byTablet].map(([id, keys]) => shard(this.env, baseName(select.table, id,schema), { op: 'getMany', keys, snapshot, epoch: tabletEpoch(schema,id) })));
        original[select.table] = bases.flatMap(p => p.rows);
        plan = { type: 'index-range', index: index.name, indexTabletsVisited: selected.length, indexTabletsTotal: index.tablets.length, tabletsVisited: selected.length, tabletsTotal: index.tablets.length, baseTabletsVisited: byTablet.size, rowsRead: entries.length, rowsFetched: original[select.table].length, versionsRead: [...parts, ...bases].reduce((n,p) => n + p.versionsRead, 0) };
      } else {
        const range = primaryRangeForFilters(schema,select.filter), selected = selectTablets(schema, range);
        const parts = await Promise.all(selected.map(t => shard(this.env, baseName(select.table, t.id,schema), { op: 'scan', range, snapshot, epoch: tabletEpoch(schema,t.id) })));
        original[select.table] = parts.flatMap(p => p.rows);
        plan = { type: bounded(schema.primaryKey) ? 'primary-range' : 'scan', tabletsVisited: selected.length, tabletsTotal: schema.tablets.length, rowsRead: original[select.table].length, rowsFetched: original[select.table].length, versionsRead: parts.reduce((n,p)=>n+p.versionsRead,0) };
      }
    } else {
      for (const [table, schema] of Object.entries(catalog)) {
        if(schema.state==='dropped'){original[table]=[];continue;}
        const parts = await Promise.all(schema.tablets.map(t => shard(this.env, baseName(table, t.id,schema), { op: 'scan', range: allRange(), snapshot, epoch: tabletEpoch(schema,t.id) })));
        original[table] = parts.flatMap(p => p.rows); plan.tabletsVisited += parts.length; plan.rowsRead += original[table].length;
      }
    }
    const currentCatalog = copy(catalog), tables = copy(original), results = [];
    const baseRead = (table, range) => {
      const schema = catalog[table]; if (!schema || schema.state==='dropped') return;
      for (const t of selectTablets(schema, range)) addRead(baseName(table, t.id,schema), range, tabletEpoch(schema,t.id));
    };
    const indexRead = (table, index, range) => {
      const existing = catalog[table]?.indexes?.find(i => i.name === index.name); if (!existing) return;
      for (const t of selectTablets(existing, range)) addRead(indexName(table, existing.storageName || existing.name, t.id,catalog[table]), range, tabletEpoch(existing,t.id));
    };
    for (const statement of statements) {
      const table = statement.table, schema = currentCatalog[table], before = copy(tables[table] || []);
      if (schema && schema.state!=='dropped' && ['select', 'update', 'delete'].includes(statement.kind)) {
        const filter = statement.filter || [], pkBound = filter.some(f => f.column === schema.primaryKey && ['=', '<', '<=', '>', '>='].includes(f.op));
        const index = !pkBound && schema.indexes?.find(i => !['building','validating'].includes(i.state) && filter.some(f => f.column === i.column && ['=', '<', '<=', '>', '>='].includes(f.op)));
        if (index) {
          const range = rangeForFilters(filter, index.column, { compositePrefix: true }); indexRead(table, index, range);
          for (const row of before) if (inRange([row[index.column]], range)) baseRead(table, point(keyOfRow(schema,row)));
        } else baseRead(table, primaryRangeForFilters(schema,filter));
      }
      if (schema && ['insert', 'upsert'].includes(statement.kind)) {
        for (const key of insertKeys(schema,statement)) baseRead(table,point(key));
      }
      if (statement.kind === 'createIndex') baseRead(table, allRange());
      const part = execute([statement], currentCatalog, tables); results.push(...part.results);
      const afterSchema = currentCatalog[table];
      if (afterSchema && afterSchema.state!=='dropped' && statement.kind !== 'select') {
        const changes = diffRows(before, tables[table], afterSchema);
        for (const index of afterSchema.indexes || []) if (index.unique) for (const change of changes) if (change.row && change.row[index.column] !== null) indexRead(table, index, rangeForFilters([{ column: index.column, op: '=', value: change.row[index.column] }], index.column, { compositePrefix: true }));
      }
    }
    if (Object.values(tables).reduce((n,rows)=>n+rows.length,0) > 20000) throw new Error('DDL materialization exceeds 20000 row work budget');
    const writes = new Set(statements.filter(s => s.kind !== 'select').map(s => s.table)), events = [];
    let baseChanges = 0, indexChanges = 0;
    const sizing = { splitBytes: Number(this.env.TABLET_SPLIT_BYTES || 4096), mergeBytes: Number(this.env.TABLET_MERGE_BYTES || 1024), maxTablets: Number(this.env.TABLET_MAX_COUNT || 256) };
    const materialize = (old, next, partitions, changes, nameOf) => {
      const topology = !old || JSON.stringify(old.tablets) !== JSON.stringify(next.tablets);
      for (const part of partitions) {
        const existed = old?.tablets?.some(t => t.id === part.id);
        const delta = existed ? changes.filter(c => routeTablet(next, c.key) === part.id) : part.rows.map(row => ({ key: keyOf(next, row), row }));
        if (delta.length || !existed) {
          const t = target(nameOf(part.id), existed ? tabletEpoch(old,part.id) : 0); t.changes = delta;
          if (!existed) t.meta = { epoch: part.epoch, retired: false };
        }
      }
      if (topology) for (const oldPart of old?.tablets || []) {
        if (!next.tablets.some(t => t.id === oldPart.id)) {
          addRead(nameOf(oldPart.id), allRange(), tabletEpoch(old,oldPart.id));
          target(nameOf(oldPart.id), tabletEpoch(old,oldPart.id)).meta = { epoch: next.epoch, retired: true };
        }
      }
    };
    for (const table of writes) {
      const previousSchema=catalog[table], current=currentCatalog[table];
      const sameGeneration=previousSchema && (previousSchema.generation||0)===(current.generation||0);
      const old=previousSchema?.state!=='dropped' && sameGeneration ? previousSchema : undefined;
      if(previousSchema && previousSchema.state!=='dropped' && (current.state==='dropped'||!sameGeneration)) {
        for(const part of previousSchema.tablets||[]){const name=baseName(table,part.id,previousSchema);addRead(name,allRange(),tabletEpoch(previousSchema,part.id));target(name,tabletEpoch(previousSchema,part.id)).meta={epoch:tabletEpoch(previousSchema,part.id)+1,retired:true};}
        for(const index of previousSchema.indexes||[])for(const part of index.tablets){const name=indexName(table,index.storageName||index.name,part.id,previousSchema);addRead(name,allRange(),tabletEpoch(index,part.id));target(name,tabletEpoch(index,part.id)).meta={epoch:tabletEpoch(index,part.id)+1,retired:true};}
      }
      if(current.state==='dropped'){target(catalogName(table)).changes=[{key:'schema',row:current}];events.push({table,type:'drop',generation:current.generation||0});continue;}
      const changed = diffRows(old ? original[table] || [] : [], tables[table], current); baseChanges += changed.length;
      const planned = planTablets(current, tables[table], {...sizing,maxTablets:current.tablets?sizing.maxTablets:Math.min(16,sizing.maxTablets)}), next = planned.schema;
      events.push(...planned.events.map(e=>({table,...e})));
      materialize(old, next, planned.partitions, changed, id => baseName(table,id,next));
      const indexes = [];
      for (const index of next.indexes || []) {
        const existing = old?.indexes?.find(i=>i.name===index.name && (i.storageName || i.name)===(index.storageName || index.name));
        const idxSchema = {...index,keyColumns:indexKeyColumns(next,index)};
        const entries = indexRows(tables[table],next,index), previous = existing ? indexRows(original[table],old,existing) : [];
        const deltas = diffRows(previous,entries,idxSchema); indexChanges += deltas.length;
        const indexed = planTablets(idxSchema,entries,{...sizing,...(idxSchema.sizing||{}),maxTablets:idxSchema.tablets?sizing.maxTablets:Math.min(16,sizing.maxTablets)}); indexes.push(indexed.schema);
        events.push(...indexed.events.map(e=>({table,index:index.name,...e})));
        materialize(existing,indexed.schema,indexed.partitions,deltas,id=>indexName(table,index.storageName || index.name,id,next));
      }
      for (const removed of old?.indexes || []) if (!indexes.some(i=>(i.storageName || i.name)===(removed.storageName || removed.name))) {
        for (const tablet of removed.tablets) {
          const name=indexName(table,removed.storageName || removed.name,tablet.id,old);
          addRead(name,allRange(),tabletEpoch(removed,tablet.id));
          target(name,tabletEpoch(removed,tablet.id)).meta={epoch:removed.epoch+1,retired:true};
        }
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
      if (size(pending) > 2 * 1024 * 1024) throw new Error('Prepared participant exceeds 2 MiB payload budget');
    }
    return {targets:list,response:{results,plan,changes:{baseRows:baseChanges,indexRows:indexChanges},participants:list.map(t=>({name:t.name})),routing:Object.entries(currentCatalog).map(([table,s])=>({table,epoch:s.epoch,tablets:s.tablets,indexes:(s.indexes||[]).map(i=>({name:i.name,keyColumns:i.keyColumns,epoch:i.epoch,tablets:i.tablets}))})),events}};
  }
}

export class Coordinator extends CoordinatorService {
  constructor(state,env){super(state,env,TransactionCoordinator);}
}

async function body(request) {
  if (Number(request.headers.get('content-length')) > 65536) throw new Error('Body too large');
  const reader=request.body?.getReader();if(!reader)throw new Error('JSON body required');
  let size=0;const chunks=[];
  while(true){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>65536){await reader.cancel();throw new Error('Body too large');}chunks.push(value);}
  const data=new Uint8Array(size);let offset=0;for(const chunk of chunks){data.set(chunk,offset);offset+=chunk.length;}
  return JSON.parse(new TextDecoder().decode(data));
}
function ownerActor(env,owner){return owner.startsWith('schema-job/')?{namespace:env.SCHEMA,name:owner.slice(11),recover:'resume'}:owner.startsWith('cursor-job/')?{namespace:env.CURSORS,name:owner.slice(11),recover:'recover'}:{namespace:env.COORDINATOR,name:coordinatorName(owner),scope:owner,recover:'recover'};}
async function recoverOwners(env,owners){await Promise.all([...new Set(owners)].map(owner=>{const actor=ownerActor(env,owner);return actorCall(actor.namespace,actor.name,{op:actor.recover,...(actor.scope?{scope:actor.scope}:{})});}));}
async function helpDependencies(env,result){
 for(const dependency of result.dependencies||[]){const assigned=await mediator(env,{op:'lookup',id:dependency.id});if(assigned.state==='assigned'){await actorCall(env.MEDIATORS,assigned.association.bucket,{op:'register',association:assigned.association});await actorCall(env.MEDIATORS,assigned.association.bucket,{op:'drive',id:dependency.id,release:true});}}
 if(result.owners?.length)await recoverOwners(env,result.owners.filter(Boolean));
}
export default {
  async fetch(request,env){
    const url=new URL(request.url);
    if(url.pathname==='/health'&&request.method==='GET')return json({ok:true,engine:'murmur-services-v7',coordinators:'independent-context-pool',coordinatorServices:COORDINATOR_POOL_SIZE});
    if(request.method!=='POST'||!['/query','/cursor','/schema','/snapshot','/recover','/maintenance','/debug/tablet'].includes(url.pathname))return json({error:'POST /query, /snapshot or /recover'},404);
    if(env.API_TOKEN&&request.headers.get('authorization')!==`Bearer ${env.API_TOKEN}`)return json({error:'Unauthorized'},401);
    if(!env.API_TOKEN&&!['localhost','127.0.0.1','[::1]'].includes(url.hostname))return json({error:'Set API_TOKEN for public access'},403);
    let input;try{input=await body(request);}catch(e){return json({error:e.message},e.message==='Body too large'?413:400);}
    if(!input||typeof input!=='object'||Array.isArray(input))return json({error:'JSON object required'},400);
    try{
      if(url.pathname==='/cursor'){
        const epoch=input.requestEpoch??0;await mediator(env,{op:'epoch',requestEpoch:epoch});
        let id=input.id,operation=input.action||'start';
        if(operation==='start'){
          if(typeof input.idempotencyKey!=='string'||!input.idempotencyKey.length||input.idempotencyKey.length>128||typeof input.sql!=='string'||input.sql.length>32768)return json({error:'Cursor start requires bounded SQL and idempotencyKey'},400);
          id=(await coordinatorFor(`${epoch}:cursor:${input.idempotencyKey}`)).replace('tx/','cursor_');
        }
        if(typeof id!=='string'||!/^cursor_[a-f0-9]{64}$/.test(id)||!['start','page','ack','renew','close','status'].includes(operation))return json({error:'Invalid cursor action or identity'},400);
        if(operation!=='start'&&['sql','params','snapshot','pageSize','idempotencyKey',...(operation==='renew'?[]:['ttlMs'])].some(field=>input[field]!==undefined))return json({error:'Continuation query context is immutable; use the original token',code:'CURSOR_MISMATCH'},409);
        const actor=env.CURSORS.get(env.CURSORS.idFromName(id));
        for(let attempt=0;attempt<40;attempt++){
          const response=await actor.fetch('https://cursor',{method:'POST',body:JSON.stringify({...input,op:operation,id,epoch})});
          const result=await decodeRpcResponse(response,`cursor/${id}:${operation}`);
          if(response.status===409&&result.code==='VISIBILITY_PENDING'){await helpDependencies(env,result);continue;}
          return json(result,response.status);
        }
        return json({error:'Cursor dependencies are still resolving',code:'VISIBILITY_PENDING',retryable:true},503);
      }
      if(url.pathname==='/snapshot') {
        if (!input.action) return json(await mediator(env,{op:'snapshot'}));
        if (input.action === 'rotateEpoch') return json(await mediator(env,{op:'rotateEpoch'}));
        if (!['lease','renew','release'].includes(input.action)) return json({error:'Unknown snapshot action'},400);
        return json(await mediator(env,{op:input.action,id:input.id || (input.action==='lease'?crypto.randomUUID():''),snapshot:input.snapshot,ttlMs:input.ttlMs,epoch:input.requestEpoch ?? 0}));
      }
      if(url.pathname==='/maintenance') {
        if(input.action==='retireEpochs')return json(await mediator(env,{op:'retireEpochs',before:input.before}));
        if(input.action==='collectEpochs') {
          const page=await mediator(env,{op:'retiredOwners',cursor:input.ownerCursor,limit:4});
          let ownerPending=false,ownersCollected=0;
          for(const item of page.owners) {
            const actor=ownerActor(env,item.owner);
            const collected=await actorCall(actor.namespace,actor.name,{op:'collect',epoch:item.epoch,...(actor.scope?{scope:actor.scope}:{})});
            if(collected.collected===false||collected.done===false){ownerPending=true;continue;}
            await mediator(env,{op:'forgetOwner',owner:item.owner,epoch:item.epoch});ownersCollected++;
          }
          const markers=await mediator(env,{op:'collectEpochs',cursor:input.cursor,limit:128});
          const bucketCount=markers.bucketCount;
          const requested=Number.isSafeInteger(input.bucket)&&input.bucket>=0&&input.bucket<bucketCount?input.bucket:0;
          const bucket=input.bucketEpochFloor!==undefined&&input.bucketEpochFloor!==markers.rejectBeforeEpoch?0:requested;
          const bucketCollection=await actorCall(env.MEDIATORS,`m${bucket}`,{op:'collect',limit:64});
          markers.bucketCollection=bucketCollection;markers.nextBucket=bucketCollection.done?(bucket+1)%bucketCount:bucket;markers.nextBucketEpochFloor=markers.rejectBeforeEpoch;
          return json({...markers,done:!!markers.done&&!ownerPending&&!page.nextCursor&&bucketCollection.done&&bucket===bucketCount-1,ownersCollected,ownerPending,ownerCursor:ownerPending?(input.ownerCursor??null):page.nextCursor});
        }
        // One bounded shard compaction batch per HTTP request. The returned
        // continuation is resumable; no actor enumerates the whole database.
        const retention = await mediator(env,{op:'compact',retainSteps:input.retainSteps});
        const next = input.continuation || {};
        if (typeof next !== 'object' || Array.isArray(next) || [next.after,next.shard,next.cursor].some(v=>v!==undefined&&v!==null&&typeof v!=='string')) return json({error:'Invalid maintenance continuation'},400);
        let name = next.shard;
        if (!name) {
          const page=await mediator(env,{op:'shards',cursor:next.after,limit:1});
          name=page.names[0];
          if(!name)return json({done:true,retentionFloor:retention.retentionFloor,continuation:null});
        }
        const compacted=await shard(env,name,{op:'compact',cursor:next.cursor,limit:128});
        return json({retentionFloor:retention.retentionFloor,shard:name,...compacted,done:false,shardDone:compacted.done,continuation:compacted.done?{after:name}:{after:next.after,shard:name,cursor:compacted.nextCursor}});
      }
      if(url.pathname==='/recover') {
        const page=await mediator(env,{op:'owners',cursor:input.cursor,limit:16});
        const status=await mediator(env,{op:'snapshot'});
        // Assigned data outcomes do not depend on the originating coordinator.
        // Help immutable bucket associations first, then recover request receipts.
        await mediator(env,{op:'drive'});
        for(const plan of status.pending||[]) {
          const found=await mediator(env,{op:'lookup',id:plan.id});
          if(found.state==='assigned')await actorCall(env.MEDIATORS,found.association.bucket,{op:'drive',id:plan.id,release:true});
        }
        const ownerErrors=[];
        for(const owner of page.owners)try{await recoverOwners(env,[owner]);}catch(error){ownerErrors.push({owner,error:error.message});}
        return json({...await mediator(env,{op:'snapshot'}),nextCursor:page.nextCursor,ownerErrors});
      }
      if(url.pathname==='/debug/tablet'){
        if(env.ALLOW_FAULTS!=='true')return json({error:'Not found'},404);
        const status=await mediator(env,{op:'snapshot'});
        let name=input.name;
        if(!name){const catalog=await shard(env,catalogName(input.table),{op:'get',key:'schema',snapshot:input.snapshot??status.snapshot});if(!catalog.rows[0]||catalog.rows[0].state==='dropped')return json({error:'Unknown table'},404);name=baseName(input.table,input.tablet,catalog.rows[0]);}
        const stub=env.SHARDS.get(env.SHARDS.idFromName(name));
        const response=await stub.fetch('https://actor',{method:'POST',body:JSON.stringify({op:'scan',snapshot:input.snapshot??status.snapshot,epoch:input.epoch,range:allRange()})});return json(await decodeRpcResponse(response,`${name}:scan`),response.status);
      }
      if(url.pathname==='/schema') {
        const epoch=input.requestEpoch??0;
        await mediator(env,{op:'epoch',requestEpoch:epoch});
        if(input.action!=='start') {
          if(!['status','resume','cancel'].includes(input.action)||typeof input.id!=='string'||!/^[a-f0-9]{64}$/.test(input.id))return json({error:'Valid schema job action/id required'},400);
          const response=await env.SCHEMA.get(env.SCHEMA.idFromName(input.id)).fetch('https://schema',{method:'POST',body:JSON.stringify({op:input.action,id:input.id,epoch})});return json(await decodeRpcResponse(response,`schema/${input.id}:${input.action}`),response.status);
        }
        if(typeof input.sql!=='string'||input.sql.length>32768||typeof input.idempotencyKey!=='string'||!/^[a-zA-Z0-9_-]{1,100}$/.test(input.idempotencyKey))return json({error:'Schema SQL and idempotencyKey required'},400);
        let statement;try{const parsed=parse(input.sql,input.params);if(parsed.length!==1||!['createIndex','dropIndex','dropTable'].includes(parsed[0].kind))throw new Error('Online schema jobs accept one CREATE INDEX, DROP INDEX or DROP TABLE statement');statement=parsed[0];}catch(e){return json({error:e.message},400);}
        const id=(await coordinatorFor(`${epoch}:${input.idempotencyKey}`)).slice(3);
        const response=await env.SCHEMA.get(env.SCHEMA.idFromName(id)).fetch('https://schema',{method:'POST',body:JSON.stringify({op:'start',kind:statement.kind,id,epoch,table:statement.table,index:statement.index,column:statement.column,unique:statement.unique})});return json(await decodeRpcResponse(response,`schema/${id}:start`),response.status);
      }
      if(typeof input.sql!=='string'||input.sql.length>32768)return json({error:'SQL string required, max32768 characters'},400);
      if(typeof input.idempotencyKey!=='string'||!/^[a-zA-Z0-9_-]{1,100}$/.test(input.idempotencyKey))return json({error:'Valid idempotencyKey required'},400);
      delete input.op;
      const epoch=input.requestEpoch??0;
      await mediator(env,{op:'epoch',requestEpoch:epoch}); // Admission BEFORE owner lookup: an expired retry never becomes new work.
      const owner=await coordinatorFor(`${epoch}:${input.idempotencyKey}`),coordinator=env.COORDINATOR.get(env.COORDINATOR.idFromName(coordinatorName(owner)));
      for(let attempt=0;attempt<40;attempt++){
        const response=await coordinator.fetch('https://coordinator',{method:'POST',body:JSON.stringify({...input,coordinatorId:owner,scope:owner})});
        const result=await decodeRpcResponse(response,`${owner}:query`);
        if(response.status===409&&result.retryable){
          await helpDependencies(env,result);
          await new Promise(resolve=>setTimeout(resolve,5+attempt*2));continue;
        }
        // All own participants are applied before Coordinator returns success.
        // Reads at allocated snapshots wait only on relevant local dependencies.
        return json(result,response.status);
      }
      return json({error:'Serialization conflict; retry same key',code:'CONFLICT',retryable:true},503);
    }catch(error){return json({error:error.message,code:error.code||'RECOVERY_REQUIRED',retryable:![400,410].includes(error.status),rpc:error.rpc,retryAfterMs:error.retryAfterMs},error.status||503);}
  }
};
