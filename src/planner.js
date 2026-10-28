import { decodeRpcResponse } from './rpc-response.js';
import { physicalTableName, indexEntry, insertKeys } from './schema.js';
import { execute } from './sql.js';
import { encodeKey, rangeForFilters, keyOfRow, primaryRangeForFilters } from './keys.js';
import { routeTablet, selectTablets, planAffectedTablets, tabletEpoch } from './tablets.js';

const copy = structuredClone;
const point = key => ({ lower: encodeKey(key), upper: encodeKey(key), lowerInclusive: true, upperInclusive: true, empty: false });
const baseName = (table, tablet, schema) => `base/${physicalTableName(table,schema)}/${tablet}`;
const indexName = (table, index, tablet, schema) => `index/${physicalTableName(table,schema)}/${index}/${tablet}`;
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const rowBytes = row => row === null ? 0 : new TextEncoder().encode(JSON.stringify(row)).byteLength + 1;

async function rpc(env, name, input) {
  const response = await env.SHARDS.get(env.SHARDS.idFromName(name)).fetch('https://actor/internal', { method: 'POST', body: JSON.stringify(input) });
  const result = await decodeRpcResponse(response,`${name}:${input.op}`);
  if (!response.ok) { const error = new Error(result.error || 'Shard request failed'); error.code = result.code; error.status = response.status; error.owners = result.owners; error.dependencies = result.dependencies; error.rpc=result.rpc;error.retryAfterMs=result.retryAfterMs;throw error; }
  return result;
}

/** Ordinary DML planner. DDL and explicit topology maintenance use their own
 * materialization path. No ordinary point write reads unrelated table rows.
 * Size-triggered topology work materializes affected tablets only; unrelated
 * tablets retain their routing epochs without reading their data.
 */
export async function tryBuildSelective(env, statements, names, snapshot) {
  if (statements.some(s => !['select', 'insert', 'upsert', 'update', 'delete'].includes(s.kind))) return null;
  const catalog = {}, states = new Map(), targets = new Map();
  const plan = { type: 'selective', tabletsVisited: 0, rowsRead: 0, rowsFetched: 0, versionsRead: 0 };
  const target = (name, epoch) => {
    if (!targets.has(name)) targets.set(name, { name, reads: [], changes: [], ...(epoch === undefined ? {} : { expectedEpoch: epoch }) });
    return targets.get(name);
  };
  const readDependency = (name, range, epoch) => {
    if (range.empty) return;
    const t = target(name, epoch);
    if (!t.reads.some(r => same(r, range))) t.reads.push(range);
  };
  let readBytes = 0;
  const read = async (name, input) => {
    const rows = []; let cursor;
    plan.tabletsVisited++;
    do {
      const result = await rpc(env, name, { snapshot, ...input, ...(cursor ? { cursor } : {}) });
      plan.versionsRead += result.versionsRead || 0;
      readBytes += new TextEncoder().encode(JSON.stringify(result.rows)).byteLength;
      if (readBytes > 8 * 1024 * 1024) throw new Error('Transaction reads exceed 8 MiB work budget; narrow the predicate');
      rows.push(...result.rows);
      cursor = result.cursor || result.nextCursor;
    } while (input.op === 'scan' && cursor);
    return rows;
  };
  for (const table of names) {
    const found = await rpc(env, `catalog/${table}`, { op: 'getMany', keys: ['schema'], snapshot });
    if (!found.rows[0] || found.rows[0].state==='dropped') return null;
    catalog[table] = found.rows[0];
    readDependency(`catalog/${table}`, point('schema'));
    states.set(table, { original: new Map(), overlay: new Map() });
  }
  const remember = (table, rows) => {
    const { original } = states.get(table), schema = catalog[table];
    for (const row of rows) if (!original.has(encodeKey(keyOfRow(schema,row)))) original.set(encodeKey(keyOfRow(schema,row)), copy(row));
    return rows;
  };
  const basePoints = async (table, keys) => {
    const schema = catalog[table], byTablet = new Map(), unique = new Map(keys.map(key => [encodeKey(key), key]));
    for (const key of unique.values()) {
      const id = routeTablet(schema, key);
      if (!byTablet.has(id)) byTablet.set(id, []);
      byTablet.get(id).push(key);
      readDependency(baseName(table, id, schema), point(key), tabletEpoch(schema,id));
    }
    const rows = [];
    for (const [id, batch] of byTablet) for (let i = 0; i < batch.length; i += 2000) rows.push(...await read(baseName(table, id, schema), { op: 'getMany', keys: batch.slice(i, i + 2000), epoch: tabletEpoch(schema,id) }));
    remember(table, rows);
    const original = states.get(table).original;
    for (const key of unique.keys()) if (!original.has(key)) original.set(key, null);
    plan.rowsFetched += rows.length;
    return { rows, tablets: byTablet.size };
  };
  const indexScan = async (table, index, range) => {
    const tablets = selectTablets(index, range), rows = [];
    for (const tablet of tablets) {
      const name = indexName(table, index.storageName || index.name, tablet.id,catalog[table]);
      readDependency(name, range, tabletEpoch(index,tablet.id));
      rows.push(...await read(name, { op: 'scan', range, epoch: tabletEpoch(index,tablet.id) }));
    }
    return { rows, tablets: tablets.length };
  };
  const candidates = async statement => {
    const table = statement.table, schema = catalog[table];
    if (['insert', 'upsert'].includes(statement.kind)) {
      // Validate literals/schema before routing, including omitted/null PKs.
      execute([statement], copy(catalog), { [table]: [] });
      return (await basePoints(table, insertKeys(schema,statement))).rows;
    }
    // Even an impossible/empty range must reject invalid columns and types.
    execute([statement], copy(catalog), { [table]: [] });
    const filter = statement.filter || [];
    const bounded = column => filter.some(f => f.column === column && ['=', '<', '<=', '>', '>='].includes(f.op));
    let index = !bounded(schema.primaryKey) && schema.indexes?.find(i => !['building','validating'].includes(i.state) && bounded(i.column));
    if(statements.length===1&&statement.kind==='select'&&statement.limit!==null&&statement.order){
      if(statement.order.column===schema.primaryKey)index=null;
      else index=schema.indexes?.find(i=>!['building','validating'].includes(i.state)&&i.column===statement.order.column)||index;
    }
    // A single read-only ordered LIMIT is consumed page-by-page, after residual
    // predicates, so LIMIT 1 never materializes the entire matching range.
    const orderedColumn=index?index.column:schema.primaryKey;
    if(statements.length===1 && statement.kind==='select' && statement.limit!==null && (!statement.order || statement.order.column===orderedColumn)) {
      const range=index?rangeForFilters(filter,index.column,{compositePrefix:true}):primaryRangeForFilters(schema,filter);
      let tablets=selectTablets(index||schema,range);const reverse=!!statement.order?.desc;
      if(reverse)tablets=[...tablets].reverse();
      const rows=[],baseVisited=new Set();let visited=0;
      for(const tablet of tablets){
        if(rows.length>=statement.limit)break;
        const map=index||schema,name=index?indexName(table,index.storageName||index.name,tablet.id,schema):baseName(table,tablet.id,schema);
        let cursor;visited++;plan.tabletsVisited++;readDependency(name,range,tabletEpoch(map,tablet.id));
        do {
          const page=await rpc(env,name,{op:'scan',snapshot,range,epoch:tabletEpoch(map,tablet.id),reverse,pageSize:Math.min(256,Math.max(1,statement.limit-rows.length)),...(cursor?{cursor}:{})});
          plan.versionsRead+=page.versionsRead||0;plan.rowsRead+=page.rows.length;
          readBytes+=new TextEncoder().encode(JSON.stringify(page.rows)).byteLength;
          if(readBytes>8*1024*1024)throw new Error('Transaction reads exceed 8 MiB work budget; narrow the predicate');
          let values=page.rows;
          if(index){
            const fetched=await basePoints(table,page.rows.map(entry=>entry.pk));
            const byKey=new Map(fetched.rows.map(row=>[encodeKey(keyOfRow(schema,row)),row]));
            values=page.rows.map(entry=>byKey.get(encodeKey(entry.pk))).filter(Boolean);
            for(const entry of page.rows)baseVisited.add(routeTablet(schema,entry.pk));
          }else{remember(table,values);plan.rowsFetched+=values.length;}
          const matched=execute([{...statement,columns:'*',order:null,limit:null}],copy(catalog),{[table]:values}).results[0].rows;
          rows.push(...matched.slice(0,statement.limit-rows.length));cursor=page.nextCursor;
        }while(cursor&&rows.length<statement.limit);
      }
      Object.assign(plan,{type:index?'index-range':bounded(schema.primaryKey)?'primary-range':'scan',limitPushed:true,tabletsTotal:(index||schema).tablets.length,...(index?{index:index.name,indexTabletsVisited:visited,indexTabletsTotal:index.tablets.length,baseTabletsVisited:baseVisited.size}:{})});
      return rows;
    }
    if (index) {
      const found = await indexScan(table, index, rangeForFilters(filter, index.column, { compositePrefix: true }));
      const fetched = await basePoints(table, found.rows.map(entry => entry.pk));
      plan.rowsRead += found.rows.length;
      if (statements.length === 1) Object.assign(plan, { type: 'index-range', index: index.name, indexTabletsVisited: found.tablets, indexTabletsTotal: index.tablets.length, baseTabletsVisited: fetched.tablets, tabletsTotal: index.tablets.length });
      return fetched.rows;
    }
    const range = primaryRangeForFilters(schema,filter), tablets = selectTablets(schema, range), rows = [];
    for (const tablet of tablets) {
      const name = baseName(table, tablet.id,schema);
      readDependency(name, range, tabletEpoch(schema,tablet.id));
      rows.push(...await read(name, { op: 'scan', range, epoch: tabletEpoch(schema,tablet.id) }));
    }
    remember(table, rows); plan.rowsRead += rows.length; plan.rowsFetched += rows.length;
    if (statements.length === 1) Object.assign(plan, { type: bounded(schema.primaryKey) ? 'primary-range' : 'scan', tabletsTotal: schema.tablets.length });
    return rows;
  };
  const results = [];
  for (const statement of statements) {
    const table = statement.table, schema = catalog[table], state = states.get(table);
    const loaded = await candidates(statement), working = new Map(loaded.map(row => [encodeKey(keyOfRow(schema,row)), copy(row)]));
    // Overlay all transaction-local rows, including inserts absent from indexes
    // and rows moved into a queried range by earlier statements.
    for (const [key, row] of state.overlay) { if (row === null) working.delete(key); else working.set(key, copy(row)); }
    const before = new Map([...working].map(([key, row]) => [key, copy(row)]));
    const executed = execute([statement], copy(catalog), { [table]: [...working.values()] });
    results.push(...executed.results);
    if (statement.kind === 'select') continue;
    const after = new Map(executed.tables[table].map(row => [encodeKey(keyOfRow(schema,row)), row]));
    const changed = [];
    for (const key of new Set([...before.keys(), ...after.keys()])) {
      const previous = before.get(key) || null, next = after.get(key) || null;
      if (same(previous, next)) continue;
      if (!state.original.has(key)) state.original.set(key, copy(previous));
      state.overlay.set(key, copy(next));
      changed.push(next);
    }
    // A unique index is global: probe the actual durable prefix, then reconcile
    // it against this transaction's overlay (deletes/moves must free values).
    for (const index of schema.indexes || []) if (index.unique && index.state !== 'building') {
      const values = new Map(changed.filter(row => row && row[index.column] !== null).map(row => [encodeKey(row[index.column]), row[index.column]]));
      for (const value of values.values()) {
        const range = rangeForFilters([{ column: index.column, op: '=', value }], index.column, { compositePrefix: true });
        const matches = new Map();
        for (const entry of (await indexScan(table, index, range)).rows) matches.set(encodeKey(entry.pk), entry.pk);
        for (const [key, row] of state.overlay) { matches.delete(key); if (row && row[index.column] === value) matches.set(key, keyOfRow(schema,row)); }
        if (matches.size > 1) throw new Error(`Unique index ${index.name} violation`);
      }
    }
  }
  let baseRows = 0, indexRows = 0;
  const maintenance = [];
  const addDelta = (name, epoch, key, row) => {
    const t = target(name, epoch), encoded = encodeKey(key), existing = t.changes.findIndex(c => encodeKey(c.key) === encoded);
    if (existing >= 0) t.changes[existing] = { key, row }; else t.changes.push({ key, row });
  };
  for (const [table, state] of states) {
    const schema = catalog[table], stats = new Map();
    const deltaStats = (name, previous, next) => {
      const stat = stats.get(name) || { name, rowsDelta: 0, bytesDelta: 0 };
      stat.rowsDelta += Number(next !== null) - Number(previous !== null);
      stat.bytesDelta += rowBytes(next) - rowBytes(previous);
      stats.set(name, stat);
    };
    for (const [key, next] of state.overlay) {
      const previous = state.original.get(key) || null;
      if (same(previous, next)) continue;
      const pk = keyOfRow(schema,next || previous), base = baseName(table, routeTablet(schema, pk),schema);
      addDelta(base, tabletEpoch(schema,routeTablet(schema,pk)), pk, next); baseRows++; deltaStats(base, previous, next);
      for (const index of schema.indexes || []) {
        const entry = row => indexEntry(schema,index,row);
        const oldEntry = entry(previous), newEntry = entry(next);
        if (same(oldEntry, newEntry)) continue;
        const keyOf = row => keyOfRow(index,row);
        if (oldEntry) { const k = keyOf(oldEntry), name = indexName(table, index.storageName || index.name, routeTablet(index, k),schema); addDelta(name, tabletEpoch(index,routeTablet(index,k)), k, null); deltaStats(name, oldEntry, null); }
        if (newEntry) { const k = keyOf(newEntry), name = indexName(table, index.storageName || index.name, routeTablet(index, k),schema); addDelta(name, tabletEpoch(index,routeTablet(index,k)), k, newEntry); deltaStats(name, null, newEntry); }
      }
    }
    if (stats.size) maintenance.push({ table, targets: [...stats.values()] });
  }
  indexRows = [...targets.values()].filter(t=>t.name.startsWith('index/')).reduce((n,t)=>n+t.changes.length,0);
  // Stats only schedule work. Snapshot scans plus predicate/epoch validation
  // establish correctness, never current (possibly above-frontier) counters.
  const statsCache = new Map(), events = [];
  const projected = async (name, deltas) => {
    if (!statsCache.has(name)) statsCache.set(name, await rpc(env, name, { op: 'stats' }));
    const current = statsCache.get(name), delta = deltas.get(name);
    const rowsDelta = delta?.rowsDelta || 0, rows = current.rows + rowsDelta;
    const payload = current.payloadBytes ?? (current.bytes - 2 - Math.max(0, current.rows - 1));
    return { rows, bytes: 2 + payload + (delta?.bytesDelta || 0) - rowsDelta + Math.max(0, rows - 1) };
  };
  const defaultSizing = { splitBytes: Number(env.TABLET_SPLIT_BYTES || 4096), mergeBytes: Number(env.TABLET_MERGE_BYTES || 1024), maxTablets: Number(env.TABLET_MAX_COUNT || 256) };
  const allRange = { lower:null, upper:null, lowerInclusive:true, upperInclusive:false, empty:false };
  for (const item of maintenance) {
    const schema = catalog[item.table], deltas = new Map(item.targets.map(t => [t.name, t]));
    const newIndexes = [];
    let nextSchema = schema;
    for (const shape of [schema, ...(schema.indexes || [])]) {
      const sizing = { ...defaultSizing, ...(shape.sizing || {}) };
      const nameOf = id => shape === schema ? baseName(item.table,id,schema) : indexName(item.table,shape.storageName || shape.name,id,schema);
      const load = new Set();
      const mergeable = async (a,b) => {
        const left=await projected(nameOf(a.id),deltas), right=await projected(nameOf(b.id),deltas);
        return left.bytes+right.bytes-(left.rows>0&&right.rows>0?3:2)<=sizing.mergeBytes;
      };
      for (let i=0;i<shape.tablets.length;i++) {
        const tablet=shape.tablets[i], delta=deltas.get(nameOf(tablet.id));
        if (!delta) continue;
        const next=await projected(nameOf(tablet.id),deltas);
        if (next.rows>1 && next.bytes>sizing.splitBytes && shape.tablets.length<sizing.maxTablets) load.add(tablet.id);
        if (delta.bytesDelta<0 || delta.rowsDelta<0) for (const neighbor of [shape.tablets[i-1],shape.tablets[i+1]]) {
          if (neighbor && await mergeable(tablet,neighbor)) {load.add(tablet.id);load.add(neighbor.id);}
        }
      }
      // Cascading adjacent merges may extend beyond the initially changed pair.
      for (let expanded=true;expanded;) {
        expanded=false;
        for(let i=0;i+1<shape.tablets.length;i++) {
          const a=shape.tablets[i],b=shape.tablets[i+1];
          if(load.has(a.id)===load.has(b.id))continue;
          if(await mergeable(a,b)){load.add(a.id);load.add(b.id);expanded=true;}
        }
      }
      let planned={schema:shape,changed:false,events:[],partitions:[]};
      if(load.size) {
        const materialized=new Map();
        for(const tablet of shape.tablets.filter(t=>load.has(t.id))) {
          const name=nameOf(tablet.id);
          readDependency(name,allRange,tabletEpoch(shape,tablet.id));
          const rows=await read(name,{op:'scan',range:allRange,epoch:tabletEpoch(shape,tablet.id)});
          plan.maintenanceRowsRead=(plan.maintenanceRowsRead||0)+rows.length;
          const keyOf=row=>keyOfRow(shape,row);
          const working=new Map(rows.map(row=>[encodeKey(keyOf(row)),row]));
          for(const change of targets.get(name)?.changes||[]) {
            const key=encodeKey(change.key);
            if(change.row===null)working.delete(key);else working.set(key,change.row);
          }
          materialized.set(tablet.id,[...working.values()]);
        }
        planned=planAffectedTablets(shape,materialized,sizing);
        if(planned.changed) {
          const nextIds=new Set(planned.schema.tablets.map(t=>t.id));
          // Catalog OCC fences routing changes. Only replaced identities
          // change actor metadata; unrelated tablets keep their own epochs.
          for(const tablet of shape.tablets) if(!nextIds.has(tablet.id)) {
            const t=target(nameOf(tablet.id),tabletEpoch(shape,tablet.id));
            t.meta={epoch:planned.schema.epoch,retired:true};t.changes=[];
          }
          for(const part of planned.partitions) if(!shape.tablets.some(t=>t.id===part.id)) {
            const t=target(nameOf(part.id),0);
            t.meta={epoch:part.epoch,retired:false};
            t.changes=part.rows.map(row=>({key:keyOfRow(shape,row),row}));
          }
          events.push(...planned.events.map(event=>({table:item.table,...(shape===schema?{}:{index:shape.name}),...event})));
        }
      }
      if(shape===schema)nextSchema=planned.schema;else newIndexes.push(planned.schema);
    }
    nextSchema={...nextSchema,indexes:newIndexes};
    if(!same(schema,nextSchema)) {
      catalog[item.table]=nextSchema;
      target(`catalog/${item.table}`).changes=[{key:'schema',row:nextSchema}];
    }
  }
  const list = [...targets.values()].sort((a, b) => a.name.localeCompare(b.name));
  if (!statements.every(s=>s.kind==='select') && list.length > 64) throw new Error('At most 64 participants');
  return { targets: list, response: { results, plan, changes: { baseRows, indexRows }, participants: list.map(t => ({ name: t.name })), routing: Object.entries(catalog).map(([table, s]) => ({ table, epoch: s.epoch, tablets: s.tablets, indexes: (s.indexes || []).map(i => ({ name: i.name, keyColumns: i.keyColumns, epoch: i.epoch, tablets: i.tablets })) })), events, maintenance } };
}
