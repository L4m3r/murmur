import { compareKeys, encodeKey, keyOfRow } from './keys.js';
// Hash retained only for deterministic coordinator selection, never row routing.
export function keyHash(key) {
  const text = JSON.stringify(key);
  if (text === undefined) throw new Error('Key is not serializable');
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 16777619);
  return hash >>> 0;
}
/** Catalog maps are validated when planned/published. Reads binary-search the
 * first exclusive end above the key, without rescanning the whole descriptor map. */
export function routeTablet(schema, key) {
  const encoded = encodeKey(key), tablets = schema.tablets || [];
  let low = 0, high = tablets.length;
  while (low < high) {
    const middle = low + Math.floor((high - low) / 2), end = tablets[middle].end;
    if (end === null || encoded < encodeKey(end)) high = middle;
    else low = middle + 1;
  }
  const tablet = tablets[low];
  if (!tablet || (tablet.start !== null && encodeKey(tablet.start) > encoded)) throw new Error('Missing tablet route');
  return tablet.id;
}
/** Catalog generations change for any topology edit; actor routing epochs do not.
 * Missing descriptor epochs are supported for explicit old-format test fixtures. */
const tabletIds = new WeakMap();
export function tabletEpoch(schema, id) {
  const descriptors = schema.tablets || [];
  let cached=tabletIds.get(descriptors);
  if(!cached || cached.length!==descriptors.length) {cached={length:descriptors.length,byId:new Map(descriptors.map(t=>[t.id,t]))};tabletIds.set(descriptors,cached);}
  const tablet = cached.byId.get(id);
  if (!tablet) throw new Error('Unknown tablet epoch');
  const epoch = tablet.epoch === undefined ? schema.epoch : tablet.epoch;
  if (!Number.isSafeInteger(epoch) || epoch < 1 || epoch > schema.epoch) throw new Error('Invalid tablet epoch');
  return epoch;
}
function normalizeEpochs(schema) {
  schema.tablets = schema.tablets.map(t => ({ ...t, epoch: tabletEpoch(schema, t.id) }));
}
/** O(log N + intersecting tablets), matching ordered range partition routing.
 * Query bounds are encoded and tablet ends are exclusive. */
export function selectTablets(schema, range) {
  if (range.empty) return [];
  const tablets = schema.tablets, selected = [];
  let low = 0, high = tablets.length;
  if (range.lower !== null) {
    while (low < high) {
      const middle = low + Math.floor((high - low) / 2), end = tablets[middle].end;
      if (end === null || encodeKey(end) > range.lower) high = middle;
      else low = middle + 1;
    }
  }
  for (let i = low; i < tablets.length; i++) {
    const tablet = tablets[i], start = tablet.start === null ? null : encodeKey(tablet.start);
    if (start !== null && range.upper !== null && (start > range.upper || (start === range.upper && !range.upperInclusive))) break;
    selected.push(tablet);
  }
  return selected;
}
export const rowsBytes = rows => new TextEncoder().encode(JSON.stringify(rows)).byteLength;
function validate(tablets) {
  if (!tablets.length || tablets[0].start !== null || tablets.at(-1).end !== null) throw new Error('Ordered tablet map must cover negative to positive infinity');
  const ids = new Set();
  for (let i = 0; i < tablets.length; i++) {
    const t = tablets[i];
    if (typeof t.id !== 'string' || !t.id || ids.has(t.id) || (i && t.start === null) || (i < tablets.length - 1 && t.end === null) || (t.start !== null && t.end !== null && compareKeys(t.start, t.end) >= 0) || (i && compareKeys(tablets[i-1].end, t.start) !== 0)) throw new Error('Invalid or overlapping ordered tablet map');
    if (t.start !== null) encodeKey(t.start);
    if (t.end !== null) encodeKey(t.end);
    ids.add(t.id);
  }
}
export function planTablets(inputSchema, rows, options = {}) {
  const { splitBytes = 1024, mergeBytes = 256, maxTablets = 256 } = options;
  if (!Number.isSafeInteger(splitBytes) || splitBytes < 4 || !Number.isSafeInteger(mergeBytes) || mergeBytes < 2 || mergeBytes >= splitBytes || !Number.isSafeInteger(maxTablets) || maxTablets < 1 || maxTablets > 4096) throw new Error('Invalid tablet sizing options');
  const schema = structuredClone(inputSchema), initialized = !schema.tablets;
  if (initialized) { schema.tablets = [{ id: 't0', start: null, end: null, epoch: 1 }]; schema.epoch = 1; }
  if (!Number.isSafeInteger(schema.epoch) || schema.epoch < 1) throw new Error('Invalid routing epoch');
  normalizeEpochs(schema);
  validate(schema.tablets);
  if (schema.tablets.length > maxTablets) throw new Error('Existing tablet count exceeds configured maximum');
  const sorted = [...rows].sort((a,b)=>compareKeys(keyOfRow(schema,a),keyOfRow(schema,b)));
  for(let i=0;i<sorted.length;i++) {
    encodeKey(keyOfRow(schema,sorted[i]));
    if(i && compareKeys(keyOfRow(schema,sorted[i-1]),keyOfRow(schema,sorted[i])) === 0) throw new Error('Duplicate physical tablet key');
  }
  const baseEpoch = schema.epoch; let sequence = 0;
  const nextId = () => { if (!Number.isSafeInteger(baseEpoch + 1)) throw new Error('Routing epoch exhausted'); return `e${baseEpoch + 1}t${++sequence}`; };
  const events = initialized ? [{ type: 'initialize', tablet: 't0' }] : [];
  const partitions = schema.tablets.map(t => ({ ...t, rows: [] }));
  for (const row of sorted) partitions.find(t=>t.id===routeTablet(schema,keyOfRow(schema,row))).rows.push(row);
  // Adjacent ordered ranges can merge regardless of their historical split parent.
  for (let i = 0; i + 1 < partitions.length;) {
    const a=partitions[i], b=partitions[i+1], combined=[...a.rows,...b.rows];
    if (rowsBytes(combined)>mergeBytes) { i++; continue; }
    const next={id:nextId(),start:a.start,end:b.end,epoch:baseEpoch+1,rows:combined};
    partitions.splice(i,2,next); events.push({type:'merge',from:[a.id,b.id],to:next.id});
    if(i) i--; // recheck the now-adjacent left range
  }
  // Split at an actual key around half the serialized byte weight. Each child
  // has >=1 row; single oversized rows remain oversized rather than splitting empty.
  for (let i=0;i<partitions.length && partitions.length<maxTablets;i++) {
    const current=partitions[i];
    if(current.rows.length<2 || rowsBytes(current.rows)<=splitBytes) continue;
    const weights=current.rows.map(row=>rowsBytes([row])-1), half=weights.reduce((a,b)=>a+b,0)/2;
    let sum=0, pivot=1;
    for(let j=0;j<weights.length-1;j++) {sum+=weights[j];pivot=j+1;if(sum>=half)break;}
    const boundary=structuredClone(keyOfRow(schema,current.rows[pivot]));
    const left={id:nextId(),start:current.start,end:boundary,epoch:schema.epoch+1,rows:current.rows.slice(0,pivot)};
    const right={id:nextId(),start:boundary,end:current.end,epoch:schema.epoch+1,rows:current.rows.slice(pivot)};
    partitions.splice(i,1,left,right);events.push({type:'split',from:current.id,to:[left.id,right.id],boundary}); i--;
  }
  const topologyChanged=events.some(e=>e.type==='split'||e.type==='merge');
  if(topologyChanged)schema.epoch++;
  schema.tablets=partitions.map(({id,start,end,epoch})=>({id,start,end,epoch})); validate(schema.tablets);
  return {schema,changed:initialized||topologyChanged,events,partitions:partitions.map(({id,rows,epoch})=>({id,rows,epoch,bytes:rowsBytes(rows)}))};
}

/** Repartition only explicitly materialized tablets. Missing Map entries mean
 * "not loaded", never an empty tablet. Unaffected ranges remain byte-for-byte
 * unchanged, including their actor routing epoch. Only replacement tablets get
 * the next catalog epoch; no metadata fanout to unaffected actors is needed.
 */
export function planAffectedTablets(inputSchema, materialized, options = {}) {
  const { splitBytes = 1024, mergeBytes = 256, maxTablets = 256 } = options;
  if (!Number.isSafeInteger(splitBytes) || splitBytes < 4 || !Number.isSafeInteger(mergeBytes) || mergeBytes < 2 || mergeBytes >= splitBytes || !Number.isSafeInteger(maxTablets) || maxTablets < 1 || maxTablets > 4096) throw new Error('Invalid tablet sizing options');
  const schema = structuredClone(inputSchema);
  if (!Number.isSafeInteger(schema.epoch) || schema.epoch < 1) throw new Error('Invalid routing epoch');
  normalizeEpochs(schema);
  validate(schema.tablets);
  if (schema.tablets.length > maxTablets) throw new Error('Existing tablet count exceeds configured maximum');
  for (const id of materialized.keys()) if (!schema.tablets.some(t => t.id === id)) throw new Error('Unknown materialized tablet');
  const partitions = schema.tablets.map(t => {
    if (!materialized.has(t.id)) return { ...t, rows: null };
    const rows = [...materialized.get(t.id)].sort((a,b) => compareKeys(keyOfRow(schema,a),keyOfRow(schema,b)));
    for (let i = 0; i < rows.length; i++) {
      const key = keyOfRow(schema,rows[i]); encodeKey(key);
      if (routeTablet(schema,key) !== t.id) throw new Error('Materialized row outside tablet range');
      if (i && compareKeys(keyOfRow(schema,rows[i-1]),key) === 0) throw new Error('Duplicate physical tablet key');
    }
    return { ...t, rows };
  });
  let sequence = 0;
  const nextId = () => { if (!Number.isSafeInteger(schema.epoch + 1)) throw new Error('Routing epoch exhausted'); return `e${schema.epoch + 1}t${++sequence}`; };
  const events = [];
  for (let i = 0; i + 1 < partitions.length;) {
    const a = partitions[i], b = partitions[i+1];
    if (a.rows === null || b.rows === null || rowsBytes([...a.rows,...b.rows]) > mergeBytes) { i++; continue; }
    const next = { id: nextId(), start:a.start, end:b.end, epoch:schema.epoch+1, rows:[...a.rows,...b.rows] };
    partitions.splice(i,2,next); events.push({type:'merge',from:[a.id,b.id],to:next.id});
    if (i) i--;
  }
  for (let i = 0; i < partitions.length && partitions.length < maxTablets; i++) {
    const current = partitions[i];
    if (current.rows === null || current.rows.length < 2 || rowsBytes(current.rows) <= splitBytes) continue;
    const weights = current.rows.map(row=>rowsBytes([row])-1), half = weights.reduce((a,b)=>a+b,0)/2;
    let sum = 0, pivot = 1;
    for (let j=0;j<weights.length-1;j++) { sum+=weights[j];pivot=j+1;if(sum>=half)break; }
    const boundary = structuredClone(keyOfRow(schema,current.rows[pivot]));
    const left={id:nextId(),start:current.start,end:boundary,epoch:schema.epoch+1,rows:current.rows.slice(0,pivot)};
    const right={id:nextId(),start:boundary,end:current.end,epoch:schema.epoch+1,rows:current.rows.slice(pivot)};
    partitions.splice(i,1,left,right);events.push({type:'split',from:current.id,to:[left.id,right.id],boundary});i--;
  }
  if (events.length) schema.epoch++;
  schema.tablets=partitions.map(({id,start,end,epoch})=>({id,start,end,epoch}));validate(schema.tablets);
  return {schema,changed:events.length>0,events,partitions:partitions.filter(p=>p.rows!==null).map(({id,rows,epoch})=>({id,rows,epoch,bytes:rowsBytes(rows)}))};
}
