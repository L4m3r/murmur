import { compareKeys, encodeKey, keyOfRow } from './keys.js';
// Hash retained only for deterministic coordinator selection, never row routing.
export function keyHash(key) {
  const text = JSON.stringify(key);
  if (text === undefined) throw new Error('Key is not serializable');
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 16777619);
  return hash >>> 0;
}
export function routeTablet(schema, key) {
  encodeKey(key);
  const tablet = schema.tablets?.find(t => (t.start === null || compareKeys(t.start, key) <= 0) && (t.end === null || compareKeys(key, t.end) < 0));
  if (!tablet) throw new Error('Missing tablet route');
  return tablet.id;
}
export function selectTablets(schema, range) {
  if (range.empty) return [];
  return schema.tablets.filter(t => {
    const start = t.start === null ? null : encodeKey(t.start), end = t.end === null ? null : encodeKey(t.end);
    if (end !== null && range.lower !== null && end <= range.lower) return false;
    if (start !== null && range.upper !== null && (start > range.upper || (start === range.upper && !range.upperInclusive))) return false;
    return true;
  });
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
  const { splitBytes = 1024, mergeBytes = 256, maxTablets = 16 } = options;
  if (!Number.isSafeInteger(splitBytes) || splitBytes < 4 || !Number.isSafeInteger(mergeBytes) || mergeBytes < 2 || mergeBytes >= splitBytes || !Number.isSafeInteger(maxTablets) || maxTablets < 1 || maxTablets > 256) throw new Error('Invalid tablet sizing options');
  const schema = structuredClone(inputSchema), initialized = !schema.tablets;
  if (initialized) { schema.tablets = [{ id: 't0', start: null, end: null }]; schema.epoch = 1; }
  if (!Number.isSafeInteger(schema.epoch) || schema.epoch < 1) throw new Error('Invalid routing epoch');
  validate(schema.tablets);
  if (schema.tablets.length > maxTablets) throw new Error('Existing tablet count exceeds configured maximum');
  const sorted = [...rows].sort((a,b)=>compareKeys(keyOfRow(schema,a),keyOfRow(schema,b)));
  for(let i=0;i<sorted.length;i++) {
    encodeKey(keyOfRow(schema,sorted[i]));
    if(i && compareKeys(keyOfRow(schema,sorted[i-1]),keyOfRow(schema,sorted[i])) === 0) throw new Error('Duplicate physical tablet key');
  }
  const baseEpoch = schema.epoch; let sequence = 0;
  const nextId = () => `e${baseEpoch + 1}t${++sequence}`;
  const events = initialized ? [{ type: 'initialize', tablet: 't0' }] : [];
  const partitions = schema.tablets.map(t => ({ ...t, rows: [] }));
  for (const row of sorted) partitions.find(t=>t.id===routeTablet(schema,keyOfRow(schema,row))).rows.push(row);
  // Adjacent ordered ranges can merge regardless of their historical split parent.
  for (let i = 0; i + 1 < partitions.length;) {
    const a=partitions[i], b=partitions[i+1], combined=[...a.rows,...b.rows];
    if (rowsBytes(combined)>mergeBytes) { i++; continue; }
    const next={id:nextId(),start:a.start,end:b.end,rows:combined};
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
    const left={id:nextId(),start:current.start,end:boundary,rows:current.rows.slice(0,pivot)};
    const right={id:nextId(),start:boundary,end:current.end,rows:current.rows.slice(pivot)};
    partitions.splice(i,1,left,right);events.push({type:'split',from:current.id,to:[left.id,right.id],boundary}); i--;
  }
  const topologyChanged=events.some(e=>e.type==='split'||e.type==='merge');
  if(topologyChanged)schema.epoch++;
  schema.tablets=partitions.map(({id,start,end})=>({id,start,end})); validate(schema.tablets);
  return {schema,changed:initialized||topologyChanged,events,partitions:partitions.map(({id,rows})=>({id,rows,bytes:rowsBytes(rows)}))};
}
