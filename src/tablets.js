/** Pure, deterministic size-based tablet routing. Byte size is UTF-8 JSON rows,
 * not physical storage usage. Call only while holding the table's exclusive lock;
 * publish this catalog and move/retire its tablets in one durable transaction. */
export const HASH_SPACE = 0x100000000;
export function keyHash(key) {
  const text = JSON.stringify(key);
  if (text === undefined) throw new Error('Primary key is not serializable');
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 16777619);
  return hash >>> 0;
}
export function routeTablet(schema, key) {
  const hash = keyHash(key);
  const tablet = schema.tablets?.find(t => t.start <= hash && hash < t.end);
  if (!tablet) throw new Error('Missing tablet route');
  return tablet.id;
}
export const rowsBytes = rows => new TextEncoder().encode(JSON.stringify(rows)).byteLength;
function validate(tablets) {
  if (!tablets.length || tablets[0].start !== 0 || tablets.at(-1).end !== HASH_SPACE) throw new Error('Tablet map must cover hash space');
  const ids = new Set();
  for (let i = 0; i < tablets.length; i++) {
    const t = tablets[i];
    if (typeof t.id !== 'string' || !t.id || ids.has(t.id) || !Number.isInteger(t.start) || !Number.isInteger(t.end) || t.start >= t.end || (i && tablets[i - 1].end !== t.start)) throw new Error('Invalid or overlapping tablet map');
    ids.add(t.id);
  }
}
export function planTablets(inputSchema, rows, options = {}) {
  const { splitBytes = 1024, mergeBytes = 256, maxTablets = 16 } = options;
  if (!Number.isSafeInteger(splitBytes) || splitBytes < 4 || !Number.isSafeInteger(mergeBytes) || mergeBytes < 2 || mergeBytes >= splitBytes || !Number.isSafeInteger(maxTablets) || maxTablets < 1 || maxTablets > 256) throw new Error('Invalid tablet sizing options');
  const schema = structuredClone(inputSchema);
  const initialized = !schema.tablets;
  if (initialized) { schema.tablets = [{ id: 't0', start: 0, end: HASH_SPACE }]; schema.epoch = 1; }
  if (!Number.isSafeInteger(schema.epoch) || schema.epoch < 1) throw new Error('Invalid routing epoch');
  validate(schema.tablets);
  if (schema.tablets.length > maxTablets) throw new Error('Existing tablet count exceeds configured maximum');
  const baseEpoch = schema.epoch;
  let sequence = 0;
  const nextId = () => `e${baseEpoch + 1}t${++sequence}`;
  const events = initialized ? [{ type: 'initialize', tablet: 't0' }] : [];
  const partitions = schema.tablets.map(t => ({ ...t, rows: [] }));
  for (const row of rows) {
    const hash = keyHash(row[schema.primaryKey]);
    const partition = partitions.find(t => t.start <= hash && hash < t.end);
    if (!partition) throw new Error('Row is outside tablet map');
    partition.rows.push(row);
  }
  // Merge only aligned equal-width siblings. Repeat bottom-up; sparse siblings
  // may collapse several levels in one transaction, without changing routing early.
  let merged;
  do {
    merged = false;
    for (let i = 0; i + 1 < partitions.length; i++) {
      const a = partitions[i], b = partitions[i + 1], width = a.end - a.start;
      if (b.end - b.start !== width || a.end !== b.start || a.start % (2 * width) !== 0) continue;
      const combined = [...a.rows, ...b.rows];
      if (rowsBytes(combined) > mergeBytes) continue;
      const next = { id: nextId(), start: a.start, end: b.end, rows: combined };
      partitions.splice(i, 2, next);
      events.push({ type: 'merge', from: [a.id, b.id], to: next.id });
      merged = true; break;
    }
  } while (merged);
  // Strict midpoint partitioning means a single hot key / one-sided range can
  // remain oversized. Never manufacture empty tablets just to satisfy a target.
  for (let i = 0; i < partitions.length && partitions.length < maxTablets; i++) {
    const current = partitions[i];
    if (rowsBytes(current.rows) <= splitBytes || current.end - current.start < 2) continue;
    const midpoint = Math.floor((current.start + current.end) / 2);
    const leftRows = [], rightRows = [];
    for (const row of current.rows) (keyHash(row[schema.primaryKey]) < midpoint ? leftRows : rightRows).push(row);
    if (!leftRows.length || !rightRows.length) continue;
    const left = { id: nextId(), start: current.start, end: midpoint, rows: leftRows };
    const right = { id: nextId(), start: midpoint, end: current.end, rows: rightRows };
    partitions.splice(i, 1, left, right);
    events.push({ type: 'split', from: current.id, to: [left.id, right.id], boundary: midpoint });
    i--; // recursively consider the left child, then its right sibling
  }
  const topologyChanged = events.some(e => e.type === 'split' || e.type === 'merge');
  if (topologyChanged) schema.epoch++;
  schema.tablets = partitions.map(({ id, start, end }) => ({ id, start, end }));
  validate(schema.tablets);
  return {
    schema, changed: initialized || topologyChanged, events,
    partitions: partitions.map(({ id, rows }) => ({ id, rows, bytes: rowsBytes(rows) })),
  };
}
