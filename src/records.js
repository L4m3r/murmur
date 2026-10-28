/** Chunked JSON records for DO storage. All mutation calls MUST run inside the
 * caller's storage.transaction callback: chunks, pointer publication and old
 * generation cleanup then commit or roll back together. Use that same contract
 * for reads if concurrent writers are possible; a nontransactional read is safe
 * only when the owning actor serializes access. No legacy inline-data fallback.
 *
 * The default byte budget is configurable backpressure, not a storage-value
 * limit. Small chunks remain below both legacy KV's 128 KiB value ceiling
 * and SQLite-backed DO's 2 MB combined key/value ceiling.
 */
export const RECORD_CHUNK_BYTES = 48 * 1024;
export const DEFAULT_RECORD_MAX_BYTES = 16 * 1024 * 1024;
export const RECORD_INLINE_BYTES = 16 * 1024;
const PREFIX = '__records_chunk_v1/';
const KIND = 'murmur.chunked-json';
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });
const hex = bytes => Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, '0')).join('');
const sha256 = async bytes => hex(await crypto.subtle.digest('SHA-256', bytes));
const corrupt = message => Object.assign(new Error(`Corrupt chunked record: ${message}`), { code: 'RECORD_CORRUPT' });
function validateKey(key) {
  if (typeof key !== 'string' || key.length === 0 || encoder.encode(key).byteLength > 2048 || key.startsWith(PREFIX)) throw new TypeError('Record key must be 1..2048 UTF-8 bytes and outside the reserved chunk namespace');
}
function validateBudget(maxBytes) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) throw new TypeError('maxBytes must be a nonnegative safe integer');
}
function isRaw(pointer) {
  return !pointer || typeof pointer !== 'object' || !['kind', 'encoding', 'generation', 'keyHash', 'chunkBytes', 'chunks'].some(field => Object.hasOwn(pointer, field));
}
async function validateManifest(key, manifest) {
  if (!manifest || typeof manifest !== 'object' || manifest.kind !== KIND || manifest.version !== 1 || !['inline','chunks'].includes(manifest.encoding) || typeof manifest.generation !== 'string' || !/^[a-f0-9-]{36}$/.test(manifest.generation) || typeof manifest.hash !== 'string' || !/^[a-f0-9]{64}$/.test(manifest.hash) || !Number.isSafeInteger(manifest.bytes) || manifest.bytes < 1) throw corrupt('invalid format/version envelope');
  if (manifest.encoding === 'inline') {
    if (typeof manifest.json !== 'string' || manifest.bytes > RECORD_INLINE_BYTES || manifest.chunks !== 0) throw corrupt('invalid inline envelope');
  } else if (manifest.chunkBytes !== RECORD_CHUNK_BYTES || !Number.isSafeInteger(manifest.chunks) || manifest.chunks !== Math.ceil(manifest.bytes / RECORD_CHUNK_BYTES)) throw corrupt('invalid chunk envelope');
  if (manifest.keyHash !== await sha256(encoder.encode(key))) throw corrupt('logical key digest mismatch');
  return manifest;
}
function chunkKey(manifest, index) {
  return `${PREFIX}${manifest.keyHash}/${manifest.generation}/${String(index).padStart(8, '0')}`;
}
async function removeChunks(storage, manifest) {
  for (let i = 0; i < manifest.chunks; i++) await storage.delete(chunkKey(manifest, i));
}

/** Return the JSON value or undefined for an absent record. A read budget avoids
 * allocation from an unexpectedly large pointer. Raise maxBytes consistently
 * with the writer when an application intentionally supports larger records. */
export async function readRecord(storage, key, { maxBytes = DEFAULT_RECORD_MAX_BYTES, allowRaw = false } = {}) {
  validateKey(key); validateBudget(maxBytes);
  const pointer = await storage.get(key);
  if (pointer === undefined) return undefined;
  if (allowRaw && isRaw(pointer)) return pointer; // explicit fixture adapter only
  const manifest = await validateManifest(key, pointer);
  if (manifest.bytes > maxBytes) throw Object.assign(new Error(`Record exceeds read budget (${manifest.bytes} > ${maxBytes} bytes)`), { code: 'RECORD_TOO_LARGE' });
  const payload = manifest.encoding === 'inline' ? encoder.encode(manifest.json) : new Uint8Array(manifest.bytes);
  if (payload.byteLength !== manifest.bytes) throw corrupt('inline payload length mismatch');
  for (let i = 0; i < manifest.chunks; i++) {
    const stored = await storage.get(chunkKey(manifest, i));
    const chunk = stored instanceof Uint8Array ? stored : stored instanceof ArrayBuffer ? new Uint8Array(stored) : null;
    const expected = Math.min(RECORD_CHUNK_BYTES, manifest.bytes - i * RECORD_CHUNK_BYTES);
    if (!chunk || chunk.byteLength !== expected) throw corrupt(`missing or invalid chunk ${i}`);
    payload.set(chunk, i * RECORD_CHUNK_BYTES);
  }
  if (await sha256(payload) !== manifest.hash) throw corrupt('payload digest mismatch');
  try { return JSON.parse(decoder.decode(payload)); }
  catch { throw corrupt('payload is not valid UTF-8 JSON'); }
}

/** Transaction-only mutation. A complete new immutable generation is written
 * before its manifest pointer, then the old generation is removed in the SAME
 * caller transaction. Returns compact accounting metadata for resource budgets. */
export async function writeRecord(storage, key, value, { maxBytes = DEFAULT_RECORD_MAX_BYTES, inlineBytes = RECORD_INLINE_BYTES, allowRaw = false } = {}) {
  validateKey(key); validateBudget(maxBytes);
  if (!Number.isSafeInteger(inlineBytes) || inlineBytes < 0 || inlineBytes > RECORD_INLINE_BYTES) throw new TypeError('inlineBytes must be 0..16384');
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new TypeError('Record value must be JSON-serializable');
  const payload = encoder.encode(serialized);
  if (payload.byteLength > maxBytes) throw Object.assign(new Error(`Record exceeds write budget (${payload.byteLength} > ${maxBytes} bytes)`), { code: 'RECORD_TOO_LARGE' });
  const oldPointer = await storage.get(key);
  const old = oldPointer === undefined || (allowRaw && isRaw(oldPointer)) ? undefined : await validateManifest(key, oldPointer);
  const manifest = {
    kind: KIND, version: 1, encoding: payload.byteLength <= inlineBytes ? 'inline' : 'chunks', generation: crypto.randomUUID(),
    keyHash: await sha256(encoder.encode(key)), hash: await sha256(payload),
    bytes: payload.byteLength, chunkBytes: RECORD_CHUNK_BYTES,
    chunks: payload.byteLength <= inlineBytes ? 0 : Math.ceil(payload.byteLength / RECORD_CHUNK_BYTES),
    ...(payload.byteLength <= inlineBytes ? { json: serialized } : {}),
  };
  for (let i = 0; i < manifest.chunks; i++) await storage.put(chunkKey(manifest, i), payload.slice(i * RECORD_CHUNK_BYTES, (i + 1) * RECORD_CHUNK_BYTES));
  await storage.put(key, manifest);
  if (old) await removeChunks(storage, old);
  return { bytes: manifest.bytes, chunks: manifest.chunks, hash: manifest.hash, generation: manifest.generation };
}

/** Transaction-only deletion. Does not enumerate or delete unrelated records. */
export async function deleteRecord(storage, key, { allowRaw = false } = {}) {
  validateKey(key);
  const pointer = await storage.get(key);
  if (pointer === undefined) return false;
  if (allowRaw && isRaw(pointer)) { await storage.delete(key); return true; }
  const manifest = await validateManifest(key, pointer);
  await storage.delete(key);
  await removeChunks(storage, manifest);
  return true;
}
