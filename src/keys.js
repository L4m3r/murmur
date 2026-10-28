/** Order-preserving POC keys: NULL, IEEE-754 number, UTF-8 TEXT, BOOLEAN.
 * Composite keys concatenate self-delimiting components with no closing suffix,
 * so encodeKey([value]) is a true prefix of encodeKey([value, primaryKey]). */
const encoder = new TextEncoder();
function encodeUnchecked(key) {
  const scalar = value => {
    if (value === null) return '0';
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) throw new Error('Key number must be finite');
      const bytes = new ArrayBuffer(8), view = new DataView(bytes);
      view.setFloat64(0, Object.is(value, -0) ? 0 : value, false);
      let bits = view.getBigUint64(0, false);
      bits = bits & 0x8000000000000000n ? (~bits & 0xffffffffffffffffn) : (bits ^ 0x8000000000000000n);
      return '1' + bits.toString(16).padStart(16, '0');
    }
    if (typeof value === 'string') {
      // TextEncoder would silently replace malformed UTF-16 with U+FFFD.
      for (let i = 0; i < value.length; i++) {
        const c = value.charCodeAt(i);
        if (c >= 0xd800 && c <= 0xdbff) { const d = value.charCodeAt(++i); if (!(d >= 0xdc00 && d <= 0xdfff)) throw new Error('TEXT key contains lone surrogate'); }
        else if (c >= 0xdc00 && c <= 0xdfff) throw new Error('TEXT key contains lone surrogate');
      }
      return '2' + Array.from(encoder.encode(value), byte => (byte + 1).toString(16).padStart(3, '0')).join('') + '000';
    }
    if (typeof value === 'boolean') return value ? '31' : '30';
    throw new Error('Unsupported key type');
  };
  const encoded = Array.isArray(key) ? '4' + key.map(scalar).join('') : scalar(key);
  return encoded;
}
export function encodeKey(key) {
  const encoded = encodeUnchecked(key);
  if (encoded.length > 1800) throw new Error('Encoded key exceeds 1800 bytes');
  return encoded;
}
export function compareKeys(a, b) {
  const x = encodeUnchecked(a), y = encodeUnchecked(b);
  return x === y ? 0 : x < y ? -1 : 1;
}
export function keyOfRow(schema, row) {
  return schema.keyColumns ? schema.keyColumns.map(column => row[column]) : row[schema.primaryKey];
}
export function rangeForFilters(filters, column, { compositePrefix = false } = {}) {
  const range = { lower: null, upper: null, lowerInclusive: true, upperInclusive: false, empty: false };
  function lower(value, inclusive) {
    if (range.lower === null || value > range.lower) { range.lower = value; range.lowerInclusive = inclusive; }
    else if (value === range.lower) range.lowerInclusive &&= inclusive;
  }
  function upper(value, inclusive) {
    if (range.upper === null || value < range.upper) { range.upper = value; range.upperInclusive = inclusive; }
    else if (value === range.upper) range.upperInclusive &&= inclusive;
  }
  for (const f of filters || []) {
    if (f.column !== column) continue;
    if (f.value === null) { range.empty = true; continue; } // all SQL comparisons with NULL are unknown
    if (!['=', '<', '<=', '>', '>=', '!=', '<>'].includes(f.op)) continue;
    // SQL comparisons never match NULL, including the unbounded side of '<'.
    lower(compositePrefix ? encodeKey([null]) + '~' : encodeKey(null), compositePrefix);
    if (f.op === '!=' || f.op === '<>') continue; // residual comparison, but NULL is still pruned
    const encoded = encodeKey(compositePrefix ? [f.value] : f.value);
    if (compositePrefix) {
      if (f.op === '=') { lower(encoded, true); upper(encoded + '~', false); }
      if (f.op === '>') lower(encoded + '~', true);
      if (f.op === '>=') lower(encoded, true);
      if (f.op === '<') upper(encoded, false);
      if (f.op === '<=') upper(encoded + '~', false);
    } else {
      if (f.op === '=') { lower(encoded, true); upper(encoded, true); }
      if (f.op === '>') lower(encoded, false);
      if (f.op === '>=') lower(encoded, true);
      if (f.op === '<') upper(encoded, false);
      if (f.op === '<=') upper(encoded, true);
    }
  }
  if (range.lower !== null && range.upper !== null && (range.lower > range.upper || (range.lower === range.upper && !(range.lowerInclusive && range.upperInclusive)))) range.empty = true;
  return range;
}
/** Caller must short-circuit range.empty before issuing a storage operation. */
export function storageRange(range) {
  return {
    prefix: 'row:',
    start: range.lower === null ? 'row:' : 'row:' + range.lower + (range.lowerInclusive ? '' : '\u0000'),
    end: range.upper === null ? 'row:~' : 'row:' + range.upper + (range.upperInclusive ? '\u0000' : ''),
  };
}
