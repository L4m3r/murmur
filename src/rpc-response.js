/** Decode actor replies without turning runtime plaintext failures into JSON syntax errors.
 * Valid JSON (including application error envelopes) is returned unchanged: callers
 * retain their existing status/error mapping. This helper never retries a request.
 */
const ERROR_BYTES = 4096;
const MESSAGE_CHARS = 512;

async function boundedText(response) {
  if (!response.body) return {text:'',truncated:false};
  const reader = response.body.getReader();
  const chunks = [];
  let length = 0, truncated = false;
  try {
    while (length < ERROR_BYTES) {
      const {value,done} = await reader.read();
      if (done) break;
      const take = Math.min(value.byteLength, ERROR_BYTES - length);
      chunks.push(value.subarray(0,take));
      length += take;
      if (take < value.byteLength || length === ERROR_BYTES) {
        truncated = true;
        // Do not await cancellation: an upstream producer may never acknowledge it.
        void reader.cancel().catch(() => {});
        break;
      }
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk,offset); offset += chunk.byteLength; }
  return {text:new TextDecoder().decode(bytes),truncated};
}

function transportError(response, actor, text, truncated = false) {
  const retryAfter = response.headers.get('retry-after');
  const seconds = retryAfter !== null && /^\d+(?:\.\d+)?$/.test(retryAfter.trim()) ? Number(retryAfter) : null;
  const parsedDate = retryAfter === null || seconds !== null ? NaN : Date.parse(retryAfter);
  const delay = seconds !== null ? seconds * 1000 : Number.isFinite(parsedDate) ? parsedDate - Date.now() : 0;
  const retryAfterMs = Math.min(60000,Math.max(0,delay));
  const code = response.ok ? 'RPC_PROTOCOL_ERROR'
    : response.headers.has('x-celld-overload') ? 'RPC_BACKPRESSURE' : 'RPC_UNAVAILABLE';
  const excerpt = String(text).replace(/[\u0000-\u001f\u007f]/g,' ').trim().slice(0,MESSAGE_CHARS);
  return Object.assign(new Error(`${code}: actor ${String(actor).slice(0,128)} returned HTTP ${response.status}${excerpt ? `: ${excerpt}` : ' without a JSON response'}${truncated ? ' [truncated]' : ''}`), {
    code, status:response.ok ? 502 : response.status, responseStatus:response.status,
    actor, retryAfter, retryAfterMs,
    rpc: {
      actor:String(actor).slice(0,128), responseStatus:response.status,
      overload:response.headers.get('x-celld-overload')?.slice(0,64) ?? null,
      retryAfter:retryAfter?.slice(0,128) ?? null,
      bodyExcerpt:excerpt, truncated,
    },
  });
}

export async function decodeRpcResponse(response, actor = 'unknown') {
  const type = (response.headers.get('content-type') || '').split(';',1)[0].trim().toLowerCase();
  const declaredJson = type === 'application/json' || type.endsWith('+json');
  // JSON actor envelopes may legitimately exceed the diagnostic-body budget.
  // Preserve these bodies and all metadata for the caller, rather than truncating
  // a valid error envelope into an unrelated transport failure.
  if (response.ok || declaredJson) {
    try { return await response.json(); }
    catch { throw transportError(response,actor,'Malformed JSON response'); }
  }
  const {text,truncated} = await boundedText(response);
  if (!truncated) {
    try { return JSON.parse(text); } catch { /* bounded diagnostic follows */ }
  }
  throw transportError(response,actor,text,truncated);
}
