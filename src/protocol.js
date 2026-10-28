import { decodeRpcResponse } from './rpc-response.js';
/** Synthetic actor-local P-256 identities authenticate immutable plan votes. */
export function canonical(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  return `{${Object.keys(value).sort().filter(k => value[k] !== undefined).map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
}
const bytes = value => new TextEncoder().encode(canonical(value));
const hex = value => [...new Uint8Array(value)].map(v => v.toString(16).padStart(2,'0')).join('');
export async function hash(value) { return hex(await crypto.subtle.digest('SHA-256', bytes(value))); }
export const manifestHash = participants => hash([...participants].sort((a,b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
export async function generateIdentity() {
  const keys = await crypto.subtle.generateKey({name:'ECDSA',namedCurve:'P-256'},true,['sign','verify']);
  return {publicKey:await crypto.subtle.exportKey('jwk',keys.publicKey),privateKey:await crypto.subtle.exportKey('jwk',keys.privateKey)};
}
async function sign(key, body) {
  const imported = await crypto.subtle.importKey('jwk',key,{name:'ECDSA',namedCurve:'P-256'},false,['sign']);
  return {...body,signature:hex(await crypto.subtle.sign({name:'ECDSA',hash:'SHA-256'},imported,bytes(body)))};
}
async function verify(key, record) {
  try {
    if (!record || typeof record.signature !== 'string' || !/^[0-9a-f]{128}$/.test(record.signature)) return false;
    const {signature,...body} = record;
    const imported = await crypto.subtle.importKey('jwk',key,{name:'ECDSA',namedCurve:'P-256'},false,['verify']);
    return crypto.subtle.verify({name:'ECDSA',hash:'SHA-256'},imported,Uint8Array.from(signature.match(/../g),v=>parseInt(v,16)),bytes(body));
  } catch { return false; }
}
function identity(a,name,domain) { return {domain,version:1,id:a.id,epoch:a.epoch,step:a.step,name,manifestHash:a.manifestHash}; }
function participant(a,name) { const p=a.participants.find(p=>p.name===name); if(!p) throw new Error('Unknown plan participant'); return p; }
export async function signReadSet(key,a,name,outcome) {
  if(!['COMMIT','ABORT'].includes(outcome)) throw new Error('Invalid vote');
  return sign(key,{...identity(a,name,'murmur/readset'),bodyHash:participant(a,name).bodyHash,outcome});
}
export async function verifyReadSet(a,r) {
  try { const p=participant(a,r.name); const expected={...identity(a,r.name,'murmur/readset'),bodyHash:p.bodyHash,outcome:r.outcome}; return ['COMMIT','ABORT'].includes(r.outcome) && canonical({...r,signature:undefined})===canonical(expected) && await verify(p.publicKey,r); } catch { return false; }
}
export const decisionHash = (a,outcome) => hash({...identity(a,undefined,'murmur/decision'),outcome});
export async function validateCertificate(a,certificate) {
  if(!Array.isArray(certificate)||certificate.length>64||new Set(certificate.map(r=>r?.name)).size!==certificate.length) throw new Error('Invalid certificate');
  for(const r of certificate) if(!await verifyReadSet(a,r)) throw new Error('Unauthenticated ReadSet');
  const outcome=certificate.some(r=>r.outcome==='ABORT')?'ABORT':'COMMIT';
  if(outcome==='COMMIT' && (certificate.length!==a.participants.length||a.participants.some(p=>!certificate.some(r=>r.name===p.name)))) throw new Error('Incomplete COMMIT certificate');
  return {outcome,decisionHash:await decisionHash(a,outcome),certificate:[...certificate].sort((a,b)=>a.name<b.name?-1:1)};
}
export async function signAck(key,a,name,outcome,decision,phase='applied') {
  return sign(key,{...identity(a,name,'murmur/ack'),phase,outcome,decisionHash:decision});
}
export async function verifyAck(a,r,phase='applied') {
  try { const p=participant(a,r.name), expected={...identity(a,r.name,'murmur/ack'),phase,outcome:r.outcome,decisionHash:r.decisionHash};
    if(canonical({...r,signature:undefined})!==canonical(expected)) return false;
    if(phase==='delivered') { if(r.outcome!=='DELIVERED'||r.decisionHash!==null) return false; }
    else if(!['COMMIT','ABORT'].includes(r.outcome)||r.decisionHash!==await decisionHash(a,r.outcome)) return false;
    return verify(p.publicKey,r);
  } catch { return false; }
}
export async function rpc(namespace,name,input) {
  const response=await namespace.get(namespace.idFromName(name)).fetch('https://actor/',{method:'POST',body:JSON.stringify(input)});
  const value=await decodeRpcResponse(response,`${name}:${input.op}`); if(!response.ok) throw Object.assign(new Error(value.error||'Actor request failed'),{status:response.status,code:value.code,rpc:value.rpc,retryAfterMs:value.retryAfterMs}); return value;
}
