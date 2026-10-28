import { decodeRpcResponse } from './rpc-response.js';
import { canonical, hash, generateIdentity, signReadSet, verifyReadSet, validateCertificate, signAck, rpc } from './protocol.js';
import { encodeKey } from './keys.js';
import { readRecord, writeRecord, deleteRecord } from './records.js';
const RECORD_OPTIONS = { maxBytes: 2 * 1024 * 1024 };
const jsonBytes = value => new TextEncoder().encode(JSON.stringify(value)).byteLength;

const json = (value, status = 200) => Response.json(value, { status });
const all = { lower: null, upper: null, lowerInclusive: true, upperInclusive: false };
const conflict = (message, owners = []) => Object.assign(new Error(message), { status: 409, code: 'CONFLICT', owners: [...new Set(owners.filter(Boolean))] });
const token = step => String(step).padStart(16, '0');
const rowPrefix = key => `row:${encodeKey(key)}:`;
const contains = (range, encoded) => !range.empty &&
  (range.lower == null || encoded > range.lower || (encoded === range.lower && range.lowerInclusive)) &&
  (range.upper == null || encoded < range.upper || (encoded === range.upper && range.upperInclusive));
const touches = (reads, changes, key) => reads.some(r => contains(r, key)) || changes.some(c => c.encoded === key);
function stable(value) {
  if (Array.isArray(value)) return '[' + value.map(stable).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).filter(k => value[k] !== undefined).sort().map(k => JSON.stringify(k) + ':' + stable(value[k])).join(',') + '}';
  return JSON.stringify(value);
}
async function digest(text) {
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(hash)].map(v => v.toString(16).padStart(2, '0')).join('');
}
const READ_PAGE_SIZE = 256;
const READ_RESPONSE_BYTES = 2 * 1024 * 1024 - 1024;
function headBounds(range = all, cursor, reverse = false) {
  let start = range.lower == null ? 'head:' : 'head:' + range.lower + (range.lowerInclusive ? '' : '\u0000');
  let end = range.upper == null ? 'head:~' : 'head:' + range.upper + (range.upperInclusive ? '\u0000' : '');
  if (cursor !== undefined && cursor !== null) {
    if (typeof cursor !== 'string' || cursor.length > 1800 || !/^[0-9a-f]+$/.test(cursor)) throw new Error('Invalid scan cursor');
    if (reverse) {
      // Storage end is exclusive regardless of direction: resume below the
      // last consumed logical key, without fetching it again or skipping it.
      const before = 'head:' + cursor;
      if (before < end) end = before;
    } else {
      const after = 'head:' + cursor + '\u0000';
      if (after > start) start = after;
    }
  }
  return { prefix: 'head:', start, end, ...(reverse ? {reverse:true} : {}) };
}

const requestEpoch = id => { const match = /^e(\d+)_/.exec(String(id)); const epoch = match ? Number(match[1]) : 0; if (!Number.isSafeInteger(epoch)) throw new Error('Invalid request epoch'); return epoch; };
const requestExpired = floor => Object.assign(new Error(`Request epoch is below permanent rejection floor ${floor}`), { status: 410, code: 'REQUEST_EXPIRED' });
const expired = floor => Object.assign(new Error(`Snapshot is below retention floor ${floor}`), { status: 410, code: 'SNAPSHOT_EXPIRED' });
function validSnapshot(value) { if (!Number.isSafeInteger(value) || value < 0) throw new Error('Invalid MVCC snapshot'); }

/** One actor implementation for catalog records, base rows and index entries.
 * Durable proposals are ordered by allocator plans; shard-local validation
 * emits immutable signed peer ReadSets. Only earlier overlapping write
 * footprints conflict, not all work on the tablet. Versions are retained until
 * the fenced snapshot-retention floor permits bounded pruning.
 */
export class DataShard {
  constructor(state, env) { this.storage = state.storage; this.stateId = state.id; this.env = env; this.tail = Promise.resolve(); }
  local(fn) { const next = this.tail.then(fn); this.tail = next.catch(() => {}); return next; }
  fetch(request) {
    return request.json().then(input => ['scan','scanKeys','read','getMany','get'].includes(input.op) ? this.readPeer(input) : input.op === 'drive' ? this.drive(input) : ['acceptPlan','plan','readSet','finalize'].includes(input.op) ? (input.op==='readSet'?this.receiveReadSet(input):input.op==='finalize'?this.finalize(input):this.acceptPlan(input)) : this.local(() => this.handle(input)))
      .catch(e => json({ error: e.message, code: e.code || 'SHARD_ERROR', rpc:e.rpc,retryAfterMs:e.retryAfterMs,retryable: e.status !== 410 && e.status !== 400, owners: e.owners || [], dependencies: e.dependencies || [] }, e.status || 503));
  }
  async alarm() {
    const batch=await this.local(async()=>{
      const ids=(await this.footprints()).map(f=>f.id).sort();
      const cursor=await this.storage.get('alarmCursor');
      const after=cursor?ids.findIndex(id=>id>cursor):0;
      const start=after<0?0:after;
      const selected=[...ids.slice(start),...ids.slice(0,start)].slice(0,8);
      if(selected.length)await this.storage.put('alarmCursor',selected.at(-1));
      return {ids:selected,remaining:ids.length};
    });
    await Promise.all(batch.ids.map(async id=>{try{await this.drive({id});}catch{}}));
    if(batch.remaining&&this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1000);
  }
  async mediator(input) {
    const authority = this.env.ALLOCATOR;
    const response = await authority.get(authority.idFromName('global')).fetch('https://mediator/internal', { method: 'POST', body: JSON.stringify(input) });
    const body = await decodeRpcResponse(response,`allocator:${input.op}`);
    if (!response.ok) throw Object.assign(new Error(body.error || 'Mediator unavailable'), { ...body, status: response.status });
    return body;
  }
  async meta(storage, snapshot = Number.MAX_SAFE_INTEGER) {
    const history = await storage.list({ prefix: 'meta:', end: `meta:${token(snapshot)}\u0000`, reverse: true, limit: 1 });
    return history.values().next().value || { step: 0, epoch: 0, retired: false };
  }
  async version(storage, key, snapshot, keysOnly = false) {
    const prefix = rowPrefix(key);
    const history = await storage.list({ prefix, end: `${prefix}${token(snapshot)}\u0000`, reverse: true, limit: 1 });
    const keyAtSnapshot = history.keys().next().value;
    if (keyAtSnapshot === undefined) return undefined;
    const root = history.values().next().value;
    if (root && Object.hasOwn(root, 'step') && Object.hasOwn(root, 'row')) return root;
    if (keysOnly) {
      // writeVersion chunks only large, non-null rows. A tombstone always fits
      // the raw root; presence can therefore be established without fetching
      // payload chunks. Validate the pointer and physical version identity.
      const stepText = keyAtSnapshot.slice(prefix.length), step = Number(stepText);
      if (!/^\d{16}$/.test(stepText) || !Number.isSafeInteger(step) ||
          root?.kind !== 'murmur.chunked-json' || root.version !== 1 || root.encoding !== 'chunks' ||
          !Number.isSafeInteger(root.bytes) || root.bytes <= 16384 || root.bytes > RECORD_OPTIONS.maxBytes ||
          root.chunkBytes !== 49152 || root.chunks !== Math.ceil(root.bytes / root.chunkBytes) ||
          !/^[a-f0-9-]{36}$/.test(root.generation || '') || !/^[a-f0-9]{64}$/.test(root.hash || '') ||
          root.keyHash !== await digest(keyAtSnapshot)) {
        throw Object.assign(new Error('Invalid version presence root'), { code: 'RECORD_CORRUPT' });
      }
      return { key, step, row: true };
    }
    return readRecord(storage, keyAtSnapshot, { ...RECORD_OPTIONS, allowRaw: true });
  }
  async assertReadVisible(input, capturedFootprints) {
    const pointLookup = input.op === 'getMany' || input.op === 'get';
    const keys = pointLookup ? (input.op === 'get' ? [input.key] : input.keys) : null;
    if (pointLookup && (!Array.isArray(keys) || keys.length > 2000)) throw Object.assign(new Error('At most 2000 point keys'), { status: 400 });
    const requested = pointLookup ? new Set(keys.map(key => encodeKey(key))) : null;
    const range = input.range || all;
    // Check the complete logical predicate, not just this physical page or
    // existing heads: a planned insert may be an absent-key phantom. Missing
    // plans can only receive a step above this already-fixed allocated snapshot.
      for(const pending of capturedFootprints || await this.footprints()) {
        const relevant=pending.meta||pending.changes.some(c=>requested?requested.has(c.encoded):contains(range,c.encoded));
        if(!relevant)continue;
        const found=await rpc(this.env.ALLOCATOR,'global',{op:'lookup',id:pending.id});
        if(found.state==='assigned'&&found.association.step<=input.snapshot)throw Object.assign(new Error('Relevant planned writes are not yet applied'),{status:409,code:'VISIBILITY_PENDING',owners:[],dependencies:[{id:pending.id,bucket:found.association.bucket}]});
        if(!['absent','aborted','assigned'].includes(found.state))throw new Error('Unknown allocation coverage');
      }
  }
  async readPeer(input) {
    validSnapshot(input.snapshot);
    const footprints=await this.local(async()=>{
      const floor=await this.storage.get('collectionFloor')||0;
      if(input.snapshot<floor)throw expired(floor);
      return this.footprints();
    });
    // Every allocation <= this fixed snapshot required an already-persisted
    // proposal. Later proposals cannot enter its visibility dependency set.
    await this.assertReadVisible(input,footprints);
    return this.local(()=>this.read(input,true));
  }
  async read(input, visibilityChecked = false) {
    validSnapshot(input.snapshot);
    const floor = await this.storage.get('collectionFloor') || 0;
    if (input.snapshot < floor) throw expired(floor);
    if(!visibilityChecked)await this.assertReadVisible(input);
    const meta = await this.meta(this.storage, input.snapshot);
    if (meta.retired || (input.epoch !== undefined && input.epoch !== meta.epoch)) return json({ error: 'Stale routing epoch at snapshot', code: 'STALE_EPOCH' }, 409);
    const pointLookup = input.op === 'getMany' || input.op === 'get';
    if(input.reverse !== undefined && typeof input.reverse !== 'boolean')return json({error:'reverse must be boolean'},400);
    const reverse = !pointLookup && input.reverse === true;
    let keys = [], nextCursor = null, headEntriesRead = 0, moreHeads = false;
    if (pointLookup) {
      keys = input.op === 'get' ? [input.key] : input.keys;
      if (!Array.isArray(keys) || keys.length > 2000) return json({ error: 'At most 2000 point keys' }, 400);
      keys = [...new Map(keys.map(key => [encodeKey(key), key])).values()];
    } else if (!input.range?.empty) {
      const pageSize = input.pageSize ?? READ_PAGE_SIZE;
      if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > READ_PAGE_SIZE) return json({ error: 'Scan pageSize must be 1..256' }, 400);
      const options = headBounds(input.range, input.cursor, reverse);
      // Each page walks logical keys, not retained versions. A newer-than-snapshot
      // key can yield no row, but still advances the cursor to guarantee progress.
      const heads = options.start >= options.end ? new Map() : await this.storage.list({ ...options, limit: pageSize + 1 });
      headEntriesRead = heads.size;
      const page = [...heads.values()].slice(0, pageSize);
      keys = page.map(head => head.key);
      moreHeads = heads.size > pageSize;
    }
    const keysOnly = input.op === 'scanKeys';
    const latest = [];
    let versionsRead = 0, keysRead = 0, processedKeys = 0, rowsBytes = 2, lastProcessed = null, byteLimited = false;
    for (const key of keys) {
      const version = await this.version(this.storage, key, input.snapshot, keysOnly); keysRead++;
      if (version) {
        versionsRead++;
        if (version.row !== null) {
          const nextBytes = rowsBytes + jsonBytes(keysOnly ? key : version.row) + (latest.length ? 1 : 0);
          if (nextBytes > (keysOnly ? 64 * 1024 : READ_RESPONSE_BYTES) && latest.length) {
            if (pointLookup) return json({ error: 'Point batch response exceeds 2 MiB; request fewer keys', code: 'RESPONSE_TOO_LARGE', retryable: false }, 400);
            // The overflowing key was fetched but not consumed. Resume after
            // the previous processed head, never after the prefetched page tail.
            nextCursor = lastProcessed; byteLimited = true; break;
          }
          rowsBytes = nextBytes; latest.push(keysOnly ? { key } : version);
        }
      }
      lastProcessed = encodeKey(key); processedKeys++;
    }
    if (!pointLookup && !byteLimited && moreHeads) nextCursor = lastProcessed;
    latest.sort((a, b) => { const x = encodeKey(a.key), y = encodeKey(b.key); return (x < y ? -1 : x > y ? 1 : 0) * (reverse ? -1 : 1); });
    if (keysOnly) return json({ keys: latest.map(version => version.key), versionsRead, keysRead, processedKeys, keysBytes: rowsBytes, headEntriesRead, nextCursor, done: nextCursor === null, epoch: meta.epoch });
    const rows = latest.map(version => version.row);
    return json({ rows, rowsRead: rows.length, versionsRead, keysRead, processedKeys, rowsBytes, headEntriesRead, nextCursor, done: nextCursor === null,
      epoch: meta.epoch, retired: meta.retired, ...(input.op === 'get' ? { row: rows[0] || null } : {}) });
  }
  async validateHeads(txn, snapshot, reads, changes) {
    const checked = new Set();
    const check = head => {
      if (!head || checked.has(head.encoded)) return;
      checked.add(head.encoded);
      if (head.step > snapshot) throw conflict('Read/write version changed since snapshot');
    };
    // Point writes/reads use a single durable head lookup. Range validation still
    // observes deleted keys and versions above the safe frontier, preventing
    // phantom/lost-update gaps without reading historical row payloads.
    for (const change of changes) check(await txn.get(`head:${change.encoded}`));
    for (const range of new Map(reads.map(range => [stable(range), range])).values()) {
      if (range.empty) continue;
      if (range.lower != null && range.lower === range.upper && range.lowerInclusive && range.upperInclusive) {
        check(await txn.get(`head:${range.lower}`)); continue;
      }
      let cursor;
      do {
        const options = headBounds(range, cursor);
        if (options.start >= options.end) break;
        const page = await txn.list({ ...options, limit: READ_PAGE_SIZE });
        for (const head of page.values()) check(head);
        if (page.size < READ_PAGE_SIZE) break;
        cursor = [...page.values()].at(-1).encoded;
      } while (true);
    }
  }
  async identity() {
    let identity = await this.storage.get('identity');
    if (!identity) { identity = await generateIdentity(); await this.storage.put('identity', identity); }
    return identity;
  }
  async footprints(storage = this.storage) {
    const result = [];
    for (const key of (await storage.list({prefix:'foot:'})).keys()) result.push(await readRecord(storage,key,{maxBytes:4*1024*1024}));
    return result;
  }
  async propose(input) {
    const epoch = requestEpoch(input.id);
    if (this.stateId && String(this.stateId)!==String(this.env.SHARDS.idFromName(input.name))) throw new Error('Proposal actor name mismatch');
    if (input.epoch !== epoch) throw new Error('Proposal epoch mismatch');
    await rpc(this.env.ALLOCATOR,'global',{op:'epoch',requestEpoch:epoch});
    if((await rpc(this.env.ALLOCATOR,'global',{op:'lookup',id:input.id})).state==='aborted')throw conflict('Proposal globally aborted');
    validSnapshot(input.snapshot);
    const names = [...(input.participants || [])].sort();
    if (!names.length || names.length>64 || new Set(names).size!==names.length || !names.includes(input.name)) throw new Error('Invalid proposal participant list');
    if (!Array.isArray(input.reads) || !Array.isArray(input.changes)) throw new Error('Invalid proposal footprint');
    const changes = input.changes.map(c=>({key:c.key,encoded:encodeKey(c.key),row:c.row}));
    if (changes.some(c=>c.row===undefined) || new Set(changes.map(c=>c.encoded)).size!==changes.length) throw new Error('Invalid proposal delta');
    if(input.holdFinalization && ![true,'true'].includes(this.env.ALLOW_FAULTS))throw new Error('Fault injection disabled');
    const body = {id:input.id,name:input.name,owner:input.owner,epoch,snapshot:input.snapshot,participants:names,expectedEpoch:input.expectedEpoch,reads:input.reads,changes,meta:input.meta,...(input.holdFinalization?{holdFinalization:true}:{})};
    const bodyHash = await hash(body), identity = await this.identity();
    const pending = {...body,bodyHash,vote:bodyHash};
    const footprint = {...body,changes:changes.map(({encoded})=>({encoded})),bodyHash};
    if(jsonBytes(pending)>RECORD_OPTIONS.maxBytes) throw new Error('Proposal exceeds 2 MiB backpressure limit');
    await this.storage.transaction(async txn=>{
      const rejected = await txn.get('rejectBeforeEpoch') || 0;
      if(epoch<rejected)throw requestExpired(rejected);
      const floor=await txn.get('collectionFloor') || 0;if(input.snapshot<floor)throw expired(floor);
      if(await txn.get(`aborted:${input.id}`))throw conflict('Proposal permanently aborted');
      const old=await txn.get(`proposal:${input.id}`);
      if(old){if(old.bodyHash!==bodyHash)throw conflict('Proposal identity mismatch');return;}
      const feet=await this.footprints(txn);
      if(feet.length>=64 || feet.reduce((n,f)=>n+jsonBytes(f),jsonBytes(footprint))>4*1024*1024)throw conflict('Active proposal footprint capacity reached',feet.map(f=>f.owner));
      await writeRecord(txn,`pending:${input.id}`,pending,RECORD_OPTIONS);
      await writeRecord(txn,`foot:${input.id}`,footprint,{maxBytes:4*1024*1024});
      if(input.holdFinalization)await txn.put(`hold:${input.id}`,true);
      await txn.put(`proposal:${input.id}`,{id:input.id,name:input.name,bodyHash,publicKey:identity.publicKey,participants:names,epoch,snapshot:input.snapshot,owner:input.owner});
    });
    if(this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1000);
    return json({id:input.id,bodyHash,publicKey:identity.publicKey});
  }
  async checkedAssociation(association) {
    const result=await rpc(this.env.ALLOCATOR,'global',{op:'lookup',id:association.id});
    if(result.state!=='assigned'||canonical(result.association)!==canonical(association))throw new Error('Unassigned or substituted association');
    const proposal=await this.storage.get(`proposal:${association.id}`);
    if(!proposal)throw new Error('Missing durable proposal');
    const participant=association.participants.find(p=>p.name===proposal.name);
    if(!participant||participant.bodyHash!==proposal.bodyHash||canonical(participant.publicKey)!==canonical(proposal.publicKey)||canonical(association.participants.map(p=>p.name).sort())!==canonical(proposal.participants)||association.snapshot!==proposal.snapshot||association.epoch!==proposal.epoch)throw new Error('Association proposal mismatch');
    return proposal;
  }
  async acceptPlan({association}) {
    const proposal=await this.checkedAssociation(association);
    return this.local(async()=>{
      const floor=await this.storage.get('rejectBeforeEpoch')||0;if(association.epoch<floor)throw requestExpired(floor);
      const old=await this.storage.get(`association:${association.id}`);
      if(old&&canonical(old)!==canonical(association))throw new Error('Plan replay mismatch');
      await this.storage.put(`association:${association.id}`,association);
      if(this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1);
      const identity=await this.identity();
      return json({ack:await signAck(identity.privateKey,association,proposal.name,'DELIVERED',null,'delivered')});
    });
  }
  async receiveReadSet({association,readSet}) {
    await this.acceptPlan({association});
    if(!await verifyReadSet(association,readSet))throw new Error('Invalid peer ReadSet');
    return this.local(async()=>{
      const floor=await this.storage.get('rejectBeforeEpoch')||0;if(association.epoch<floor)throw requestExpired(floor);
      const key=`inbox:${association.id}:${readSet.name}`;
      const old=await this.storage.get(key);
      if(old&&canonical(old)!==canonical(readSet))throw new Error('Immutable ReadSet changed');
      await this.storage.put(key,readSet);
      if(this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1);
      return json({received:true});
    });
  }
  earlierConflict(own,other) {
    if(other.meta&&(own.expectedEpoch!==undefined||own.meta||own.reads.length||own.changes.length))return true;
    if(own.meta&&(other.meta||other.changes.length))return true;
    return other.changes.some(c=>touches(own.reads,own.changes,c.encoded));
  }
  async drive({id,release = false,readSetLimit=8,deferFinalize=false}) {
    if(!Number.isSafeInteger(readSetLimit)||readSetLimit<1||readSetLimit>8||typeof deferFinalize!=='boolean')throw Object.assign(new Error('Invalid shard drive work budget'),{status:400,code:'INVALID_DRIVE_BUDGET'});
    if(release)await this.local(async()=>{const floor=await this.storage.get('rejectBeforeEpoch')||0;if(requestEpoch(id)<floor)throw requestExpired(floor);await this.storage.delete(`hold:${id}`);});
    // Only short local stages hold the actor queue. No peer/bucket/allocator
    // delivery is awaited while holding it, avoiding mutual callback deadlock.
    let association=await this.local(()=>this.storage.get(`association:${id}`));
    if(!association){const found=await rpc(this.env.ALLOCATOR,'global',{op:'lookup',id});if(found.state==='aborted'){await this.local(async()=>{await this.storage.transaction(async txn=>{const floor=await txn.get('rejectBeforeEpoch')||0;if(requestEpoch(id)<floor)throw requestExpired(floor);await txn.put(`aborted:${id}`,true);await deleteRecord(txn,`pending:${id}`,RECORD_OPTIONS);await deleteRecord(txn,`foot:${id}`,{maxBytes:4*1024*1024});});});return json({done:true,outcome:'ABORT',applied:false});}if(found.state!=='assigned')return json({pending:true,state:found.state});association=found.association;await this.acceptPlan({association});}
    const delivery=await this.local(async()=>{const proposal=await this.storage.get(`proposal:${id}`),identity=await this.identity();return signAck(identity.privateKey,association,proposal.name,'DELIVERED',null,'delivered');});
    await rpc(this.env.MEDIATORS,association.bucket,{op:'delivered',id,ack:delivery});
    const captured=await this.local(async()=>({own:await readRecord(this.storage,`foot:${id}`,{maxBytes:4*1024*1024}),vote:await this.storage.get(`readset:${id}`),candidates:await this.footprints()}));
    if(!captured.vote&&captured.own){
      let priorConflict=false;
      for(const other of captured.candidates){
        if(other.id===id||!this.earlierConflict(captured.own,other))continue;
        const found=await rpc(this.env.ALLOCATOR,'global',{op:'lookup',id:other.id});
        if(found.state==='assigned'&&found.association.step>captured.own.snapshot&&found.association.step<association.step){
          const state=await rpc(this.env.MEDIATORS,found.association.bucket,{op:'get',id:other.id});
          if(state.decision?.outcome!=='ABORT')priorConflict=true;
        } else if(!['absent','aborted','assigned'].includes(found.state))throw new Error('Ambiguous allocation coverage');
      }
      await this.local(async()=>{
        if(await this.storage.get(`readset:${id}`))return;
        const own=await readRecord(this.storage,`foot:${id}`,{maxBytes:4*1024*1024});if(!own)return;
        let outcome=priorConflict?'ABORT':'COMMIT';
        try{
          const latest=await this.meta(this.storage),snapshot=await this.meta(this.storage,own.snapshot);
          if(latest.retired||(own.expectedEpoch!==undefined&&(snapshot.epoch!==own.expectedEpoch||snapshot.retired||latest.step>own.snapshot))||(own.meta&&latest.step>own.snapshot))throw conflict('Metadata changed');
          await this.validateHeads(this.storage,own.snapshot,own.reads,own.changes);
        }catch(e){if(e.code==='CONFLICT')outcome='ABORT';else throw e;}
        const identity=await this.identity(),readSet=await signReadSet(identity.privateKey,association,own.name,outcome);
        await this.storage.transaction(async txn=>{await txn.put(`readset:${id}`,readSet);await txn.put(`inbox:${id}:${own.name}`,readSet);await txn.put(`outbox:${id}`,association.participants.filter(p=>p.name!==own.name).map(p=>p.name));});
      });
    }
    const outgoing=await this.local(async()=>{
      const floor=await this.storage.get('rejectBeforeEpoch')||0;if(requestEpoch(id)<floor)throw requestExpired(floor);
      const peers=await this.storage.get(`outbox:${id}`)||[];
      // Rotate before delivery, so unavailable peers cannot starve later peers.
      if(peers.length>readSetLimit)await this.storage.put(`outbox:${id}`,[...peers.slice(readSetLimit),...peers.slice(0,readSetLimit)]);
      return {vote:await this.storage.get(`readset:${id}`),peers};
    });
    await Promise.all(outgoing.peers.slice(0,readSetLimit).map(async name=>{try{await rpc(this.env.SHARDS,name,{op:'readSet',association,readSet:outgoing.vote});await this.local(async()=>{const floor=await this.storage.get('rejectBeforeEpoch')||0;if(requestEpoch(id)<floor)throw requestExpired(floor);const peers=await this.storage.get(`outbox:${id}`)||[];await this.storage.put(`outbox:${id}`,peers.filter(p=>p!==name));});}catch{}}));
    const certificate=await this.local(async()=>[...(await this.storage.list({prefix:`inbox:${id}:`})).values()]);
    let state=await rpc(this.env.MEDIATORS,association.bucket,{op:'get',id});
    if(!state.decision&&(certificate.some(r=>r.outcome==='ABORT')||certificate.length===association.participants.length)){await rpc(this.env.MEDIATORS,association.bucket,{op:'decide',id,certificate});state=await rpc(this.env.MEDIATORS,association.bucket,{op:'get',id});}
    if(state.decision&&deferFinalize){if(this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1000);return json({pending:true,vote:outgoing.vote?.outcome,decision:state.decision,applied:false,outcome:state.decision.outcome});}
    if(state.decision){const response=await this.finalize({association,decision:state.decision});const body=await response.json();if(body.held)return json({done:false,held:true,vote:outgoing.vote?.outcome,decision:state.decision,applied:false,outcome:state.decision.outcome});await rpc(this.env.MEDIATORS,association.bucket,{op:'applied',id,ack:body.ack});return json({done:true,vote:outgoing.vote?.outcome,decision:state.decision,applied:true,outcome:state.decision.outcome});}
    if(this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1000);
    return json({pending:true,vote:outgoing.vote?.outcome,decision:null,applied:false});
  }
  async finalize({association,decision,release = false}) {
    const proposal=await this.checkedAssociation(association);
    const state=await rpc(this.env.MEDIATORS,association.bucket,{op:'get',id:association.id});
    if(!state.decision||state.decision.decisionHash!==decision.decisionHash||state.decision.outcome!==decision.outcome)throw new Error('Decision not authoritative');
    const verified=await validateCertificate(association,decision.certificate);
    if(verified.decisionHash!==decision.decisionHash||verified.outcome!==decision.outcome)throw new Error('Invalid decision certificate');
    return this.local(async()=>{
    const floor=await this.storage.get('rejectBeforeEpoch')||0;if(association.epoch<floor)throw requestExpired(floor);
    if(release)await this.storage.delete(`hold:${association.id}`);
    if(await this.storage.get(`hold:${association.id}`))return json({held:true,outcome:decision.outcome});
    await this.storage.transaction(async txn=>{
      const prior=await txn.get(`terminal:${association.id}`);
      if(prior){if(prior!==decision.decisionHash)throw new Error('Terminal decision changed');return;}
      const pending=await readRecord(txn,`pending:${association.id}`,RECORD_OPTIONS);
      if(!pending||pending.bodyHash!==proposal.bodyHash)throw new Error('Missing immutable transaction body');
      if(decision.outcome==='COMMIT'){await this.appendVersions(txn,pending,association.step);await txn.put(`applied:${association.id}`,{step:association.step,vote:proposal.bodyHash});}
      else await txn.put(`aborted:${association.id}`,true);
      await txn.put(`terminal:${association.id}`,decision.decisionHash);
      await deleteRecord(txn,`pending:${association.id}`,RECORD_OPTIONS);
      await deleteRecord(txn,`foot:${association.id}`,{maxBytes:4*1024*1024});
    });
    const identity=await this.identity();return json({ack:await signAck(identity.privateKey,association,proposal.name,decision.outcome,decision.decisionHash)});
    });
  }
  async abortUnassigned(input) {
    if(requestEpoch(input.id)<(await this.storage.get('rejectBeforeEpoch')||0))return json({aborted:true,retired:true});
    const result=await rpc(this.env.ALLOCATOR,'global',{op:'abortIfUnassigned',id:input.id});
    if(result.state!=='aborted')throw conflict('Assigned proposal cannot be aborted outside ReadSet protocol');
    await this.storage.transaction(async txn=>{await txn.put(`aborted:${input.id}`,true);await deleteRecord(txn,`pending:${input.id}`,RECORD_OPTIONS);await deleteRecord(txn,`foot:${input.id}`,{maxBytes:4*1024*1024});});
    return json({aborted:true});
  }
  async appendVersions(txn, pending, step) {
      const stats = await txn.get('stats') || { rows: 0, bytes: 2, payloadBytes: 0 };
      for (const delta of pending.changes) {
        const versionKey = `row:${delta.encoded}:${token(step)}`;
        const version = { key: delta.key, row: delta.row, step: step };
        if (jsonBytes(version) <= 16384) await txn.put(versionKey, version);
        else await writeRecord(txn, versionKey, version, RECORD_OPTIONS);
        const previous = await txn.get(`head:${delta.encoded}`);
        // Independently planned disjoint work can apply out of step order. Heads
        // and maintenance hints must never move backwards on replay/late apply.
        if (!previous || previous.step < step) {
          const present = delta.row !== null;
          const bytes = present ? new TextEncoder().encode(JSON.stringify(delta.row)).byteLength : 0;
          stats.rows += Number(present) - Number(previous?.present || false);
          stats.payloadBytes += bytes - (previous?.bytes || 0);
          await txn.put(`head:${delta.encoded}`, { key: delta.key, encoded: delta.encoded, step: step, present, bytes });
        }
      }
      if (pending.changes.length) {
        stats.bytes = 2 + stats.payloadBytes + Math.max(0, stats.rows - 1);
        await txn.put('stats', stats);
      }
      if (pending.meta) await txn.put(`meta:${token(step)}`, { ...pending.meta, step: step });
  }
  async compact(input) {
    const limit = input.limit ?? 64, pageSize = input.pageSize ?? 32;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 256 || !Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 32) return json({ error: 'Compaction limit1..256 and pageSize1..32 required' }, 400);
    const authority = await this.mediator({ op: 'status' });
    const authorityFloor = authority.retentionFloor ?? 0;
    validSnapshot(authorityFloor);
    if (input.floor !== undefined) validSnapshot(input.floor);
    const floors = await this.storage.transaction(async txn => {
      let rejectedEpoch = authority.rejectBeforeEpoch ?? 0;
      validSnapshot(rejectedEpoch);
      let next = Math.min(authorityFloor, input.floor ?? authorityFloor);
      for (const key of (await txn.list({ prefix: 'pending:' })).keys()) {
        const reservation = await readRecord(txn, key, RECORD_OPTIONS);
        next = Math.min(next, reservation.snapshot);
        rejectedEpoch = Math.min(rejectedEpoch, requestEpoch(reservation.id));
      }
      next = Math.max(await txn.get('collectionFloor') || 0, next);
      await txn.put('collectionFloor', next);
      rejectedEpoch = Math.max(await txn.get('rejectBeforeEpoch') || 0, rejectedEpoch);
      await txn.put('rejectBeforeEpoch', rejectedEpoch);
      return { collectionFloor: next, rejectBeforeEpoch: rejectedEpoch };
    });
    const { collectionFloor, rejectBeforeEpoch } = floors;
    // Publish the local rejection fence durably before any physical deletion.
    // A crash between these transactions expires reads early, never too late.
    let position = { v: 1, floor: collectionFloor, phase: 'markers', markerPrefix: 0, afterMarker: null, afterHead: null, currentKey: null, afterVersion: null };
    if (input.cursor !== undefined && input.cursor !== null) {
      try { position = JSON.parse(atob(input.cursor)); } catch { return json({ error: 'Invalid compaction cursor' }, 400); }
      const encoded = key => key === null || (typeof key === 'string' && key.length <= 1800 && /^[0-9a-f]+$/.test(key));
      if (position.v !== 1 || !Number.isSafeInteger(position.floor) || position.floor < 0 || position.floor > collectionFloor || !['markers','meta','rows'].includes(position.phase) || !encoded(position.afterHead) || !encoded(position.currentKey) || !(position.afterVersion === null || typeof position.afterVersion === 'string')) return json({ error: 'Invalid compaction cursor' }, 400);
    }
    const result = await this.storage.transaction(async txn => {
      let deletedVersions = 0, deletedMetadata = 0, keysVisited = 0, deletedMarkers = 0, markersVisited = 0, deletedHeads = 0;
      const usage = () => deletedVersions + deletedMetadata + markersVisited;
      const hasBudget = () => usage() < limit;
      const report = done => ({ done, deletedVersions, deletedMetadata, keysVisited, deletedMarkers, markersVisited, deletedHeads });
      const latestMetadata = await this.meta(txn);
      const retiredEligible = latestMetadata.retired && latestMetadata.step <= collectionFloor;
      const prune = async (prefix, kind, removeAnchor = false, removeAll = false) => {
        const anchor = removeAll ? new Map() : await txn.list({ prefix, end: `${prefix}${token(position.floor)}\u0000`, reverse: true, limit: 1 });
        const anchorKey = anchor.keys().next().value;
        if (!removeAll && !anchorKey) { position.afterVersion = null; return true; }
        let start = prefix;
        if (position.afterVersion !== null) {
          if (!position.afterVersion.startsWith(prefix) || !/^\d{16}$/.test(position.afterVersion.slice(prefix.length))) throw Object.assign(new Error('Cursor version is outside its logical key'), { status: 400 });
          start = position.afterVersion + '\u0000';
        }
        const end = removeAll ? prefix + '~' : removeAnchor ? `${prefix}${token(position.floor)}\u0000` : anchorKey;
        if (start >= end) { position.afterVersion = null; return true; }
        const budget = limit - usage();
        const obsolete = await txn.list({ prefix, start, end, limit: budget });
        for (const key of obsolete.keys()) {
          await deleteRecord(txn, key, { allowRaw: true });
          position.afterVersion = key;
          if (kind === 'meta') deletedMetadata++; else deletedVersions++;
        }
        if (obsolete.size === budget) return false; // continuation checks whether any remain
        position.afterVersion = null;
        return true;
      };
      if (position.phase === 'markers') {
        const prefixes = ['aborted:', 'applied:', 'proposal:', 'association:', 'readset:', 'inbox:', 'outbox:', 'terminal:', 'hold:'];
        if (!Number.isInteger(position.markerPrefix) || position.markerPrefix < 0 || position.markerPrefix > prefixes.length || !(position.afterMarker === null || typeof position.afterMarker === 'string')) throw Object.assign(new Error('Invalid marker cursor'), { status: 400 });
        while (position.markerPrefix < prefixes.length && rejectBeforeEpoch > 0) {
          const prefix = prefixes[position.markerPrefix];
          if (position.afterMarker !== null && (!position.afterMarker.startsWith(prefix) || position.afterMarker.length > 256)) throw Object.assign(new Error('Marker cursor outside prefix'), { status: 400 });
          const budget = limit - usage();
          if (!budget) return report(false);
          const page = await txn.list({ prefix, start: position.afterMarker === null ? prefix : position.afterMarker + '\u0000', limit: budget });
          for (const key of page.keys()) {
            markersVisited++;
            if (requestEpoch(key.slice(prefix.length)) < rejectBeforeEpoch) { await txn.delete(key); deletedMarkers++; }
            position.afterMarker = key;
          }
          if (page.size === budget) return report(false);
          position.markerPrefix++; position.afterMarker = null;
        }
        position.phase = 'meta'; position.afterVersion = null;
      }
      const pruneHead = async head => {
        const dead = retiredEligible || (head.present === false && head.step <= position.floor);
        if (!await prune(rowPrefix(head.key), 'row', dead, retiredEligible)) return false;
        if (dead) {
          await txn.delete(`head:${head.encoded}`); deletedHeads++;
          if (head.present) {
            const stats = await txn.get('stats') || { rows: 0, payloadBytes: 0 };
            stats.rows = Math.max(0, stats.rows - 1);
            stats.payloadBytes = Math.max(0, stats.payloadBytes - head.bytes);
            stats.bytes = 2 + stats.payloadBytes + Math.max(0, stats.rows - 1);
            await txn.put('stats', stats);
          }
        }
        return true;
      };
      if (position.phase === 'meta') {
        if (!await prune('meta:', 'meta')) return report(false);
        position.phase = 'rows';
      }
      if (position.currentKey !== null) {
        const head = await txn.get(`head:${position.currentKey}`);
        if (head && !await pruneHead(head)) return report(false);
        position.afterHead = position.currentKey; position.currentKey = null; position.afterVersion = null; keysVisited++;
      }
      while (hasBudget() && keysVisited < pageSize) {
        const options = headBounds(all, position.afterHead);
        const requested = pageSize - keysVisited;
        const heads = await txn.list({ ...options, limit: requested });
        if (!heads.size) return report(true);
        for (const head of heads.values()) {
          position.currentKey = head.encoded;
          if (!await pruneHead(head)) return report(false);
          position.afterHead = head.encoded; position.currentKey = null; position.afterVersion = null; keysVisited++;
          if (!hasBudget()) return report(false);
        }
        if (heads.size < requested) return report(true);
      }
      return report(false);
    });
    return json({ ...result, collectionFloor, rejectBeforeEpoch, floor: collectionFloor, nextCursor: result.done ? null : btoa(JSON.stringify(position)) });
  }
  async handle(input) {
    if(input.op==='propose')return this.propose(input);
    if(input.op==='proposal'){const epoch=requestEpoch(input.id);await rpc(this.env.ALLOCATOR,'global',{op:'epoch',requestEpoch:epoch});const floor=await this.storage.get('rejectBeforeEpoch')||0;if(epoch<floor)throw requestExpired(floor);return json(await this.storage.get(`proposal:${input.id}`)||{missing:true});}
    if(input.op==='abort')return this.abortUnassigned(input);

    if (input.op === 'compact') return this.compact(input);
    if (input.op === 'stats') return json(await this.storage.get('stats') || { rows: 0, bytes: 2, payloadBytes: 0 });
    if (input.op === 'inspect') {
      const roots = await this.storage.list({ prefix: 'pending:' });
      return json({ pending: await Promise.all([...roots.keys()].map(key => readRecord(this.storage, key, RECORD_OPTIONS))) });
    }
    return json({ error: 'Unknown DataShard operation' }, 400);
  }
}
