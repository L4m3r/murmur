import { canonical, validateCertificate, verifyAck, rpc } from './protocol.js';
const json=(value,status=200)=>Response.json(value,{status});
/** Durable peer-plan routing; never hold local serialization across an actor RPC. */
export class PlanBucket {
  constructor(state,env) {this.storage=state.storage;this.env=env;this.tail=Promise.resolve();}
  local(fn) {const task=this.tail.then(fn);this.tail=task.catch(()=>{});return task;}
  async epoch(id,epoch) {const match=/^e([0-9]+)_/.exec(id),derived=Number(match?.[1]||0);if(match&&epoch!==undefined&&epoch!==derived)throw Object.assign(new Error('Transaction epoch cannot be relabeled'),{status:400,code:'INVALID_REQUEST_EPOCH'});return rpc(this.env.ALLOCATOR,'global',{op:'epoch',requestEpoch:epoch??derived});}
  async page(prefix,limit) {return this.local(async()=>{const cursor=await this.storage.get(`cursor:${prefix}`);let page=await this.storage.list({prefix,start:cursor?cursor+'\u0000':prefix,limit});if(!page.size&&cursor)page=await this.storage.list({prefix,limit});if(page.size)await this.storage.put(`cursor:${prefix}`,[...page.keys()].at(-1));return page;});}
  async fetch(request) {
    try {
      const input=await request.json();
      if(input.op==='diagnostics')return await this.local(()=>this.diagnostics(input));
      if(input.op==='collect') return json(await this.collect(input));
      if(input.op==='register') return json(await this.register(input.association));
      if(input.op==='drive') return json(await this.drive(input.id,input.release===true,input));
      if(typeof input.id!=='string'||input.id.length>128||!input.id.length) return json({error:'Invalid plan id'},400);
      await this.epoch(input.id,input.epoch);
      if(input.op==='get') return json(await this.local(()=>this.storage.get(`plan:${input.id}`))||{missing:true});
      if(input.op==='decide') return json(await this.decide(input));
      if(['delivered','applied'].includes(input.op)) return json(await this.ack(input));
      return json({error:'Unknown bucket operation'},400);
    } catch(error) {return json({error:error.message,code:error.code||'BUCKET_ERROR',rpc:error.rpc,retryAfterMs:error.retryAfterMs,retryable:(error.status||503)>=500},error.status||503);}
  }
  async diagnostics(input) {
    if(typeof input.id!=='string'||!input.id.length||input.id.length>128)return json({error:'Diagnostic transaction id required'},400);
    const record=await this.storage.get(`plan:${input.id}`);
    if(!record)return json({kind:'bucket',transaction:null});
    const a=record.association,names=a.participants.map(p=>p.name),undelivered=names.filter(name=>!record.delivered.includes(name)),unapplied=names.filter(name=>!record.applied.includes(name));
    return json({kind:'bucket',transaction:{id:a.id,step:a.step,epoch:a.epoch,participantCount:names.length,deliveredCount:record.delivered.length,appliedCount:record.applied.length,decision:record.decision?.outcome??null,undelivered:undelivered.slice(0,16),unapplied:unapplied.slice(0,16),truncated:undelivered.length>16||unapplied.length>16}});
  }
  async register(a) {
    if(!a||typeof a.id!=='string') throw Object.assign(new Error('Association required'),{status:400});
    const found=await rpc(this.env.ALLOCATOR,'global',{op:'lookup',id:a.id,epoch:a.epoch});
    if(found.state!=='assigned'||canonical(found.association)!==canonical(a)) throw Object.assign(new Error('Unanchored association'),{status:409});
    return this.local(()=>this.storage.transaction(async txn=> {
      if(a.epoch<(await txn.get('rejectBeforeEpoch')||0))throw Object.assign(new Error('Request epoch retired'),{status:410,code:'REQUEST_EXPIRED'});
      const previous=await txn.get(`plan:${a.id}`);
      if(previous) {if(canonical(previous.association)!==canonical(a)) throw Object.assign(new Error('Association changed'),{status:409});return previous;}
      const record={association:a,decision:null,delivered:[],applied:[]};
      await txn.put(`plan:${a.id}`,record);await txn.put(`pending:${a.id}`,a.id);await txn.put(`epoch-plan:${String(a.epoch).padStart(16,'0')}:${a.id}`,a.id);
      if(txn.setAlarm) await txn.setAlarm(Date.now()+100);return record;
    }));
  }
  async decide(input) {
    const record=await this.local(()=>this.storage.get(`plan:${input.id}`));
    if(!record) throw Object.assign(new Error('Unregistered plan'),{status:404});
    let decision;try {decision=await validateCertificate(record.association,input.certificate);} catch(error) {throw Object.assign(error,{status:409,code:'INVALID_CERTIFICATE'});}
    return this.local(()=>this.storage.transaction(async txn=> {
      const current=await txn.get(`plan:${input.id}`);
      if(current.decision&&current.decision.outcome!==decision.outcome) throw Object.assign(new Error('Irrevocable decision conflict'),{status:409,code:'DECISION_CONFLICT'});
      if(!current.decision) {current.decision=decision;await txn.put(`plan:${input.id}`,current);if(txn.setAlarm)await txn.setAlarm(Date.now()+100);}
      return current;
    }));
  }
  async ack(input) {
    const record=await this.local(()=>this.storage.get(`plan:${input.id}`));
    if(!record) throw Object.assign(new Error('Unknown plan'),{status:404});
    if(!await verifyAck(record.association,input.ack,input.op)) throw Object.assign(new Error('Invalid signed acknowledgement'),{status:409});
    return this.local(()=>this.storage.transaction(async txn=> {
      const current=await txn.get(`plan:${input.id}`),name=input.ack.name;
      if(input.op==='applied'&&(!current.decision||current.decision.outcome!==input.ack.outcome||current.decision.decisionHash!==input.ack.decisionHash)) throw Object.assign(new Error('Acknowledgement decision mismatch'),{status:409});
      if(input.op==='applied'&&!current.delivered.includes(name)) throw Object.assign(new Error('Delivery acknowledgement required'),{status:409});
      if(!current[input.op].includes(name))current[input.op].push(name);
      await txn.put(`plan:${input.id}`,current);
      // Completion notification is a durable outbox too; lost replies simply replay.
      await txn.put(`notify:${input.id}`,input.id);
      if(txn.setAlarm) await txn.setAlarm(Date.now()+100);
      return current;
    }));
  }
  async drive(id,release=false,options={}) {
    const participantLimit=options.participantLimit??8,readSetLimit=options.readSetLimit??8,finalizeLimit=options.finalizeLimit??8,notifyLimit=options.notifyLimit??8,deferFinalize=options.deferFinalize??false;
    if([participantLimit,readSetLimit,notifyLimit].some(n=>!Number.isSafeInteger(n)||n<1||n>8)||!Number.isSafeInteger(finalizeLimit)||finalizeLimit<0||finalizeLimit>8||typeof deferFinalize!=='boolean')throw Object.assign(new Error('Invalid peer drive work budget'),{status:400,code:'INVALID_DRIVE_BUDGET'});
    // Validate before durable cursors, alarms, holds or remote work can change.
    let processed=0,failed=0;
    const ids=id?[id]:[...(await this.page('pending:',1)).values()];
    if(this.storage.setAlarm&&(ids.length||(await this.storage.list({prefix:'notify:',limit:1})).size))await this.storage.setAlarm(Date.now()+1000);
    for(const txid of ids) {try {
      let record=await this.local(()=>this.storage.get(`plan:${txid}`));if(!record)continue;
      const a=record.association;
      const targets=a.participants.filter(p=>!record.delivered.includes(p.name)).slice(0,participantLimit);
      for(const p of targets) {
        const result=await rpc(this.env.SHARDS,p.name,{op:'acceptPlan',association:a});
        await this.ack({op:'delivered',id:txid,ack:result.ack});
      }
      record=await this.local(()=>this.storage.get(`plan:${txid}`));
      if(!record.decision) {
        // Participants independently exchange signed ReadSets and may decide.
        const start=record.driveCursor||0;
        const peers=Array.from({length:Math.min(participantLimit,a.participants.length)},(_,i)=>a.participants[(start+i)%a.participants.length]);
        await this.local(async()=>{const current=await this.storage.get(`plan:${txid}`);current.driveCursor=(start+peers.length)%a.participants.length;await this.storage.put(`plan:${txid}`,current);});
        for(const p of peers) {try{await rpc(this.env.SHARDS,p.name,{op:'drive',id:txid,release,readSetLimit,deferFinalize});}catch{failed++;}}
      }
      record=await this.local(()=>this.storage.get(`plan:${txid}`));
      if(record.decision) for(const p of a.participants.filter(p=>!record.applied.includes(p.name)).slice(0,finalizeLimit)) {
        if(!record.delivered.includes(p.name)) continue;
        const result=await rpc(this.env.SHARDS,p.name,{op:'finalize',association:a,decision:record.decision,release});
        if(result.ack) await this.ack({op:'applied',id:txid,ack:result.ack});
      }
      record=await this.local(()=>this.storage.get(`plan:${txid}`));
      if(record.applied.length===a.participants.length) await this.local(()=>this.storage.delete(`pending:${txid}`));
      processed++;
    }catch{failed++;}}
    for(const [key,txid] of await this.page('notify:',notifyLimit)) {try {
      const accepted=await rpc(this.env.ALLOCATOR,'global',{op:'completion',id:txid}); if(accepted.applied)await this.local(()=>this.storage.delete(key));
    }catch{failed++;}}
    if((await this.storage.list({prefix:'pending:',limit:1})).size||(await this.storage.list({prefix:'notify:',limit:1})).size)if(this.storage.setAlarm)await this.storage.setAlarm(Date.now()+100);
    return {processed,failed};
  }
  async collect(input) {
    const state=await rpc(this.env.ALLOCATOR,'global',{op:'epoch'}),limit=input.limit??64;
    if(!Number.isSafeInteger(limit)||limit<1||limit>128)throw Object.assign(new Error('Invalid collection limit'),{status:400});
    return this.local(()=>this.storage.transaction(async txn=>{
      const floor=Math.max(await txn.get('rejectBeforeEpoch')||0,state.rejectBeforeEpoch);await txn.put('rejectBeforeEpoch',floor);
      const entries=await txn.list({prefix:'epoch-plan:',end:`epoch-plan:${String(floor).padStart(16,'0')}:`,limit});let deleted=0;
      for(const [key,id]of entries){const record=await txn.get(`plan:${id}`);if(record&&record.applied.length!==record.association.participants.length)continue;await txn.delete(`plan:${id}`);await txn.delete(`pending:${id}`);await txn.delete(`notify:${id}`);await txn.delete(key);deleted++;}
      return {rejectBeforeEpoch:floor,deleted,done:(await txn.list({prefix:'epoch-plan:',end:`epoch-plan:${String(floor).padStart(16,'0')}:`,limit:1})).size===0};
    }));
  }
  async alarm(){try{await this.drive();}catch{if(this.storage.setAlarm)await this.storage.setAlarm(Date.now()+1000);}}
}
