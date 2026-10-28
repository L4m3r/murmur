import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';

const base=process.env.BASE_URL||'http://127.0.0.1:9876';
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
// Serialize once: transport failures and retryable 503s MUST reuse the same
// mutation identity and cursor token, never silently start fresh operations.
async function post(path,body){
 const encoded=JSON.stringify(body),started=Date.now(),deadline=started+30000;let attempt=0,pendingResponses=0,unexpectedRetries=0;
 while(true){
  const remaining=deadline-Date.now();if(remaining<=0)throw new Error(`HTTP operation deadline exceeded: ${path}`);
  let response;
  console.error(JSON.stringify({event:'request_start',path,attempt:attempt+1,sql:body.sql?.slice(0,160),action:body.action,elapsedMs:Date.now()-started}));
  try{response=await fetch(base+path,{method:'POST',headers:{'content-type':'application/json'},body:encoded,signal:AbortSignal.timeout(Math.min(10000,remaining))});}
  catch(error){console.error(JSON.stringify({event:'request_timeout_or_transport',path,attempt:++attempt,elapsedMs:Date.now()-started,error:error.message}));throw error;}
  const result={status:response.status,body:await response.json()};attempt++;
  console.error(JSON.stringify({event:'request_reply',path,attempt,status:response.status,code:result.body.code,elapsedMs:Date.now()-started}));
  if(Date.now()>deadline)throw new Error(`HTTP response exceeded operation deadline: ${path}`);
  if(!(result.status===503&&result.body.retryable)){console.error(JSON.stringify({event:'operation_complete',path,sql:body.sql?.slice(0,160),attempts:attempt,pendingResponses,unexpectedRetries,elapsedMs:Date.now()-started,status:result.status}));return result;}
  const pending=result.body.code==='PLAN_PENDING';if(pending)pendingResponses++;else unexpectedRetries++;
  console.error(JSON.stringify({event:pending?'PLAN_PENDING':'RETRY',path,attempt,elapsedMs:Date.now()-started,error:result.body.error,code:result.body.code,rpc:result.body.rpc,retryAfterMs:result.body.retryAfterMs}));
  await delay(Math.min(Math.max(250,result.body.retryAfterMs||0),Math.max(0,deadline-Date.now())));
 }
}
async function q(sql,params){const result=await post('/query',{sql,params,idempotencyKey:randomUUID()});assert.equal(result.status,200,JSON.stringify(result));return result.body;}
const rows=result=>result.results.at(-1).rows;
async function startCursor(sql,extra={}){
 const body={action:'start',sql,idempotencyKey:randomUUID(),pageSize:7,ttlMs:300000,...extra};
 const response=await post('/cursor',body);assert.equal(response.status,200,JSON.stringify(response));
 assert.deepEqual(await post('/cursor',body),response,'Exact start replay preserves lease/token identity');
 return response.body;
}
async function page(cursor,token){const result=await post('/cursor',{action:'page',id:cursor.id,token});assert.equal(result.status,200,JSON.stringify(result));const p=result.body;
 assert.equal(p.snapshot,cursor.snapshot);assert.ok(p.rows.length<=cursor.pageSize);assert.ok(p.plan.shardCalls<=8);assert.ok(p.plan.keysRead<=256);assert.ok(Buffer.byteLength(JSON.stringify(p.rows))<=512*1024);return p;
}
async function ack(cursor,p){const result=await post('/cursor',{action:'ack',id:cursor.id,throughSeq:p.page});assert.equal(result.status,200,JSON.stringify(result));}
async function close(cursor){const result=await post('/cursor',{action:'close',id:cursor.id});assert.equal(result.status,200,JSON.stringify(result));assert.equal(result.body.closed,true);}
async function drain(cursor,firstToken=cursor.token){const output=[];let token=firstToken,pages=0;
 while(token){const p=await page(cursor,token);output.push(...p.rows);await ack(cursor,p);token=p.nextToken;if(++pages>300)throw Error('Cursor exceeded bounded page-progress budget');}
 return output;
}
async function dropTable(table){const started=await post('/schema',{action:'start',sql:`DROP TABLE ${table}`,idempotencyKey:randomUUID()});assert.ok([200,202].includes(started.status),JSON.stringify(started));let result=started;
 for(let n=0;n<300;n++){if(result.body.job.state==='dropped')return result.body;assert.ok(!['failed','cancelled'].includes(result.body.job.state),JSON.stringify(result));result=await post('/schema',{action:'resume',id:started.body.job.id});assert.ok([200,202].includes(result.status),JSON.stringify(result));}
 throw Error('DROP TABLE cleanup exceeded bounded progress budget');
}

const original=Array.from({length:96},(_,id)=>({id,bucket:id<64?'same':`z${id}`,keep:id%3===0,payload:'x'.repeat(80)}));
await q('CREATE TABLE paged (id INTEGER PRIMARY KEY,bucket TEXT,keep BOOLEAN,payload TEXT)');
for(let offset=0;offset<original.length;offset+=16){const batch=original.slice(offset,offset+16);await q(`INSERT INTO paged(id,bucket,keep,payload) VALUES ${batch.map(r=>`(${r.id},'${r.bucket}',${r.keep},'${r.payload}')`).join(',')}`);}
await q('CREATE INDEX by_bucket ON paged(bucket)');
for(const [sql,expected] of [
 ['SELECT id FROM paged WHERE id>=0 AND keep=true ORDER BY id ASC LIMIT 5',[0,3,6,9,12]],
 ['SELECT id FROM paged WHERE id<96 AND keep=true ORDER BY id DESC LIMIT 5',[93,90,87,84,81]],
 ["SELECT id FROM paged WHERE bucket='same' AND keep=true ORDER BY bucket ASC LIMIT 5",[0,3,6,9,12]],
 ["SELECT id FROM paged WHERE bucket='same' AND keep=true ORDER BY bucket DESC LIMIT 5",[63,60,57,54,51]],
]){const result=await q(sql);assert.deepEqual(rows(result),expected.map(id=>({id})));assert.equal(result.plan.limitPushed,true);assert.ok(result.plan.rowsRead<=32,JSON.stringify(result.plan));}
console.log('PASS primary/index ASC/DESC LIMIT pushdown applies residual predicates with bounded reads');

const window=await startCursor('SELECT id FROM paged ORDER BY id',{pageSize:3});
const p0=await page(window,window.token),p1=await page(window,p0.nextToken);
assert.deepEqual(await page(window,window.token),p0,'Unacknowledged page retry is byte-for-byte stable');
const renewed=await post('/cursor',{action:'renew',id:window.id,ttlMs:300000});assert.equal(renewed.status,200,JSON.stringify(renewed));assert.equal(renewed.body.snapshot,window.snapshot);assert.ok(renewed.body.expiresAt>=window.expiresAt);
assert.deepEqual(await page(window,window.token),p0,'Renewal preserves exact cached page');
assert.equal((await post('/cursor',{action:'page',id:window.id,token:window.token,sql:'SELECT id FROM paged LIMIT 1'})).status,409);
assert.equal((await post('/cursor',{action:'page',id:window.id,token:p1.nextToken})).status,429);
assert.equal((await post('/cursor',{action:'ack',id:window.id,throughSeq:999})).status,400);
await ack(window,p0);await ack(window,p0);
const expired=await post('/cursor',{action:'page',id:window.id,token:window.token});assert.equal(expired.status,410);assert.equal(expired.body.code,'PAGE_EXPIRED');
const p2=await page(window,p1.nextToken);await ack(window,p2);
const status=await post('/cursor',{action:'status',id:window.id});assert.equal(status.status,200);assert.equal(status.body.cachePages,0);
await close(window);assert.equal((await post('/cursor',{action:'page',id:window.id,token:p2.nextToken})).status,410);
console.log('PASS exact cursor retries, receipt-window backpressure, ACK expiry and explicit close');

await q('CREATE TABLE composite_pages (tenant TEXT,id INTEGER,bucket TEXT,keep BOOLEAN,PRIMARY KEY(tenant,id))');
const composite=[];
for(const tenant of ['a','\uE000','😀'])for(let id=0;id<16;id++)composite.push({tenant,id,bucket:'same',keep:id%2===0});
for(let offset=0;offset<composite.length;offset+=16){const batch=composite.slice(offset,offset+16);await q(`INSERT INTO composite_pages(tenant,id,bucket,keep) VALUES ${batch.map(r=>`('${r.tenant}',${r.id},'same',${r.keep})`).join(',')}`);}
await q('CREATE INDEX by_duplicate ON composite_pages(bucket)');
const ordered=[...composite].filter(r=>r.keep).sort((a,b)=>Buffer.compare(Buffer.from(b.tenant),Buffer.from(a.tenant))||b.id-a.id).slice(0,17).map(({tenant,id,bucket})=>({tenant,id,bucket}));
const compositeCursor=await startCursor("SELECT tenant,id,bucket FROM composite_pages WHERE bucket='same' AND keep=true ORDER BY bucket DESC LIMIT 17",{pageSize:7});
assert.deepEqual(await drain(compositeCursor),ordered);await close(compositeCursor);
console.log('PASS duplicate-index prefixes, composite PK suffixes, UTF8 reverse order and residual LIMIT across pages');

const stable=await startCursor('SELECT id,payload FROM paged ORDER BY id DESC',{pageSize:7});
const first=await page(stable,stable.token);
const before=(await q('SELECT id FROM paged LIMIT 1')).routing[0].tablets.length;
let after=before;
for(let offset=96;offset<128;offset+=8){const values=Array.from({length:8},(_,i)=>`(${offset+i},'future',true,'${'y'.repeat(240)}')`);const added=await q(`INSERT INTO paged(id,bucket,keep,payload) VALUES${values.join(',')}`);after=added.routing[0].tablets.length;}
assert.ok(after>before,'Concurrent inserts must cause an actual tablet split');
await dropTable('paged');
await q("CREATE TABLE paged(id INTEGER PRIMARY KEY,payload TEXT); INSERT INTO paged(id,payload) VALUES(999,'replacement')");
assert.deepEqual(await page(stable,stable.token),first,'Same old token remains exact after split/drop/recreate');
await ack(stable,first);
assert.deepEqual([...first.rows,...await drain(stable,first.nextToken)],[...original].reverse().map(({id,payload})=>({id,payload})));
assert.deepEqual(rows(await q('SELECT * FROM paged')),[{id:999,payload:'replacement'}]);await close(stable);
console.log('PASS leased cursor remains on its original generation through split, logical DROP and recreation');

const short=await startCursor('SELECT id FROM paged ORDER BY id',{pageSize:1,ttlMs:3000});
await delay(Math.max(0,short.expiresAt-Date.now())+100);
const leaseExpired=await post('/cursor',{action:'page',id:short.id,token:short.token});assert.equal(leaseExpired.status,410);assert.equal(leaseExpired.body.code,'CURSOR_EXPIRED');
console.log('PASS real-time cursor lease expiration returns 410');
console.log('5 query pagination integration groups passed');
