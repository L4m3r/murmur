import assert from'node:assert/strict';
import fs from'node:fs/promises';
import path from'node:path';
import{createRequire}from'node:module';
import{randomUUID}from'node:crypto';
import{queryRequest}from'./http-query.mjs';
const require=createRequire(import.meta.url);
const backendOnly=process.env.BACKEND_ONLY==='1'||process.env.OBSERVE_BACKEND_ONLY==='1';
const chromium=backendOnly?null:require(process.env.PLAYWRIGHT_CORE_PATH||'playwright-core').chromium;
const base=process.env.BASE_URL||'http://127.0.0.1:9996',token=process.env.API_TOKEN,out=process.env.OBSERVE_ARTIFACT_DIR;
assert.ok(token&&out,'Synthetic API_TOKEN and artifact dir required');await fs.mkdir(out,{recursive:true});
const headers={'content-type':'application/json',authorization:`Bearer ${token}`},secret='PRIVATE_ROW_SENTINEL_67284';
async function post(endpoint,body,extra={}){const r=await fetch(base+endpoint,{method:'POST',headers:{...headers,...extra},body:JSON.stringify(body),signal:AbortSignal.timeout(10000)});return{status:r.status,body:await r.json()};}
async function query(sql,extra={},expected=200){const r=await queryRequest(base,{sql,idempotencyKey:randomUUID(),...extra},{headers,expected});assert.equal(r.status,expected,JSON.stringify(r.body));return r.body;}
const shell=await fetch(base+'/observe');assert.equal(shell.status,200);assert.match(shell.headers.get('content-security-policy'),/default-src 'none'/);assert.equal(shell.headers.get('cache-control'),'no-store');assert.ok(!(await shell.text()).includes(token));
for(const authorization of ['', 'Bearer wrong-synthetic-token']){const r=await post('/observe/api',{action:'overview'},{authorization});assert.equal(r.status,401);}
assert.equal((await post('/observe/api',{action:'overview'},{origin:'https://not-local.invalid'})).status,403);
assert.equal((await post('/observe/api',{action:'query',sql:'DELETE FROM accounts'})).status,400);
assert.equal((await post('/observe/api',{action:'table',table:'accounts',limit:9})).status,400);
const getApi=await fetch(base+'/observe/api',{headers,signal:AbortSignal.timeout(10000)});assert.equal(getApi.status,405);
const tooLarge=await post('/observe/api',{action:'overview',padding:'x'.repeat(5000)});assert.equal(tooLarge.status,413);
console.log('PASS shell CSP/no-store, missing/wrong authorization, origin, method/body bounds and read-only input validation');
if(backendOnly){
 const empty=await post('/observe/api',{action:'overview'});assert.equal(empty.status,200);assert.equal(empty.body.runtime.pending.length,0);
 await query(`CREATE TABLE observe_accounts(id INTEGER PRIMARY KEY,code TEXT,balance INTEGER,note TEXT); INSERT INTO observe_accounts(id,code,balance,note) VALUES ${Array.from({length:12},(_,i)=>`(${i},'code-${i}',100,'${secret}${'x'.repeat(60)}')`).join(',')}; CREATE INDEX by_code ON observe_accounts(code)`);
 const held=await query('UPDATE observe_accounts SET balance=balance+1 WHERE id=0',{fault:'after-first-commit'},503);assert.equal(held.faultReached,true);held.transactionId=held.faultState.transactionId;assert.equal(held.faultState.appliedCount,1);assert.ok(held.faultState.participantCount>1);
 const before=await post('/snapshot',{}),samples=[];
 for(const input of [{action:'overview'},{action:'table',table:'observe_accounts',limit:2},{action:'table',table:'observe_accounts',index:'by_code',limit:2},{action:'transaction',transactionId:held.transactionId}]){const r=await post('/observe/api',input);assert.equal(r.status,200,JSON.stringify(r));samples.push(r.body);}
 let next=samples[1].topology.nextOffset;const seen=samples[1].topology.tablets.map(t=>t.id);
 while(next!==null&&next!==undefined){const r=await post('/observe/api',{action:'table',table:'observe_accounts',offset:next,limit:2});assert.equal(r.status,200,JSON.stringify(r));seen.push(...r.body.topology.tablets.map(t=>t.id));next=r.body.topology.nextOffset;samples.push(r.body);}
 assert.equal(new Set(seen).size,seen.length);assert.equal(seen.length,samples[1].topology.totalTablets);
 assert.ok(samples[0].runtime.pending.some(x=>x.id===held.transactionId));assert.equal(samples[1].topology.kind,'base');assert.equal(samples[2].topology.index,'by_code');assert.equal(samples[3].progress.appliedCount,1);
 assert.equal((await post('/observe/api',{action:'table',table:'missing_table',limit:1})).status,404);
 const absent=await post('/observe/api',{action:'transaction',transactionId:'e0_'+randomUUID()});assert.equal(absent.status,200);assert.equal(absent.body.transaction.state,'absent');
 const after=await post('/snapshot',{});assert.equal(after.body.allocated,before.body.allocated);assert.ok(after.body.pending.some(x=>x.id===held.transactionId));
 const serialized=JSON.stringify(samples);for(const value of [secret,token,'UPDATE observe_accounts','code-0'])assert.ok(!serialized.includes(value));
 await fs.writeFile(path.join(out,'backend-evidence.json'),JSON.stringify({mode:'backend-only; browser unverified',transactionId:held.transactionId,samples,allocatedBefore:before.body.allocated,allocatedAfter:after.body.allocated},null,2));
 console.log('PASS actual celld diagnostics empty/overview/paged-base/index/pending/absent/not-found, metadata privacy, no read-induced recovery');console.log('BROWSER_NOT_RUN: explicit BACKEND_ONLY=1; screenshots and visual QA unverified');process.exit(0);
}
const browser=await chromium.launch({executablePath:process.env.CHROMIUM_BIN||'/usr/bin/chromium',headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--disable-background-networking']});
const context=await browser.newContext({viewport:{width:1440,height:1100}});const page=await context.newPage();
const errors=[],requests=[],responses=[];page.on('pageerror',e=>errors.push(e.message));
page.on('request',r=>{if(r.url().startsWith(base))requests.push({url:r.url(),method:r.method(),body:r.postData()});else errors.push('External request '+r.url());});
page.on('response',async r=>{if(r.url().endsWith('/observe/api'))try{responses.push(await r.text())}catch{}});
const shot=async name=>{await page.screenshot({path:path.join(out,name+'.png'),fullPage:true});};
const waitText=(id,value)=>page.waitForFunction(([id,value])=>document.querySelector(id).textContent.includes(value),[id,value],{timeout:10000});
try{
 await page.goto(base+'/observe');await shot('01-disconnected-desktop');assert.equal(await page.locator('#refresh').isDisabled(),true);
 await page.locator('#token').fill('wrong-synthetic-token');await page.locator('#connect').click();await waitText('#overview-error','Unauthorized');await shot('02-auth-error');assert.equal(await page.locator('#token').inputValue(),'');
 let hold;const gate=new Promise(r=>hold=r);let first=true;
 await page.route('**/observe/api',async route=>{if(first){first=false;await gate;}await route.continue();});
 await page.locator('#token').fill(token);await page.locator('#connect').click();await waitText('#connection-label','Loading');await shot('03-loading');hold();await page.unroute('**/observe/api');
 await waitText('#connection-label','Connected');assert.equal(await page.locator('#token').inputValue(),'');await waitText('#pending-list','No pending plans');await shot('04-empty-connected');
 console.log('PASS actual browser disconnected/auth failure/loading/empty views');
 const table='observe_accounts';
 await query(`CREATE TABLE ${table}(id INTEGER PRIMARY KEY, code TEXT, balance INTEGER, note TEXT); INSERT INTO ${table}(id,code,balance,note) VALUES ${Array.from({length:12},(_,i)=>`(${i},'code-${i}',100,'${secret}${'x'.repeat(60)}')`).join(',')}; CREATE INDEX by_code ON ${table}(code)`);
 const pending=await query(`UPDATE ${table} SET balance=balance+1 WHERE id=0`,{fault:'after-first-commit'},503);assert.equal(pending.faultReached,true);pending.transactionId=pending.faultState.transactionId;assert.equal(pending.faultState.appliedCount,1);assert.ok(pending.faultState.participantCount>1);
 const before=await post('/snapshot',{});assert.equal(before.status,200);
 await page.locator('#refresh').click();await waitText('#connection-label','Connected');await page.waitForFunction(()=>document.querySelector('#metric-pending').textContent!=='0');
 await page.locator('#table-name').fill(table);await page.locator('#table-submit').click();await waitText('#table-summary','Base table');await page.locator('#tablet-list table').waitFor();await shot('05-overview-base-desktop');
 assert.match(await page.locator('#index-summary').innerText(),/by_code/);
 await page.locator('#index-name').fill('by_code');await page.locator('#table-submit').click();await waitText('#table-summary','Index by_code');await shot('06-index-topology');
 await page.locator('#tx-id').fill(pending.transactionId);await page.locator('#tx-submit').click();await waitText('#tx-state','assigned');await shot('07-pending-transaction');
 const tx=await post('/observe/api',{action:'transaction',transactionId:pending.transactionId});assert.equal(tx.status,200,JSON.stringify(tx));assert.equal(tx.body.transaction.state,'assigned');assert.equal(tx.body.progress.appliedCount,1);
 const after=await post('/snapshot',{});assert.equal(after.body.allocated,before.body.allocated);assert.ok(after.body.pending.some(x=>x.id===pending.transactionId));
 for(const text of [...responses,JSON.stringify(tx.body),await page.locator('body').innerText()]){assert.ok(!text.includes(secret));assert.ok(!text.includes(token));assert.ok(!text.includes('UPDATE observe_accounts'));}
 assert.deepEqual(await page.evaluate(()=>({local:localStorage.length,session:sessionStorage.length,cookie:document.cookie})),{local:0,session:0,cookie:''});
 assert.ok(requests.every(r=>new URL(r.url).pathname.startsWith('/observe')),'Dashboard only calls diagnostics, never query/recover/maintenance');
 assert.ok(requests.every(r=>!r.url.includes(token)&&!r.body?.includes(token)),'Token only in authorization header');
 console.log('PASS overview/table/index/transaction show actual actor metadata without rows/SQL/secrets or recovery');
 await page.setViewportSize({width:390,height:844});await shot('08-mobile');assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'No document-level mobile overflow');
 await page.locator('#table-name').fill('missing_table');await page.locator('#index-name').fill('');await page.locator('#table-submit').click();await page.waitForFunction(()=>document.querySelector('#table-error').textContent.length>0);await shot('09-table-error-mobile');
 await page.locator('#disconnect').click();assert.equal(await page.locator('#token').inputValue(),'');assert.equal(await page.locator('#refresh').isDisabled(),true);
 await page.reload();assert.equal(await page.locator('#token').inputValue(),'');assert.equal(await page.locator('#refresh').isDisabled(),true);
 assert.equal(errors.length,0,JSON.stringify(errors));
 console.log('PASS mobile layout, scoped error, disconnect/reload and memory-only token; no browser JS errors');
 await fs.writeFile(path.join(out,'browser-evidence.json'),JSON.stringify({browser:await browser.version(),requests:requests.map(({url,method})=>({url,method})),pageErrors:errors,transactionId:pending.transactionId,screenshots:9,checks:'actual celld+Chromium, metadata-only/no recovery, bounded auth-protected API'},null,2));
}finally{await browser.close();}
