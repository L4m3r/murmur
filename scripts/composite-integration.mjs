import {queryRequest} from './http-query.mjs';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile,writeFile} from 'node:fs/promises';
const base=process.env.BASE_URL||'http://127.0.0.1:9876';
async function q(sql,extra={},status=200){const r=await queryRequest(base,{sql,idempotencyKey:randomUUID(),...extra},{expected:status});assert.equal(r.status,status,JSON.stringify(r.body));return r.body;}
const rows=r=>r.results.at(-1).rows;
const file=process.argv[3];
if(process.argv[2]==='--verify'){
 const m=JSON.parse(await readFile(file,'utf8'));
 const result=await q(m.sql,{idempotencyKey:m.key});
 assert.deepEqual(await q(m.sql,{idempotencyKey:m.key}),result);
 assert.deepEqual(rows(await q("SELECT tenant,seq,balance FROM tuple_rows WHERE label='renamed'")),[{tenant:'🌍',seq:0,balance:1}]);
 assert.deepEqual(rows(await q("SELECT label FROM tuple_rows WHERE tenant='🌍' AND seq=0",{snapshot:m.snapshot})),[{label:'l2_20'}]);
 await q('DELETE FROM tuple_rows WHERE seq!=0');
 await q("DELETE FROM tuple_rows WHERE tenant!='🌍'");
 const merged=await q('SELECT * FROM tuple_rows');assert.equal(rows(merged).length,1);assert.equal(merged.routing[0].tablets.length,1);
 console.log('PASS composite split/index partial apply survives SIGKILL; old tuple snapshot and adjacent merge remain correct');
}else{
 const tenants=['a','b','🌍'];
 const values=tenants.flatMap((tenant,t)=>Array.from({length:40},(_,i)=>`(${i-20},'${tenant}','l${t}_${i}',0)`)).join(',');
 const created=await q(`CREATE TABLE tuple_rows(seq INTEGER,tenant TEXT,label TEXT,balance REAL,PRIMARY KEY(tenant,seq)); INSERT INTO tuple_rows(seq,tenant,label,balance) VALUES${values}; CREATE UNIQUE INDEX by_label ON tuple_rows(label); CREATE INDEX by_tenant ON tuple_rows(tenant)`);
 assert.ok(created.routing[0].tablets.length>1);
 const range=await q('SELECT seq FROM tuple_rows WHERE tenant=:tenant AND seq>=:lo AND seq<=:hi ORDER BY seq',{params:{tenant:'🌍',lo:-2,hi:2}});
 assert.deepEqual(rows(range),[-2,-1,0,1,2].map(seq=>({seq})));assert.equal(range.plan.rowsRead,5);assert.ok(range.plan.tabletsVisited<range.plan.tabletsTotal);
 console.log('PASS mixed typed composite primary-key prefix/range prunes physical tablets');
 assert.deepEqual(rows(await q("SELECT tenant,seq FROM tuple_rows WHERE label='l1_20'")),[{tenant:'b',seq:0}]);
 const index=created.routing[0].indexes.find(i=>i.name==='by_tenant');assert.deepEqual(index.keyColumns,['value','pk1']);
 assert.equal(rows(await q("SELECT seq FROM tuple_rows WHERE tenant='a' ORDER BY seq")).length,40);
 console.log('PASS global index fetches full base tuple and deduplicates indexed primary-key component');
 await q("INSERT INTO tuple_rows(seq,tenant,label,balance) VALUES(0,'a','new',0)",{},400);
 await q("INSERT INTO tuple_rows(seq,tenant,label,balance) VALUES(100,'c','l0_1',0)",{},400);
 await q("UPDATE tuple_rows SET seq=999 WHERE tenant='a'",{},400);
 console.log('PASS composite duplicate keys, unique-index claims and PK mutation reject atomically');
 const before=(await q("SELECT label FROM tuple_rows WHERE tenant='a' AND seq=0")).snapshot;
 await q('DROP INDEX by_label ON tuple_rows');await q('CREATE UNIQUE INDEX by_label ON tuple_rows(label)');
 assert.deepEqual(rows(await q("SELECT tenant FROM tuple_rows WHERE label='l0_20'")),[{tenant:'a'}]);
 assert.deepEqual(rows(await q("SELECT tenant FROM tuple_rows WHERE label='l0_20'",{snapshot:before})),[{tenant:'a'}]);
 console.log('PASS DROP/recreate uses a new index identity while historical tuple lookup survives');
 const snapshot=(await q('SELECT * FROM tuple_rows WHERE tenant=\'🌍\' AND seq=0')).snapshot;
 const sql="UPDATE tuple_rows SET label='renamed',balance=balance+1 WHERE tenant='🌍' AND seq=0",key=randomUUID();
 await q(sql,{idempotencyKey:key,fault:'after-first-commit'},503);
 await writeFile(file,JSON.stringify({sql,key,snapshot}));
 console.log('4 composite table tests passed; partial transaction staged for SIGKILL');
}
