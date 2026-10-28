import test from 'node:test';
import assert from 'node:assert/strict';
import {parse,execute} from '../src/sql.js';
const run = (sql, c={},t={}) => execute(parse(sql),c,t);
test('typed CRUD, SQL escaping, predicates, ordering, atomic wrapper',()=>{
 const x=run("BEGIN; CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL, name TEXT, active BOOLEAN); INSERT INTO accounts (id,balance,name,active) VALUES (1,100,'O''Neil',true),(2,50,'Bee',false); UPDATE accounts SET balance=balance-10 WHERE id=1; UPDATE accounts SET balance=balance+10 WHERE id=2; SELECT name,balance FROM accounts WHERE balance>=60 AND active=true ORDER BY balance DESC LIMIT 1; COMMIT;");
 assert.deepEqual(x.results.at(-1).rows,[{name:"O'Neil",balance:90}]);
 assert.equal(x.tables.accounts.reduce((a,r)=>a+r.balance,0),150);
 assert.equal(run('DELETE FROM accounts WHERE id=2',x.catalog,x.tables).results[0].rowsAffected,1);
 assert.equal(run("UPSERT INTO accounts (id,balance) VALUES (1,12)",x.catalog,x.tables).tables.accounts[0].balance,12);
});
test('reject unsupported/malformed syntax',()=>{
 for(const sql of ['SELECT * FROM a garbage','BEGIN; SELECT * FROM a;','COMMIT','CREATE TABLE a (id INTEGER)','SELECT * FROM a WHERE id=1 OR id=2',"SELECT * FROM a WHERE id='oops",'SELECT * FROM __proto__','SELECT * FROM a LIMIT -1',Array(33).fill('SELECT * FROM a').join(';')]) assert.throws(()=>parse(sql),sql);
});
test('strict types, schema and immutable PK',()=>{
 const {catalog,tables}=run('CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, label TEXT); INSERT INTO a (id,n) VALUES (1,2)');
 for(const sql of ["INSERT INTO a (id,n) VALUES (2,'3')",'INSERT INTO a (id,n) VALUES (1,3)','UPDATE a SET id=3','UPDATE a SET n=9007199254740992','SELECT absent FROM a','UPDATE a SET n=n+true','INSERT INTO a (id) VALUES (2)','SELECT * FROM a WHERE id=true','UPDATE a SET n=1,n=2']) assert.throws(()=>run(sql,structuredClone(catalog),structuredClone(tables)),sql);
});
test('updates evaluate assignments from old row and NULL is SQL unknown',()=>{
 const x=run('CREATE TABLE a (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER); INSERT INTO a (id,x,y) VALUES (1,2,3),(2,NULL,4); UPDATE a SET x=y,y=x WHERE id=1; SELECT * FROM a WHERE x=NULL');
 assert.deepEqual(x.tables.a[0],{id:1,x:3,y:2}); assert.deepEqual(x.results.at(-1).rows,[]);
});
test('CREATE INDEX AST, metadata and per-table name constraints',()=>{
 assert.deepEqual(parse('CREATE UNIQUE INDEX by_label ON a(label)'),[{kind:'createIndex',table:'a',index:'by_label',column:'label',unique:true}]);
 const x=run('CREATE TABLE a (id INTEGER PRIMARY KEY, label TEXT); CREATE INDEX by_label ON a(label)');
 assert.deepEqual(x.catalog.a.indexes,[{name:'by_label',column:'label',unique:false}]);
 assert.throws(()=>run('CREATE INDEX by_label ON a(id)',structuredClone(x.catalog),structuredClone(x.tables)),/exists/);
 assert.throws(()=>run('CREATE INDEX nope ON a(missing)',structuredClone(x.catalog),structuredClone(x.tables)),/Unknown column/);
 assert.throws(()=>parse('CREATE UNIQUE TABLE a (id INTEGER PRIMARY KEY)'));
 assert.throws(()=>parse('CREATE INDEX compound ON a(id,label)'));
});
test('unique indexes enforce insert/upsert/update and allow repeated NULL',()=>{
 const x=run("CREATE TABLE a (id INTEGER PRIMARY KEY, label TEXT); CREATE UNIQUE INDEX label_unique ON a(label); INSERT INTO a (id,label) VALUES (1,'one'),(2,'two'),(3,NULL),(4,NULL)");
 for (const sql of ["INSERT INTO a (id,label) VALUES (5,'one')", "UPSERT INTO a (id,label) VALUES (2,'one')", "UPDATE a SET label='same' WHERE id<=2"]) {
   assert.throws(()=>run(sql,structuredClone(x.catalog),structuredClone(x.tables)),/Unique index/);
   assert.deepEqual(x.tables.a.map(r=>r.label),['one','two',null,null]);
 }
 const y=run("UPDATE a SET label='changed' WHERE id=1; DELETE FROM a WHERE id=2; INSERT INTO a (id,label) VALUES (5,'two')",structuredClone(x.catalog),structuredClone(x.tables));
 assert.equal(y.tables.a.at(-1).label,'two');
});
test('unique index build rejects existing duplicates, ordinary index permits them',()=>{
 const x=run("CREATE TABLE a (id INTEGER PRIMARY KEY, label TEXT); INSERT INTO a (id,label) VALUES (1,'same'),(2,'same')");
 assert.throws(()=>run('CREATE UNIQUE INDEX label_unique ON a(label)',structuredClone(x.catalog),structuredClone(x.tables)),/Unique index/);
 assert.deepEqual(x.catalog.a.indexes,[]);
 assert.equal(run('CREATE INDEX label_nonunique ON a(label)',x.catalog,x.tables).catalog.a.indexes.length,1);
});
test('range predicates and ORDER BY use numeric rather than encoded lexical ordering',()=>{
 const x=run('CREATE TABLE n (id REAL PRIMARY KEY); INSERT INTO n (id) VALUES (-10),(-2.5),(0),(2),(10); SELECT * FROM n WHERE id>=-2.5 AND id<10 ORDER BY id DESC');
 assert.deepEqual(x.results.at(-1).rows,[{id:2},{id:0},{id:-2.5}]);
});
test('TEXT uses UTF-8 byte ordering including astral versus BMP private-use characters',()=>{
 const x=run("CREATE TABLE textkeys (id TEXT PRIMARY KEY); INSERT INTO textkeys (id) VALUES ('😀'),('\uE000'),('a'); SELECT * FROM textkeys ORDER BY id; SELECT * FROM textkeys WHERE id > '\uE000' ORDER BY id");
 assert.deepEqual(x.results.at(-2).rows.map(r=>r.id),['a','\uE000','😀']);
 assert.deepEqual(x.results.at(-1).rows,[{id:'😀'}]);
});
test('reject unpaired UTF-16 surrogates in data, filters and updates even with no matching rows',()=>{
 const x=run('CREATE TABLE a (id INTEGER PRIMARY KEY, text TEXT)');
 for (const invalid of ['\uD800','\uDC00','\uD800x','x\uDC00','\uD800\uD800']) {
   for (const sql of [`INSERT INTO a (id,text) VALUES (1,'${invalid}')`,`SELECT * FROM a WHERE text='${invalid}'`,`UPDATE a SET text='${invalid}' WHERE id=999`]) {
     assert.throws(()=>run(sql,structuredClone(x.catalog),structuredClone(x.tables)),/well-formed UTF-16/);
   }
 }
 assert.equal(run("INSERT INTO a (id,text) VALUES (1,'😀')",x.catalog,x.tables).tables.a[0].text,'😀');
});
test('named parameters bind scalar AST literals, never SQL source',()=>{
 const attack="x'); DELETE FROM a; --", params={Id:1,value:attack,amount:4,enabled:true,empty:null};
 const sql='CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT, n INTEGER, enabled BOOLEAN, empty TEXT); INSERT INTO a (id,value,n,enabled,empty) VALUES (:Id,:value,:amount,:enabled,:empty); UPDATE a SET n=n+:amount WHERE id=:Id; SELECT * FROM a WHERE value=:value LIMIT :Id';
 const ast=parse(sql,params), result=execute(ast,{},{});
 assert.equal(ast[1].values[0][1],attack);
 assert.deepEqual(result.results.at(-1).rows,[{id:1,value:attack,n:8,enabled:true,empty:null}]);
 params.value='changed after binding';assert.equal(ast[1].values[0][1],attack);
 assert.deepEqual(parse('SELECT * FROM a WHERE id=-:amount',{amount:4})[0].filter,[{column:'id',op:'=',value:-4}]);
});
test('parameters reject missing, unused, structural, accessor and nonfinite values',()=>{
 assert.throws(()=>parse('SELECT * FROM a WHERE id=:id'),/Missing parameter id/);
 assert.throws(()=>parse('SELECT * FROM a',{id:1}),/Unused parameter id/);
 assert.throws(()=>parse('SELECT * FROM a WHERE id=:Id',{id:1}),/Missing parameter Id/);
 for (const value of [undefined,{},[],()=>{},Symbol('x'),1n,NaN,Infinity]) assert.throws(()=>parse('SELECT * FROM a WHERE id=:id',{id:value}),/scalar|finite/);
 for (const params of [null,[],new Date(),1]) assert.throws(()=>parse('SELECT * FROM a',params),/plain object/);
 assert.throws(()=>parse('SELECT * FROM a WHERE id=:id',Object.defineProperty({},'id',{get(){throw new Error('must not invoke');}})),/accessors/);
 assert.throws(()=>parse('SELECT * FROM :table',{table:'a'}),/identifier/);
 assert.throws(()=>parse(':select * FROM a WHERE id=:select',{select:1}),/Unsupported statement/);
 assert.throws(()=>parse("'select' * FROM a"),/Unsupported statement/);
 assert.throws(()=>parse('SELECT * FROM a WHERE id=?',{id:1}),/Unsupported SQL character/);
 assert.throws(()=>parse("SELECT * FROM a WHERE id=':id'",{id:1}),/Unused/);
 assert.deepEqual(parse('SELECT * FROM a WHERE id=:constructor',JSON.parse('{"constructor":2}'))[0].filter[0].value,2);
});
test('bound values still undergo strict schema type and UTF16 validation',()=>{
 const x=run('CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT)');
 for (const [sql,params,error] of [
   ['INSERT INTO a(id) VALUES(:id)',{id:'1'},/INTEGER/],
   ['INSERT INTO a(id,value) VALUES(1,:value)',{value:'\uD800'},/UTF-16/],
   ['SELECT * FROM a WHERE id=:id',{id:9007199254740992},/INTEGER/],
   ['SELECT * FROM a LIMIT :limit',{limit:'1'},/LIMIT/],
 ]) assert.throws(()=>execute(parse(sql,params),structuredClone(x.catalog),structuredClone(x.tables)),error);
});
test('DROP INDEX removes metadata and uniqueness, rejects unknown index and supports recreation',()=>{
 assert.deepEqual(parse('DROP INDEX by_value ON a'),[{kind:'dropIndex',table:'a',index:'by_value'}]);
 const x=run("CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT); CREATE UNIQUE INDEX by_value ON a(value); INSERT INTO a(id,value) VALUES (1,'same'); DROP INDEX by_value ON a; INSERT INTO a(id,value) VALUES (2,'same')");
 assert.deepEqual(x.catalog.a.indexes,[]);assert.equal(x.tables.a.length,2);
 assert.throws(()=>run('DROP INDEX by_value ON a',structuredClone(x.catalog),structuredClone(x.tables)),/Unknown index/);
 assert.throws(()=>run('DROP INDEX by_value ON missing',structuredClone(x.catalog),structuredClone(x.tables)),/Unknown table/);
 assert.throws(()=>run('CREATE UNIQUE INDEX by_value ON a(value)',structuredClone(x.catalog),structuredClone(x.tables)),/Unique index/);
 assert.equal(run('CREATE INDEX by_value ON a(value)',x.catalog,x.tables).catalog.a.indexes.length,1);
});
test('composite PRIMARY KEY preserves declared order and distinguishes all components',()=>{
 const x=run("CREATE TABLE a (tenant TEXT, id INTEGER, n INTEGER, PRIMARY KEY (id,tenant)); INSERT INTO a (tenant,id,n) VALUES ('😀',-2,1),('\uE000',-2,2),('😀',3,3); UPSERT INTO a (tenant,id,n) VALUES ('😀',-2,4)");
 assert.deepEqual(x.catalog.a.primaryKeys,['id','tenant']);assert.equal(x.catalog.a.primaryKey,'id');
 assert.ok(x.catalog.a.columns.filter(c=>['id','tenant'].includes(c.name)).every(c=>c.primaryKey&&c.notNull));
 assert.equal(x.tables.a.length,3);assert.equal(x.tables.a[0].n,4);
 assert.throws(()=>run("INSERT INTO a(tenant,id,n) VALUES('😀',-2,9)",structuredClone(x.catalog),structuredClone(x.tables)),/Duplicate primary key/);
 for(const sql of ["INSERT INTO a(tenant,n) VALUES('x',1)","INSERT INTO a(id,n) VALUES(1,1)","UPDATE a SET id=2", "UPDATE a SET tenant='x'"])assert.throws(()=>run(sql,structuredClone(x.catalog),structuredClone(x.tables)),/NULL|Primary key updates/);
});
test('composite PRIMARY KEY declarations reject invalid or mixed key definitions',()=>{
 for(const sql of ['CREATE TABLE a (x INTEGER,y INTEGER,PRIMARY KEY(x,x))','CREATE TABLE a (x INTEGER,PRIMARY KEY(missing))','CREATE TABLE a (x INTEGER PRIMARY KEY,y INTEGER,PRIMARY KEY(y))','CREATE TABLE a (x INTEGER PRIMARY KEY,y INTEGER PRIMARY KEY)','CREATE TABLE a (x INTEGER,PRIMARY KEY(x),y INTEGER)','CREATE TABLE a (PRIMARY KEY(x))'])assert.throws(()=>parse(sql));
 const x=run('CREATE TABLE a (id INTEGER PRIMARY KEY)');assert.deepEqual(x.catalog.a.primaryKeys,['id']);
 const y=run('CREATE TABLE a (id INTEGER,PRIMARY KEY(id))');assert.deepEqual(y.catalog.a.primaryKeys,['id']);
});

test('BUILDING unique indexes are maintained metadata but not promised constraints until VALIDATING',()=>{
 const x=run("CREATE TABLE a(id INTEGER PRIMARY KEY, tag TEXT); CREATE UNIQUE INDEX by_tag ON a(tag); INSERT INTO a(id,tag) VALUES(1,'same')");
 x.catalog.a.indexes[0].state='building';assert.equal(run("INSERT INTO a(id,tag) VALUES(2,'same')",x.catalog,x.tables).tables.a.length,2);
 x.catalog.a.indexes[0].state='validating';assert.throws(()=>run("INSERT INTO a(id,tag) VALUES(3,'same')",structuredClone(x.catalog),structuredClone(x.tables)),/Unique index/);
});

test('DROP TABLE preserves full tombstone and recreates strictly fresh generation',()=>{
 assert.deepEqual(parse('DROP TABLE a'),[{kind:'dropTable',table:'a'}]);
 const x=run("CREATE TABLE a(id INTEGER PRIMARY KEY,tag TEXT); CREATE INDEX by_tag ON a(tag); INSERT INTO a(id,tag) VALUES(1,'old')");const schema=structuredClone(x.catalog.a);
 run('DROP TABLE a',x.catalog,x.tables);assert.deepEqual(x.catalog.a,{...schema,state:'dropped'});assert.deepEqual(x.tables.a,[]);
 for(const sql of ['SELECT * FROM a','INSERT INTO a(id) VALUES(2)','DROP TABLE a','CREATE INDEX x ON a(tag)','DROP INDEX by_tag ON a'])assert.throws(()=>run(sql,structuredClone(x.catalog),structuredClone(x.tables)),/Unknown table/);
 run("CREATE TABLE a(id TEXT PRIMARY KEY); INSERT INTO a(id) VALUES('fresh')",x.catalog,x.tables);assert.equal(x.catalog.a.generation,1);assert.equal(x.catalog.a.storageName,'a~g1');assert.deepEqual(x.catalog.a.indexes,[]);assert.deepEqual(x.tables.a,[{id:'fresh'}]);
 run('DROP TABLE a; CREATE TABLE a(id INTEGER PRIMARY KEY)',x.catalog,x.tables);assert.equal(x.catalog.a.generation,2);assert.equal(x.catalog.a.storageName,'a~g2');
});
test('DROP/CREATE pure transaction remains rollback-safe when caller clones staged state',()=>{
 const original=run('CREATE TABLE a(id INTEGER PRIMARY KEY); INSERT INTO a(id) VALUES(1)');
 assert.throws(()=>run('DROP TABLE a; CREATE TABLE a(id INTEGER PRIMARY KEY); INSERT INTO a(id) VALUES(NULL)',structuredClone(original.catalog),structuredClone(original.tables)),/NULL/);
 assert.equal(original.catalog.a.generation,0);assert.deepEqual(original.tables.a,[{id:1}]);
});
