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
