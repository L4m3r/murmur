import { assertFaultBoundary } from './fault-proof.mjs';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
const base = process.env.BASE_URL || 'http://127.0.0.1:9876';
const auth = process.env.API_TOKEN ? { authorization: `Bearer ${process.env.API_TOKEN}` } : {};
async function query(sql, extra = {}, expected = 200) {
  const payload = JSON.stringify({ sql, idempotencyKey: randomUUID(), ...extra });
  const deadline = Date.now() + 30000;
  while (true) {
    const response = await fetch(`${base}/query`, { method:'POST', headers:{'content-type':'application/json',...auth}, body:payload });
    const value = await response.json();
    if ((expected === 200 || (expected === 503 && extra.fault && value.code === 'PLAN_PENDING')) && response.status === 503 && value.retryable === true && Date.now() < deadline) {
      console.error(`${value.code==='PLAN_PENDING'?'PLAN_PENDING':'RETRY'} same idempotency key after retryable503: ${value.error}; diagnostics=${JSON.stringify({code:value.code,rpc:value.rpc,retryAfterMs:value.retryAfterMs})}`);
      await new Promise(resolve => setTimeout(resolve,Math.min(Math.max(250,value.retryAfterMs||0),Math.max(0,deadline-Date.now())))); continue;
    }
    assert.equal(response.status,expected,`${sql.slice(0,300)}: ${JSON.stringify(value)}`);
    if (expected === 503 && extra.fault) assertFaultBoundary(extra.fault,value);
    return value;
  }
}
async function frontier(){const response=await fetch(`${base}/snapshot`,{method:'POST',headers:{'content-type':'application/json',...auth},body:'{}'});assert.equal(response.status,200);return (await response.json()).frontier;}
const rows = result => result.results.at(-1).rows;
const routing = (result, table) => result.routing.find(route => route.table === table);
const create = table => `CREATE TABLE ${table} (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL, tag TEXT NOT NULL, email TEXT NOT NULL, payload TEXT)`;
const insert = (table, count = 32) => `INSERT INTO ${table} (id,balance,tag,email,payload) VALUES ` + Array.from({ length: count }, (_, i) => `(${i+1},100,'${i%2 ? 'odd' : 'even'}','person${i+1}','${'x'.repeat(96)}')`).join(',');
const mode = process.argv[2], manifest = process.argv[3];
if (mode === '--migration-seed') {
  const table = `migration_${Date.now().toString(36)}`;
  await query(create(table));
  const sql = `${insert(table)}; CREATE UNIQUE INDEX migration_email ON ${table}(email)`;
  const idempotencyKey = randomUUID();
  await query(sql, { idempotencyKey, fault: 'after-first-commit' }, 503);
  await writeFile(manifest, JSON.stringify({ table, sql, idempotencyKey }));
  console.log('PASS tablet split plus index creation partially committed for SIGKILL');
} else if (mode === '--migration-verify') {
  const { table, sql, idempotencyKey } = JSON.parse(await readFile(manifest, 'utf8'));
  const replay = await query(sql, { idempotencyKey });
  assert.ok(routing(replay, table).tablets.length > 1);
  assert.equal(rows(await query(`SELECT * FROM ${table}`)).length, 32);
  const indexed = await query(`SELECT * FROM ${table} WHERE email = 'person17'`);
  assert.equal(rows(indexed)[0].id, 17);
  assert.equal(indexed.plan.type, 'index-range');
  assert.equal(indexed.plan.rowsFetched, 1);
  assert.deepEqual(await query(sql, { idempotencyKey }), replay);
  console.log('PASS SIGKILL restart recovers tablet migration and durable secondary index atomically');
} else if (mode === '--merge-seed') {
  const { table } = JSON.parse(await readFile(manifest, 'utf8'));
  await query(`INSERT INTO ${table} (id,balance,tag,email,payload) VALUES ` + Array.from({length:32},(_,i)=>`(${i+33},100,'extra','person${i+33}','${'x'.repeat(96)}')`).join(','));
  const previous = routing(await query(`SELECT * FROM ${table}`), table);
  assert.ok(previous.tablets.length > 1);
  assert.ok(previous.indexes[0].tablets.length > 1);
  const before = await frontier();
  const sql = `DELETE FROM ${table} WHERE id > 1`, idempotencyKey = randomUUID();
  await query(sql, { idempotencyKey, fault:'after-first-commit' }, 503);
  await writeFile(manifest, JSON.stringify({table,sql,idempotencyKey,previous,before}));
  console.log('PASS ordered base/index tablet merge partially committed for SIGKILL');
} else if (mode === '--merge-verify') {
  const {table,sql,idempotencyKey,previous,before}=JSON.parse(await readFile(manifest,'utf8'));
  const replay=await query(sql,{idempotencyKey}), current=routing(replay,table);
  assert.equal(current.tablets.length,1);
  assert.equal(current.indexes[0].tablets.length,1);
  assert.deepEqual(rows(await query(`SELECT id FROM ${table} ORDER BY id`)),[{id:1}]);
  const indexed=await query(`SELECT id FROM ${table} WHERE email = 'person1'`);
  assert.deepEqual(rows(indexed),[{id:1}]);assert.equal(indexed.plan.type,'index-range');
  assert.equal(indexed.plan.indexTabletsTotal,1);
  assert.deepEqual(rows(await query(`SELECT id FROM ${table} WHERE email = 'person17'`)),[]);
  const historical=await query(`SELECT id FROM ${table} ORDER BY id`,{snapshot:before});
  assert.equal(rows(historical).length,64);
  assert.equal(routing(historical,table).tablets.length,previous.tablets.length);
  const oldIndex=await query(`SELECT id FROM ${table} WHERE email = 'person17'`,{snapshot:before});
  assert.deepEqual(rows(oldIndex),[{id:17}]);
  assert.deepEqual(await query(sql,{idempotencyKey}),replay);
  console.log('PASS SIGKILL restart recovers ordered base/index merge, historical snapshot routes and idempotent replay');
} else {
  const table = `phase2_${Date.now().toString(36)}`;
  const initial = await query(create(table));
  assert.equal(routing(initial, table).tablets.length, 1);
  const initialSnapshot=await frontier();
  const loaded = await query(insert(table));
  const split = routing(loaded, table);
  assert.ok(split.tablets.length > 1, 'byte threshold should split tablets');
  assert.ok(split.epoch > routing(initial, table).epoch);
  assert.equal(rows(await query(`SELECT * FROM ${table}`)).length, 32);
  console.log(`PASS automatic size split: 1 -> ${split.tablets.length} tablets, epoch ${split.epoch}`);
  const old=await query(`SELECT * FROM ${table}`,{snapshot:initialSnapshot});
  assert.deepEqual(rows(old),[]);assert.equal(routing(old,table).tablets.length,1);
  console.log('PASS historical snapshot retains pre-split tablet routing');
  await query(`CREATE INDEX by_tag ON ${table}(tag); CREATE UNIQUE INDEX by_email ON ${table}(email)`);
  let indexed = await query(`SELECT id FROM ${table} WHERE email = 'person17'`);
  assert.deepEqual(rows(indexed), [{ id: 17 }]);
  assert.equal(indexed.plan.type, 'index-range');
  assert.equal(indexed.plan.rowsFetched, 1);
  const byTag = await query(`SELECT id FROM ${table} WHERE tag = 'odd' ORDER BY id`);
  assert.equal(rows(byTag).length, 16);
  assert.equal(byTag.plan.type, 'index-range');
  console.log('PASS durable unique and nonunique secondary index query plans fetch matching rows');
  await query(`UPDATE ${table} SET email = 'renamed' WHERE id = 17`);
  assert.equal(rows(await query(`SELECT * FROM ${table} WHERE email = 'person17'`)).length, 0);
  assert.equal(rows(await query(`SELECT * FROM ${table} WHERE email = 'renamed'`))[0].id, 17);
  await query(`UPDATE ${table} SET email = 'person1' WHERE id = 17`, {}, 400);
  assert.equal(rows(await query(`SELECT * FROM ${table} WHERE email = 'renamed'`))[0].id, 17);
  console.log('PASS index maintenance and unique constraint rollback');
  const writes = [], reads = [];
  for (let i = 0; i < 20; i++) {
    writes.push(query(`UPDATE ${table} SET balance = balance - 1 WHERE id = 1; UPDATE ${table} SET balance = balance + 1 WHERE id = 32`));
    reads.push(query(`SELECT balance FROM ${table}`).then(result => assert.equal(rows(result).reduce((sum, row) => sum + row.balance, 0), 3200)));
  }
  const results = await Promise.all(writes);
  await Promise.all(reads);
  assert.ok(new Set(results.map(result => result.coordinatorId)).size > 1, 'requests should use multiple coordinators');
  assert.ok(results.some(result => result.participants.filter(p => p.name.startsWith('base/')).length > 1), 'transfer should touch separate row tablets');
  console.log('PASS concurrent multi-coordinator cross-tablet writes and reads preserve transfer invariant');
  const merged = await query(`DELETE FROM ${table} WHERE id > 1`);
  assert.ok(routing(merged, table).tablets.length < split.tablets.length, 'small dataset should merge tablets');
  assert.equal(rows(await query(`SELECT * FROM ${table}`)).length, 1);
  assert.equal(rows(await query(`SELECT * FROM ${table} WHERE email = 'renamed'`)).length, 0);
  console.log(`PASS automatic merge after deletes: ${split.tablets.length} -> ${routing(merged, table).tablets.length} tablets; deleted index entries gone`);
  console.log('6 phase-two integration tests passed');
}
