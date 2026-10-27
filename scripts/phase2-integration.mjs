import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { keyHash } from '../src/tablets.js';
const base = process.env.BASE_URL || 'http://127.0.0.1:9876';
const auth = process.env.API_TOKEN ? { authorization: `Bearer ${process.env.API_TOKEN}` } : {};
async function query(sql, extra = {}, expected = 200) {
  const response = await fetch(`${base}/query`, { method: 'POST', headers: { 'content-type': 'application/json', ...auth }, body: JSON.stringify({ sql, idempotencyKey: randomUUID(), ...extra }) });
  const value = await response.json();
  assert.equal(response.status, expected, `${sql.slice(0, 300)}: ${JSON.stringify(value)}`);
  return value;
}
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
  assert.equal(indexed.plan.type, 'index');
  assert.equal(indexed.plan.rowsFetched, 1);
  assert.deepEqual(await query(sql, { idempotencyKey }), replay);
  console.log('PASS SIGKILL restart recovers tablet migration and durable secondary index atomically');
} else {
  const table = `phase2_${Date.now().toString(36)}`;
  const initial = await query(create(table));
  assert.equal(routing(initial, table).tablets.length, 1);
  const loaded = await query(insert(table));
  const split = routing(loaded, table);
  assert.ok(split.tablets.length > 1, 'byte threshold should split tablets');
  assert.ok(split.epoch > routing(initial, table).epoch);
  assert.equal(rows(await query(`SELECT * FROM ${table}`)).length, 32);
  console.log(`PASS automatic size split: 1 -> ${split.tablets.length} tablets, epoch ${split.epoch}`);
  const stale = await fetch(`${base}/debug/tablet`, { method:'POST', headers:{'content-type':'application/json',...auth}, body:JSON.stringify({table,tablet:routing(initial,table).tablets[0].id,epoch:routing(initial,table).epoch}) });
  assert.equal(stale.status,409);
  assert.equal((await stale.json()).code,'STALE_EPOCH');
  console.log('PASS retired tablet rejects stale routing epoch');
  await query(`CREATE INDEX by_tag ON ${table}(tag); CREATE UNIQUE INDEX by_email ON ${table}(email)`);
  let indexed = await query(`SELECT id FROM ${table} WHERE email = 'person17'`);
  assert.deepEqual(rows(indexed), [{ id: 17 }]);
  assert.equal(indexed.plan.type, 'index');
  assert.equal(indexed.plan.rowsFetched, 1);
  const byTag = await query(`SELECT id FROM ${table} WHERE tag = 'odd' ORDER BY id`);
  assert.equal(rows(byTag).length, 16);
  assert.equal(byTag.plan.type, 'index');
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
  assert.ok(results.some(result => result.participants.filter(p => p.kind === 'row').length > 1), 'transfer should touch separate row tablets');
  console.log('PASS concurrent multi-coordinator cross-tablet writes and reads preserve transfer invariant');
  await query(`UPDATE ${table} SET balance = balance + 500 WHERE id = 1`, { fault: 'after-lock' }, 503);
  assert.equal(rows(await query(`SELECT balance FROM ${table} WHERE id = 1`))[0].balance, 80);
  console.log('PASS another coordinator recovers abandoned acquired table lock');
  const merged = await query(`DELETE FROM ${table} WHERE id > 1`);
  assert.ok(routing(merged, table).tablets.length < split.tablets.length, 'small dataset should merge tablets');
  assert.equal(rows(await query(`SELECT * FROM ${table}`)).length, 1);
  assert.equal(rows(await query(`SELECT * FROM ${table} WHERE email = 'renamed'`)).length, 0);
  console.log(`PASS automatic merge after deletes: ${split.tablets.length} -> ${routing(merged, table).tablets.length} tablets; deleted index entries gone`);
  const left = `left_${Date.now().toString(36)}`, right = `right_${Date.now().toString(36)}`;
  await query(`CREATE TABLE ${left} (id INTEGER PRIMARY KEY, n INTEGER); CREATE TABLE ${right} (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO ${left} (id,n) VALUES (1,0); INSERT INTO ${right} (id,n) VALUES (1,0)`);
  const leftKey = randomUUID(); let rightKey = randomUUID();
  while(keyHash(leftKey)%4===keyHash(rightKey)%4) rightKey=randomUUID();
  const concurrent = [query(`UPDATE ${left} SET n=n+1`,{idempotencyKey:leftKey,pauseAfterLockMs:700}),query(`UPDATE ${right} SET n=n+1`,{idempotencyKey:rightKey,pauseAfterLockMs:700})];
  let overlap = false;
  for(let attempt=0;attempt<100;attempt++) {
    const response=await fetch(`${base}/debug/locks`,{method:'POST',headers:{'content-type':'application/json',...auth},body:'{}'});
    const {locks}=await response.json();
    if(locks[`lock:${left}`] && locks[`lock:${right}`]) {
      assert.notEqual(locks[`lock:${left}`].owner,locks[`lock:${right}`].owner); overlap=true; break;
    }
    await new Promise(resolve=>setTimeout(resolve,5));
  }
  await Promise.all(concurrent);
  assert.ok(overlap,'two independent coordinators must hold disjoint table grants simultaneously');
  console.log('PASS real celld overlap: two coordinators simultaneously hold disjoint table locks');
  console.log('8 phase-two integration tests passed');
}
