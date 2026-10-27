import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { writeFile, readFile } from 'node:fs/promises';

const base = process.env.BASE_URL || 'http://127.0.0.1:9876';
const auth = process.env.API_TOKEN ? { authorization: `Bearer ${process.env.API_TOKEN}` } : {};
const key = () => randomUUID();
async function query(sql, options = {}, status = 200) {
  const response = await fetch(`${base}/query`, { method: 'POST', headers: { 'content-type': 'application/json', ...auth }, body: JSON.stringify({ sql, idempotencyKey: key(), ...options }) });
  const value = await response.json();
  assert.equal(response.status, status, `${sql}: ${JSON.stringify(value)}`);
  return value;
}
const rows = value => value.results.at(-1).rows;
let passed = 0;
async function test(name, fn) { await fn(); passed++; console.log(`PASS ${name}`); }
const mode = process.argv[2];
const manifestPath = process.argv[3] || '/tmp/actor-db-restart.json';
if (mode === '--restart-seed' || mode === '--abort-seed') {
  const table = `restart_${Date.now().toString(36)}`;
  await query(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL); INSERT INTO ${table} (id,balance) VALUES (1,100),(2,100)`);
  const sql = `UPDATE ${table} SET balance = balance - 7 WHERE id = 1; UPDATE ${table} SET balance = balance + 7 WHERE id = 2`;
  const idempotencyKey = key();
  await query(sql, { idempotencyKey, fault: mode === '--abort-seed' ? 'after-prepare' : 'after-first-commit' }, 503);
  await writeFile(manifestPath, JSON.stringify({ table, sql, idempotencyKey }));
  console.log(`PASS ${mode === '--abort-seed' ? 'prepared undecided' : 'durable partial-commit'} fault staged for process restart`);
} else if (mode === '--abort-verify') {
  const { table } = JSON.parse(await readFile(manifestPath, 'utf8'));
  assert.deepEqual(rows(await query(`SELECT * FROM ${table} ORDER BY id`)), [{ id: 1, balance: 100 }, { id: 2, balance: 100 }]);
  console.log('PASS process restart: prepared transaction without durable commit decision aborted');
} else if (mode === '--restart-verify') {
  const { table, sql, idempotencyKey } = JSON.parse(await readFile(manifestPath, 'utf8'));
  const result = await query(sql, { idempotencyKey });
  assert.ok(result.participants.length >= 2, 'cross-shard transaction expected');
  assert.deepEqual(rows(await query(`SELECT * FROM ${table} ORDER BY id`)), [{ id: 1, balance: 93 }, { id: 2, balance: 107 }]);
  assert.deepEqual(await query(sql, { idempotencyKey }), result);
  console.log('PASS process restart: durable decision recovered all shards and retry did not double-apply');
} else {
  const table = `accounts_${Date.now().toString(36)}`;
  const select = () => query(`SELECT * FROM ${table} ORDER BY id`);
  await test('health', async () => {
    const response = await fetch(`${base}/health`); assert.equal(response.status, 200); assert.equal((await response.json()).ok, true);
  });
  await test('typed catalog and distributed insert', async () => {
    const result = await query(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL, owner TEXT, active BOOLEAN); INSERT INTO ${table} (id,balance,owner,active) VALUES (1,1000,'Ada',true),(2,1000,'Grace',true)`);
    assert.ok(result.participants.filter(p => p.kind === 'row').length >= 2);
    assert.deepEqual(rows(await select()).map(r => r.balance), [1000, 1000]);
  });
  await test('filter, projection, ordering and limit', async () => {
    assert.deepEqual(rows(await query(`SELECT owner FROM ${table} WHERE active = true AND balance >= 1000 ORDER BY id DESC LIMIT 1`)), [{ owner: 'Grace' }]);
  });
  await test('rollback of complete batch on statement error', async () => {
    await query(`UPDATE ${table} SET balance = balance - 100 WHERE id = 1; INSERT INTO ${table} (id,balance) VALUES (2,10)`, {}, 400);
    assert.deepEqual(rows(await select()).map(r => r.balance), [1000, 1000]);
  });
  await test('strict types, invalid SQL and key mutation rejected', async () => {
    await query(`UPDATE ${table} SET balance = 'wrong' WHERE id = 1`, {}, 400);
    await query(`SELECT * FROM ${table} garbage`, {}, 400);
    await query(`UPDATE ${table} SET id = 4 WHERE id = 1`, {}, 400);
  });
  await test('idempotency replay and payload mismatch', async () => {
    const idempotencyKey = key(); const sql = `UPDATE ${table} SET balance = balance + 1 WHERE id = 1`;
    const first = await query(sql, { idempotencyKey });
    assert.deepEqual(await query(sql, { idempotencyKey }), first);
    await query(`SELECT * FROM ${table}`, { idempotencyKey }, 409);
    assert.equal(rows(await select())[0].balance, 1001);
  });
  await test('30 concurrent cross-shard transfers preserve invariant', async () => {
    const sql = `BEGIN; UPDATE ${table} SET balance = balance - 1 WHERE id = 1; UPDATE ${table} SET balance = balance + 1 WHERE id = 2; COMMIT;`;
    const results = await Promise.all(Array.from({ length: 30 }, () => query(sql)));
    assert.equal(new Set(results.map(r => r.version)).size, 30);
    assert.ok(results.every(r => r.participants.filter(p => p.kind === 'row').length >= 2));
    assert.deepEqual(rows(await select()).map(r => r.balance), [971, 1030]);
  });
  await test('prepared transaction is aborted after failure', async () => {
    await query(`UPDATE ${table} SET balance = balance + 50`, { fault: 'after-prepare' }, 503);
    assert.deepEqual(rows(await select()).map(r => r.balance), [971, 1030]);
  });
  await test('durable commit decision is recovered', async () => {
    const idempotencyKey = key(); const sql = `UPDATE ${table} SET balance = balance - 5 WHERE id = 1; UPDATE ${table} SET balance = balance + 5 WHERE id = 2`;
    await query(sql, { idempotencyKey, fault: 'after-decision' }, 503);
    assert.deepEqual(rows(await select()).map(r => r.balance), [966, 1035]);
    await query(sql, { idempotencyKey });
    assert.deepEqual(rows(await select()).map(r => r.balance), [966, 1035]);
  });
  await test('partial cross-shard commit never leaks through API', async () => {
    await query(`UPDATE ${table} SET balance = balance - 6 WHERE id = 1; UPDATE ${table} SET balance = balance + 6 WHERE id = 2`, { fault: 'after-first-commit' }, 503);
    assert.deepEqual(rows(await select()).map(r => r.balance), [960, 1041]);
  });
  await test('upsert and delete', async () => {
    await query(`UPSERT INTO ${table} (id,balance,owner,active) VALUES (3,10,'Lin',false); DELETE FROM ${table} WHERE id = 3`);
    assert.equal(rows(await select()).length, 2);
  });
  await test('HTTP input bounds and malformed JSON', async () => {
    let response = await fetch(`${base}/query`, { method: 'POST', headers: auth, body: '{' }); assert.equal(response.status, 400);
    response = await fetch(`${base}/query`, { method: 'POST', headers: auth, body: ' '.repeat(70000) }); assert.equal(response.status, 413);
    await query(`SELECT * FROM ${table}`, { idempotencyKey: '' }, 400);
  });
  console.log(`\n${passed} integration tests passed against ${base}`);
}
