import { randomUUID } from 'node:crypto';
const base = process.env.BASE_URL || 'http://127.0.0.1:9876';
const table = `demo_${Date.now().toString(36)}`;
async function run(sql) {
  const response = await fetch(`${base}/query`, { method: 'POST', headers: { 'content-type': 'application/json', ...(process.env.API_TOKEN ? { authorization: `Bearer ${process.env.API_TOKEN}` } : {}) }, body: JSON.stringify({ sql, idempotencyKey: randomUUID() }) });
  const result = await response.json();
  console.log(`\nSQL> ${sql}\n${JSON.stringify(result, null, 2)}`);
  if (!response.ok) throw new Error('Demo query failed');
  return result;
}
await run(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, balance INTEGER NOT NULL); INSERT INTO ${table} (id,owner,balance) VALUES (1,'Ada',100),(2,'Grace',100)`);
const transfer = await run(`BEGIN; UPDATE ${table} SET balance = balance - 25 WHERE id = 1; UPDATE ${table} SET balance = balance + 25 WHERE id = 2; SELECT * FROM ${table} ORDER BY id; COMMIT;`);
console.log(`\nCommitted an atomic transfer using coordinator ${transfer.coordinatorId}; row tablets touched: ${transfer.participants.filter(p => p.kind === 'row').length}.`);
