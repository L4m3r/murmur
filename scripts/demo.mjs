import {queryRequest} from './http-query.mjs';
import { randomUUID } from 'node:crypto';
const base = process.env.BASE_URL || 'http://127.0.0.1:9876';
const table = `demo_${Date.now().toString(36)}`;
async function run(sql) {
  const response=await queryRequest(base,{sql,idempotencyKey:randomUUID()},{headers:process.env.API_TOKEN?{authorization:`Bearer ${process.env.API_TOKEN}`}:{}});
  const result=response.body;
  console.log(`\nSQL> ${sql}\n${JSON.stringify(result, null, 2)}`);
  if (response.status !== 200) throw new Error('Demo query failed');
  return result;
}
await run(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, balance INTEGER NOT NULL); INSERT INTO ${table} (id,owner,balance) VALUES (1,'Ada',100),(2,'Grace',100)`);
const transfer = await run(`BEGIN; UPDATE ${table} SET balance = balance - 25 WHERE id = 1; UPDATE ${table} SET balance = balance + 25 WHERE id = 2; SELECT * FROM ${table} ORDER BY id; COMMIT;`);
console.log(`\nCommitted an atomic transfer using coordinator ${transfer.coordinatorId}; row tablets touched: ${transfer.participants.filter(p => p.name.startsWith('base/')).length}.`);
