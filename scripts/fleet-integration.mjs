import {queryRequest} from './http-query.mjs';
import assert from 'node:assert/strict';
import {readFile,writeFile} from 'node:fs/promises';
import {keyHash} from '../src/tablets.js';
const [mode,path]=process.argv.slice(2),a=process.env.FLEET_A||'http://127.0.0.1:9877',b=process.env.FLEET_B||'http://127.0.0.1:9878';
async function query(base,sql,key,retry=false){
 const result=await queryRequest(base,{sql,idempotencyKey:key},{totalMs:retry?90000:10000,attemptMs:15000,pollMs:500,retryTransport:retry});
 assert.equal(result.status,200,JSON.stringify(result.body));return result.body;
}
const rows=result=>result.results.at(-1).rows;
if(mode==='seed'){
 const stamp=Date.now().toString(36),left=`fleet_left_${stamp}`,right=`fleet_right_${stamp}`;
 let keyA=`seed_a_${stamp}`,keyB=`seed_b_${stamp}`;while(keyHash(keyA)%4===keyHash(keyB)%4)keyB+='x';
 await query(a,`CREATE TABLE ${left}(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO ${left}(id,n) VALUES (1,100)`,keyA);
 await query(b,`CREATE TABLE ${right}(id INTEGER PRIMARY KEY,n INTEGER); INSERT INTO ${right}(id,n) VALUES (1,100)`,keyB);
 await query(a,`UPDATE ${left} SET n=n-7 WHERE id=1; UPDATE ${right} SET n=n+7 WHERE id=1`,`transfer_${stamp}`);
 assert.deepEqual(rows(await query(b,`SELECT n FROM ${left}`,`read_l_${stamp}`)),[{n:93}]);
 assert.deepEqual(rows(await query(a,`SELECT n FROM ${right}`,`read_r_${stamp}`)),[{n:107}]);
 await writeFile(path,JSON.stringify({left,right,stamp}));
 console.log('PASS two-node celld database: actors seeded through separate nodes and cross-table transfer committed');
}else{
 const{left,right,stamp}=JSON.parse(await readFile(path,'utf8'));
 assert.deepEqual(rows(await query(b,`SELECT n FROM ${left}`,`failover_read_${stamp}`,true)),[{n:93}]);
 await query(b,`UPDATE ${left} SET n=n-1 WHERE id=1; UPDATE ${right} SET n=n+1 WHERE id=1`,`failover_transfer_${stamp}`,true);
 assert.deepEqual(rows(await query(b,`SELECT n FROM ${left}`,`after_l_${stamp}`,true)),[{n:92}]);
 assert.deepEqual(rows(await query(b,`SELECT n FROM ${right}`,`after_r_${stamp}`,true)),[{n:108}]);
 console.log('PASS actual owner SIGKILL: surviving celld node restored database state and committed new transaction');
}
