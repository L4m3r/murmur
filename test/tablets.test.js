import test from 'node:test';
import assert from 'node:assert/strict';
import { HASH_SPACE, keyHash, planTablets, routeTablet, rowsBytes } from '../src/tablets.js';
const schema = { primaryKey: 'id', columns: [{name:'id',type:'INTEGER',primaryKey:true}], indexes: [] };
const data = Array.from({length:100},(_,id)=>({id,payload:'x'.repeat(50)}));

test('typed hash is deterministic and compatible with legacy FNV routing',()=>{
 assert.equal(keyHash(1),873244444); assert.notEqual(keyHash(1),keyHash('1'));
 assert.ok(keyHash('Unicode 界') >= 0 && keyHash('Unicode 界') < HASH_SPACE);
});
test('initial routing covers all keys without mutating inputs',()=>{
 const p=planTablets(schema,[{id:1}]);
 assert.equal(p.changed,true); assert.equal(p.schema.epoch,1); assert.equal(routeTablet(p.schema,1),'t0');
 assert.equal(schema.tablets,undefined);
 assert.equal(planTablets(p.schema,[{id:1}]).changed,false);
});
test('split uses UTF-8 sizes and unique range routing, bounded to maximum',()=>{
 const p=planTablets(schema,data,{splitBytes:300,mergeBytes:100,maxTablets:8});
 assert.ok(p.schema.tablets.length>1); assert.ok(p.schema.tablets.length<=8); assert.equal(p.schema.epoch,2);
 assert.equal(p.partitions.flatMap(x=>x.rows).length,data.length);
 for(const r of data) assert.equal(p.partitions.filter(x=>x.rows.includes(r)).length,1);
 for(const x of p.partitions) for(const r of x.rows) assert.equal(routeTablet(p.schema,r.id),x.id);
 assert.equal(rowsBytes([{id:'界'}]),new TextEncoder().encode('[{"id":"界"}]').byteLength);
});
test('deletion merges empty aligned siblings and advances one epoch',()=>{
 const split=planTablets(schema,data,{splitBytes:300,mergeBytes:100,maxTablets:8});
 const merged=planTablets(split.schema,[],{splitBytes:300,mergeBytes:100,maxTablets:8});
 assert.equal(merged.schema.tablets.length,1); assert.equal(merged.schema.epoch,split.schema.epoch+1);
 assert.ok(merged.events.every(e=>e.type==='merge'));
 assert.deepEqual(merged.schema.tablets.map(({start,end})=>({start,end})),[{start:0,end:HASH_SPACE}]);
 assert.equal(planTablets(merged.schema,[],{splitBytes:300,mergeBytes:100,maxTablets:8}).changed,false);
});
test('single oversized row and one-sided hashes do not generate empty splits',()=>{
 const p=planTablets(schema,[{id:1,payload:'界'.repeat(10000)}],{splitBytes:300,mergeBytes:100});
 assert.equal(p.schema.tablets.length,1); assert.ok(p.partitions[0].bytes>300);
 assert.equal(p.schema.epoch,1);
});
test('invalid thresholds and malformed routing fail closed',()=>{
 assert.throws(()=>planTablets(schema,[],{splitBytes:100,mergeBytes:200}));
 assert.throws(()=>planTablets({...schema,epoch:1,tablets:[{id:'a',start:0,end:1}]},[]));
 assert.throws(()=>routeTablet({tablets:[]},1));
});
