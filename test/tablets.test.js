import test from 'node:test';
import assert from 'node:assert/strict';
import { keyHash,planTablets,routeTablet,selectTablets,rowsBytes } from '../src/tablets.js';
import { compareKeys,rangeForFilters } from '../src/keys.js';
const schema={primaryKey:'id',columns:[{name:'id',type:'INTEGER',primaryKey:true}],indexes:[]};
const data=Array.from({length:100},(_,id)=>({id,payload:'x'.repeat(50)}));
test('coordinator hash retained but table routing uses actual ordered key bounds',()=>{
 assert.equal(keyHash(1),873244444);
 const p=planTablets(schema,data,{splitBytes:600,mergeBytes:100,maxTablets:8});
 assert.ok(p.schema.tablets.length>1);assert.equal(p.schema.tablets[0].start,null);assert.equal(p.schema.tablets.at(-1).end,null);
 assert.equal(p.schema.epoch,2);assert.equal(schema.tablets,undefined);
 for(let i=0;i<p.partitions.length;i++) {
  const part=p.partitions[i];assert.ok(part.rows.length>0);
  for(const row of part.rows)assert.equal(routeTablet(p.schema,row.id),part.id);
  assert.deepEqual(part.rows.map(r=>r.id),part.rows.map(r=>r.id).sort((a,b)=>a-b));
  if(i)assert.equal(p.schema.tablets[i].start,p.partitions[i].rows[0].id);
 }
 assert.equal(p.partitions.flatMap(p=>p.rows).length,100);
});
test('primary range scan prunes actual tablets at inclusive/exclusive boundaries',()=>{
 const s={...schema,epoch:1,tablets:[{id:'left',start:null,end:10},{id:'mid',start:10,end:20},{id:'right',start:20,end:null}]};
 const range=(op,value)=>rangeForFilters([{column:'id',op,value}],'id');
 assert.deepEqual(selectTablets(s,range('=',10)).map(t=>t.id),['mid']);
 assert.deepEqual(selectTablets(s,range('<',10)).map(t=>t.id),['left']);
 assert.deepEqual(selectTablets(s,range('<=',10)).map(t=>t.id),['left','mid']);
 assert.deepEqual(selectTablets(s,range('>=',20)).map(t=>t.id),['right']);
 assert.deepEqual(selectTablets(s,{empty:true}),[]);
});
test('adjacent unequal-width ranges merge; all deleted rows collapse map',()=>{
 const s={...schema,epoch:4,tablets:[{id:'a',start:null,end:2},{id:'b',start:2,end:99},{id:'c',start:99,end:null}]};
 const p=planTablets(s,[],{splitBytes:600,mergeBytes:100});
 assert.equal(p.schema.epoch,5);assert.equal(p.schema.tablets.length,1);
 assert.deepEqual(p.schema.tablets.map(({start,end})=>({start,end})),[{start:null,end:null}]);
 assert.ok(p.events.every(e=>e.type==='merge'));
 assert.equal(planTablets(p.schema,[]).changed,false);
});
test('composite index boundaries are ordered tuples, NULL entries included',()=>{
 const s={keyColumns:['value','pk']};
 const rows=Array.from({length:20},(_,pk)=>({value:pk<5?null:Math.floor(pk/5),pk,payload:'x'.repeat(40)}));
 const p=planTablets(s,rows.reverse(),{splitBytes:200,mergeBytes:50,maxTablets:8});
 const flat=p.partitions.flatMap(t=>t.rows).map(r=>[r.value,r.pk]);
 assert.deepEqual(flat,[...flat].sort(compareKeys));
 assert.ok(p.schema.tablets.slice(1).every(t=>Array.isArray(t.start)));
 const eq=rangeForFilters([{column:'value',op:'=',value:2}],'value',{compositePrefix:true});
 const selected=new Set(selectTablets(p.schema,eq).map(t=>t.id));
 for(const row of rows.filter(r=>r.value===2))assert.ok(selected.has(routeTablet(p.schema,[row.value,row.pk])));
});
test('one large row remains one tablet; actual-key pivots split clustered keys',()=>{
 assert.equal(planTablets(schema,[{id:1,payload:'界'.repeat(1000)}],{splitBytes:300,mergeBytes:100}).schema.tablets.length,1);
 const p=planTablets(schema,[{id:1,payload:'x'.repeat(200)},{id:2,payload:'x'.repeat(200)}],{splitBytes:300,mergeBytes:100});
 assert.equal(p.schema.tablets.length,2);assert.equal(p.schema.tablets[1].start,2);
 assert.equal(rowsBytes([{id:'界'}]),new TextEncoder().encode('[{"id":"界"}]').byteLength);
});
test('legacy hash maps, duplicate physical keys and invalid thresholds fail closed',()=>{
 assert.throws(()=>planTablets({...schema,epoch:1,tablets:[{id:'old',start:0,end:4294967296}]},[]));
 assert.throws(()=>planTablets(schema,[{id:1},{id:1}]),/Duplicate/);
 assert.throws(()=>planTablets(schema,[],{splitBytes:100,mergeBytes:200}));
 assert.throws(()=>planTablets({...schema,epoch:1,tablets:[{id:'a',start:null,end:10},{id:'b',start:9,end:null}]},[]));
});
