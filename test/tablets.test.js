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

test('full planning preserves unchanged actor epochs across a split',()=>{
 const s={...schema,epoch:7,tablets:[{id:'left',start:null,end:10,epoch:2},{id:'right',start:10,end:null,epoch:5}]};
 const rows=[{id:1,payload:'x'.repeat(20)},{id:10,payload:'x'.repeat(100)},{id:11,payload:'x'.repeat(100)}];
 const p=planTablets(s,rows,{splitBytes:180,mergeBytes:2});
 assert.equal(p.schema.epoch,8);
 assert.deepEqual(p.schema.tablets[0],s.tablets[0]);
 assert.equal(p.partitions.find(t=>t.id==='left').epoch,2);
 assert.ok(p.schema.tablets.slice(1).every(t=>t.epoch===8));
 assert.ok(p.partitions.filter(t=>t.id!=='left').every(t=>t.epoch===8));
 assert.deepEqual(s.tablets.map(t=>t.epoch),[2,5]);
});

test('affected planning keeps unloaded tablet identities and epochs unchanged',async()=>{
 const {planAffectedTablets,tabletEpoch}=await import('../src/tablets.js');
 const s={...schema,epoch:9,tablets:[{id:'left',start:null,end:10,epoch:3},{id:'right',start:10,end:null,epoch:7}]};
 const p=planAffectedTablets(s,new Map([['right',[{id:10,payload:'x'.repeat(100)},{id:11,payload:'x'.repeat(100)}]]]),{splitBytes:180,mergeBytes:2});
 assert.equal(p.schema.epoch,10); assert.deepEqual(p.schema.tablets[0],s.tablets[0]);
 assert.ok(!p.partitions.some(t=>t.id==='left'));
 assert.ok(p.partitions.every(t=>t.epoch===10));
 assert.equal(tabletEpoch(p.schema,'left'),3);
 const unchanged=planAffectedTablets(p.schema,new Map(),{splitBytes:180,mergeBytes:2});
 assert.equal(unchanged.changed,false);assert.deepEqual(unchanged.schema,p.schema);
});

test('merge replacements get one new epoch without changing distant actors',async()=>{
 const {planAffectedTablets}=await import('../src/tablets.js');
 const s={...schema,epoch:12,tablets:[{id:'a',start:null,end:10,epoch:2},{id:'b',start:10,end:20,epoch:9},{id:'c',start:20,end:null,epoch:11}]};
 const p=planAffectedTablets(s,new Map([['a',[]],['b',[]]]));
 assert.equal(p.schema.epoch,13);assert.equal(p.schema.tablets[0].epoch,13);
 assert.deepEqual(p.schema.tablets[1],s.tablets[2]);
 assert.deepEqual(p.schema.tablets.map(({start,end})=>({start,end})),[{start:null,end:20},{start:20,end:null}]);
 assert.equal(p.partitions.length,1);assert.equal(p.partitions[0].epoch,13);
});

test('descriptor epoch fallback, validation, initialization and overflow guard',async()=>{
 const {tabletEpoch,planAffectedTablets}=await import('../src/tablets.js');
 const legacy={...schema,epoch:6,tablets:[{id:'fixture',start:null,end:null}]};
 assert.equal(tabletEpoch(legacy,'fixture'),6);
 assert.equal(planAffectedTablets(legacy,new Map()).schema.tablets[0].epoch,6);
 assert.equal(planTablets(schema,[]).schema.tablets[0].epoch,1);
 assert.throws(()=>tabletEpoch(legacy,'missing'),/Unknown/);
 for(const epoch of [0,null,1.5,7])assert.throws(()=>planAffectedTablets({...legacy,tablets:[{...legacy.tablets[0],epoch}]},new Map()),/epoch/);
 const max={...legacy,epoch:Number.MAX_SAFE_INTEGER};
 assert.throws(()=>planTablets(max,[{id:1,payload:'x'.repeat(100)},{id:2,payload:'x'.repeat(100)}],{splitBytes:180,mergeBytes:2}),/exhausted/);
});

test('default ceiling permits more than 64 tablets and configuration remains bounded',()=>{
 const p=planTablets(schema,data,{splitBytes:50,mergeBytes:2});
 assert.equal(p.schema.tablets.length,100);
 assert.equal(planTablets(schema,[],{maxTablets:4096}).schema.tablets.length,1);
 assert.throws(()=>planTablets(schema,[],{maxTablets:4097}),/sizing/);
});

test('4096-tablet point routing and bounded range visit logarithmic descriptors',()=>{
 let inspected=0;
 const tablets=Array.from({length:4096},(_,i)=>({id:`t${i}`,epoch:1,get start(){inspected++;return i===0?null:i;},get end(){inspected++;return i===4095?null:i+1;}}));
 const s={...schema,epoch:1,tablets};
 assert.equal(routeTablet(s,2048.5),'t2048');
 assert.ok(inspected<32,`point routing inspected ${inspected} bounds`);
 inspected=0;
 const range=rangeForFilters([{column:'id',op:'>=',value:2048},{column:'id',op:'<=',value:2050}],'id');
 assert.deepEqual(selectTablets(s,range).map(t=>t.id),['t2048','t2049','t2050']);
 assert.ok(inspected<32,`range routing inspected ${inspected} bounds`);
});

test('binary routing matches linear oracle at all boundaries and unbounded ends',()=>{
 const tablets=Array.from({length:256},(_,i)=>({id:`t${i}`,start:i===0?null:i,end:i===255?null:i+1,epoch:1}));
 const s={...schema,epoch:1,tablets};
 const before=structuredClone(s);
 for(const key of [-100,0,...Array.from({length:256},(_,i)=>i),255.5,1000]) {
   const expected=tablets.find(t=>(t.start===null||key>=t.start)&&(t.end===null||key<t.end));
   assert.equal(routeTablet(s,key),expected.id);
 }
 for(const [op,value] of [['<',1],['<=',1],['>',254],['>=',255],['=',128],['=',-100],['=',1000]]) {
   const r=rangeForFilters([{column:'id',op,value}],'id');
   const expected=tablets.filter(t=>{
     if(t.end!==null && r.lower!==null && compareKeys(t.end,value)<=0 && ['>','>=','='].includes(op))return false;
     if(t.start!==null && ['<','<=','='].includes(op) && (t.start>value||(t.start===value&&op==='<')))return false;
     return true;
   });
   assert.deepEqual(selectTablets(s,r).map(t=>t.id),expected.map(t=>t.id),`${op} ${value}`);
 }
 const unbounded={lower:null,upper:null,lowerInclusive:true,upperInclusive:false,empty:false};
 assert.deepEqual(selectTablets(s,unbounded),tablets);
 assert.deepEqual(s,before);
});
