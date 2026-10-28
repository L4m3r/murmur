import test from 'node:test';
import assert from 'node:assert/strict';
import { tryBuildSelective } from '../src/planner.js';
import { parse } from '../src/sql.js';
import { encodeKey } from '../src/keys.js';

function fixture({unique=false,pageSize=2,indexState}={}) {
  const tablets=[{id:'left',start:null,end:50},{id:'right',start:50,end:null}];
  const index={...(indexState?{state:indexState}:{}),name:'by_tag',column:'tag',unique,keyColumns:['value','pk'],epoch:1,tablets:[{id:'all',start:null,end:null}]};
  const schema={primaryKey:'id',columns:[{name:'id',type:'INTEGER',primaryKey:true,notNull:true},{name:'tag',type:'TEXT',notNull:false},{name:'n',type:'INTEGER',notNull:false}],indexes:[index],epoch:1,tablets};
  const data=Array.from({length:100},(_,id)=>({id,tag:`tag${id}`,n:id}));
  const calls=[];
  const stores=new Map([['catalog/a',[{key:'schema',row:schema}]],['base/a/left',data.filter(r=>r.id<50).map(row=>({key:row.id,row}))],['base/a/right',data.filter(r=>r.id>=50).map(row=>({key:row.id,row}))],['index/a/by_tag/all',data.map(r=>({key:[r.tag,r.id],row:{value:r.tag,pk:r.id}}))]]);
  const env={TABLET_SPLIT_BYTES:'999999',TABLET_MERGE_BYTES:'2',SHARDS:{idFromName:name=>name,get(name){return{async fetch(url,init){
    const input=JSON.parse(init.body);calls.push({name,...input});let entries=stores.get(name)||[];
    if(input.op==='stats')return Response.json({rows:entries.length,bytes:Buffer.byteLength(JSON.stringify(entries.map(e=>e.row)))});
    if(input.op==='getMany')entries=entries.filter(e=>input.keys.some(k=>encodeKey(k)===encodeKey(e.key)));
    if(input.op==='scan'){
      const r=input.range;entries=entries.filter(e=>{const k=encodeKey(e.key);return !r.empty&&(r.lower===null||k>r.lower||(k===r.lower&&r.lowerInclusive))&&(r.upper===null||k<r.upper||(k===r.upper&&r.upperInclusive));});
      entries=[...entries].sort((a,b)=>encodeKey(a.key)<encodeKey(b.key)?-1:1);if(input.reverse)entries.reverse();
      const size=Math.min(pageSize,input.pageSize??pageSize),offset=Number(input.cursor||0),total=entries.length;entries=entries.slice(offset,offset+size);
      return Response.json({rows:entries.map(e=>e.row),versionsRead:entries.length,cursor:offset+entries.length<total?String(offset+entries.length):null,nextCursor:offset+entries.length<total?String(offset+entries.length):null});
    }
    return Response.json({rows:entries.map(e=>e.row),versionsRead:entries.length});
  }}}}};
  return{env,calls,data,async build(sql){return tryBuildSelective(env,parse(sql),['a'],1);}};
}

test('point mutation loads only matching row and emits incremental index deltas',async()=>{
 const f=fixture(),r=await f.build("UPDATE a SET tag='changed',n=n+1 WHERE id=5");
 assert.equal(r.response.changes.baseRows,1);assert.equal(r.response.changes.indexRows,2);
 assert.deepEqual(r.targets.find(t=>t.name==='base/a/left').changes,[{key:5,row:{id:5,tag:'changed',n:6}}]);
 const scans=f.calls.filter(c=>c.op==='scan');assert.equal(scans.length,1);assert.equal(scans[0].name,'base/a/left');assert.equal(scans[0].range.lower,encodeKey(5));
 assert.ok(!f.calls.some(c=>c.name==='base/a/right'));
 assert.equal(r.response.maintenance[0].targets.find(t=>t.name==='base/a/left').rowsDelta,0);
});
test('index-driven mutation uses actual prefix scans and point base fetches',async()=>{
 const f=fixture(),r=await f.build("DELETE FROM a WHERE tag='tag80'");
 assert.deepEqual(r.targets.find(t=>t.name==='base/a/right').changes,[{key:80,row:null}]);
 assert.ok(f.calls.some(c=>c.op==='scan'&&c.name==='index/a/by_tag/all'));
 assert.ok(f.calls.some(c=>c.op==='getMany'&&c.name==='base/a/right'&&c.keys[0]===80));
 assert.ok(!f.calls.some(c=>c.op==='scan'&&c.name.startsWith('base/')));
});
test('multi-statement overlay sees inserts, indexed moves and deletes',async()=>{
 const f=fixture(),r=await f.build("INSERT INTO a (id,tag,n) VALUES (101,'new',1); UPDATE a SET n=n+1 WHERE tag='new'; SELECT * FROM a WHERE tag='new'; DELETE FROM a WHERE id=101; SELECT * FROM a WHERE id=101");
 assert.deepEqual(r.response.results[2].rows,[{id:101,tag:'new',n:2}]);assert.deepEqual(r.response.results[4].rows,[]);
 assert.equal(r.response.changes.baseRows,0);assert.equal(r.response.changes.indexRows,0);
});
test('global UNIQUE probes reject unseen duplicate and reconcile deletes/moves',async()=>{
 const f=fixture({unique:true});
 await assert.rejects(()=>f.build("INSERT INTO a(id,tag,n) VALUES(101,'tag80',1)"),/Unique index/);
 const r=await f.build("DELETE FROM a WHERE id=80; INSERT INTO a(id,tag,n) VALUES(101,'tag80',1); SELECT id FROM a WHERE tag='tag80'");
 assert.deepEqual(r.response.results.at(-1).rows,[{id:101}]);
 assert.ok(r.targets.find(t=>t.name==='index/a/by_tag/all').reads.length>0);
});
test('existing PK duplicate remains rejected and empty predicates still validate',async()=>{
 const f=fixture();await assert.rejects(()=>f.build("INSERT INTO a(id,tag,n) VALUES(80,'new',1)"),/Duplicate primary key/);
 await assert.rejects(()=>f.build('UPDATE a SET missing=1 WHERE id>3 AND id<0'),/Unknown column/);
});
test('range pagination drains all candidate pages without whole-table scan',async()=>{
 const f=fixture({pageSize:2}),r=await f.build('UPDATE a SET n=n+1 WHERE id>=45 AND id<55');
 assert.equal(r.response.changes.baseRows,10);
 assert.equal(f.calls.filter(c=>c.op==='scan').length,6);
 assert.equal(r.response.plan.rowsRead,10);
});
test('DDL chooses maintenance path, overflow reads only affected tablet and leaves distant actor untouched',async()=>{
 const f=fixture();assert.equal(await f.build('CREATE INDEX other ON a(n)'),null);
 f.env.TABLET_SPLIT_BYTES='64';const r=await f.build('UPDATE a SET n=n+1 WHERE id=5');
 assert.ok(r.response.events.some(e=>e.type==='split'));assert.equal(r.response.routing[0].epoch,2);
 assert.ok(!f.calls.some(c=>c.name==='base/a/right'&&['scan','getMany'].includes(c.op)));
 assert.equal(r.targets.find(t=>t.name==='base/a/right'),undefined);
 assert.equal(r.response.routing[0].tablets.find(t=>t.id==='right').epoch,1);
 assert.ok(r.targets.find(t=>t.name==='base/a/left').meta.retired);
 const copied=r.targets.filter(t=>t.name.startsWith('base/a/e')).flatMap(t=>t.changes).map(c=>c.row);
 assert.equal(copied.length,50);assert.equal(copied.find(row=>row.id===5).n,6);
 assert.equal(r.response.plan.maintenanceRowsRead,50);
});


test('targeted repartition never interprets absent materialization as empty',async()=>{
 const {planAffectedTablets}=await import('../src/tablets.js');
 const schema={primaryKey:'id',epoch:1,tablets:[{id:'a',start:null,end:10},{id:'b',start:10,end:20},{id:'c',start:20,end:null}]};
 const result=planAffectedTablets(schema,new Map([['a',[]],['b',[]]]),{splitBytes:100,mergeBytes:20});
 assert.equal(result.schema.tablets.length,2);
 assert.deepEqual(result.schema.tablets.at(-1),{...schema.tablets.at(-1),epoch:1});
 assert.equal(result.partitions.length,1);assert.deepEqual(result.partitions[0].rows,[]);
 assert.equal(result.events[0].type,'merge');
 const noMerge=planAffectedTablets(schema,new Map([['b',[]]]),{splitBytes:100,mergeBytes:20});
 assert.equal(noMerge.changed,false);assert.equal(noMerge.schema.tablets.length,3);
 assert.throws(()=>planAffectedTablets(schema,new Map([['a',[{id:12}]]])),/outside/);
});

test('BUILDING unique accepts duplicate without full-base probes; VALIDATING uses only prefix guard',async()=>{
 const building=fixture({unique:true,indexState:'building'});const result=await building.build("INSERT INTO a(id,tag,n) VALUES(101,'tag80',1)");
 assert.equal(result.response.changes.baseRows,1);
 assert.ok(!building.calls.some(c=>c.op==='scan'),'Building uniqueness must not scan base or partial index');
 assert.ok(result.targets.some(t=>t.name.startsWith('index/')&&t.changes.length));
 const validating=fixture({unique:true,indexState:'validating'});await assert.rejects(()=>validating.build("INSERT INTO a(id,tag,n) VALUES(101,'tag80',1)"),/Unique index/);
 assert.ok(validating.calls.some(c=>c.op==='scan'&&c.name.startsWith('index/')));
 assert.ok(!validating.calls.some(c=>c.op==='scan'&&c.name.startsWith('base/')));
});
test('BUILDING and VALIDATING indexes are hidden from SELECT optimizer',async()=>{
 for(const indexState of ['building','validating']){
  const f=fixture({indexState}),result=await f.build("SELECT id FROM a WHERE tag='tag80'");
  assert.deepEqual(result.response.results[0].rows,[{id:80}]);assert.notEqual(result.response.plan.type,'index-range');
  assert.ok(!f.calls.some(c=>c.op==='scan'&&c.name.startsWith('index/')));
 }
});

test('ordered LIMIT chooses primary/index physical ordering even without a matching index predicate',async()=>{
 const primary=fixture(),a=await primary.build("SELECT id FROM a WHERE tag='tag80' ORDER BY id LIMIT 1");
 assert.deepEqual(a.response.results[0].rows,[{id:80}]);assert.equal(a.response.plan.limitPushed,true);assert.ok(!primary.calls.some(c=>c.name.startsWith('index/')));assert.ok(a.response.plan.rowsRead<100);
 const index=fixture(),b=await index.build("SELECT tag FROM a ORDER BY tag DESC LIMIT 3");
 assert.deepEqual(b.response.results[0].rows,[{tag:'tag99'},{tag:'tag98'},{tag:'tag97'}]);assert.equal(b.response.plan.limitPushed,true);assert.equal(b.response.plan.rowsRead,3);assert.equal(b.response.plan.index,'by_tag');
 const zero=fixture(),c=await zero.build('SELECT id FROM a LIMIT 0');assert.deepEqual(c.response.results[0].rows,[]);assert.ok(!zero.calls.some(call=>call.name.startsWith('base/')));
});
