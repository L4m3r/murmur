import test from 'node:test';
import assert from 'node:assert/strict';
import Worker, {Coordinator, DataShard, Mediator} from '../src/index.js';

// Independent model of DO ordered KV. Assertions inspect actual storage reads,
// not only optimizer plan labels. This is not a process-durability claim.
function cluster() {
  const io = [], calls = [];
  const env = {ALLOW_FAULTS:'true',TABLET_SPLIT_BYTES:'400',TABLET_MERGE_BYTES:'100'};
  function namespace(Type, kind) {
    const actors = new Map(), states = new Map();
    function storage(name) {
      const kind = name.startsWith("base/") ? "ROWS" : name.startsWith("index/") ? "INDEXES" : "CONTROL";
      const data = new Map(); let alarm=null;
      const api = {
        setAlarm:async value=>{alarm=value;},getAlarm:async()=>alarm,deleteAlarm:async()=>{alarm=null;},
        get: async key => {io.push({kind,name,op:'get',key});return structuredClone(data.get(key));},
        put: async (key,value) => data.set(key,structuredClone(value)),
        delete: async key => data.delete(key),
        list: async (options={}) => {
          const {prefix='',start,startAfter,end,limit,reverse=false} = options;
          let entries = [...data].filter(([key])=>key.startsWith(prefix) && (start===undefined||key>=start) && (startAfter===undefined||key>startAfter) && (end===undefined||key<end)).sort(([a],[b])=>a<b?-1:a>b?1:0);
          if(reverse) entries.reverse();
          if(limit!==undefined) entries=entries.slice(0,limit);
          io.push({kind,name,op:'list',options:structuredClone(options),keys:entries.map(([key])=>key),values:structuredClone(entries.map(([,value])=>value))});
          return new Map(structuredClone(entries));
        },
        transaction: async fn=>{const before=structuredClone(data);try{return await fn(api);}catch(e){data.clear();for(const [k,v]of before)data.set(k,v);throw e;}}
      };
      return api;
    }
    return {idFromName:name=>name,get(name){
      if(!states.has(name))states.set(name,{storage:storage(name),id:{name,toString:()=>name},blockConcurrencyWhile:fn=>fn()});
      if(!actors.has(name))actors.set(name,new Type(states.get(name),env));
      return {fetch:async(url,init)=>{calls.push({kind:name.startsWith("base/")?"ROWS":name.startsWith("index/")?"INDEXES":kind,name,body:JSON.parse(init.body)});return actors.get(name).fetch(new Request(url,init));}};
    },reset(){actors.clear();}};
  }
  for(const [kind,Type] of Object.entries({SHARDS:DataShard,MEDIATOR:Mediator,COORDINATOR:Coordinator}))env[kind]=namespace(Type,kind);
  let seq=0;
  return {io,calls,env,async recover(){const r=await Worker.fetch(new Request('http://127.0.0.1/recover',{method:'POST',body:'{}'}),env);assert.equal(r.status,200);},clear(){io.length=0;calls.length=0;},restart(){for(const ns of Object.values(env))ns?.reset?.();},async query(sql,extra={}){
    const result=await Worker.fetch(new Request('http://127.0.0.1/query',{method:'POST',body:JSON.stringify({sql,idempotencyKey:`ordered-${++seq}`,...extra})}),env);
    return {status:result.status,...await result.json()};
  }};
}
const rows = response => response.results.at(-1).rows;
const must = response => {assert.equal(response.status,200,JSON.stringify(response));return response;};
const literal = value => typeof value==='string'?`'${value.replaceAll("'","''")}'`:String(value);

test('ordered physical reads and tablet pruning honor every scalar bound and AND contradiction',async()=>{
  const db=cluster();
  must(await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, payload TEXT)'));
  const data=Array.from({length:40},(_,i)=>({id:i-20,payload:'x'.repeat(36)}));
  const inserted=must(await db.query(`INSERT INTO a (id,payload) VALUES ${data.map(r=>`(${r.id},'${r.payload}')`).join(',')}`));
  assert.ok(inserted.routing[0].tablets.length>2);
  const predicates=[['>',3],['>=',3],['<',-3],['<=',-3],['=',3]];
  for(const [op,pivot] of predicates){
    db.clear();
    const response=must(await db.query(`SELECT * FROM a WHERE id${op}${pivot} ORDER BY id`));
    const match=n=>op==='>'?n>pivot:op==='>='?n>=pivot:op==='<'?n<pivot:op==='<='?n<=pivot:n===pivot;
    assert.deepEqual(rows(response),data.filter(r=>match(r.id)));
    const reads=db.calls.filter(c=>c.kind==='ROWS'&&['read','get','getMany','scan'].includes(c.body.op));
    assert.ok(reads.length<inserted.routing[0].tablets.length,`${op}: must not contact all tablets`);
    const scans=db.io.filter(x=>x.kind==='ROWS'&&x.op==='list'&&x.keys.some(k=>k.startsWith('row:')));
    for(const scan of scans) for(const version of scan.values) { const row=version.row; if(row===null)continue; assert.ok(match(row.id),`${op}: storage returned out-of-range key ${row.id}`); }
  }
  for(const predicate of ['id>3 AND id<=3','id>=3 AND id<3','id=3 AND id=4','id>9 AND id<0','id=NULL']){
    db.clear();
    assert.deepEqual(rows(must(await db.query(`SELECT * FROM a WHERE ${predicate}`))),[]);
    assert.equal(db.calls.filter(c=>['ROWS','INDEXES'].includes(c.kind)&&['read','get','getMany','scan'].includes(c.body.op)).length,0,`${predicate}: impossible range must avoid data actors`);
  }
  for(const suffix of ['AND missing=1',"AND id='wrong'"]){
    assert.equal((await db.query(`SELECT * FROM a WHERE id>3 AND id<0 ${suffix}`)).status,400);
  }
  assert.equal((await db.query('SELECT missing FROM a WHERE id>3 AND id<0')).status,400);
});

test('UTF8 key comparison agrees among SQL ORDER BY, routing and physical bounds',async()=>{
  const db=cluster();
  must(await db.query('CREATE TABLE a (id TEXT PRIMARY KEY, payload TEXT)'));
  const keys=['','\u0000','a','a\u0000','a\u0000b','aa','é','界','\uE000','😀','𐀀'];
  const sorted=[...keys].sort((a,b)=>Buffer.compare(Buffer.from(a),Buffer.from(b)));
  must(await db.query(`INSERT INTO a (id,payload) VALUES ${keys.map(k=>`(${literal(k)},'${'x'.repeat(70)}')`).join(',')}`));
  assert.deepEqual(rows(must(await db.query('SELECT id FROM a ORDER BY id'))).map(r=>r.id),sorted);
  for(const bound of ['\u0000','a','a\u0000','\uE000','𐀀'])for(const op of ['>','>=','<','<=','=']){
    const compare=x=>Buffer.compare(Buffer.from(x),Buffer.from(bound));
    const expected=sorted.filter(x=>op==='>'?compare(x)>0:op==='>='?compare(x)>=0:op==='<'?compare(x)<0:op==='<='?compare(x)<=0:compare(x)===0);
    assert.deepEqual(rows(must(await db.query(`SELECT id FROM a WHERE id${op}${literal(bound)} ORDER BY id`))).map(r=>r.id),expected,`${op} ${JSON.stringify(bound)}`);
  }
  for(const invalid of ['\ud800','\udfff','a\ud800b']) assert.equal((await db.query(`INSERT INTO a (id,payload) VALUES (${literal(invalid)},'invalid')`)).status,400);
});

test('finite REAL physical ordering normalizes signed zero and numeric equivalent literals',async()=>{
  const db=cluster();
  must(await db.query('CREATE TABLE a (id REAL PRIMARY KEY, payload TEXT)'));
  const keys=[-1000,-10.5,-1,-0.125,0,0.125,1,10.5,1000];
  must(await db.query(`INSERT INTO a (id,payload) VALUES ${keys.map(k=>`(${k},'${'x'.repeat(70)}')`).join(',')}`));
  assert.equal((await db.query("INSERT INTO a (id,payload) VALUES (-0,'duplicate')")).status,400);
  assert.equal((await db.query("INSERT INTO a (id,payload) VALUES (1.0,'duplicate')")).status,400);
  for(const bound of [-10.5,-0,0.125,1])for(const op of ['>','>=','<','<=','=']){
    const expected=keys.filter(x=>op==='>'?x>bound:op==='>='?x>=bound:op==='<'?x<bound:op==='<='?x<=bound:x===bound);
    assert.deepEqual(rows(must(await db.query(`SELECT id FROM a WHERE id${op}${bound} ORDER BY id`))).map(r=>r.id),expected);
  }
});

test('secondary index prefix scans cross duplicate-value partitions without losing PK suffixes',async()=>{
  const db=cluster();
  must(await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, tag TEXT, payload TEXT)'));
  const data=Array.from({length:45},(_,id)=>({id,tag:id<3?null:id<6?'':id<40?'same':'z'}));
  must(await db.query(`INSERT INTO a (id,tag,payload) VALUES ${data.map(r=>`(${r.id},${r.tag===null?'NULL':literal(r.tag)},'${'x'.repeat(20)}')`).join(',')}`));
  must(await db.query('CREATE INDEX tag_idx ON a (tag)'));
  for(const op of ['=','>','>=','<','<=']){
    db.clear();
    const response=must(await db.query(`SELECT id,tag FROM a WHERE tag${op}'same' ORDER BY id`));
    const expected=data.filter(r=>r.tag!==null&&(op==='='?r.tag==='same':op==='>'?r.tag>'same':op==='>='?r.tag>='same':op==='<'?r.tag<'same':r.tag<='same'));
    assert.deepEqual(rows(response),expected,op);
    assert.equal(response.plan.type,'index-range');
    const batches=db.calls.filter(c=>c.kind==='ROWS'&&c.body.op==='getMany');
    assert.equal(batches.length,response.plan.baseTabletsVisited);
    assert.equal(new Set(batches.map(c=>c.name)).size,batches.length,'Only one batch per base tablet');
    assert.deepEqual(batches.flatMap(c=>c.body.keys).sort((a,b)=>a-b),expected.map(r=>r.id));
    assert.equal(db.calls.filter(c=>c.kind==='ROWS'&&['read','get','scan'].includes(c.body.op)).length,0,'Index fetches use only bounded point batches');
    const indexScans=db.io.filter(x=>x.kind==='INDEXES'&&x.op==='list'&&x.keys.some(k=>k.startsWith('row:')));
    assert.ok(indexScans.length>0);
    assert.ok(indexScans.every(x=>x.values.every(v=>v.row===null||(Object.hasOwn(v.row,'value')&&Object.hasOwn(v.row,'pk')&&!Object.hasOwn(v.row,'pks')))),'Index must physically store individual [value,PK] rows, not value buckets');
    for(const scan of indexScans)for(const version of scan.values){const row=version.row;if(row===null)continue;assert.ok(row.value!==null&&(op==='='?row.value==='same':op==='>'?row.value>'same':op==='>='?row.value>='same':op==='<'?row.value<'same':row.value<='same'),'Only qualifying entries should be read physically');}
    if(op==='=') assert.ok(indexScans.length>1,'Duplicate value prefix must span multiple index tablets');
  }
  db.clear();
  assert.deepEqual(rows(must(await db.query('SELECT * FROM a WHERE tag=NULL'))),[]);
  assert.equal(db.calls.filter(c=>['ROWS','INDEXES'].includes(c.kind)&&['read','get','getMany','scan'].includes(c.body.op)).length,0);
  assert.equal((await db.query("SELECT * FROM a WHERE tag='nohit' AND missing=1")).status,400);
  assert.equal((await db.query('SELECT * FROM a WHERE tag=123')).status,400);
});

test('unique constraints are global after range splits while multiple indexed NULL values remain legal',async()=>{
  const db=cluster();
  must(await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, tag TEXT, payload TEXT); CREATE UNIQUE INDEX tag_idx ON a (tag)'));
  const created=must(await db.query(`INSERT INTO a (id,tag,payload) VALUES ${Array.from({length:30},(_,i)=>`(${i},'tag${String(i).padStart(2,'0')}','${'x'.repeat(40)}')`).join(',')}`));
  assert.ok(created.routing[0].tablets.length>1);
  assert.equal((await db.query("INSERT INTO a (id,tag,payload) VALUES (999,'tag00','duplicate')")).status,400);
  assert.equal((await db.query("UPDATE a SET tag='tag00' WHERE id=29")).status,400);
  must(await db.query("INSERT INTO a (id,tag,payload) VALUES (100,NULL,'ok'),(101,NULL,'ok')"));
  assert.equal(rows(must(await db.query('SELECT * FROM a'))).length,32);
  assert.deepEqual(rows(must(await db.query("SELECT id FROM a WHERE tag='tag00'"))),[{id:0}]);
});

// Pure invariants complement integration checks, including sparse/empty routes.
import {encodeKey,compareKeys,rangeForFilters,storageRange} from '../src/keys.js';
import {planTablets,routeTablet,selectTablets} from '../src/tablets.js';

test('typed codec preserves numeric and UTF8 order, tuple boundaries, zero identity and malformed-key rejection',()=>{
  const numbers=[-Number.MAX_VALUE,-9007199254740991,-100,-1,-Number.MIN_VALUE,0,Number.MIN_VALUE,0.5,1,9007199254740991,Number.MAX_VALUE];
  for(let i=1;i<numbers.length;i++)assert.ok(encodeKey(numbers[i-1])<encodeKey(numbers[i]));
  assert.equal(encodeKey(-0),encodeKey(0));
  const tuples=[[null,-1],[null,0],['',-1],['',0],['a',-1],['a',0],['a\u0000',0],['ab',0],['\uE000',0],['𐀀',0]];
  for(let i=1;i<tuples.length;i++)assert.equal(compareKeys(tuples[i-1],tuples[i]),-1);
  assert.notEqual(encodeKey(['a','bc']),encodeKey(['ab','c']));
  for(const bad of [NaN,Infinity,-Infinity,undefined,{},'\ud800','\udfff','界'.repeat(1000)])assert.throws(()=>encodeKey(bad));
  for(const value of [null,false,true,0,'', 'a\u0000b','😀'])assert.equal(compareKeys(value,value),0);
});

test('ordered splits cover all keys exactly once and arbitrary adjacent sparse ranges merge',()=>{
  const schema={primaryKey:'id',columns:[{name:'id',type:'INTEGER',primaryKey:true}]};
  const data=Array.from({length:33},(_,i)=>({id:i*3-50,payload:'x'.repeat(i%3===0?100:10)}));
  const options={splitBytes:260,mergeBytes:90,maxTablets:16};
  const planned=planTablets(schema,data,options), tablets=planned.schema.tablets;
  assert.equal(tablets[0].start,null);assert.equal(tablets.at(-1).end,null);
  for(let i=1;i<tablets.length;i++)assert.equal(tablets[i-1].end,tablets[i].start);
  for(const key of [-9007199254740991,...data.map(r=>r.id),0,9007199254740991])assert.equal(tablets.filter(t=>(t.start===null||key>=t.start)&&(t.end===null||key<t.end)).length,1);
  for(const part of planned.partitions)for(const row of part.rows)assert.equal(routeTablet(planned.schema,row.id),part.id);
  for(const t of tablets.slice(1))assert.equal(routeTablet(planned.schema,t.start),t.id,'Exact pivot goes right');
  assert.throws(()=>planTablets(schema,[{id:0},{id:-0}],options),/Duplicate/);
  const uneven={...schema,epoch:7,tablets:[{id:'a',start:null,end:-100},{id:'b',start:-100,end:7},{id:'c',start:7,end:null}]};
  const merged=planTablets(uneven,[],options);
  assert.equal(merged.schema.epoch,8);assert.equal(merged.schema.tablets.length,1);
  assert.equal(merged.schema.tablets[0].start,null);assert.equal(merged.schema.tablets[0].end,null);
  for(const bad of [
    [{id:'a',start:0,end:null}],
    [{id:'a',start:null,end:1},{id:'b',start:2,end:null}],
    [{id:'a',start:null,end:2},{id:'b',start:1,end:null}],
    [{id:'a',start:null,end:1},{id:'a',start:1,end:null}],
  ])assert.throws(()=>planTablets({...schema,epoch:1,tablets:bad},[],options));
});

test('composite prefix ranges include all duplicate suffix partitions and exact half-open boundary pruning',()=>{
  const schema={keyColumns:['value','pk'],epoch:1,tablets:[
    {id:'a',start:null,end:['same',2]},
    {id:'b',start:['same',2],end:['same',9]},
    {id:'c',start:['same',9],end:['z',0]},
    {id:'d',start:['z',0],end:null},
  ]};
  const range=rangeForFilters([{column:'tag',op:'=',value:'same'}],'tag',{compositePrefix:true});
  assert.deepEqual(selectTablets(schema,range).map(t=>t.id),['a','b','c']);
  const bounds=storageRange(range);
  for(const key of [['same',-100],['same',2],['same',9],['same',100]]){
    const physical='row:'+encodeKey(key);assert.ok(physical>=bounds.start&&physical<bounds.end);
  }
  for(const key of [['sam',0],['same\u0000',0],['z',0]]){
    const physical='row:'+encodeKey(key);assert.ok(!(physical>=bounds.start&&physical<bounds.end));
  }
  const scalar={tablets:[{id:'left',start:null,end:5},{id:'right',start:5,end:null}]};
  for(const [op,expected]of [['=',['right']],['>',['right']],['>=',['right']],['<',['left']],['<=',['left','right']]])assert.deepEqual(selectTablets(scalar,rangeForFilters([{column:'id',op,value:5}],'id')).map(t=>t.id),expected);
});

test('nonkey long TEXT supports SQL comparisons and sorting but oversized physical keys fail atomically',async()=>{
  const db=cluster();
  const a='a'.repeat(1000), z='z'.repeat(1000);
  must(await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, text TEXT)'));
  must(await db.query(`INSERT INTO a (id,text) VALUES (1,${literal(z)}),(2,${literal(a)})`));
  assert.deepEqual(rows(must(await db.query('SELECT id FROM a ORDER BY text'))),[{id:2},{id:1}]);
  assert.deepEqual(rows(must(await db.query(`SELECT id FROM a WHERE text>=${literal(z)}`))),[{id:1}]);
  assert.equal((await db.query('CREATE INDEX long_idx ON a (text)')).status,400);
  assert.deepEqual(rows(must(await db.query('SELECT id FROM a ORDER BY text'))),[{id:2},{id:1}]);
  assert.equal((await db.query(`INSERT INTO a (id,text) VALUES (3,${literal('\ud800')})`)).status,400);
});

test('split and merge decisions recover both base and index epochs before exposing routes',async()=>{
  const db=cluster();
  const initial=must(await db.query("CREATE TABLE a (id INTEGER PRIMARY KEY, tag TEXT); CREATE INDEX tag_idx ON a (tag); INSERT INTO a (id,tag) VALUES (-1,'seed')"));
  const oldIndex=initial.routing[0].indexes[0];
  const sql=`INSERT INTO a (id,tag) VALUES ${Array.from({length:35},(_,i)=>`(${i},'same')`).join(',')}`;
  assert.equal((await db.query(sql,{fault:'after-first-commit',idempotencyKey:'split-crash'})).status,503);
  db.restart();
  await db.recover();
  const after=must(await db.query("SELECT id FROM a WHERE tag='same' ORDER BY id"));
  assert.deepEqual(rows(after),Array.from({length:35},(_,id)=>({id})));
  assert.deepEqual(rows(must(await db.query("SELECT id FROM a ORDER BY id",{snapshot:initial.version}))),[{id:-1}]);
  const splitIndex=after.routing[0].indexes[0];
  assert.ok(splitIndex.epoch>oldIndex.epoch);assert.ok(splitIndex.tablets.length>1);
  async function invoke(name,body){const response=await db.env.SHARDS.get(name).fetch('https://internal',{method:'POST',body:JSON.stringify(body)});return {status:response.status,...await response.json()};}
  const oldName=`index/a/tag_idx/${oldIndex.tablets[0].id}`;
  assert.equal((await invoke(oldName,{op:'scan',snapshot:after.version,epoch:oldIndex.epoch})).code,'STALE_EPOCH');
  assert.equal((await invoke(oldName,{op:'prepare',id:'delayed-index-prepare',snapshot:initial.version,expectedEpoch:oldIndex.epoch,reads:[],changes:[{key:['bad',999],row:{value:'bad',pk:999}}]})).code,'CONFLICT');
  must(await db.query(sql,{idempotencyKey:'split-crash'}));
  assert.equal((await db.query('DELETE FROM a',{fault:'after-first-commit',idempotencyKey:'merge-crash'})).status,503);
  db.restart();
  await db.recover();
  const merged=must(await db.query("SELECT * FROM a WHERE tag>=''"));
  assert.deepEqual(rows(merged),[]);
  assert.equal(merged.routing[0].tablets.length,1);assert.equal(merged.routing[0].indexes[0].tablets.length,1);
  assert.ok(merged.routing[0].indexes[0].epoch>splitIndex.epoch);
  assert.deepEqual(rows(must(await db.query("SELECT id FROM a WHERE tag='same' ORDER BY id",{snapshot:after.version}))),Array.from({length:35},(_,id)=>({id})));
  const retired=splitIndex.tablets.find(t=>t.id!==merged.routing[0].indexes[0].tablets[0].id);
  assert.equal((await invoke(`index/a/tag_idx/${retired.id}`,{op:'scan',snapshot:merged.version,epoch:splitIndex.epoch})).code,'STALE_EPOCH');
});

test('index on the primary key deduplicates the physical key suffix',async()=>{
  const db=cluster();
  const created=must(await db.query('CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO a (id,n) VALUES (2,20),(10,100); CREATE INDEX id_idx ON a (id)'));
  const index=created.routing[0].indexes[0];
  assert.deepEqual(index.keyColumns,['value']);
  db.clear();
  const entries=[];
  for(const tablet of index.tablets){
    const response=await db.env.SHARDS.get(`index/a/id_idx/${tablet.id}`).fetch('https://internal',{method:'POST',body:JSON.stringify({op:'scan',snapshot:created.version,epoch:index.epoch})});
    assert.equal(response.status,200);
    entries.push(...(await response.json()).rows);
  }
  assert.deepEqual(entries,[{value:2,pk:2},{value:10,pk:10}]);
  const keys=db.io.filter(x=>x.kind==='INDEXES'&&x.op==='list').flatMap(x=>x.keys).filter(k=>k.startsWith('row:'));
  assert.deepEqual(keys,[2,10].map(k=>'row:'+encodeKey([k])+':'+String(created.version).padStart(16,'0')));
  assert.ok(keys.every((key,i)=>!key.startsWith('row:'+encodeKey([[2,10][i],[2,10][i]])+':')));
});


test('bounded MVCC scans separate visible rows from retained physical versions',async()=>{
  const db=cluster();
  const initial=must(await db.query('CREATE TABLE history (id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO history (id,n) VALUES (1,10),(2,20),(3,30)'));
  must(await db.query('UPDATE history SET n=21 WHERE id=2'));
  must(await db.query('UPDATE history SET n=22 WHERE id=2'));
  db.clear();
  const latest=must(await db.query('SELECT * FROM history WHERE id=2'));
  assert.deepEqual(rows(latest),[{id:2,n:22}]);
  assert.equal(latest.plan.rowsRead,1);
  assert.equal(latest.plan.versionsRead,3);
  const physical=db.io.filter(x=>x.kind==='ROWS'&&x.op==='list'&&x.keys.some(k=>k.startsWith('row:')));
  assert.equal(physical.flatMap(x=>x.values).length,3);
  assert.ok(physical.every(x=>x.values.every(v=>v.key===2&&v.row.id===2)),'History reads remain physically key-bounded');
  const old=must(await db.query('SELECT * FROM history WHERE id=2',{snapshot:initial.version}));
  assert.deepEqual(rows(old),[{id:2,n:20}]);
  assert.equal(old.plan.rowsRead,1);
  assert.equal(old.plan.versionsRead,3);
});
