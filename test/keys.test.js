import test from 'node:test';
import assert from 'node:assert/strict';
import { encodeKey, compareKeys, rangeForFilters, storageRange } from '../src/keys.js';
const matches=(key,range)=>{const x=encodeKey(key);return !range.empty&&(range.lower===null||x>range.lower||(x===range.lower&&range.lowerInclusive))&&(range.upper===null||x<range.upper||(x===range.upper&&range.upperInclusive));};
test('number encoding preserves numeric order including negatives and fractions',()=>{
 const values=[-Number.MAX_VALUE,-100,-10,-2,-0.1,-Number.MIN_VALUE,0,Number.MIN_VALUE,0.1,2,10,100,Number.MAX_VALUE];
 assert.deepEqual([...values].reverse().sort(compareKeys),values);
 assert.deepEqual(values.map(encodeKey).sort(),values.map(encodeKey));
 assert.equal(encodeKey(-0),encodeKey(0));
 for(const x of [Infinity,-Infinity,NaN,{},undefined])assert.throws(()=>encodeKey(x));
});
test('UTF8 order preserves prefixes and embedded NUL, unlike UTF16 astral order',()=>{
 const values=['','\u0000','\u0000a','a','a\u0000','aa','é','界','\ue000','😀'];
 assert.deepEqual([...values].reverse().sort(compareKeys),values);
 assert.deepEqual(values.map(encodeKey).sort(),values.map(encodeKey));
 for(const x of ['\ud800','\udc00','a\ud800b'])assert.throws(()=>encodeKey(x),/surrogate/);
 assert.throws(()=>encodeKey('a'.repeat(600)),/1800/);
 assert.ok(compareKeys('a'.repeat(8192),'b'.repeat(8192))<0);
});
test('composites preserve NULL, value then PK order and are true prefixes',()=>{
 const keys=[[null,1],[null,2],[-2,3],[1,-3],[1,10],[2,1]];
 assert.deepEqual([...keys].reverse().sort(compareKeys),keys);
 assert.ok(encodeKey([1,10]).startsWith(encodeKey([1])));
 assert.ok(compareKeys(null,false)<0);assert.ok(compareKeys(false,true)<0);
});
test('scalar AND bounds, contradictions and exact inclusivity',()=>{
 const r=rangeForFilters([{column:'id',op:'>',value:2},{column:'id',op:'<=',value:10}],'id');
 assert.deepEqual([1,2,3,10,11].filter(x=>matches(x,r)),[3,10]);
 assert.equal(rangeForFilters([{column:'id',op:'>',value:2},{column:'id',op:'<=',value:2}],'id').empty,true);
 assert.equal(rangeForFilters([{column:'id',op:'!=',value:null}],'id').empty,true);
 const eq=rangeForFilters([{column:'id',op:'=',value:2}],'id');
 assert.deepEqual(storageRange(eq),{prefix:'row:',start:'row:'+encodeKey(2),end:'row:'+encodeKey(2)+'\u0000'});
 const gt=storageRange(rangeForFilters([{column:'id',op:'>',value:2}],'id'));
 assert.equal(gt.start,'row:'+encodeKey(2)+'\u0000');
});
test('index equality and inequalities span all primary-key suffixes',()=>{
 const keys=[[null,-1],[null,1],[1,-10],[1,10],[2,-100],[2,0],[2,100],[3,0]];
 for(const [op,expected] of [['=',[2,2,2]],['>',[3]],['>=',[2,2,2,3]],['<',[1,1]],['<=',[1,1,2,2,2]]]) {
   const r=rangeForFilters([{column:'value',op,value:2}],'value',{compositePrefix:true});
   assert.deepEqual(keys.filter(k=>matches(k,r)).map(k=>k[0]),expected,op);
 }
 const single=rangeForFilters([{column:'value',op:'=',value:2}],'value',{compositePrefix:true});
 assert.ok(matches([2],single));assert.ok(!matches([3],single));
});
