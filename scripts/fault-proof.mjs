import assert from 'node:assert/strict';
// Test-only response proof captured from durable state at the injection point.
// An intermediate pending response is never evidence that the point was reached.
export function assertFaultBoundary(fault,result){
 assert.notEqual(result.code,'PLAN_PENDING','Pending protocol progress is not an injected crash boundary');
 if(['after-first-commit','after-prepare','after-commit-ready'].includes(fault)){
  assert.equal(result.code,'FAULT_INJECTED',JSON.stringify(result));
  assert.equal(result.faultReached,true);
  const state=result.faultState;assert.ok(state,'Durable fault-state proof required');
  if(fault==='after-first-commit'){
   assert.equal(state.phase,'planning');assert.equal(state.decision,'COMMIT');
   assert.ok(Number.isSafeInteger(state.step)&&state.step>0);
   assert.ok(state.appliedCount>0&&state.appliedCount<state.participantCount,JSON.stringify(state));
  }else{
   assert.equal(state.phase,fault==='after-prepare'?'preparing':'planning');
   assert.equal(state.allocationState,'absent');
  }
  console.log(`FAULT_BOUNDARY ${JSON.stringify({fault,...state})}`);
 }
}
