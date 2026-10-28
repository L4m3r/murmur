/** Versioned physical routing only. Logical transaction owners remain unchanged. */
export const COORDINATOR_POOL_SIZE = 16;
export function coordinatorName(owner) {
 if(typeof owner!=='string'||owner.length<1||owner.length>128||new TextDecoder().decode(new TextEncoder().encode(owner))!==owner)throw new Error('Invalid logical coordinator owner');
 let hash=2166136261;
 for(const byte of new TextEncoder().encode(owner))hash=Math.imul(hash^byte,16777619)>>>0;
 return `coordinator-pool/v1/${hash%COORDINATOR_POOL_SIZE}`;
}
