import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import vm from 'node:vm';
import { dashboardResponse } from '../src/observability-ui.js';

test('dashboard serves a private-cache-disabled shell with exact CSP hashes and no external resources',async()=>{
 const response=await dashboardResponse(),html=await response.text();
 assert.equal(response.status,200);assert.equal(response.headers.get('cache-control'),'no-store');
 assert.equal(response.headers.get('x-content-type-options'),'nosniff');assert.equal(response.headers.get('referrer-policy'),'no-referrer');
 const script=html.match(/<script>([\s\S]*?)<\/script>/)[1],style=html.match(/<style>([\s\S]*?)<\/style>/)[1];
 const csp=response.headers.get('content-security-policy');
 for(const content of [script,style])assert.ok(csp.includes("'sha256-"+createHash('sha256').update(content).digest('base64')+"'"));
 assert.ok(csp.includes("connect-src 'self'"));assert.ok(csp.includes("frame-ancestors 'none'"));assert.ok(csp.includes("default-src 'none'"));assert.ok(!csp.includes('unsafe-inline'));
 assert.doesNotMatch(html,/<(?:script|link|img)[^>]+(?:src|href)=["']https?:/);
 new vm.Script(script);
});

test('dashboard keeps tokens in memory and exposes only bounded read-only diagnostics',async()=>{
 const html=await (await dashboardResponse()).text();
 assert.doesNotMatch(html,/localStorage|sessionStorage|document\.cookie|innerHTML|outerHTML|insertAdjacentHTML|eval\(/);
 assert.match(html,/type="password"/);assert.match(html,/credentials:'omit'/);assert.match(html,/authorization:'Bearer '\+token/);
 assert.match(html,/fetch\('\/observe\/api'/);assert.doesNotMatch(html,/fetch\('\/(?:query|recover|maintenance|schema)'/);
 assert.match(html,/limit:8/);assert.match(html,/},15000\)/);assert.match(html,/setTimeout\(\(\)=>controller.abort\(\),10000\)/);
 assert.match(html,/session\+\+;for\(const request of activeRequests\)request.abort\(\)/);
 assert.match(html,/stale diagnostic response discarded/);
 assert.match(html,/No physical placement claims/);assert.match(html,/not physical SQLite\/LTX storage or billing/);
 assert.match(html,/not a global service total/);assert.match(html,/Unknown or collected history is not proof of abort/);
});

test('dashboard includes accessible empty, loading, error and authentication affordances',async()=>{
 const html=await (await dashboardResponse()).text();
 assert.match(html,/role="status"/);assert.match(html,/role="alert"/);assert.match(html,/label[^>]*for="token"/);
 for(const id of ['auth-form','refresh','table-form','tx-form','table-prev','table-next','disconnect'])assert.ok(html.includes('id="'+id+'"'));
 assert.match(html,/Refreshing…/);assert.match(html,/Inspecting…/);assert.match(html,/No request samples loaded/);
 assert.match(html,/@media\(max-width:520px\)/);assert.match(html,/@media\(max-width:1000px\)/);
});
