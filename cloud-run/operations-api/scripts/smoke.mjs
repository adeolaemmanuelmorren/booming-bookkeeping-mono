import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
const base = process.argv[2] ?? 'https://boom-operations-api-943586541190.us-central1.run.app';
const key = process.env.OPERATIONS_API_KEY ?? readFileSync(new URL('../.env.credentials', import.meta.url), 'utf8').trim().split('=')[1];
const results = [];
async function get(path, { authorized = true, method = 'GET', body } = {}) {
  const response = await fetch(base + path, { method,
    headers: { ...(authorized ? { 'X-Api-Key': key } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(60_000) });
  const text = await response.text();
  assert.ok(!text.includes(key), 'The service returned its API key.');
  const data = JSON.parse(text);
  results.push({ path: path.split('?')[0], status: response.status, keys: Object.keys(data) });
  console.log(JSON.stringify(results.at(-1)));
  return { status: response.status, data };
}
assert.equal((await get('/v1/status', {authorized:false})).status,401);
if (!process.env.SKIP_HEALTH) assert.equal((await get('/health', {authorized:false})).status,200);
assert.equal((await get('/v1')).status,200);
const combined = await get('/v1/status');
assert.ok([200,503].includes(combined.status));
assert.notEqual(combined.data.checks.bigquery.status,'unavailable');
assert.notEqual(combined.data.checks.meta.status,'unavailable');
assert.notEqual(combined.data.checks.jitsu.status,'unavailable');
const runs = await get('/v1/dataform/invocations?limit=3');
assert.equal(runs.status,200);
if (runs.data.nextPageToken) {
  assert.notEqual(runs.data.nextPageToken,'[redacted]');
  assert.equal((await get(`/v1/dataform/invocations?limit=3&pageToken=${encodeURIComponent(runs.data.nextPageToken)}`)).status,200);
}
const id = runs.data.workflowInvocations[0].name.split('/').at(-1);
const invocation = await get(`/v1/dataform/invocations/${id}`);
assert.equal(invocation.status,200);
const compilationId = invocation.data.resolvedCompilationResult.split('/').at(-1);
assert.equal((await get(`/v1/dataform/compilations/${compilationId}`)).status,200);
const actions = await get(`/v1/dataform/invocations/${id}/actions?limit=200`);
assert.equal(actions.status,200);
assert.equal((await get('/v1/dataform/workflows')).status,200);
assert.equal((await get('/v1/bigquery/datasets')).status,200);
assert.equal((await get('/v1/bigquery/datasets/booming_data_analytics/tables?limit=5')).status,200);
assert.equal((await get('/v1/bigquery/datasets/booming_data_analytics/tables/stg_facebook_ads')).status,200);
assert.equal((await get('/v1/bigquery/datasets/booming_data_analytics/tables/stg_facebook_ads/rows?limit=1')).status,200);
const query = await get('/v1/bigquery/query',{method:'POST',body:{sql:'SELECT value FROM UNNEST([1,2,3]) value ORDER BY value',limit:1}});
assert.equal(query.status,200);
assert.ok(query.data.nextPageToken);
assert.notEqual(query.data.nextPageToken,'[redacted]');
const next = await get(`/v1/bigquery/query/${query.data.jobId}?limit=1&pageToken=${encodeURIComponent(query.data.nextPageToken)}`);
assert.equal(next.status,200);
assert.equal(next.data.rows[0].value,2);
const write = await get('/v1/bigquery/query',{method:'POST',body:{sql:'CREATE TABLE `able-folio-499722.booming_data_analytics.operations_api_write_probe` AS SELECT 1'}});
assert.ok([400,403].includes(write.status));
assert.equal((await get('/v1/bigquery/datasets/airbyte_internal/tables')).status,403);
for (const path of ['/v1/jitsu/workspace','/v1/jitsu/connections','/v1/jitsu/metrics',
  '/v1/jitsu/invocations?limit=3','/v1/jitsu/deliveries?limit=3','/v1/jitsu/deliveries?mode=stream&limit=3',
  '/v1/meta/account','/v1/meta/insights?limit=3','/v1/meta/reconciliation']) {
  assert.equal((await get(path)).status,200,path);
}
const summary = { checkedAt: new Date().toISOString(), base, results,
  providerStatus: Object.fromEntries(Object.entries(combined.data.checks).map(([name,check])=>[name,check.status])),
  dataformFailures: (actions.data.workflowInvocationActions??[]).filter(action=>action.state==='FAILED')
    .map(action=>({target:action.target,failureReason:action.failureReason})) };
writeFileSync(new URL('../verification.json',import.meta.url),JSON.stringify(summary,null,2)+'\n');
console.log(JSON.stringify({verified: true, providerStatus:summary.providerStatus, failureCount:summary.dataformFailures.length}));
