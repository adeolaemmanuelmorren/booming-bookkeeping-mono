import { readFile, writeFile } from 'node:fs/promises';
import { tinybirdRequest } from './tinybird.mjs';

const { environments } = await (await tinybirdRequest('/v1/environments')).json();
const branch = environments.find(row => row.name === 'v1_facts_validation');
if (branch?.id !== 'bfaead01-69a3-4215-a53b-02477bda5323' || branch?.main !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af') {
  throw new Error('Unexpected bootstrap validation branch');
}
async function request(path, options = {}) {
  const response = await fetch(new URL(path, 'https://api.us-east.tinybird.co'), {
    ...options, headers: { Authorization: `Bearer ${branch.token}` },
    signal: AbortSignal.timeout(60_000), redirect: 'error',
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Validation schema request failed with HTTP ${response.status}`);
  }
  return response.json();
}
const name = 'v1_bootstrap_membership_keys';
const inventory = await request('/v0/datasources');
let created = false;
if (!inventory.datasources.some(row => row.name === name)) {
  const source = await readFile(new URL(`../datasources/${name}.datasource`, import.meta.url), 'utf8');
  const schema = source.match(/SCHEMA >\n([\s\S]*?)\nENGINE /)?.[1].trim();
  const engine = source.match(/^ENGINE (\w+)$/m)?.[1];
  const sorting = source.match(/^ENGINE_SORTING_KEY (.+)$/m)?.[1];
  if (!schema || !engine || !sorting) throw new Error('Incomplete membership schema');
  await request('/v0/datasources', { method: 'POST', body: new URLSearchParams({ name, mode: 'create', format: 'ndjson', schema, engine, engine_sorting_key: sorting }) });
  created = true;
}
const result = { checked_at: new Date().toISOString(), branch_id: branch.id, name, created };
await writeFile(new URL('../evidence/tests/bootstrap-v2-schema.json', import.meta.url), JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify(result));
