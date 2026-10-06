import { readFile, writeFile } from 'node:fs/promises';
import { tinybirdRequest } from './tinybird.mjs';

const names = ['v1_identity_records', 'v1_identity_commits', 'v1_session_records', 'v1_session_commits'];
const inventory = await (await tinybirdRequest('/v0/datasources')).json();
const existing = new Set(inventory.datasources.map(source => source.name));
const results = [];
for (const name of names) {
  if (existing.has(name)) {
    results.push({ name, existing: true });
    continue;
  }
  const source = await readFile(new URL(`../datasources/${name}.datasource`, import.meta.url), 'utf8');
  const schema = source.match(/SCHEMA >\n([\s\S]*?)\nENGINE /)?.[1].trim();
  const engine = source.match(/^ENGINE (\w+)$/m)?.[1];
  const sorting = source.match(/^ENGINE_SORTING_KEY (.+)$/m)?.[1];
  if (!schema || !engine || !sorting) throw new Error(`Incomplete schema for ${name}`);
  const response = await tinybirdRequest('/v0/datasources', {
    method: 'POST',
    body: new URLSearchParams({ name, mode: 'create', format: 'ndjson', schema, engine, engine_sorting_key: sorting }),
  });
  const result = await response.json();
  results.push({ name, created: true, id: result.id });
  console.log(JSON.stringify({ name, created: true }));
}
await writeFile(new URL('../evidence/storage-created.json', import.meta.url), JSON.stringify(results, null, 2) + '\n');
