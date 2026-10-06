import { readFile, writeFile } from 'node:fs/promises';
import { cloudflareRequest } from './cloudflare.mjs';
import { query, tinybirdRequest } from './tinybird.mjs';

const workerNames = ['bigquery-tinybird-sync', 'jitsu-tinybird-ingest', 'boom-tinybird-facts-v1'];
const workers = {};
for (const name of workerNames) {
  const result = await cloudflareRequest(`/workers/scripts/${name}/deployments`);
  workers[name] = result.deployments?.[0]?.versions ?? [];
}
const { ADMIN_TOKEN } = JSON.parse(await readFile(new URL('../.dev.vars.admin.json', import.meta.url), 'utf8'));
const response = await fetch('https://boom-tinybird-facts-v1.bill-3e3.workers.dev/admin/browser/buffer-status', {
  method: 'POST', headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
  signal: AbortSignal.timeout(30_000), redirect: 'error',
});
if (!response.ok) throw new Error(`Buffer status failed with HTTP ${response.status}`);
const buffer = await response.json();
if (buffer.mode !== 'buffer') throw new Error('Browser ingress is no longer buffering');
const sources = await (await tinybirdRequest('/v0/datasources')).json();
const pipes = await (await tinybirdRequest('/v0/pipes')).json();
const oldJitsu = sources.datasources.some(row => row.name === 'jitsu_events_api_observations')
  ? (await query('SELECT count() AS rows,max(ingested_at) AS latest FROM jitsu_events_api_observations')).data[0]
  : null;
const result = { checked_at: new Date().toISOString(), workers, buffer, old_jitsu: oldJitsu,
  datasources: sources.datasources.length, pipes: pipes.pipes.length };
await writeFile(new URL('../evidence/cutover/current-state.json', import.meta.url), JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify(result));
