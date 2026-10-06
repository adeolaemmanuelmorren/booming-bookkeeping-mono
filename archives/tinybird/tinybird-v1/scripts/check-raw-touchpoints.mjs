import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { tinybirdConfig, query } from './tinybird.mjs';
import { cloudflareRequest } from './cloudflare.mjs';
import { Tinybird } from '../worker/storage/tinybird.ts';
import { rawObservations, publishRawObservations } from '../worker/browser/raw-storage.ts';
import { sha256 } from '../worker/storage/json.ts';

const mode = process.argv[2] ?? 'status';
if (!['table-probe', 'receive-probe', 'start', 'pause-complete', 'status'].includes(mode)) throw new Error('Unknown check');
const host = 'https://boom-tinybird-facts-v1.bill-3e3.workers.dev';
const evidence = new URL('../evidence/cutover/', import.meta.url);
const { ADMIN_TOKEN } = JSON.parse(await readFile(new URL('../.dev.vars.admin.json', import.meta.url), 'utf8'));
async function admin(path, body = {}) {
  const response = await fetch(host + path, { method: 'POST',
    headers: { Authorization: `Bearer ${ADMIN_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.timeout(60_000), redirect: 'manual' });
  if (!response.ok) { await response.body?.cancel(); throw new Error(`Raw operation failed with HTTP ${response.status}`); }
  return response.json();
}
function probe() {
  const id = `verification:raw:${randomUUID()}`;
  const now = new Date().toISOString();
  return { schema_version: 'jitsu_events_api_v1', producer_id: id, events: [{
    tenant_id: 'boom', producer_id: id, message_id: id, delivery_event_id: id, event_kind: 'v1_raw_probe',
    observed_at: now, ingested_at: now, source_fact_version: 1, source_deleted: 0,
    fact_payload: JSON.stringify({ messageId: id, type: 'v1_raw_probe' }), unknown_probe_field: { retain: true },
  }] };
}
let result;
if (mode === 'table-probe') {
  const config = await tinybirdConfig();
  const client = new Tinybird({ TINYBIRD_URL: config.host, TINYBIRD_TOKEN: config.token });
  const envelope = probe();
  const rows = await rawObservations([{ envelope, hash: await sha256(JSON.stringify(envelope)), receivedAt: new Date().toISOString() }]);
  let writes = 0;
  const lostResponse = {
    query: sql => client.query(sql),
    async append(table, values) { writes++; await client.append(table, values); throw new Error('Simulated response loss'); },
  };
  await publishRawObservations(lostResponse, rows);
  await publishRawObservations(lostResponse, rows);
  assert.equal(writes, 1);
  result = { verified: true, simulated_lost_response_recovered: true, append_calls: writes, envelope_hash: rows[0].envelope_hash };
} else if (mode === 'receive-probe') {
  const envelope = probe();
  const first = await admin('/admin/browser/receive', envelope);
  const second = await admin('/admin/browser/receive', envelope);
  assert.equal(first.status, 'stored'); assert.deepEqual(first, second);
  const rows = await query(`SELECT count() AS physical_rows, uniqExact(tuple(envelope_hash,event_index)) AS logical_rows,
    countIf(observation_hash != lower(hex(SHA256(observation_json)))) AS invalid_hashes
    FROM v1_jitsu_observations WHERE tenant_id='boom' AND envelope_hash='${first.sha256}'`);
  assert.equal(Number(rows.data[0].logical_rows), 1); assert.equal(Number(rows.data[0].invalid_hashes), 0);
  result = { verified: true, receipt: first, stored: rows.data[0] };
} else if (mode === 'start') {
  result = await admin('/admin/browser/raw-replay/start');
} else if (mode === 'pause-complete') {
  const status = await admin('/admin/browser/raw-replay/status');
  if (status.scans < 2 || status.pending !== 0 || status.lastError) throw new Error('Raw replay has not finished two scans without pending work');
  result = { verified: true, completed: status, paused: await admin('/admin/browser/raw-replay/pause') };
} else {
  const [health, replay, identity, buffer, queue, data] = await Promise.all([
    fetch(host + '/health').then(response => response.json()), admin('/admin/browser/raw-replay/status'),
    admin('/admin/identity/status'), admin('/admin/browser/buffer-status'),
    cloudflareRequest('/queues/f52227f713ed4530afebe91d2994820e'),
    query(`SELECT count() AS physical_rows, uniqExact(tuple(envelope_hash,event_index)) AS unique_observations,
      uniqExact(delivery_event_id) AS distinct_batch_events,
      uniqExact(envelope_hash) AS envelopes, countIf(observation_hash != lower(hex(SHA256(observation_json)))) AS invalid_hashes,
      maxIf(parseDateTime64BestEffortOrNull(JSONExtractString(observation_json,'ingested_at'),6), event_kind != 'v1_raw_probe') AS latest_real_ingested_at
      FROM v1_jitsu_observations WHERE tenant_id='boom'`),
  ]);
  result = { health, replay, identity, buffer, queue: { queue_name: queue.queue_name, consumers: queue.consumers }, raw: data.data[0] };
}
const proof = { checked_at: new Date().toISOString(), ...result };
await writeFile(new URL(`raw-touchpoints-${mode}.json`, evidence), JSON.stringify(proof, null, 2));
console.log(JSON.stringify(proof));
