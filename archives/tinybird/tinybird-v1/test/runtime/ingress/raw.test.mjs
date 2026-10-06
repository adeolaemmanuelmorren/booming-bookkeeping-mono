import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { Miniflare, convertV4MiniflareOptions } from 'miniflare';

const directory = dirname(fileURLToPath(import.meta.url));
const bundle = await build({ entryPoints: [join(directory, 'raw-worker.ts')], bundle: true, write: false,
  format: 'esm', platform: 'browser', target: 'es2022', external: ['cloudflare:workers'] });
const hash = value => createHash('sha256').update(value).digest('hex');
function archived(id) {
  const envelope = { schema_version: 'jitsu_events_api_v1', producer_id: 'jitsu', events: [{
    tenant_id: 'boom', producer_id: 'jitsu', message_id: id, delivery_event_id: `delivery:${id}`,
    event_kind: 'page_view', observed_at: '2026-09-05T01:00:00.123456Z', ingested_at: '2026-09-05T01:00:01.654321Z',
    source_fact_version: 1, source_deleted: 0, fact_payload: '{"anonymousId":"visitor"}', unknown: { keep: id },
  }] };
  const body = JSON.stringify(envelope);
  return { envelope, body, key: `jitsu/envelopes/${hash(body)}.json` };
}

async function runtime(t, bindings = {}) {
  const persistence = await mkdtemp(join(tmpdir(), 'raw-replay-test-'));
  const remote = { rows: [], writes: 0, loseResponse: false, hideReads: 0 };
  const options = {
    ...convertV4MiniflareOptions({
      name: 'raw-test', modules: true, script: bundle.outputFiles[0].text, compatibilityDate: '2026-09-05',
      r2Buckets: { BROWSER_BUFFER: 'raw-test' },
      durableObjects: { REPLAY: { className: 'TestRawReplay', useSQLite: true },
        BROWSER_IDENTITY_REPLAY: { className: 'TestIdentityStage', useSQLite: true } },
      bindings: { TENANT_ID: 'boom', BROWSER_INGRESS_MODE: 'collect', TINYBIRD_URL: 'https://api.us-east.tinybird.co',
        TINYBIRD_TOKEN: 'test', INGESTION_ENABLED: 'false', ...bindings },
      serviceBindings: { BROWSER_INGRESS: { name: 'raw-test', entrypoint: 'BrowserIngress' } },
      outboundService: async request => {
        const url = new URL(request.url);
        assert.equal(url.origin, 'https://api.us-east.tinybird.co');
        if (url.pathname === '/v0/events') {
          const rows = (await request.text()).trim().split('\n').map(JSON.parse);
          remote.rows.push(...rows); remote.writes++;
          if (remote.loseResponse) { remote.loseResponse = false; return new Response('lost', { status: 503 }); }
          return Response.json({ successful_rows: rows.length, quarantined_rows: 0 });
        }
        const sql = (await request.formData()).get('q');
        if (remote.hideReads > 0) { remote.hideReads--; return Response.json({ data: [] }); }
        const selected = remote.rows.filter(row => sql.includes(`'${row.envelope_hash}'`));
        const groups = new Map();
        for (const row of selected) {
          const key = `${row.envelope_hash}:${row.event_index}`;
          groups.set(key, [...groups.get(key) ?? [], row]);
        }
        return Response.json({ data: [...groups.values()].map(rows => ({
          envelope_hash: rows[0].envelope_hash, event_index: rows[0].event_index, stored_hash: rows[0].observation_hash,
          variants: new Set(rows.map(row => JSON.stringify([row.observation_hash, row.observation_json]))).size,
          invalid_hashes: rows.filter(row => row.observation_hash !== hash(row.observation_json)).length,
        })) });
      },
    }),
    resourcePersistencePath: persistence, isolatedResourcePersistencePath: persistence,
  };
  let mf = new Miniflare(options);
  t.after(async () => { await mf.dispose(); await rm(persistence, { recursive: true, force: true }); });
  return {
    remote, bucket: () => mf.getR2Bucket('BROWSER_BUFFER'),
    async put(item) { await (await mf.getR2Bucket('BROWSER_BUFFER')).put(item.key, item.body); },
    async restart() { await mf.dispose(); mf = new Miniflare(options); },
    async call(path, body = {}) {
      const response = await mf.dispatchFetch(`https://test.invalid${path}`, { method: 'POST', body: JSON.stringify(body) });
      const value = await response.json();
      if (!response.ok) throw new Error(value.error);
      return value;
    },
  };
}

test('collect stores exact originals with model ingestion disabled and no identity baseline', async t => {
  const r = await runtime(t); const item = archived('live');
  const receipts = await r.call('/receive', [item.envelope]);
  assert.equal(receipts[0].status, 'stored');
  assert.equal(receipts[0].key, item.key);
  assert.deepEqual(await r.call('/staged'), [item.key]);
  assert.deepEqual(JSON.parse(r.remote.rows[0].observation_json), item.envelope.events[0]);
  const originalTime = r.remote.rows[0].received_at;
  await r.restart(); await r.call('/receive', [item.envelope]);
  assert.equal(r.remote.writes, 1);
  assert.equal(r.remote.rows[0].received_at, originalTime);
  assert.equal(await (await (await r.bucket()).get(item.key)).text(), item.body);
});

test('unavailable identity staging cannot block verified raw delivery', async t => {
  const r = await runtime(t, { FAIL_IDENTITY_STAGE: 'true' });
  const item = archived('identity-unavailable');
  const [receipt] = await r.call('/receive', [item.envelope]);
  assert.equal(receipt.status, 'stored');
  assert.equal(r.remote.rows.length, 1);
  assert.ok(await (await r.bucket()).head(item.key));
});

test('raw replay verifies a lost append response and keeps that checkpoint across restart', async t => {
  const r = await runtime(t); const item = archived('replay');
  await r.put(item); await r.call('/start'); r.remote.loseResponse = true;
  const verified = await r.call('/step');
  assert.equal(verified.pending, 0); assert.equal(verified.lastError, null);
  assert.equal(r.remote.rows.length, 1);
  await r.restart();
  const done = await r.call('/step');
  assert.equal(done.pending, 0); assert.equal(done.verified_events, 1);
  assert.equal(r.remote.writes, 1);
  assert.ok(await (await r.bucket()).head(item.key));
});

test('lost response waits for query visibility before acknowledging or appending again', async t => {
  const r = await runtime(t); const item = archived('delayed-readback');
  r.remote.loseResponse = true;
  r.remote.hideReads = 3;
  const receipt = await r.call('/receive', [item.envelope]);
  assert.equal(receipt[0].status, 'stored');
  assert.equal(r.remote.hideReads, 0);
  assert.equal(r.remote.writes, 1);
  assert.equal(r.remote.rows.length, 1);
});

test('complete rescans discover a later key behind the previous listing cursor', async t => {
  const r = await runtime(t);
  const items = Array.from({ length: 102 }, (_, i) => archived(`item-${i}`)).sort((a,b) => a.key.localeCompare(b.key));
  for (const item of items.slice(1)) await r.put(item);
  await r.call('/start'); await r.call('/step');
  await r.put(items[0]);
  for (let i = 0; i < 4; i++) await r.call('/step');
  const result = await r.call('/status');
  assert.equal(result.envelopes, 102); assert.equal(result.pending, 0); assert.equal(result.verified_events, 102);
  assert.equal(r.remote.rows.length, 102);
  assert.equal((await (await r.bucket()).list()).objects.length, 102);
});

test('corrupt originals cannot be marked delivered and remain available for repair', async t => {
  const r = await runtime(t); const item = archived('corrupt');
  await r.put({ ...item, body: '{}' }); await r.call('/start');
  const result = await r.call('/step');
  assert.equal(result.pending, 1); assert.equal(result.verified_events, 0); assert.ok(result.lastError);
  assert.equal(r.remote.writes, 0);
  assert.ok(await (await r.bucket()).head(item.key));
});

test('one failed replay group retains its keys while independent groups finish', async t => {
  const r = await runtime(t);
  const items = Array.from({ length: 32 }, (_, index) => archived(`group-${index}`)).sort((a,b) => a.key.localeCompare(b.key));
  for (const item of items) await r.put(item);
  await r.put({ ...items[0], body: '{}' });
  await r.call('/start');
  const partial = await r.call('/step');
  assert.equal(partial.verified_events, 24);
  assert.equal(partial.pending, 8);
  await r.put(items[0]);
  const complete = await r.call('/step');
  assert.equal(complete.verified_events, 32);
  assert.equal(complete.pending, 0);
  assert.equal((await (await r.bucket()).list()).objects.length, 32);
});

test('raw replay refuses buffer mode and pause survives restart', async t => {
  const disabled = await runtime(t, { BROWSER_INGRESS_MODE: 'buffer' });
  await assert.rejects(disabled.call('/start'), /collect mode/);
  const r = await runtime(t); await r.put(archived('paused'));
  await r.call('/start'); await r.call('/pause'); await r.restart();
  const result = await r.call('/step');
  assert.equal(result.enabled, false); assert.equal(result.alarmAt, null); assert.equal(r.remote.writes, 0);
});
