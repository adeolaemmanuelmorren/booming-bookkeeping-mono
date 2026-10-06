import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHash } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

import { Miniflare, convertV4MiniflareOptions } from 'miniflare';
import { build } from 'esbuild';
const directory = dirname(fileURLToPath(import.meta.url));
const built = await build({ entryPoints: [join(directory, 'test-worker.ts')], bundle: true, write: false,
  format: 'esm', platform: 'browser', target: 'es2022', external: ['cloudflare:workers'] });

function fact(key = 'source:a', version = 1, identifiers = ['email:ada@example.com'], deleted = false) {
  const payload = JSON.stringify({ first_name: 'Ada', last_name: 'Lovelace' });
  return {
    eventId: `${key}:${version}:${deleted}`, producerId: 'test-source', observedAt: '2026-09-01T00:00:00.000000Z',
    ingestedAt: '2026-09-05T00:00:00.000000Z', factKind: 'web', factKey: key,
    sourceFactVersion: version, factDeleted: deleted,
    factPayloadHash: createHash('sha256').update(payload + JSON.stringify(identifiers) + deleted).digest('hex'),
    factPayload: payload, evidenceKeys: identifiers,
  };
}

function sqlValues(value) {
  return [...value.matchAll(/'((?:\\.|[^'])*)'/g)].map(match => match[1].replace(/\\(.)/g, '$1'));
}

function remoteTinybird() {
  return {
    tables: { v1_identity_records: [], v1_identity_commits: [] }, calls: [],
    failAfter: null, partialWrite: false, quarantine: false, corruptRead: false,
    blockWrite: null, writeStarted: null,
    async fetch(request) {
      const url = new URL(request.url);
      assert.equal(url.origin, 'https://api.us-east.tinybird.co');
      if (url.pathname === '/v0/events') {
        const table = url.searchParams.get('name');
        assert.ok(Object.hasOwn(this.tables, table));
        const rows = (await request.text()).trim().split('\n').filter(Boolean).map(JSON.parse);
        this.calls.push({ operation: 'append', table, rows: structuredClone(rows) });
        if (table === 'v1_identity_records') {
          this.writeStarted?.();
          if (this.blockWrite) await this.blockWrite;
        }
        const partial = table === 'v1_identity_records' && (this.partialWrite || this.quarantine);
        const written = partial ? rows.slice(0, Math.max(0, rows.length - 1)) : rows;
        this.tables[table].push(...structuredClone(written));
        if (this.failAfter === table) {
          this.failAfter = null;
          return new Response('Lost response after remote write', { status: 503 });
        }
        return Response.json({ successful_rows: this.quarantine ? written.length : rows.length, quarantined_rows: this.quarantine ? rows.length - written.length : 0 });
      }
      assert.equal(url.pathname, '/v0/sql');
      const sql = new URLSearchParams(await request.text()).get('q');
      this.calls.push({ operation: 'query', sql });
      const batch = /AND batch_id = ('(?:\\.|[^'])*')/.exec(sql);
      if (batch) {
        const batchId = sqlValues(batch[1])[0];
        const table = sql.includes('FROM v1_identity_records') ? 'v1_identity_records' : 'v1_identity_commits';
        const rows = structuredClone(this.tables[table].filter(row => row.batch_id === batchId))
          .map(row => ({ ...row, batch_version: String(row.batch_version) }));
        if (table === 'v1_identity_records' && this.corruptRead && rows.length) rows[0].payload_json = '{"corrupt":true}';
        return Response.json({ data: rows });
      }
      const kind = sqlValues(/state_kind = ('(?:\\.|[^'])*')/.exec(sql)[1])[0];
      const keys = new Set(sqlValues(/lookup_key IN \(([^)]*)\)/.exec(sql)[1]));
      const version = Number(/batch_version <= (\d+)/.exec(sql)[1]);
      const committed = new Set(this.tables.v1_identity_commits.filter(row => Number(row.batch_version) <= version).map(row => row.batch_id));
      const latest = new Map();
      for (const record of this.tables.v1_identity_records) {
        if (record.state_kind !== kind || !keys.has(record.lookup_key) || Number(record.batch_version) > version || !committed.has(record.batch_id)) continue;
        const previous = latest.get(record.state_key);
        if (!previous || Number(previous.batch_version) < Number(record.batch_version)) latest.set(record.state_key, record);
      }
      return Response.json({ data: [...latest.values()].map(row => ({ payload_json: row.payload_json, is_deleted: row.is_deleted })) });
    },
    commits() { return [...new Map(this.tables.v1_identity_commits.map(row => [row.batch_id, row])).values()]; },
  };
}

async function runtime(t) {
  const storage = await mkdtemp(join(tmpdir(), 'identity-coordinator-test-'));
  const remote = remoteTinybird();
  const options = convertV4MiniflareOptions({
    name: 'identity-test', modules: true, script: built.outputFiles[0].text, compatibilityDate: '2026-09-05',
    durableObjects: { IDENTITY: { className: 'TestIdentityCoordinator', useSQLite: true } },
    durableObjectsPersist: storage, unsafeInspectDurableObjects: true,
    bindings: { TENANT_ID: 'boom', TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: 'local-test-only' },
    outboundService: request => remote.fetch(request),
  });
  const miniflare = new Miniflare(options);
  t.after(async () => { await miniflare.dispose(); await rm(storage, { recursive: true, force: true }); });
  async function call(path, body) {
    const response = await miniflare.dispatchFetch(`https://test.invalid${path}`, {
      method: body ? 'POST' : 'GET', body: body ? JSON.stringify(body) : undefined,
    });
    if (!response.ok) throw new Error(await response.text());
    return response.json();
  }
  const evict = () => miniflare.unsafeEvictDurableObject('identity-test', 'TestIdentityCoordinator', { name: 'boom' });
  return { miniflare, remote, call, evict };
}

test('durable inbox accepts one event and ignores its identical delivery retry', async t => {
  const { call } = await runtime(t);
  assert.equal((await call('/enqueue', [fact()])).accepted, 1);
  assert.equal((await call('/enqueue', [{ ...fact(), ingestedAt: '2026-09-05T01:00:00Z' }])).accepted, 0);
  const status = await call('/status');
  assert.equal(status.pending, 1);
  assert.ok(status.alarmAt > Date.now());
});

test('a conflicting retry rolls back earlier inserts from that receipt', async t => {
  const { call } = await runtime(t);
  await call('/enqueue', [fact()]);
  await assert.rejects(call('/enqueue', [fact('source:new'), { ...fact(), evidenceKeys: ['email:wrong@example.com'] }]), /Conflicting identity event retry/);
  assert.equal((await call('/status')).pending, 1);
});

test('alarm scheduling failure rolls back the newly inserted durable inbox rows', async t => {
  const { call } = await runtime(t);
  await assert.rejects(call('/fail-wake', [fact()]), /Injected alarm scheduling failure/);
  const status = await call('/status');
  assert.equal(status.pending, 0);
  assert.equal(status.alarmAt, null);
});

test('eviction preserves inbox and computation publishes complete visible graph state', async t => {
  const { call, evict, remote } = await runtime(t);
  await call('/enqueue', [fact('source:a', 1, ['email:ada@example.com', 'anonymous_id:visitor'])]);
  await evict();
  assert.equal((await call('/status')).pending, 1);
  const published = await call('/run');
  assert.equal(published.pending, 0);
  assert.equal(published.publishedVersion, 1);
  assert.equal(published.activeBatch, null);
  assert.equal(remote.commits().length, 1);
  const kinds = new Set(remote.tables.v1_identity_records.map(row => row.state_kind));
  assert.deepEqual([...kinds].sort(), ['evidence', 'fact', 'mapping', 'profile']);
});

test('restart restores an alarm when durable input exists but the alarm is missing', async t => {
  const { call, evict } = await runtime(t);
  await call('/enqueue', [fact()]);
  await call('/clear-alarm');
  await evict();
  const status = await call('/status');
  assert.equal(status.pending, 1);
  assert.ok(status.alarmAt !== null, 'constructor must restore durable pending work');
});

test('partial stored rows cannot commit even when ingestion claims full success', async t => {
  const { call, remote } = await runtime(t);
  remote.partialWrite = true;
  await call('/enqueue', [fact()]);
  await assert.rejects(call('/run'), /not yet fully visible/);
  assert.equal(remote.commits().length, 0);
  assert.equal((await call('/status')).pending, 1);
  remote.partialWrite = false;
  assert.equal((await call('/run')).publishedVersion, 1);
});

test('quarantined rows leave the durable job and inbox available for retry', async t => {
  const { call, remote } = await runtime(t);
  remote.quarantine = true;
  await call('/enqueue', [fact()]);
  await assert.rejects(call('/run'), /did not accept every/);
  assert.equal(remote.commits().length, 0);
  assert.ok((await call('/status')).activeBatch);
  remote.quarantine = false;
  assert.equal((await call('/run')).pending, 0);
});

test('ambiguous record append survives eviction without recomputing or changing its saved batch', async t => {
  const { call, remote, evict } = await runtime(t);
  remote.failAfter = 'v1_identity_records';
  await call('/enqueue', [fact()]);
  await assert.rejects(call('/run'), /HTTP 503/);
  const batch = (await call('/status')).activeBatch;
  const currentQueries = remote.calls.filter(call => call.sql?.includes('argMax')).length;
  await evict();
  assert.equal((await call('/run')).publishedVersion, 1);
  assert.equal(remote.commits()[0].batch_id, batch);
  assert.equal(remote.calls.filter(call => call.sql?.includes('argMax')).length, currentQueries);
});

test('ambiguous commit resumes the same batch and does not drop later inbox input', async t => {
  const { call, remote } = await runtime(t);
  remote.failAfter = 'v1_identity_commits';
  await call('/enqueue', [fact()]);
  await assert.rejects(call('/run'), /HTTP 503/);
  const saved = structuredClone(remote.commits()[0]);
  await call('/enqueue', [fact('source:b', 1, ['email:grace@example.com'])]);
  const completed = await call('/run');
  assert.equal(completed.publishedVersion, 1);
  assert.equal(completed.pending, 1);
  assert.deepEqual(remote.commits()[0], saved);
  assert.equal((await call('/run')).publishedVersion, 2);
  assert.equal(remote.commits().length, 2);
});

test('a live lease blocks competing publication and an expired lease is reclaimed after eviction', async t => {
  const { call, remote, evict } = await runtime(t);
  await call('/enqueue', [fact()]);
  await call('/prime-lease');
  const held = await call('/run');
  assert.equal(held.activeBatch, 'crashed-batch');
  assert.equal(remote.calls.length, 0);
  assert.ok(held.alarmAt > Date.now());
  await call('/expire-lease');
  await evict();
  assert.equal((await call('/run')).publishedVersion, 1);
  assert.equal(remote.commits()[0].batch_id, 'crashed-batch');
});

test('input received during remote publication is excluded from the active cutoff', async t => {
  const { call, remote } = await runtime(t);
  let release;
  remote.blockWrite = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { remote.writeStarted = resolve; });
  await call('/enqueue', [fact()]);
  const running = call('/run');
  await started;
  await call('/enqueue', [fact('source:b', 1, ['email:grace@example.com'])]);
  const competing = await call('/run');
  assert.equal(competing.pending, 2);
  assert.equal(remote.calls.filter(call => call.operation === 'append').length, 1);
  release();
  assert.equal((await running).pending, 1);
  remote.blockWrite = null;
  assert.equal((await call('/run')).pending, 0);
});

test('completed source replay preserves mappings and stale input cannot resurrect deleted evidence', async t => {
  const { call, remote } = await runtime(t);
  const original = fact();
  await call('/enqueue', [original]);
  await call('/run');
  await call('/enqueue', [original]);
  await call('/run');
  assert.equal(remote.commits()[1].row_count, 0);
  await call('/enqueue', [fact('source:a', 2, [], true)]);
  await call('/run');
  await call('/enqueue', [original]);
  await call('/run');
  assert.equal(remote.commits()[3].row_count, 0);
  const latestFact = remote.tables.v1_identity_records.filter(row => row.state_kind === 'fact').at(-1);
  assert.equal(JSON.parse(latestFact.payload_json).factDeleted, true);
});

test('stored payload corruption prevents an identity commit', async t => {
  const { call, remote } = await runtime(t);
  remote.corruptRead = true;
  await call('/enqueue', [fact()]);
  await assert.rejects(call('/run'), /records did not verify/);
  assert.equal(remote.commits().length, 0);
  assert.equal((await call('/status')).pending, 1);
});
