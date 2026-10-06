import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHash } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Miniflare, convertV4MiniflareOptions } from 'miniflare';
import { build } from 'esbuild';
import { fact, remoteTinybird } from './remote.mjs';

const directory = dirname(fileURLToPath(import.meta.url));
const built = await build({ entryPoints: [join(directory, 'activation-worker.ts')], bundle: true, write: false,
  format: 'esm', platform: 'browser', target: 'es2022', external: ['cloudflare:workers'] });
const SOURCE_SEAL = 'a'.repeat(64);
function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}
function hash(value) { return createHash('sha256').update(canonical(value)).digest('hex'); }
function manifest(remote, kind, key, value, tenant = 'boom', baseline = 'baseline-one') {
  remote.manifests.set(JSON.stringify([tenant, baseline, kind, key]), { manifest_key: key, payload_json: canonical(value), payload_hash: hash(value) });
}
function seal(remote) {
  const membership = { version: 1, buckets: 16384, pageSize: 256, pages: { source: [], visitor: [] } };
  for (const index of ['source', 'visitor']) for (let page = 0; page < 64; page++) {
    const descriptor = { index, page, buckets: Array.from({ length: 256 }, () => ({ count: 0, hash: hash([]) })) };
    membership.pages[index].push(hash(descriptor));
    manifest(remote, 'membership-page', `${index}:${page}`, descriptor);
  }
  const value = { runId: 'baseline-one', tenantId: 'boom', sourceSeal: SOURCE_SEAL, inputManifestHash: 'b'.repeat(64),
    finalSessionSequence: 10, identityVersion: 1, visitorRevision: '1',
    counts: { visitors: 0, pageHeads: 0, sessions: 0, identityFacts: 1, identityComponents: 1 }, receiptsHash: 'c'.repeat(64), membership };
  manifest(remote, 'seal', 'complete', value);
  return value;
}
async function runtime(t) {
  const persistence = await mkdtemp(join(tmpdir(), 'baseline-activation-test-'));
  const remote = remoteTinybird(); const savedSeal = seal(remote);
  let bindings = { TENANT_ID: 'boom', TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: 'local-only',
    BROWSER_BASELINE_ID: 'baseline-one', BROWSER_BASELINE_SEAL: SOURCE_SEAL, BROWSER_BASELINE_SEQUENCE: '10' };
  const create = () => new Miniflare({
    ...convertV4MiniflareOptions({
      name: 'activation-test', modules: true, script: built.outputFiles[0].text, compatibilityDate: '2026-09-05',
      durableObjects: { IDENTITY: { className: 'TestIdentity', useSQLite: true } }, bindings,
      serviceBindings: { BASELINE: { name: 'activation-test', entrypoint: 'BootstrapBaseline' } },
      outboundService: request => remote.fetch(request),
    }),
    resourcePersistencePath: persistence, isolatedResourcePersistencePath: persistence,
  });
  let mf = create();
  t.after(async () => { await mf.dispose(); await rm(persistence, { recursive: true, force: true }); });
  const call = async (path, body = {}) => {
    const response = await mf.dispatchFetch(`https://test.invalid${path}`, { method: 'POST', body: JSON.stringify(body) });
    const result = await response.json(); if (!response.ok) throw new Error(result.error); return result;
  };
  return { call, remote, savedSeal, async restart(changes = {}) { await mf.dispose(); bindings = { ...bindings, ...changes }; mf = create(); } };
}

test('activation reads the scoped saved seal and starts live work at version two using existing facts', async t => {
  const r = await runtime(t);
  await r.call('/seed', [fact('old-page', 1, ['email:ada@example.com', 'anonymous_id:visitor'])]);
  const oldRows = structuredClone(r.remote.tables.v1_identity_records);
  const receipt = await r.call('/activate');
  assert.equal(receipt.identityVersion, 1);
  assert.equal(receipt.sealHash, hash(r.savedSeal));
  assert.equal((await r.call('/status')).publishedVersion, 1);
  assert.deepEqual(r.remote.tables.v1_identity_records, oldRows);
  await r.call('/enqueue', [fact('new-payment', 1, ['anonymous_id:visitor', 'phone:+15550001111'])]);
  assert.equal((await r.call('/run')).publishedVersion, 2);
  const latestMappings = new Map();
  for (const row of r.remote.tables.v1_identity_records.filter(row => row.state_kind === 'mapping' && !row.is_deleted)) latestMappings.set(row.lookup_key, JSON.parse(row.payload_json));
  assert.equal(latestMappings.get('phone:+15550001111').profileId, latestMappings.get('email:ada@example.com').profileId);
  assert.ok(r.remote.calls.some(call => call.sql?.includes('batch_version <= 1') && call.sql.includes('argMax')));
  assert.deepEqual(r.remote.commits().map(row => Number(row.batch_version)), [1, 2]);
});

test('configured identity inputs are blocked until the baseline is activated', async t => {
  const r = await runtime(t);
  await assert.rejects(r.call('/enqueue', [fact()]), /has not been activated/);
  await assert.rejects(r.call('/run'), /has not been activated/);
  const status = await r.call('/status');
  assert.equal(status.pending, 0); assert.equal(status.publishedVersion, 0);
  await r.call('/activate');
  assert.equal((await r.call('/enqueue', [fact()])).accepted, 1);
});

test('same-seal retries survive restart and never rewind a live version or pending inbox', async t => {
  const r = await runtime(t);
  const original = await r.call('/activate'); await r.restart();
  assert.deepEqual(await r.call('/activate'), original);
  await r.call('/enqueue', [fact()]);
  assert.deepEqual(await r.call('/activate'), original);
  assert.equal((await r.call('/status')).pending, 1);
  await r.call('/run');
  assert.deepEqual(await r.call('/activate'), original);
  assert.equal((await r.call('/status')).publishedVersion, 2);
});

test('changed seal contents or baseline configuration cannot replace an active identity baseline', async t => {
  const r = await runtime(t); await r.call('/activate');
  manifest(r.remote, 'seal', 'complete', { ...r.savedSeal, receiptsHash: 'd'.repeat(64) });
  await assert.rejects(r.call('/activate'), /cannot change/);
  assert.equal((await r.call('/status')).publishedVersion, 1);
  await r.restart({ BROWSER_BASELINE_ID: 'another-run' });
  await assert.rejects(r.call('/enqueue', [fact()]), /configuration changed/);
  await assert.rejects(r.call('/activate'), /does not match/);
});

test('first activation rejects prior inbox, job, outbox, publication and drained inbox history', async t => {
  for (const kind of ['inbox', 'job', 'outbox', 'published', 'drained']) {
    const r = await runtime(t); await r.call('/inject', { kind });
    await assert.rejects(r.call('/activate'), /before any live input or job/);
    assert.equal((await r.call('/status')).baseline, null);
  }
});

test('activation rechecks local state after waiting for the remote seal', async t => {
  const r = await runtime(t); let release;
  r.remote.holdSeal = new Promise(resolve => { release = resolve; });
  const entered = new Promise(resolve => { r.remote.sealStarted = resolve; });
  const pending = r.call('/activate');
  const rejected = assert.rejects(pending, /before any live input or job/);
  await entered; await r.call('/inject', { kind: 'inbox' }); release(); await rejected;
  assert.equal((await r.call('/status')).publishedVersion, 0);
});

test('missing, wrong-tenant and wrong-version saved seals do not initialize identity', async t => {
  for (const mutation of [value => ({ ...value, tenantId: 'other' }), value => ({ ...value, runId: 'other' }), value => ({ ...value, sourceSeal: 'f'.repeat(64) }), value => ({ ...value, finalSessionSequence: 9 }), value => ({ ...value, identityVersion: 2 }), value => ({ ...value, membership: undefined })]) {
    const r = await runtime(t); const changed = mutation(r.savedSeal);
    if (changed.membership === undefined) delete changed.membership;
    manifest(r.remote, 'seal', 'complete', changed);
    await assert.rejects(r.call('/activate'));
    assert.equal((await r.call('/status')).publishedVersion, 0);
  }
  const r = await runtime(t); r.remote.manifests.delete(JSON.stringify(['boom', 'baseline-one', 'seal', 'complete']));
  await assert.rejects(r.call('/activate'), /does not match/);
});

test('actual stored manifest hash is checked rather than trusting seal fields alone', async t => {
  const r = await runtime(t); r.remote.corruptManifest = true;
  await assert.rejects(r.call('/activate'), /Conflicting bootstrap manifest/);
  assert.equal((await r.call('/status')).publishedVersion, 0);
});

test('private baseline RPC returns authenticated absence for all three reader methods', async t => {
  const r = await runtime(t);
  const keys = [{ event_kind: 'page_view', source_record_id: 'absent-page' }];
  assert.deepEqual(await r.call('/heads', keys), [{ key: keys[0], heads: [] }]);
  assert.deepEqual(await r.call('/members', ['absent-visitor']), []);
  assert.equal(await r.call('/visitor', { tenant: 'boom', visitor: 'absent-visitor' }), null);
  assert.ok(r.remote.calls.filter(call => call.sql?.includes("manifest_kind = 'membership-page'")).length >= 3);
  assert.equal(r.remote.calls.filter(call => call.operation === 'append').length, 0);
});

test('private baseline RPC rejects missing proofs, wrong tenant and wrong configured sequence', async t => {
  const r = await runtime(t);
  await assert.rejects(r.call('/visitor', { tenant: 'other', visitor: 'absent' }), /tenant mismatch/);
  const withoutProof = { ...r.savedSeal }; delete withoutProof.membership;
  manifest(r.remote, 'seal', 'complete', withoutProof);
  await assert.rejects(r.call('/heads', []), /membership proofs/);
  await r.restart({ BROWSER_BASELINE_SEQUENCE: '11' });
  await assert.rejects(r.call('/members', []), /publication versions/);
});
