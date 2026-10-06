import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { cp, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { FivetranCoordinatorClient } from '../scripts/fivetran-coordinator-client.ts';
import { packageBootstrap, RUNTIME_FILES } from '../scripts/package-bootstrap.mjs';
import type { BulkBootstrapCheckpoint, PreparedScope } from '../worker/fivetran/contracts.ts';
import { bootstrapFixture } from './helpers/bootstrap-fixture.ts';
import { applyNativeImportFixtureCounts, nativeImportProof } from './helpers/import-proof-fixture.ts';

const SNAPSHOT_ID = 'fivetran-20260905T222800Z';
const SNAPSHOT_AT = '2026-09-05T22:28:00.000000Z';
const ROOT = fileURLToPath(new URL('../', import.meta.url));

test('coordinator uses the authenticated Worker request shapes and rejects another snapshot', async t => {
  const calls: { url: string; options: RequestInit; body: Record<string, unknown> }[] = [];
  t.mock.method(globalThis, 'fetch', async (input: string, options: RequestInit) => {
    calls.push({ url: String(input), options, body: JSON.parse(String(options.body)) });
    return Response.json(String(input).endsWith('/checkpoint-load') ? null : { ok: true });
  });
  const client = new FivetranCoordinatorClient('synthetic-token', SNAPSHOT_ID);
  const pipeline = 'stripe_main';
  await client.initializePipeline({ pipeline, snapshotAt: SNAPSHOT_AT });
  assert.equal(await client.loadBulkBootstrapCheckpoint({ pipeline, snapshotId: SNAPSHOT_ID }), null);
  const checkpoint: BulkBootstrapCheckpoint = { pipeline, snapshotId: SNAPSHOT_ID, snapshotAt: SNAPSHOT_AT,
    afterCursor: 'synthetic-charge', complete: false, publishedScopes: 1, identityFactCount: 1 };
  await client.saveBulkBootstrapCheckpoint(checkpoint);
  await client.finalizeBulkBootstrap({ pipeline, snapshotAt: SNAPSHOT_AT });
  assert.deepEqual(calls.map(call => call.url), ['initialize', 'checkpoint-load', 'checkpoint-save', 'finalize']
    .map(method => `https://boom-tinybird-facts-v1.bill-3e3.workers.dev/admin/fivetran/stripe_main/${method}`));
  assert.deepEqual(calls.map(call => call.body), [
    { pipeline, snapshotAt: SNAPSHOT_AT, snapshotId: SNAPSHOT_ID },
    { pipeline, snapshotId: SNAPSHOT_ID }, checkpoint, { pipeline, snapshotAt: SNAPSHOT_AT },
  ]);
  for (const call of calls) {
    assert.equal(call.options.method, 'POST');
    assert.equal(call.options.redirect, 'error');
    assert.equal(new Headers(call.options.headers).get('Authorization'), 'Bearer synthetic-token');
    assert.ok(call.options.signal instanceof AbortSignal);
  }
  assert.throws(() => client.loadBulkBootstrapCheckpoint({ pipeline, snapshotId: 'another' }), /another snapshot/);
  await assert.rejects(client.saveBulkBootstrapCheckpoint({ ...checkpoint, snapshotId: 'another' }), /another snapshot/);
  assert.equal(calls.length, 4);
});

test('coordinator seed batches obey UTF-8 byte limits without losing scopes and redact server errors', async t => {
  const requests: { bytes: number; prepared: PreparedScope[] }[] = [];
  t.mock.method(globalThis, 'fetch', async (_input: string, options: RequestInit) => {
    requests.push({ bytes: Buffer.byteLength(String(options.body)), prepared: JSON.parse(String(options.body)).prepared });
    return Response.json({ ok: true });
  });
  const client = new FivetranCoordinatorClient('synthetic-token', SNAPSHOT_ID);
  const prepared: PreparedScope[] = Array.from({ length: 205 }, (_, index) => ({
    windowId: `bootstrap:${SNAPSHOT_ID}`, scopeId: `stripe:main:charge:synthetic-${index}`, inputHash: 'a'.repeat(64),
    compactState: { synthetic: 'é'.repeat(6000) },
    replacement: { source: 'stripe', source_account: 'main', scope_id: `stripe:main:charge:synthetic-${index}`,
      replacement_id: `synthetic-${index}`, observed_at: SNAPSHOT_AT, observation_sequence: 1,
      rows: [], evidence_inbox_ids: [], source_evidence: {} },
  }));
  await client.seedBulkBootstrapScopes({ pipeline: 'stripe_main', snapshotAt: SNAPSHOT_AT, prepared });
  assert.ok(requests.length > 2, 'UTF-8 bytes, rather than character count, force extra requests');
  assert.ok(requests.every(request => request.bytes <= 900_000 && request.prepared.length <= 100));
  assert.deepEqual(requests.flatMap(request => request.prepared.map(scope => scope.scopeId)), prepared.map(scope => scope.scopeId));
  const sent = requests.length;
  await client.seedBulkBootstrapScopes({ pipeline: 'stripe_main', snapshotAt: SNAPSHOT_AT, prepared: [] });
  await assert.rejects(client.seedBulkBootstrapScopes({ pipeline: 'stripe_main', snapshotAt: SNAPSHOT_AT,
    prepared: [{ ...prepared[0], compactState: { synthetic: 'é'.repeat(500_000) } }] }), /exceeds/);
  assert.equal(requests.length, sent);
  t.mock.method(globalThis, 'fetch', async () => new Response('synthetic-sensitive-provider-detail', { status: 503 }));
  await assert.rejects(client.finalizeBulkBootstrap({ pipeline: 'stripe_main', snapshotAt: SNAPSHOT_AT }), error => {
    assert.equal((error as Error).message, 'Fivetran coordinator request failed with HTTP 503');
    return true;
  });
});

test('packaged runner imports under Node22 strip-types and excludes source secrets and backups', async () => {
  const folder = await mkdtemp(join(tmpdir(), 'bootstrap-package-'));
  try {
    const source = join(folder, 'source');
    await mkdir(source);
    for (const name of [...RUNTIME_FILES, 'Dockerfile.bootstrap-all']) {
      await mkdir(dirname(join(source, name)), { recursive: true });
      await cp(join(ROOT, name), join(source, name));
    }
    await writeFile(join(source, '.tinyb'), 'synthetic-secret-sentinel');
    await writeFile(join(source, '.env'), 'synthetic-secret-sentinel');
    await writeFile(join(source, 'customer-backup.json'), 'synthetic-payload-sentinel');
    const configPath = join(folder, 'config.json');
    const config = await metadataConfig();
    await writeFile(configPath, JSON.stringify(config));
    const proofPath = join(folder, 'verified-imports.json');
    await writeFile(proofPath, JSON.stringify(nativeImportProof()));
    const output = join(folder, 'context');
    const result = await packageBootstrap(configPath, output, source, proofPath);
    assert.equal(result.sourceFiles, 42);
    const actual = (await readdir(output, { recursive: true, withFileTypes: true }))
      .filter(entry => entry.isFile()).map(entry => join(entry.parentPath, entry.name).slice(output.length + 1)).sort();
    assert.deepEqual(actual, [...RUNTIME_FILES, 'Dockerfile', 'package.json', 'config/bootstrap.json', 'config/native-import-verification.json', '.dockerignore', 'build-manifest.json'].sort());
    const packaged = JSON.parse(await readFile(join(output, 'config/bootstrap.json'), 'utf8'));
    assert.deepEqual(packaged, { ...config, nativeImportProofSha256: result.nativeImportProofSha256 });
    assert.equal(await readFile(join(output, 'config/native-import-verification.json'), 'utf8'), await readFile(proofPath, 'utf8'));
    assert.match(await readFile(join(output, 'Dockerfile'), 'utf8'), /scripts\/bootstrap-all\.ts/);
    // Empty environment guarantees import verification cannot use local credentials or start a job.
    await assert.rejects(promisify(execFile)(process.execPath, ['--experimental-strip-types', join(output, 'scripts/bootstrap-all.ts')], { env: {} }), error => {
      assert.match(String((error as { stderr: string }).stderr), /Missing BOOTSTRAP_CONFIG_PATH/);
      return true;
    });
    await assert.rejects(packageBootstrap(configPath, output, source, proofPath), /EEXIST/);
    assert.equal(await readFile(join(source, '.tinyb'), 'utf8'), 'synthetic-secret-sentinel');
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});

test('packaging rejects credentials, payload fields, stale bulk settings, and symlinked source files', async () => {
  const folder = await mkdtemp(join(tmpdir(), 'bootstrap-package-reject-'));
  try {
    const configPath = join(folder, 'config.json');
    const valid = await metadataConfig();
    const proofPath = join(folder, 'verified-imports.json');
    await writeFile(proofPath, JSON.stringify(nativeImportProof()));
    const cases = [{ ...valid, TINYBIRD_TOKEN: 'synthetic-token' }, { ...valid, bulk: false },
      { ...valid, live: { ...valid.live, payload: 'synthetic-raw' } },
      { ...valid, inputs: valid.inputs.map((input, index) => index ? input : { ...input, credentials: 'synthetic' }) }];
    for (const config of cases) {
      await writeFile(configPath, JSON.stringify(config));
      await assert.rejects(packageBootstrap(configPath, join(folder, 'rejected'), ROOT, proofPath), /configuration|Configuration/);
    }
    await writeFile(configPath, JSON.stringify(valid));
    const source = join(folder, 'symlink-source');
    await mkdir(join(source, 'scripts'), { recursive: true });
    await symlink(join(ROOT, 'scripts/bootstrap-all.ts'), join(source, 'scripts/bootstrap-all.ts'));
    await assert.rejects(packageBootstrap(configPath, join(folder, 'rejected'), source, proofPath), /regular files/);
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});

async function metadataConfig() {
  const { config } = await bootstrapFixture('booming_bookkeeping', 'boom-browser-20260905-v1');
  config.tenantId = 'boom';
  config.workspaceId = '00c04079-d0b4-4d8b-8de6-6fa8072b85af';
  config.startedAt = SNAPSHOT_AT;
  config.bulk = true;
  for (const input of config.inputs) input.landingTable = input.landingTable.replace('v1_smoke_raw_', 'v1_history_');
  config.live!.table = 'v1_history_live_jitsu';
  applyNativeImportFixtureCounts(config);
  return config;
}
