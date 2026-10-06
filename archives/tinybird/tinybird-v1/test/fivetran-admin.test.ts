import assert from 'node:assert/strict';
import test from 'node:test';
import { handleFivetranAdmin, type FivetranAdminEnv } from '../worker/fivetran/admin-routes.ts';

function fixture(baseline: unknown = null) {
  const calls: Array<{ name: string; input?: unknown }> = [];
  const coordinator = {
    initializePipeline: async (input: unknown) => { calls.push({ name: 'initialize', input }); return { initialized: true }; },
    loadBulkBootstrapCheckpoint: async (input: unknown) => { calls.push({ name: 'checkpoint-load', input }); return null; },
    start: async (input: unknown) => { calls.push({ name: 'start', input }); return { activated: true }; },
  };
  const env = {
    TENANT_ID: 'boom', INGESTION_ENABLED: 'false',
    FIVETRAN_FACTS: { getByName(name: string) { calls.push({ name }); return coordinator; } },
    IDENTITY: { getByName() { return { status: async () => ({ baseline, publishedVersion: baseline ? 1 : 0 }) }; } },
  } as unknown as FivetranAdminEnv;
  return { env, calls };
}

function request(method: string, body: unknown): Request {
  return new Request(`https://worker.example/admin/fivetran/stripe_main/${method}`, { method: 'POST', body: JSON.stringify(body) });
}

test('bootstrap requests use the stable per-pipeline coordinator and preserve a missing checkpoint', async () => {
  const { env, calls } = fixture();
  const initialized = await handleFivetranAdmin(request('initialize', { snapshotId: 's1', snapshotAt: '2026-09-05T22:28:00Z' }), env);
  assert.equal(initialized.status, 200);
  assert.deepEqual(calls, [
    { name: 'fivetran:stripe_main' },
    { name: 'initialize', input: { pipeline: 'stripe_main', snapshotId: 's1', snapshotAt: '2026-09-05T22:28:00Z' } },
  ]);
  const checkpoint = await handleFivetranAdmin(request('checkpoint-load', { pipeline: 'stripe_main', snapshotId: 's1' }), env);
  assert.equal(await checkpoint.json(), null);
});

test('misrouted and oversized chunked requests never reach a coordinator', async () => {
  const { env, calls } = fixture();
  assert.equal((await handleFivetranAdmin(request('initialize', { pipeline: 'activecampaign' }), env)).status, 400);
  const stream = new ReadableStream({ start(controller) {
    controller.enqueue(new TextEncoder().encode('{"padding":"'));
    controller.enqueue(new Uint8Array(950_000));
    controller.close();
  } });
  const oversized = new Request('https://worker.example/admin/fivetran/stripe_main/seed', { method: 'POST', body: stream, duplex: 'half' } as RequestInit);
  assert.equal((await handleFivetranAdmin(oversized, env)).status, 413);
  assert.equal(calls.length, 0);
});

test('live processing requires enabled ingestion and the actual active identity seal', async () => {
  const { env, calls } = fixture({ sealHash: 'verified-seal' });
  const start = () => request('start', { snapshotId: 's1', activationId: 'verified-seal' });
  assert.equal((await handleFivetranAdmin(start(), env)).status, 409);
  env.INGESTION_ENABLED = 'true';
  assert.equal((await handleFivetranAdmin(request('start', { snapshotId: 's1', activationId: 'another-seal' }), env)).status, 409);
  assert.equal(calls.some(call => call.name === 'start'), false);
  assert.equal((await handleFivetranAdmin(start(), env)).status, 200);
  assert.deepEqual(calls.at(-1), { name: 'start', input: { pipeline: 'stripe_main', snapshotId: 's1', activationId: 'verified-seal' } });
  const missing = fixture();
  missing.env.INGESTION_ENABLED = 'true';
  assert.equal((await handleFivetranAdmin(start(), missing.env)).status, 409);
  assert.equal(missing.calls.some(call => call.name === 'start'), false);
});
