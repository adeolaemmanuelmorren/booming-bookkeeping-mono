import { readFile } from 'node:fs/promises';
import { getHeapStatistics } from 'node:v8';
import { Tinybird } from '../worker/storage/tinybird.ts';
import { sourceRecords } from '../worker/conversions/publication.ts';
import { conversionIdentityFacts } from '../worker/conversions/identity.ts';
import { normalizeStripeChargeSnapshot } from '../worker/conversions/stripe.mjs';
import { buildActiveCampaignContactReplacement } from '../worker/conversions/activecampaign.mjs';
import { TinybirdFivetranFactReader } from '../worker/fivetran/tinybird-reader.ts';
import { TinybirdBulkBootstrapPublisher, type PendingIdentityFact } from '../worker/fivetran/bootstrap-publisher.ts';
import { runBulkBootstrapAndSeal } from '../worker/fivetran/bulk-bootstrap.ts';
import { FivetranCoordinatorClient } from './fivetran-coordinator-client.ts';
import { TinybirdBootstrapStorage } from '../worker/bootstrap/tinybird-storage.ts';
import { executeBootstrap } from '../worker/bootstrap/executor.ts';
import { hash } from '../worker/bootstrap/hash.ts';
import { loadNativeImportProof, verifyCurrentImportCounts } from './bootstrap-import-proof.ts';

const SNAPSHOT_ID = 'fivetran-20260905T222800Z';
const SNAPSHOT_AT = '2026-09-05T22:28:00.000000Z';
const HOST = 'https://api.us-east.tinybird.co';

const configPath = required('BOOTSTRAP_CONFIG_PATH');
const frozenImports = await loadNativeImportProof({
  configBytes: await readFile(configPath),
  proofPath: new URL('../config/native-import-verification.json', import.meta.url),
  buildManifestPath: new URL('../build-manifest.json', import.meta.url),
});
// Import verification is deployment metadata; model plans remain unchanged on a safe rebuild.
const { nativeImportProofSha256: _nativeImportProofSha256, ...config } = frozenImports.config;
if (config.workspaceId !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af' || config.tenantId !== 'boom') {
  throw new Error('Bootstrap configuration belongs to another workspace');
}
if (!config.bulk || config.region !== 'us-east4' || required('BOOTSTRAP_SINGLE_TASK') !== '1') {
  throw new Error('Use one supervised bulk task in us-east4');
}
if (getHeapStatistics().heap_size_limit < 4 * 1024 ** 3) throw new Error('Bootstrap requires at least a 4 GiB Node heap');

const token = required('TINYBIRD_TOKEN');
const workspaceResponse = await fetch(`${HOST}/v1/workspace`, {
  headers: { Authorization: `Bearer ${token}` }, redirect: 'error', signal: AbortSignal.timeout(20_000),
});
if (!workspaceResponse.ok) throw new Error('Cannot verify bootstrap workspace');
const workspace = await workspaceResponse.json() as { id: string; name: string };
if (workspace.id !== config.workspaceId || workspace.name !== config.workspaceName) throw new Error('Bootstrap token belongs to another workspace');

const startedAt = Date.now();
const requests = { sql: 0, appends: 0, uploadBytes: 0, failures: 0 };
const client = new Tinybird({ TINYBIRD_URL: HOST, TINYBIRD_TOKEN: token }, async (input, init) => {
  if (new URL(String(input)).pathname === '/v0/events') {
    requests.appends++;
    requests.uploadBytes += Buffer.byteLength(String(init?.body ?? ''));
  } else requests.sql++;
  try {
    const response = await fetch(input, init);
    if (!response.ok) {
      requests.failures++;
      report('request-failed', { path: new URL(String(input)).pathname, httpStatus: response.status });
    }
    return response;
  } catch (error) {
    requests.failures++;
    throw error;
  }
}, { requestTimeoutMs: 60_000, appendMaxBytes: 8_000_000, appendConcurrency: 4 });

await verifyCurrentImportCounts(client, frozenImports.proof);
report('native-imports-verified', { tables: frozenImports.proof.tables.length, proofSha256: frozenImports.proofSha256 });

const coordinator = new FivetranCoordinatorClient(required('V1_ADMIN_TOKEN'), SNAPSHOT_ID);
const publisher = new TinybirdBulkBootstrapPublisher({
  tenantId: config.tenantId, snapshotId: SNAPSHOT_ID, snapshotAt: SNAPSHOT_AT,
  client, maximumAppendBytes: 8_000_000,
  transforms: {
    sourceRecords,
    identityFacts: async replacement => (await conversionIdentityFacts(replacement)).map(fact => {
      if (!['stripe', 'stripe_kajabi', 'activecampaign'].includes(fact.factKind)) throw new Error('Unexpected conversion identity kind');
      return fact as PendingIdentityFact;
    }),
  },
});

try {
  const source = await runBulkBootstrapAndSeal({
    snapshotId: SNAPSHOT_ID, snapshotAt: SNAPSHOT_AT, sealedAt: SNAPSHOT_AT,
    pageScopes: 5_000, identitySealer: publisher,
    dependencies: {
      reader: new TinybirdFivetranFactReader(client, config.tenantId, SNAPSHOT_AT),
      publisher, checkpoints: coordinator, seeder: coordinator,
      stripeNormalizer: normalizeStripeChargeSnapshot,
      activeCampaignNormalizer: buildActiveCampaignContactReplacement,
    },
    onProgress: progress => report('conversions', {
      pipeline: progress.pipeline, scopes: progress.publishedScopes,
      identityFacts: progress.identityFactCount, complete: progress.complete,
    }),
  });
  const { sealedAt: _sealedAt, ...conversionIdentity } = source.identity;
  config.conversionIdentity = conversionIdentity;
  config.sourceSeal = await hash({
    baselineId: config.baselineId, inputManifestHash: config.inputManifestHash,
    importMode: 'one-object-per-content', inputs: config.inputs,
    live: config.live ?? null, conversionIdentity,
  });
  report('conversion-snapshot-sealed', { ...conversionIdentity, sourceSeal: config.sourceSeal });
  const store = new TinybirdBootstrapStorage(client, config, { bulk: true });
  const result = await executeBootstrap(config, store, report);
  report('sealed', { result, conversions: source.checkpoints });
} catch (error) {
  report('stopped', { sealed: false, errorType: error instanceof Error ? error.name : 'UnknownError' });
  process.exitCode = 1;
}

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}

function report(stage: string, counts: Record<string, unknown>): void {
  process.stdout.write(JSON.stringify({
    timestamp: new Date().toISOString(), stage, ...counts,
    elapsedSeconds: Math.round((Date.now() - startedAt) / 1000),
    requests, rssBytes: process.memoryUsage().rss,
  }) + '\n');
}
