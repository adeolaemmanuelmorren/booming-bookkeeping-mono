import { createHash } from 'node:crypto';
import { lstat, mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { validateNativeImportProof, validateProductionBrowserInputs } from './bootstrap-import-proof.ts';

const DEFAULT_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const SNAPSHOT_AT = '2026-09-05T22:28:00.000000Z';

// This list is the combined runner's complete local import graph, including type imports.
// New runtime dependencies must be added explicitly and pass the package import test.
export const RUNTIME_FILES = Object.freeze([
  "scripts/bootstrap-all.ts",
  "scripts/bootstrap-import-proof.ts",
  "scripts/fivetran-coordinator-client.ts",
  "worker/bootstrap/baseline.ts",
  "worker/bootstrap/conversion-identity.ts",
  "worker/bootstrap/executor.ts",
  "worker/bootstrap/hash.ts",
  "worker/bootstrap/identity.ts",
  "worker/bootstrap/input-counts.ts",
  "worker/bootstrap/manifest.ts",
  "worker/bootstrap/membership.ts",
  "worker/bootstrap/replay.ts",
  "worker/bootstrap/sessions.ts",
  "worker/bootstrap/tinybird-storage.ts",
  "worker/browser/identity.ts",
  "worker/browser/normalize.ts",
  "worker/browser/publication.ts",
  "worker/browser/tinybird-storage.ts",
  "worker/conversions/activecampaign.mjs",
  "worker/conversions/identity.ts",
  "worker/conversions/publication.ts",
  "worker/conversions/shared.mjs",
  "worker/conversions/stripe.mjs",
  "worker/fivetran/activecampaign-fivetran.ts",
  "worker/fivetran/bootstrap-publisher.ts",
  "worker/fivetran/bulk-bootstrap.ts",
  "worker/fivetran/contracts.ts",
  "worker/fivetran/json.ts",
  "worker/fivetran/raw-collapse.ts",
  "worker/fivetran/stripe-fivetran.ts",
  "worker/fivetran/timestamp.ts",
  "worker/fivetran/tinybird-reader.ts",
  "worker/identity/engine.ts",
  "worker/identity/state.ts",
  "worker/identity/storage.ts",
  "worker/sessions/md5.ts",
  "worker/sessions/page-revisions.ts",
  "worker/sessions/session-engine.ts",
  "worker/sessions/session-publication.ts",
  "worker/sessions/tinybird-storage.ts",
  "worker/storage/json.ts",
  "worker/storage/tinybird.ts"
]);

/** Create a fresh, allowlisted build context. No network, build, or deployment. */
export async function packageBootstrap(configPath, outputPath, sourceRoot = DEFAULT_ROOT, proofPath = join(sourceRoot, 'restore/native-import-verification.json')) {
  const sourceConfigBytes = await regularFile(configPath, 2_000_000);
  const config = JSON.parse(sourceConfigBytes.toString('utf8'));
  validateMetadataConfig(config);
  const proofBytes = await regularFile(proofPath, 2_000_000);
  const proof = validateNativeImportProof(JSON.parse(proofBytes.toString('utf8')));
  const proofSha256 = sha256(proofBytes);
  if (config.nativeImportProofSha256 !== undefined && config.nativeImportProofSha256 !== proofSha256) {
    throw new Error('Frozen configuration refers to a different native import proof');
  }
  for (const input of [...config.inputs.map(input => ({ table: input.landingTable, expectedPhysicalRows: input.expectedPhysicalRows })), ...(config.live ? [config.live] : [])]) {
    if (proof.tables.find(table => table.table === input.table)?.expectedRows !== input.expectedPhysicalRows) {
      throw new Error('Native import proof disagrees with the frozen browser input count');
    }
  }
  const configBytes = Buffer.from(JSON.stringify({ ...config, nativeImportProofSha256: proofSha256 }, null, 2) + '\n');
  const files = new Map();
  for (const name of RUNTIME_FILES) files.set(name, await regularFile(join(sourceRoot, name), 2_000_000));
  files.set('Dockerfile', await regularFile(join(sourceRoot, 'Dockerfile.bootstrap-all'), 16_000));
  files.set('package.json', Buffer.from('{"name":"boom-bootstrap-runner","private":true,"type":"module"}\n'));
  files.set('config/bootstrap.json', configBytes);
  files.set('config/native-import-verification.json', proofBytes);
  files.set('.dockerignore', Buffer.from('**\n!Dockerfile\n!package.json\n!worker/\n!worker/**\n!scripts/\n!scripts/**\n!config/\n!config/bootstrap.json\n!config/native-import-verification.json\n!build-manifest.json\n'));
  const manifest = {
    snapshotAt: SNAPSHOT_AT,
    entrypoint: 'scripts/bootstrap-all.ts',
    runtimeFiles: RUNTIME_FILES.length,
    sourceConfigSha256: sha256(sourceConfigBytes),
    files: [...files].map(([path, bytes]) => ({ path, bytes: bytes.length, sha256: sha256(bytes) })),
  };
  files.set('build-manifest.json', Buffer.from(JSON.stringify(manifest, null, 2) + '\n'));

  const output = resolve(outputPath);
  // A fresh directory prevents stale credentials or backups entering the build context.
  await mkdir(output, { mode: 0o700 });
  for (const [name, bytes] of files) {
    await mkdir(dirname(join(output, name)), { recursive: true, mode: 0o700 });
    await writeFile(join(output, name), bytes, { flag: 'wx', mode: 0o600 });
  }
  return { output, fileCount: files.size, sourceFiles: RUNTIME_FILES.length,
    bytes: [...files.values()].reduce((sum, bytes) => sum + bytes.length, 0),
    configSha256: sha256(configBytes), nativeImportProofSha256: proofSha256 };
}

/** Every accepted value is configuration metadata, never a raw event or credential. */
export function validateMetadataConfig(config) {
  objectKeys(config, ['baselineId', 'tenantId', 'algorithmVersion', 'workspaceId', 'workspaceName', 'startedAt',
    'region', 'inputs', 'inputManifestHash', 'sourceSeal', 'bulk', 'pageSize', 'batchSize', 'maxVisitorsPerChunk',
    'maxSessionRecordsPerChunk', 'maxIdentityFactsPerBatch', 'maxIdentityFactsPerComponent', 'maxIdentifiers'],
  ['live', 'conversionIdentity', 'nativeImportProofSha256']);
  validateProductionBrowserInputs(config);
  for (const key of ['baselineId', 'algorithmVersion', 'workspaceName']) text(config[key], /^[a-zA-Z0-9_:-]{1,160}$/);
  if (config.tenantId !== 'boom' || config.workspaceId !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af'
    || config.region !== 'us-east4' || config.startedAt !== SNAPSHOT_AT || config.bulk !== true) {
    throw new Error('Expected the frozen Boom bulk configuration at 2026-09-05T22:28:00Z in us-east4');
  }
  for (const key of ['inputManifestHash', 'sourceSeal']) text(config[key], /^[a-f0-9]{64}$/);
  if (config.nativeImportProofSha256 !== undefined) text(config.nativeImportProofSha256, /^[a-f0-9]{64}$/);
  for (const key of ['pageSize', 'batchSize', 'maxVisitorsPerChunk', 'maxSessionRecordsPerChunk',
    'maxIdentityFactsPerBatch', 'maxIdentityFactsPerComponent', 'maxIdentifiers']) integer(config[key], 1);
  if (!Array.isArray(config.inputs) || config.inputs.length !== 10) throw new Error('Expected all ten frozen browser sources');
  const tables = new Set();
  for (const input of config.inputs) {
    objectKeys(input, ['partition', 'landingTable', 'expectedPhysicalRows'], ['landingRecordIdColumn']);
    objectKeys(input.partition, ['table', 'source', 'kind', 'columns', 'inputManifestHash']);
    const partition = input.partition;
    const matched = /^raw_(boom_domains|jitsu_data)_(pages|identifies|form_submitted|order_completed|attr)$/.exec(partition.table);
    const kinds = { pages: 'page_view', identifies: 'identify', form_submitted: 'client_form', order_completed: 'client_order', attr: 'attribution' };
    if (!matched || tables.has(partition.table) || partition.source !== matched[1] || partition.kind !== kinds[matched[2]]
      || partition.inputManifestHash !== config.inputManifestHash) throw new Error('Frozen browser source metadata is inconsistent');
    tables.add(partition.table);
    if (!Array.isArray(partition.columns) || !partition.columns.length || partition.columns.length > 1000
      || new Set(partition.columns).size !== partition.columns.length) throw new Error('Invalid source column metadata');
    for (const column of partition.columns) text(column, /^[a-z_][a-z0-9_]{0,255}$/);
    text(input.landingTable, /^v1_history_[a-z0-9_]{1,100}$/);
    if (input.landingRecordIdColumn !== undefined) text(input.landingRecordIdColumn, /^[a-z_][a-z0-9_]{0,255}$/);
    integer(input.expectedPhysicalRows, 0);
  }
  if (config.live !== undefined) {
    objectKeys(config.live, ['table', 'cutoff', 'expectedPhysicalRows']);
    text(config.live.table, /^v1_history_[a-z0-9_]{1,100}$/);
    timestamp(config.live.cutoff);
    integer(config.live.expectedPhysicalRows, 0);
  }
  if (config.conversionIdentity !== undefined) {
    const snapshot = config.conversionIdentity;
    objectKeys(snapshot, ['snapshotId', 'snapshotAt', 'expectedDistinctFactCount', 'canonicalHash']);
    if (snapshot.snapshotId !== 'fivetran-20260905T222800Z' || snapshot.snapshotAt !== SNAPSHOT_AT) {
      throw new Error('Conversion identity metadata belongs to another snapshot');
    }
    integer(snapshot.expectedDistinctFactCount, 0);
    text(snapshot.canonicalHash, /^[a-f0-9]{64}$/);
  }
}

function objectKeys(value, required, optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected configuration metadata object');
  const accepted = new Set([...required, ...optional]);
  if (Object.keys(value).some(key => !accepted.has(key)) || required.some(key => !(key in value))) {
    throw new Error('Configuration has missing or unexpected fields; credentials and payloads cannot be packaged');
  }
}

function text(value, pattern) {
  if (typeof value !== 'string' || !pattern.test(value)) throw new Error('Invalid configuration metadata string');
}

function integer(value, minimum) {
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error('Invalid configuration count or limit');
}

function timestamp(value) {
  text(value, /^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:\.\d{1,6})?Z?$/);
  if (!Number.isFinite(Date.parse(value.replace(' ', 'T').replace(/Z?$/, 'Z')))) throw new Error('Invalid configuration timestamp');
}

async function regularFile(path, maximumBytes) {
  const info = await lstat(path);
  if (!info.isFile() || info.size > maximumBytes) throw new Error('Build inputs must be bounded regular files');
  return readFile(path);
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [configPath, outputPath, sourceRoot, proofPath] = process.argv.slice(2);
  if (!configPath || !outputPath) throw new Error('Usage: node --experimental-strip-types scripts/package-bootstrap.mjs <frozen-config.json> <new-context-directory> [source-root] [verified-import-proof.json]');
  process.stdout.write(JSON.stringify(await packageBootstrap(configPath, outputPath, sourceRoot, proofPath)) + '\n');
}
