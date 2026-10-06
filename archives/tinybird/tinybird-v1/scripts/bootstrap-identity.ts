import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { hash } from '../worker/bootstrap/hash.ts';
import { Tinybird, type JsonRecord } from '../worker/storage/tinybird.ts';
import { IdentityBootstrapStore } from '../worker/identity/bootstrap-store.ts';
import { initializeIdentity, type IdentityBootstrapPlan } from '../worker/identity/bootstrap-runner.ts';
import { validateNativeImportProof, verifyCurrentImportCounts, type NativeImportProof } from './bootstrap-import-proof.ts';
import { IDENTITY_WORKSPACE, validateProductionIdentityPlan } from '../worker/identity/bootstrap-plan.ts';

/** One supervised process. Tokens come only from the job's secret environment. */
export async function bootstrapIdentity(planPath: string, proofPath: string, manifestPath: string): Promise<void> {
  const files = await Promise.all([readFile(planPath), readFile(proofPath), readFile(manifestPath)]);
  const { plan, proof } = await loadIdentityBootstrapFiles(...files);
  const url = process.env.TINYBIRD_URL;
  const token = process.env.TINYBIRD_TOKEN;
  if (url !== 'https://api.us-east.tinybird.co' || !token) throw new Error('The identity job needs its US East Tinybird environment');
  const response = await fetch(`${url}/v1/workspace`, { headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(60_000), redirect: 'manual' });
  if (!response.ok) throw new Error('Cannot verify identity destination workspace');
  const workspace = await response.json() as { id?: string; name?: string };
  if (workspace.id !== IDENTITY_WORKSPACE.id || workspace.name !== IDENTITY_WORKSPACE.name) throw new Error('Identity destination workspace differs from the frozen plan');
  const client = new MeasuredBootstrapClient({ TINYBIRD_URL: url, TINYBIRD_TOKEN: token }, fetch,
    { requestTimeoutMs: 60_000, appendMaxBytes: 8_000_000, appendConcurrency: 2 });
  await verifyCurrentImportCounts(client, proof);
  const store = new IdentityBootstrapStore(client, plan.tenantId, plan.baselineId);
  const seal = await initializeIdentity(plan, store, (stage, counts) => console.log(JSON.stringify({
    stage, ...counts, io: client.measurements,
    rssMiB: Math.round(process.memoryUsage().rss / 1_048_576),
    cpuSeconds: Math.round((process.cpuUsage().user + process.cpuUsage().system) / 1_000_000),
  })));
  console.log(JSON.stringify({ stage: 'complete', sealHash: await hash(seal), sourceSeal: seal.sourceSeal, counts: seal.counts }));
}

/** Aggregate timings only. Source SQL, keys and payloads never enter the logs. */
class MeasuredBootstrapClient extends Tinybird {
  readonly measurements = { reads: 0, readMs: 0, maxReadMs: 0, factReads: 0, factReadMs: 0,
    sourceReads: 0, sourceReadMs: 0, writes: 0, writeMs: 0, attemptedRows: 0 };

  async query<T>(sql: string): Promise<T[]> {
    const started = performance.now();
    try { return await super.query<T>(sql); }
    finally {
      const elapsed = Math.round(performance.now() - started);
      this.measurements.reads++;
      this.measurements.readMs += elapsed;
      this.measurements.maxReadMs = Math.max(this.measurements.maxReadMs, elapsed);
      if (sql.includes('FROM v1_identity_bootstrap_facts')) {
        this.measurements.factReads++;
        this.measurements.factReadMs += elapsed;
      }
      if (/FROM v1_(history|snapshot)_/.test(sql)) {
        this.measurements.sourceReads++;
        this.measurements.sourceReadMs += elapsed;
      }
    }
  }

  async append(table: string, rows: readonly JsonRecord[]): Promise<void> {
    const started = performance.now();
    try { await super.append(table, rows); }
    finally {
      this.measurements.writes++;
      this.measurements.writeMs += Math.round(performance.now() - started);
      this.measurements.attemptedRows += rows.length;
    }
  }
}

/** Validate frozen proof bytes before any destination lookup or mutation. */
export async function loadIdentityBootstrapFiles(planBytes: Uint8Array, proofBytes: Uint8Array, manifestBytes: Uint8Array): Promise<{ plan: IdentityBootstrapPlan; proof: NativeImportProof }> {
  const plan = JSON.parse(Buffer.from(planBytes).toString('utf8')) as IdentityBootstrapPlan;
  const manifest = JSON.parse(Buffer.from(manifestBytes).toString('utf8')) as { snapshotAt: string; files: { path: string; bytes: number; sha256: string }[] };
  await validateProductionIdentityPlan(plan);
  const digest = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex');
  if (plan.nativeImportProofSha256 !== digest(proofBytes)) throw new Error('Identity native import proof differs from its pinned digest');
  if (Date.parse(manifest.snapshotAt) !== Date.parse(plan.startedAt) || !Array.isArray(manifest.files)) throw new Error('Identity build manifest is invalid');
  for (const [path, bytes] of [['config/identity-plan.json',planBytes],['config/native-import-verification.json',proofBytes]] as const) {
    const entries = manifest.files.filter(file => file.path === path);
    if (entries.length !== 1 || entries[0].bytes !== bytes.byteLength || entries[0].sha256 !== digest(bytes)) throw new Error('Identity input bytes differ from the build manifest');
  }
  const proof = validateNativeImportProof(JSON.parse(Buffer.from(proofBytes).toString('utf8')));
  const expected = [...plan.inputs.browser,plan.inputs.live,...plan.inputs.fivetran.tables];
  for (const input of expected) {
    if (proof.tables.find(table => table.table === input.table)?.expectedRows !== input.expectedPhysicalRows) throw new Error('Native proof differs from an identity input count');
  }
  return { plan, proof };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (process.argv.length !== 5) throw new Error('Usage: bootstrap-identity.ts <identity-plan.json> <native-import-verification.json> <build-manifest.json>');
  await bootstrapIdentity(process.argv[2], process.argv[3], process.argv[4]);
}
