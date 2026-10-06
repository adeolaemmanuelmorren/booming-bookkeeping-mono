import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { EXPECTED_IMPORT_TABLES, loadNativeImportProof, validateNativeImportProof, verifyCurrentImportCounts } from '../scripts/bootstrap-import-proof.ts';
import { packageBootstrap, validateMetadataConfig } from '../scripts/package-bootstrap.mjs';
import { bootstrapFixture } from './helpers/bootstrap-fixture.ts';
import { applyNativeImportFixtureCounts, nativeImportProof } from './helpers/import-proof-fixture.ts';

const ROOT = fileURLToPath(new URL('../', import.meta.url));

test('proof validation requires the complete fixed table set, native receipts, and preserved supplemental row', () => {
  const proof = nativeImportProof();
  assert.equal(validateNativeImportProof(proof).tables.length, 44);
  const mutations: ((value: any) => void)[] = [
    value => { value.verified = false; },
    value => { value.snapshotAt = '2026-09-05T22:29:00Z'; },
    value => { value.tables.pop(); },
    value => { value.tables[0] = value.tables[1]; },
    value => { value.tables[0].table = 'v1_unexpected'; },
    value => { value.tables[0].verified = false; },
    value => { value.tables[0].rows--; },
    value => { value.tables[0].acceptedRows--; },
    value => { value.tables[0].quarantineRows = 1; },
    value => { value.tables[0].successfulFiles = 0; },
    value => { value.tables[0].missingFiles = ['synthetic.parquet']; },
    value => { value.tables[0].problems = ['synthetic import failure']; },
    value => { value.tables[0].expectedRows = -1; },
    value => { value.tables[0].expectedNativeRows = 0; },
    value => { value.tables[0].supplementalRows = 1; },
    value => { value.tables.find((row: any) => row.supplementalReceipt).supplementalReceipt.sha256 = '0'.repeat(64); },
    value => { delete value.tables.find((row: any) => row.supplementalReceipt).supplementalReceipt; },
    value => {
      const table = value.tables.find((row: any) => row.supplementalReceipt);
      table.supplementalRows = 0; table.expectedRows--; table.rows--; delete table.supplementalReceipt;
    },
    value => { value.credentials = 'synthetic-secret'; },
  ];
  for (const mutate of mutations) {
    const invalid = structuredClone(proof);
    mutate(invalid);
    assert.throws(() => validateNativeImportProof(invalid), /[Ii]mport|supplemental/);
  }
});

test('current count check requires all 44 exact physical counts and cannot accept duplicate or omitted tables', async () => {
  const proof = nativeImportProof();
  const correct = proof.tables.map(table => ({ table: table.table, rows: String(table.expectedRows) }));
  const statements: string[] = [];
  const client = (rows: typeof correct) => ({ query: async <Row extends Record<string, unknown>>(sql: string) => {
    statements.push(sql);
    return structuredClone(rows) as unknown as Row[];
  } });
  await verifyCurrentImportCounts(client(correct), proof);
  assert.equal(statements[0].split('UNION ALL').length, 44);
  for (const table of EXPECTED_IMPORT_TABLES) assert.ok(statements[0].includes(`FROM ${table}`));
  const missing = correct.slice(1);
  const duplicate = correct.map((row, index) => index ? row : correct[1]);
  const wrong = correct.map((row, index) => index ? row : { ...row, rows: String(Number(row.rows) + 1) });
  for (const rows of [missing, duplicate, wrong]) await assert.rejects(verifyCurrentImportCounts(client(rows), proof), /[Ii]mport/);
});

test('internally consistent reduced conversion rows or selected file counts cannot redefine the frozen inputs', () => {
  for (const name of ['v1_snapshot_stripe_charge', 'v1_snapshot_activecampaign_contact_tag', 'v1_history_stripe_customer']) {
    const proof = nativeImportProof();
    const table = proof.tables.find(table => table.table === name)!;
    table.expectedNativeRows--;
    table.acceptedRows--;
    table.expectedRows--;
    table.rows--;
    assert.equal(table.expectedRows, table.expectedNativeRows + table.supplementalRows);
    assert.equal(table.acceptedRows, table.expectedNativeRows);
    assert.equal(table.rows, table.expectedRows);
    assert.throws(() => validateNativeImportProof(proof), /frozen source manifests/);
  }
  const proof = nativeImportProof();
  const table = proof.tables.find(table => table.table === 'v1_snapshot_stripe_charge')!;
  table.expectedFiles--;
  table.successfulFiles--;
  assert.equal(table.expectedFiles, table.successfulFiles);
  assert.throws(() => validateNativeImportProof(proof), /frozen source manifests/);
});

test('packaging rejects a partial proof; packaged config and proof bytes both require their recorded hashes', async () => {
  const folder = await mkdtemp(join(tmpdir(), 'bootstrap-proof-hashes-'));
  try {
    const { configPath, proofPath } = await syntheticInputs(folder);
    const originalConfig = await readFile(configPath, 'utf8');
    const originalProof = await readFile(proofPath);
    const partial = JSON.parse(Buffer.from(originalProof).toString('utf8'));
    partial.verified = false;
    await writeFile(proofPath, JSON.stringify(partial));
    await assert.rejects(packageBootstrap(configPath, join(folder, 'partial'), ROOT, proofPath), /incomplete/);
    await writeFile(proofPath, originalProof);
    const output = join(folder, 'context');
    await packageBootstrap(configPath, output, ROOT, proofPath);
    const proofLocation = join(output, 'config/native-import-verification.json');
    const configBytes = await readFile(join(output, 'config/bootstrap.json'));
    const manifestPath = join(output, 'build-manifest.json');
    const load = () => loadNativeImportProof({ configBytes, proofPath: proofLocation, buildManifestPath: manifestPath });
    assert.equal((await load()).proof.tables.length, 44);
    await writeFile(proofLocation, Buffer.concat([originalProof, Buffer.from('\n')]));
    await assert.rejects(load(), /configuration hash/);
    await writeFile(proofLocation, originalProof);
    const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
    manifest.files.find((file: any) => file.path === 'config/bootstrap.json').sha256 = '0'.repeat(64);
    await writeFile(manifestPath, JSON.stringify(manifest));
    await assert.rejects(load(), /build manifest/);
    assert.equal(await readFile(configPath, 'utf8'), originalConfig, 'source configuration was not rewritten');
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});

test('production packaging refuses omitted live data or a browser input redirected to another restored table', async () => {
  const folder = await mkdtemp(join(tmpdir(), 'bootstrap-browser-input-pins-'));
  try {
    const { config, configPath, proofPath } = await syntheticInputs(folder);
    for (const failure of ['missing-live', 'wrong-landing', 'wrong-live-cutoff', 'wrong-source'] as const) {
      const invalid = structuredClone(config);
      if (failure === 'missing-live') delete invalid.live;
      if (failure === 'wrong-live-cutoff') invalid.live!.cutoff = '2026-09-05 20:24:51.606074';
      if (failure === 'wrong-source') invalid.inputs[0].partition.source = 'jitsu_data';
      if (failure === 'wrong-landing') {
        const input = invalid.inputs.find(input => input.partition.table === 'raw_jitsu_data_pages')!;
        input.landingTable = 'v1_history_boom_domains_pages';
        input.expectedPhysicalRows = 854367;
      }
      assert.throws(() => validateMetadataConfig(invalid), /Production.*(input|mapping)/);
      await writeFile(configPath, JSON.stringify(invalid));
      await assert.rejects(packageBootstrap(configPath, join(folder, failure), ROOT, proofPath), /Production.*(input|mapping)/);
    }
    await writeFile(configPath, JSON.stringify(config));
    const output = join(folder, 'valid-context');
    await packageBootstrap(configPath, output, ROOT, proofPath);
    const packaged = JSON.parse(await readFile(join(output, 'config/bootstrap.json'), 'utf8'));
    delete packaged.live;
    const alteredBytes = Buffer.from(JSON.stringify(packaged));
    const manifestPath = join(output, 'build-manifest.json');
    const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
    const configRecord = manifest.files.find((file: any) => file.path === 'config/bootstrap.json');
    configRecord.sha256 = createHash('sha256').update(alteredBytes).digest('hex');
    configRecord.bytes = alteredBytes.length;
    await writeFile(manifestPath, JSON.stringify(manifest));
    await assert.rejects(loadNativeImportProof({ configBytes: alteredBytes,
      proofPath: join(output, 'config/native-import-verification.json'), buildManifestPath: manifestPath }), /requires the frozen live Jitsu input/);
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});

test('actual combined entrypoint checks current counts before any coordinator request', async () => {
  const folder = await mkdtemp(join(tmpdir(), 'bootstrap-proof-entrypoint-'));
  try {
    const { configPath, proofPath, config } = await syntheticInputs(folder);
    const output = join(folder, 'context');
    await packageBootstrap(configPath, output, ROOT, proofPath);
    const proof = nativeImportProof();
    const callsPath = join(folder, 'synthetic-calls.json');
    const preloadPath = join(folder, 'mock-provider.mjs');
    // Every fetch is intercepted. The test process cannot contact any provider.
    await writeFile(preloadPath, `
      import { writeFileSync } from 'node:fs';
      const calls=[];
      process.on('exit',()=>writeFileSync(${JSON.stringify(callsPath)},JSON.stringify(calls)));
      globalThis.fetch=async(input,init)=>{
        const url=new URL(String(input));
        calls.push(url.pathname);
        if(url.pathname==='/v1/workspace') return Response.json(${JSON.stringify({ id: config.workspaceId, name: config.workspaceName })});
        if(url.pathname==='/v0/sql') {
          const data=${JSON.stringify(proof.tables.map(table => ({ table: table.table, rows: String(table.expectedRows) })))};
          if(process.env.SYNTHETIC_MISSING_IMPORT==='1') data.pop();
          return Response.json({data});
        }
        if(url.pathname==='/admin/fivetran/stripe_main/initialize') return new Response('synthetic-stop-after-gate',{status:503});
        throw new Error('Unexpected request in isolated gate test');
      };
    `);
    for (const missing of ['1', '0']) {
      let failed: { stdout: string; stderr: string } | undefined;
      try {
        await promisify(execFile)(process.execPath, ['--experimental-strip-types', '--max-old-space-size=4096', '--import', preloadPath,
          join(output, 'scripts/bootstrap-all.ts')], { env: { BOOTSTRAP_CONFIG_PATH: join(output, 'config/bootstrap.json'),
          BOOTSTRAP_SINGLE_TASK: '1', TINYBIRD_TOKEN: 'synthetic-token', V1_ADMIN_TOKEN: 'synthetic-token', SYNTHETIC_MISSING_IMPORT: missing } });
      } catch (error) {
        failed = error as { stdout: string; stderr: string };
      }
      assert.ok(failed, 'the test always stops before real bootstrap work');
      const calls = JSON.parse(await readFile(callsPath, 'utf8'));
      if (missing === '1') {
        assert.deepEqual(calls, ['/v1/workspace', '/v0/sql']);
        assert.match(failed.stderr, /omitted an expected table/);
      } else {
        assert.deepEqual(calls, ['/v1/workspace', '/v0/sql', '/admin/fivetran/stripe_main/initialize']);
        assert.match(failed.stdout, /native-imports-verified/);
      }
    }
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});

async function syntheticInputs(folder: string) {
  const { config } = await bootstrapFixture('booming_bookkeeping', 'boom-browser-20260905-v1');
  config.tenantId = 'boom';
  config.workspaceId = '00c04079-d0b4-4d8b-8de6-6fa8072b85af';
  config.startedAt = '2026-09-05T22:28:00.000000Z';
  config.bulk = true;
  for (const input of config.inputs) input.landingTable = input.landingTable.replace('v1_smoke_raw_', 'v1_history_');
  config.live!.table = 'v1_history_live_jitsu';
  applyNativeImportFixtureCounts(config);
  const configPath = join(folder, 'config.json');
  const proofPath = join(folder, 'proof.json');
  await writeFile(configPath, JSON.stringify(config));
  await writeFile(proofPath, JSON.stringify(nativeImportProof()));
  return { configPath, proofPath, config };
}
