import test from 'node:test';
import assert from 'node:assert/strict';
import { bootstrapFixture, TIME } from './helpers/bootstrap-fixture.ts';
import { attachConversionSnapshot } from './helpers/conversion-fixture.ts';
import { MemoryTinybird } from './helpers/memory-tinybird.ts';
import { TinybirdBootstrapStorage } from '../worker/bootstrap/tinybird-storage.ts';
import { CONVERSION_FACTS_TABLE, CONVERSION_MANIFESTS_TABLE, conversionPartition, restoreConversionIdentity } from '../worker/bootstrap/conversion-identity.ts';
import { executeBootstrap } from '../worker/bootstrap/executor.ts';
import { runIdentityEngine, type PendingIdentityFact } from '../worker/identity/engine.ts';
import { identityRecord } from '../worker/identity/storage.ts';
import { canonicalJson } from '../worker/sessions/session-engine.ts';
import { hashText } from '../worker/bootstrap/hash.ts';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

test('frozen conversions bridge browser profiles in the same complete version1 build and survive an interrupted copy', async () => {
  const { config, tables } = await bootstrapFixture('v1_facts_validation', 'conversion-bridge');
  config.bulk = true;
  const input = config.inputs.find(input => input.partition.table === 'raw_jitsu_data_identifies')!;
  tables[input.landingTable].push({ ...tables[input.landingTable][0], id: 'identify-future', anonymous_id: 'browser-future',
    user_id: 'second@example.invalid', email: 'second@example.invalid', phone: '+12024440199', timestamp: TIME, loaded_at: TIME });
  input.expectedPhysicalRows++;
  const conversions = await attachConversionSnapshot(config, tables);
  const memory = new MemoryTinybird(config, tables);
  const store = new TinybirdBootstrapStorage(memory.client, config, { bulk: true });
  const save = store.writeIdentityFacts.bind(store);
  let interrupted = false;
  store.writeIdentityFacts = async (phase, facts) => {
    await save(phase, facts);
    if (!interrupted && phase === 'selected' && facts.some(row => row.fact.factKind === 'stripe')) {
      interrupted = true;
      throw new Error('Interrupted after conversion fact verification');
    }
  };
  await assert.rejects(executeBootstrap(config, store), /Interrupted after conversion/);
  assert.equal(memory.rows.get('v1_identity_commits')?.length ?? 0, 0, 'full identity publication waits for the conversion receipt');
  const result = await executeBootstrap(config, store) as { counts: Record<string, number>; identityVersion: number };
  assert.equal(result.identityVersion, 1);
  assert.equal(result.counts.identityFacts, 13);
  assert.equal(result.counts.identityComponents, 4);
  assert.equal(result.counts.pageHeads, 5);
  const selected: PendingIdentityFact[] = [];
  for await (const row of store.identityFacts('selected')) selected.push(row.fact);
  assert.equal(selected.length, 13, 'retried copies retain one selected fact per kind/key');
  const run = (facts: typeof selected) => runIdentityEngine({ tenantId: config.tenantId, batchId: 'whole', batchVersion: 1,
    committedAt: config.startedAt, pendingFacts: facts, currentFacts: [], currentMappings: [], currentProfiles: [], checkpointIngestedAt: '', checkpointEventId: '' });
  const whole = await run(selected);
  const shape = (row: Record<string, unknown>) => canonicalJson({ kind: row.state_kind, key: row.state_key, payload: row.payload_json });
  const actual = [...new Set((memory.rows.get('v1_identity_records') ?? []).map(shape))].sort();
  assert.deepEqual(actual, whole.rows.map(identityRecord).map(shape).sort());
  const profile = (rows: ReturnType<typeof identityRecord>[], identifier: string) => rows.find(row => row.state_kind === 'mapping' && row.lookup_key === identifier)?.payload_json;
  const browserFacts = selected.filter(fact => !conversions.some(row => row.factKind === fact.factKind && row.factKey === fact.factKey));
  const browserOnly = (await run(browserFacts)).rows.map(identityRecord);
  const merged = whole.rows.map(identityRecord);
  const profileId = (value: string | undefined) => JSON.parse(value!).profileId;
  assert.notEqual(profileId(profile(browserOnly, 'anonymous_id:browser-b')), profileId(profile(browserOnly, 'anonymous_id:browser-future')));
  assert.equal(profileId(profile(merged, 'anonymous_id:browser-b')), profileId(profile(merged, 'anonymous_id:browser-future')));
  assert.ok(await store.getManifest('source', conversionPartition(config.conversionIdentity!)));
  assert.equal(memory.writes.filter(row => row.table.includes('inbox')).length, 0, 'bootstrap does not enqueue live identity');
  const writes = memory.writes.length;
  assert.deepEqual(await executeBootstrap(config, store), result);
  assert.equal(memory.writes.length, writes);
});

test('a short snapshot scan retries before copying, and absent referenced rows cannot be skipped', async () => {
  const { config, tables } = await bootstrapFixture();
  await attachConversionSnapshot(config, tables);
  const memory = new MemoryTinybird(config, tables);
  const store = new TinybirdBootstrapStorage(memory.client, config);
  memory.hideOnce = { table: CONVERSION_FACTS_TABLE, match: /ORDER BY fact_kind,fact_key/, predicate: row => row.fact_kind === 'stripe' };
  assert.equal(await restoreConversionIdentity(config.conversionIdentity!, store), 3);
  assert.ok(memory.queries.filter(query => query.includes(`FROM ${CONVERSION_FACTS_TABLE}`) && query.includes('ORDER BY fact_kind,fact_key')).length >= 2);
  const other = new TinybirdBootstrapStorage(memory.client, { ...config, baselineId: 'copy-with-missing-reference' });
  memory.hideOnce = { table: CONVERSION_FACTS_TABLE, match: /AND tuple\(fact_kind,fact_key\) IN/, predicate: row => row.fact_kind === 'activecampaign' };
  assert.equal(await restoreConversionIdentity(config.conversionIdentity!, other), 3);
  assert.ok(memory.queries.filter(query => query.includes(`FROM ${CONVERSION_FACTS_TABLE}`) && query.includes('AND tuple(fact_kind,fact_key) IN')).length >= 3);
});

test('wrong manifests and canonical content changes are rejected before selected conversion writes', async () => {
  for (const failure of ['manifest', 'payload', 'metadata', 'kind'] as const) {
    const { config, tables } = await bootstrapFixture();
    await attachConversionSnapshot(config, tables);
    if (failure === 'manifest') tables[CONVERSION_MANIFESTS_TABLE][0].canonical_hash = '0'.repeat(64);
    const row = tables[CONVERSION_FACTS_TABLE][0];
    if (failure === 'payload') {
      const fact = JSON.parse(String(row.canonical_fact_json));
      fact.eventId += ':changed';
      row.event_id = fact.eventId;
      row.canonical_fact_json = canonicalJson(fact);
      row.row_hash = await hashText(String(row.canonical_fact_json));
    }
    if (failure === 'metadata') row.source_priority = 0;
    if (failure === 'kind') {
      const fact = JSON.parse(String(row.canonical_fact_json));
      fact.factKind = 'segment_page_view';
      row.canonical_fact_json = canonicalJson(fact);
      row.row_hash = await hashText(String(row.canonical_fact_json));
    }
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    await assert.rejects(restoreConversionIdentity(config.conversionIdentity!, store), /Conversion/);
    assert.equal(memory.writes.length, 0);
  }
});

test('a sealed empty conversion snapshot is complete and remains distinct from a missing manifest', async () => {
  const { config, tables } = await bootstrapFixture();
  await attachConversionSnapshot(config, tables, []);
  tables[CONVERSION_MANIFESTS_TABLE].push({ ...tables[CONVERSION_MANIFESTS_TABLE][0], sealed_at: '2026-09-05T12:01:00.000000Z' });
  const memory = new MemoryTinybird(config, tables);
  const store = new TinybirdBootstrapStorage(memory.client, config);
  memory.hideOnce = { table: CONVERSION_MANIFESTS_TABLE, match: /SELECT DISTINCT/, predicate: () => true };
  assert.equal(await restoreConversionIdentity(config.conversionIdentity!, store), 0);
  assert.equal(config.conversionIdentity!.canonicalHash, await hashText(''));
  assert.equal(memory.writes.some(write => write.table === 'v1_bootstrap_identity_facts'), false);
  assert.ok(await store.getManifest('source', conversionPartition(config.conversionIdentity!)));
});

test('conflicting duplicate fact keys cannot pass the snapshot scan', async () => {
  const { config, tables } = await bootstrapFixture();
  await attachConversionSnapshot(config, tables);
  const original = tables[CONVERSION_FACTS_TABLE][0];
  const fact = JSON.parse(String(original.canonical_fact_json));
  fact.eventId += ':conflicting';
  const canonical = canonicalJson(fact);
  tables[CONVERSION_FACTS_TABLE].push({ ...original, event_id: fact.eventId, canonical_fact_json: canonical, row_hash: await hashText(canonical) });
  const memory = new MemoryTinybird(config, tables);
  await assert.rejects(restoreConversionIdentity(config.conversionIdentity!, new TinybirdBootstrapStorage(memory.client, config)), /Conflicting logical key/);
  assert.equal(memory.writes.length, 0);
});

test('configuration generation binds conversion snapshot metadata into the source seal', async () => {
  const { config, tables } = await bootstrapFixture();
  await attachConversionSnapshot(config, tables);
  const folder = await mkdtemp(join(tmpdir(), 'bootstrap-conversion-seal-'));
  try {
    const summary = { complete: true, sources: Object.fromEntries(config.inputs.map(input => [input.partition.table, {
      schemas: { synthetic: Object.fromEntries(input.partition.columns.map(column => [column, 'String'])) },
      all_objects_physical_rows: input.expectedPhysicalRows, unique_content_physical_rows: input.expectedPhysicalRows,
    }])) };
    const { inputs, sourceSeal: _sourceSeal, inputManifestHash: _inputHash, ...execution } = config;
    const settings = { ...execution, importMode: 'one-object-per-content',
      landingTables: Object.fromEntries(inputs.map(input => [input.partition.table, input.landingTable])),
      landingRecordIdColumns: Object.fromEntries(inputs.map(input => [input.partition.table, 'id'])) };
    await writeFile(join(folder, 'summary.json'), JSON.stringify(summary));
    await writeFile(join(folder, 'manifest.json'), '{}');
    const run = async () => {
      await writeFile(join(folder, 'settings.json'), JSON.stringify(settings));
      await promisify(execFile)(process.execPath, ['--experimental-strip-types', fileURLToPath(new URL('../scripts/bootstrap-config.ts', import.meta.url)),
        join(folder, 'summary.json'), join(folder, 'manifest.json'), join(folder, 'settings.json'), join(folder, 'config.json')]);
      return JSON.parse(await readFile(join(folder, 'config.json'), 'utf8'));
    };
    const first = await run();
    settings.conversionIdentity!.canonicalHash = '0'.repeat(64);
    const second = await run();
    assert.notEqual(first.sourceSeal, second.sourceSeal);
    assert.equal(first.inputManifestHash, second.inputManifestHash);
    assert.equal(second.conversionIdentity.canonicalHash, settings.conversionIdentity!.canonicalHash);
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
