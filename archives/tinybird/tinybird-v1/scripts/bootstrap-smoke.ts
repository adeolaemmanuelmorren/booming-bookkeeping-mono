import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { bootstrapFixture, smokeSchemas, SMOKE_BRANCH_ID } from '../test/helpers/bootstrap-fixture.ts';
import { Tinybird, sqlString } from '../worker/storage/tinybird.ts';
import { TinybirdBootstrapStorage } from '../worker/bootstrap/tinybird-storage.ts';
import { BootstrapBaselineReader } from '../worker/bootstrap/baseline.ts';
import { executeBootstrap } from '../worker/bootstrap/executor.ts';
import { attachConversionSnapshot } from '../test/helpers/conversion-fixture.ts';
import { CONVERSION_FACTS_TABLE, CONVERSION_MANIFESTS_TABLE } from '../worker/bootstrap/conversion-identity.ts';
const mode = process.argv[2];
const { config, tables } = await bootstrapFixture(process.env.SMOKE_WORKSPACE_NAME ?? 'v1_facts_validation', process.env.SMOKE_BASELINE_ID ?? 'bootstrap-transport-smoke-v2');
if (process.env.SMOKE_BULK === '1') {
    config.bulk = true;
    config.batchSize = 5000;
    config.maxVisitorsPerChunk = 5000;
    config.maxSessionRecordsPerChunk = 10000;
    config.maxIdentityFactsPerBatch = 10000;
    for (const input of config.inputs) input.landingRecordIdColumn = 'id';
}
if (process.env.SMOKE_CONVERSIONS === '1') await attachConversionSnapshot(config, tables);
if (mode === '--prepare') {
    const target = resolve(process.argv[3] ?? './bootstrap-smoke');
    await mkdir(`${target}/datasources`, { recursive: true });
    for (const [name, schema] of Object.entries(smokeSchemas()))
        await writeFile(`${target}/datasources/${name}.datasource`, schema);
    if (config.conversionIdentity) {
        for (const name of [CONVERSION_FACTS_TABLE, CONVERSION_MANIFESTS_TABLE]) {
            const schema = await readFile(new URL(`../datasources/${name}.datasource`, import.meta.url), 'utf8');
            await writeFile(`${target}/datasources/${name}.datasource`, schema);
        }
    }
    await writeFile(`${target}/config.json`, JSON.stringify(config, null, 2) + '\n');
    process.stdout.write(JSON.stringify({ prepared: true, target, syntheticTables: Object.keys(tables).length }) + '\n');
}
else if (mode === '--run') {
    const host = process.env.TINYBIRD_URL;
    const token = process.env.TINYBIRD_TOKEN;
    if (host !== 'https://api.us-east.tinybird.co' || !token)
        throw new Error('Explicit validation branch credentials are required');
    const response = await fetch(new URL('/v1/workspace', host), { headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(20000), redirect: 'error' });
    if (!response.ok)
        throw new Error('Cannot verify smoke workspace');
    const workspace = await response.json() as {
        id?: string;
        name?: string;
    };
    if (workspace.id !== SMOKE_BRANCH_ID || workspace.name !== config.workspaceName)
        throw new Error('Smoke is restricted to the existing validation branch');
    let failOnce = true;
    const client = new Tinybird({ TINYBIRD_URL: host, TINYBIRD_TOKEN: token }, async (input, init) => {
        const result = await fetch(input, init);
        const url = new URL(String(input));
        if (failOnce && url.pathname === '/v0/events' && url.searchParams.get('name') === 'v1_session_commits' && result.ok) {
            failOnce = false;
            await result.body?.cancel();
            throw new Error('Synthetic ambiguous session commit acknowledgement');
        }
        return result;
    }, config.bulk ? { requestTimeoutMs: 60_000, appendMaxBytes: 8_000_000, appendConcurrency: 4 } : {});
    for (const [table, rows] of Object.entries(tables)) {
        const isConversion = table === CONVERSION_FACTS_TABLE || table === CONVERSION_MANIFESTS_TABLE;
        const scope = isConversion
            ? ` WHERE tenant_id = ${sqlString(config.tenantId)} AND snapshot_id = ${sqlString(config.conversionIdentity!.snapshotId)}`
            : table === 'v1_smoke_live_jitsu' ? ` WHERE tenant_id = ${sqlString(config.tenantId)}` : '';
        const count = table === CONVERSION_FACTS_TABLE ? 'uniqExact(tuple(fact_kind,fact_key,row_hash))'
            : table === CONVERSION_MANIFESTS_TABLE ? 'uniqExact(tuple(snapshot_at,expected_distinct_fact_count,canonical_hash))' : 'count()';
        const [current] = await client.query<{
            rows: number;
        }>(`SELECT ${count} AS rows FROM ${table}${scope}`);
        if (Number(current.rows) === 0) {
            await client.append(table, rows);
            continue;
        }
        if (Number(current.rows) !== rows.length)
            throw new Error('Synthetic landing table has unexpected rows');
    }
    const store = new TinybirdBootstrapStorage(client, config, { bulk: config.bulk });
    try {
        await executeBootstrap(config, store);
    }
    catch (error) {
        if (!(error instanceof Error) || error.message !== 'Synthetic ambiguous session commit acknowledgement')
            throw error;
    }
    const result = await executeBootstrap(config, store) as {
        counts: {
            pageHeads: number;
            visitors: number;
            identityFacts: number;
            identityComponents: number;
        };
    };
    assert.equal(result.counts.identityFacts, config.conversionIdentity ? 12 : 9);
    assert.equal(result.counts.identityComponents, config.conversionIdentity ? 5 : 3);
    if (config.conversionIdentity) {
        const conversions = [];
        for await (const row of store.identityFacts('selected')) {
            if (['stripe', 'stripe_kajabi', 'activecampaign'].includes(row.fact.factKind)) conversions.push(row.fact);
        }
        assert.equal(conversions.length, 3);
        assert.equal(conversions.find(fact => fact.factKind === 'stripe')?.observedAt, null);
        assert.equal(conversions.find(fact => fact.factKind === 'stripe_kajabi')?.factDeleted, true);
    }
    const baseline = new BootstrapBaselineReader(config.tenantId, config.sourceSeal, store.baselineStore());
    const selected = await baseline.loadSourceHeads([{ event_kind: 'page_view', source_record_id: 'p1' }]);
    assert.equal(selected[0].heads.length, 3);
    assert.deepEqual(selected[0].heads.map(row => row.source.source_priority).sort(), [1, 2, 3]);
    const a = await baseline.loadVisitor(config.tenantId, 'browser-a');
    const b = await baseline.loadVisitor(config.tenantId, 'browser-b');
    const missingTime = await baseline.loadVisitor(config.tenantId, 'browser-null');
    assert.equal(a?.heads[0].page_view_id, 'p1');
    assert.equal(b?.heads.length, 2);
    assert.equal(b?.snapshot.sessions.length, 2);
    assert.equal(missingTime?.heads.length, 1);
    assert.equal(missingTime?.snapshot.sessions.length, 0);
    const [sessions] = await client.query<{
        sessions: number;
    }>(`SELECT uniqExact(session_id) AS sessions FROM v1_session_records WHERE tenant_id = ${sqlString(config.tenantId)}`);
    assert.equal(Number(sessions.sessions), 4);
    assert.deepEqual(await executeBootstrap(config, store), result);
    process.stdout.write(JSON.stringify({ sealed: true, restartVerified: !failOnce, result }) + '\n');
}
else {
    throw new Error('Use --prepare <directory> for local schema files, or --run for the fixed validation branch');
}
