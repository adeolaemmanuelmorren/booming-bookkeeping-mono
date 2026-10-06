import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { bootstrapFixture } from './helpers/bootstrap-fixture.ts';
import { MemoryTinybird } from './helpers/memory-tinybird.ts';
import { TinybirdBootstrapStorage } from '../worker/bootstrap/tinybird-storage.ts';
import { BootstrapBaselineReader } from '../worker/bootstrap/baseline.ts';
import { executeBootstrap } from '../worker/bootstrap/executor.ts';
import { hash } from '../worker/bootstrap/hash.ts';
import { membershipBucket } from '../worker/bootstrap/membership.ts';
import { runIdentityEngine } from '../worker/identity/engine.ts';
import { identityRecord } from '../worker/identity/storage.ts';
import { canonicalJson } from '../worker/sessions/session-engine.ts';
import { boundedInputCardinality } from '../worker/bootstrap/input-counts.ts';
import { Tinybird } from '../worker/storage/tinybird.ts';
async function collect<T>(input: AsyncIterable<T>): Promise<T[]> { const rows: T[] = []; for await (const row of input)
    rows.push(row); return rows; }
test('wide cardinality counts disjoint ranges, deduplicates versions, and resumes a lost receipt', async () => {
    const { config, tables } = await bootstrapFixture();
    const input = config.inputs.find(input => input.partition.table === 'raw_jitsu_data_pages')!;
    tables[input.landingTable].push(structuredClone(tables[input.landingTable][0]));
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    memory.failAfterWrite = 'v1_bootstrap_checkpoints';
    const run = () => boundedInputCardinality(store, input.partition.table, input.landingTable, input.partition.columns, 7, 'id', 2);
    await assert.rejects(run(), /Synthetic lost acknowledgement/);
    assert.equal(await run(), 6);
    assert.equal((await store.checkpoint('input-cardinality', input.partition.table))?.sequence, 3);
    const counts = memory.queries.filter(query => query.includes('uniqExact(tuple(id,anonymous_id'));
    assert.ok(counts.every(query => /WHERE (?:id > .* AND )?id <=/.test(query)));
    assert.ok(counts.length >= 3);
});
test('bulk restore preserves restart semantics and verifies actual stored payload digests', async () => {
    const { config, tables } = await bootstrapFixture();
    config.bulk = true;
    for (const input of config.inputs) input.landingRecordIdColumn = 'id';
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config, { bulk: true });
    memory.failAfterWrite = 'v1_identity_commits';
    await assert.rejects(executeBootstrap(config, store), /Synthetic lost acknowledgement/);
    const final = await executeBootstrap(config, store) as { counts: Record<string, number> };
    assert.deepEqual(final.counts, { visitors: 4, pageHeads: 5, sessions: 4, identityFacts: 9, identityComponents: 3 });
    assert.ok(memory.queries.some(query => query.includes('hex(SHA256(`event_json`)) AS event_json')));
    assert.ok(memory.queries.some(query => query.includes('tuple(state_kind,lookup_key,state_key) IN')));
    const original = { ...memory.rows.get('v1_bootstrap_events')![0] };
    memory.rows.get('v1_bootstrap_events')![0].event_json = 'corrupted stored payload';
    await assert.rejects(store.writeVerified('v1_bootstrap_events', [original], `event_hash = '${original.event_hash}'`), /Conflicting immutable/);
    await assert.rejects(executeBootstrap({ ...config, bulk: false }, store), /transport mode/);
});
test('an entirely missing final cardinality range restarts before saving its total', async () => {
    const { config, tables } = await bootstrapFixture();
    const input = config.inputs.find(input => input.partition.table === 'raw_jitsu_data_pages')!;
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    memory.hideOnce = { table: input.landingTable, match: /SELECT DISTINCT id AS record_id.*WHERE id > 'p2'/s, predicate: row => row.id === 'p3' };
    assert.equal(await boundedInputCardinality(store, input.partition.table, input.landingTable, input.partition.columns, 6, 'id', 2), 6);
    const receipts = await store.checkpointPayloads<{ rows?: number; physical: number }>('input-cardinality', input.partition.table);
    assert.ok(receipts.some(receipt => receipt.physical === 0));
    assert.equal(receipts.at(-1)!.physical, 6);
});
test('a bulk group above 500 visitors publishes and resumes through the shared session/group validators', async () => {
    const { config, tables } = await bootstrapFixture('v1_facts_validation', 'large-bulk-fixture');
    config.bulk = true;
    config.pageSize = 250;
    config.batchSize = 1000;
    config.maxVisitorsPerChunk = 1000;
    config.maxSessionRecordsPerChunk = 10000;
    config.maxIdentityFactsPerBatch = 1000;
    config.maxIdentifiers = 5000;
    const input = config.inputs.find(input => input.partition.table === 'raw_jitsu_data_pages')!;
    const template = tables[input.landingTable][2];
    for (let index = 0; index < 600; index++) {
        const key = `bulk-${String(index).padStart(4, '0')}`;
        tables[input.landingTable].push({ ...template, id: key, anonymous_id: key });
    }
    config.inputManifestHash = await hash(tables);
    config.sourceSeal = await hash([config.baselineId, config.inputManifestHash]);
    for (const source of config.inputs) {
        source.partition.inputManifestHash = config.inputManifestHash;
        source.expectedPhysicalRows = tables[source.landingTable].length;
        source.landingRecordIdColumn = 'id';
    }
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config, { bulk: true });
    memory.failAfterWrite = 'v1_browser_group_commits';
    await assert.rejects(executeBootstrap(config, store), /Synthetic lost acknowledgement/);
    const final = await executeBootstrap(config, store) as { counts: Record<string, number> };
    assert.equal(final.counts.visitors, 604);
    assert.equal(final.counts.sessions, 604);
    assert.ok(memory.writes.some(write => write.table === 'v1_session_commits' && write.rows.length === 604));
    assert.ok(memory.writes.some(write => write.table === 'v1_browser_group_members' && write.rows.length === 604));
    const receipts = await store.checkpointPayloads('sources', input.partition.table);
    assert.equal(receipts.length, 1, 'three small physical reads share one verified source receipt');
    const baseline = new BootstrapBaselineReader(config.tenantId, config.sourceSeal, store.baselineStore());
    assert.equal((await baseline.loadVisitor(config.tenantId, 'bulk-0599'))?.snapshot.sessions.length, 1);
});
test('bulk transport bounds bytes and concurrent writes, then drains in-flight requests before failing', async () => {
    let active = 0;
    let peak = 0;
    let calls = 0;
    const client = new Tinybird({ TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: 'synthetic' }, async (_input, init) => {
        calls++;
        const call = calls;
        active++;
        peak = Math.max(peak, active);
        assert.ok(Buffer.byteLength(String(init?.body)) <= 80);
        await new Promise(resolve => setTimeout(resolve, 2));
        active--;
        if (call === 2) throw new Error('lost write acknowledgement');
        return Response.json({ successful_rows: String(init?.body).trim().split('\n').length, quarantined_rows: 0 });
    }, { appendMaxBytes: 80, appendConcurrency: 4, requestTimeoutMs: 60000 });
    await assert.rejects(client.append('v1_synthetic', Array.from({ length: 12 }, (_, id) => ({ id, text: 'a'.repeat(30) }))), /lost write acknowledgement/);
    assert.equal(peak, 4);
    assert.equal(calls, 4);
    assert.equal(active, 0);
});
test('real HTTP serialization, every bootstrap phase, ambiguous publication restart, sealed lazy reads, and whole-graph parity', async () => {
    const { config, tables } = await bootstrapFixture();
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    memory.failAfterWrite = 'v1_session_commits';
    await assert.rejects(executeBootstrap(config, store), /Synthetic lost acknowledgement/);
    assert.equal(await store.getManifest('seal', 'complete'), null);
    assert.ok(memory.rows.get('v1_session_commits')?.length);
    const result = await executeBootstrap(config, store) as {
        counts: {
            visitors: number;
            sessions: number;
        };
    };
    assert.equal(result.counts.visitors, 4);
    assert.equal(result.counts.sessions, 4);
    const writes = memory.writes.length;
    assert.deepEqual(await executeBootstrap(config, store), result);
    assert.equal(memory.writes.length, writes);
    const baseline = new BootstrapBaselineReader(config.tenantId, config.sourceSeal, store.baselineStore());
    const heads = await baseline.loadSourceHeads([{ event_kind: 'page_view', source_record_id: 'p1' }, { event_kind: 'identify', source_record_id: 'absent' }]);
    assert.deepEqual(heads.map(row => row.heads.length), [3, 0]);
    const live = await baseline.loadVisitor(config.tenantId, 'browser-a');
    assert.equal(live?.heads[0].source_priority, 3);
    assert.equal(live?.heads[0].page_view_id, 'p1');
    const later = await baseline.loadVisitor(config.tenantId, 'browser-b');
    assert.equal(later?.heads.length, 2);
    assert.equal(later?.snapshot.sessions.length, 2);
    const nullTime = await baseline.loadVisitor(config.tenantId, 'browser-null');
    assert.equal(nullTime?.heads.length, 1);
    assert.equal(nullTime?.snapshot.sessions.length, 0);
    assert.equal(await baseline.loadVisitor(config.tenantId, 'absent'), null);
    const future = await baseline.loadVisitor(config.tenantId, 'browser-future');
    assert.equal(future?.heads[0].page?.page_view_timestamp, '2117-01-01T00:00:00.000001Z');
    const facts = (await collect(store.identityFacts('selected'))).map(row => row.fact);
    const whole = await runIdentityEngine({ tenantId: config.tenantId, batchId: 'whole', batchVersion: 1, committedAt: config.startedAt,
        pendingFacts: facts, currentFacts: [], currentMappings: [], currentProfiles: [], checkpointIngestedAt: '', checkpointEventId: '' });
    const shape = (record: Record<string, unknown>) => canonicalJson({ state_kind: record.state_kind, state_key: record.state_key, payload_json: record.payload_json });
    const actual = [...new Set((memory.rows.get('v1_identity_records') ?? []).map(shape))].sort();
    assert.deepEqual(actual, whole.rows.map(identityRecord).map(shape).sort());
    const eventWrites = memory.writes.filter(write => write.table === 'v1_bootstrap_events').flatMap(write => write.rows);
    assert.ok(eventWrites.some(row => row.source_revision === 4638902400000002));
    assert.ok(eventWrites.every(row => typeof row.source_revision === 'number'));
    assert.ok(memory.writes.filter(write => write.table === 'v1_session_commits').some(write => write.rows.length > 1));
});
test('immutable output conflict prevents cursor advancement and restart requires exactly the same plan', async () => {
    const { config } = await bootstrapFixture();
    const memory = new MemoryTinybird(config);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    await store.putCheckpoint('probe', 'all', 9, { cursor: 'nine' });
    await store.putCheckpoint('probe', 'all', 10, { cursor: 'ten' });
    assert.equal((await store.checkpoint('probe', 'all'))?.sequence, 10);
    const writes = memory.writes.length;
    await assert.rejects(store.putCheckpoint('probe', 'all', 10, { cursor: 'different' }), /Conflicting immutable/);
    assert.equal(memory.writes.length, writes);
    await store.putManifest('plan', 'config', config);
    await assert.rejects(executeBootstrap({ ...config, pageSize: 3 }, store), /configuration changed/);
});
test('lost source, checkpoint, member, and identity acknowledgements resume without skipping output', async () => {
    for (const target of ['v1_bootstrap_events', 'v1_bootstrap_checkpoints', 'v1_bootstrap_members', 'v1_identity_commits']) {
        const { config, tables } = await bootstrapFixture();
        const memory = new MemoryTinybird(config, tables);
        const store = new TinybirdBootstrapStorage(memory.client, config);
        memory.failAfterWrite = target;
        await assert.rejects(executeBootstrap(config, store), /Synthetic lost acknowledgement/);
        assert.equal(await store.getManifest('seal', 'complete'), null);
        const final = await executeBootstrap(config, store) as {
            counts: Record<string, number>;
        };
        assert.deepEqual(final.counts, { visitors: 4, pageHeads: 5, sessions: 4, identityFacts: 9, identityComponents: 3 });
        const receipts = await store.checkpointPayloads('sessions', 'all');
        assert.equal(receipts.length, 3);
    }
});
test('physical original-ID pruning is checked before reading resumable raw pages', async () => {
    const { config, tables } = await bootstrapFixture();
    for (const input of config.inputs)
        input.landingRecordIdColumn = 'id';
    const memory = new MemoryTinybird(config, tables);
    await executeBootstrap(config, new TinybirdBootstrapStorage(memory.client, config));
    assert.ok(memory.queries.some(query => /WHERE id >= 'p1' AND id <= .* AND tuple\(__bootstrap_id/.test(query)));
    assert.ok(memory.queries.some(query => /SELECT DISTINCT id AS record_id.*ORDER BY id/s.test(query)));
    assert.ok(memory.queries.some(query => /WHERE id != ifNull\(id, ''\)/.test(query)));
});
test('same-source same-version payload disagreement preserves raw evidence and blocks the seal', async () => {
    const { config, tables } = await bootstrapFixture();
    const page = tables.v1_smoke_raw_jitsu_data_pages[1];
    tables.v1_smoke_raw_jitsu_data_pages.push({ ...page, url: 'https://example.invalid/conflict' });
    config.inputs.find(input => input.landingTable === 'v1_smoke_raw_jitsu_data_pages')!.expectedPhysicalRows++;
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    await assert.rejects(executeBootstrap(config, store), /Conflicting source content at the same revision/);
    assert.ok((memory.rows.get('v1_bootstrap_events')?.length ?? 0) > 0);
    assert.equal(await store.getManifest('seal', 'complete'), null);
});
test('scan cannot skip a conflicting duplicate logical key on a page boundary', async () => {
    const { config } = await bootstrapFixture();
    const memory = new MemoryTinybird(config);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    memory.rows.set('v1_test', [{ key: 'a', payload: 'one' }, { key: 'b', payload: 'two' }, { key: 'b', payload: 'conflict' }, { key: 'c', payload: 'three' }]);
    await assert.rejects(collect(store.scan('v1_test', ['key'], '1', ['payload'], 2)), /page boundary/);
});
test('numeric source cursor pagination preserves 9, 10, and 11 rather than lexical order', async () => {
    const { config } = await bootstrapFixture();
    const memory = new MemoryTinybird(config);
    memory.rows.set('v1_test', [{ source_revision: 9, event_hash: 'a' }, { source_revision: 10, event_hash: 'b' }, { source_revision: 11, event_hash: 'c' }]);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    const rows = await collect(store.scan<{
        source_revision: string;
    }>('v1_test', ['source_revision', 'event_hash'], '1', [], 1));
    assert.deepEqual(rows.map(row => row.source_revision), ['9', '10', '11']);
    assert.ok(memory.queries.some(query => query.includes("toUInt64('9')")));
});
test('a missing persisted source head waits and never returns absence', async () => {
    const { config, tables } = await bootstrapFixture();
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    await executeBootstrap(config, store);
    const original = memory.rows.get('v1_bootstrap_heads')!;
    memory.rows.set('v1_bootstrap_heads', original.filter(row => !(row.selection === 'source' && row.source_system === 'boom_domains' && row.source_record_id === 'p1')));
    const baseline = new BootstrapBaselineReader(config.tenantId, config.sourceSeal, store.baselineStore());
    let settled = false;
    const reading = baseline.loadSourceHeads([{ event_kind: 'page_view', source_record_id: 'p1' }]).then(result => { settled = true; return result; });
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal(settled, false);
    memory.rows.set('v1_bootstrap_heads', original);
    assert.equal((await reading)[0].heads.length, 3);
});
test('entirely absent proof records wait, authenticated negative lookups batch, and tampered membership cannot hide a key', async () => {
    const { config, tables } = await bootstrapFixture();
    const memory = new MemoryTinybird(config, tables);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    await executeBootstrap(config, store);
    const baseline = new BootstrapBaselineReader(config.tenantId, config.sourceSeal, store.baselineStore());
    const before = memory.queries.length;
    const absent = await baseline.loadSourceHeads(Array.from({ length: 200 }, (_, i) => ({ event_kind: 'page_view', source_record_id: `missing-${i}` })));
    assert.ok(absent.every(row => row.heads.length === 0));
    assert.ok(memory.queries.length - before < 10, '200 negative keys use bounded batches rather than per-key calls');
    const all = memory.rows.get('v1_bootstrap_manifests')!;
    memory.rows.set('v1_bootstrap_manifests', all.filter(row => row.manifest_kind !== 'membership-page'));
    let settled = false;
    const pending = baseline.loadVisitor(config.tenantId, 'browser-b').then(result => { settled = true; return result; });
    await new Promise(resolve => setTimeout(resolve, 30));
    assert.equal(settled, false);
    memory.rows.set('v1_bootstrap_manifests', all);
    assert.equal((await pending)?.heads.length, 2);
    const key = canonicalJson({ event_kind: 'page_view', source_record_id: 'p1' });
    const bucket = await membershipBucket(key);
    const leaf = all.find(row => row.manifest_kind === 'membership-bucket' && row.manifest_key === `source:${bucket}`)!;
    const payload = { keys: JSON.parse(String(leaf.payload_json)).keys.filter((item: string) => item !== key) };
    leaf.payload_json = canonicalJson(payload);
    leaf.payload_hash = await hash(payload);
    await assert.rejects(baseline.loadSourceHeads([{ event_kind: 'page_view', source_record_id: 'p1' }]), /incomplete or conflicts/);
});
test('short source, logical, visitor, and identity scans retry before publishing partial derived facts', async () => {
    const scenarios = [
        { table: 'v1_bootstrap_events', match: /source_system = 'jitsu_data'.*ORDER BY event_kind/, predicate: (row: Record<string, unknown>) => row.source_system === 'jitsu_data' && row.source_record_id === 'p1' && String(row.event_json).includes('raw-campaign') },
        { table: 'v1_bootstrap_heads', match: /selection = 'source'.*ORDER BY/, predicate: (row: Record<string, unknown>) => row.source_system === 'jitsu_events_api' },
        { table: 'v1_bootstrap_pages', match: /ORDER BY visitor_key,page_id/, predicate: (row: Record<string, unknown>) => row.page_id === 'p2' },
        { table: 'v1_bootstrap_identity_facts', match: /phase = 'selected'.*ORDER BY/, predicate: (row: Record<string, unknown>) => String(row.fact_key).endsWith(':form') },
    ];
    for (const scenario of scenarios) {
        const { config, tables } = await bootstrapFixture();
        const memory = new MemoryTinybird(config, tables);
        memory.hideOnce = scenario;
        const store = new TinybirdBootstrapStorage(memory.client, config);
        const final = await executeBootstrap(config, store) as {
            counts: Record<string, number>;
        };
        assert.equal(memory.hideOnce, null);
        assert.deepEqual(final.counts, { visitors: 4, pageHeads: 5, sessions: 4, identityFacts: 9, identityComponents: 3 });
        const p1 = memory.writes.filter(write => write.table === 'v1_bootstrap_pages').flatMap(write => write.rows).filter(row => row.page_id === 'p1');
        assert.ok(p1.every(row => JSON.parse(String(row.head_json)).source_priority === 3));
        // No normal stage page adds a full-table scalar count; only bounded key lookups do.
        for (const query of memory.queries.filter(query => query.includes('AS __coverage'))) {
            assert.match(query.split('AS __coverage')[0], / IN \(|visitor_key = /);
        }
    }
});
test('checkpoint payload hash corruption and a missing receipt cannot appear complete', async () => {
    const { config } = await bootstrapFixture();
    const memory = new MemoryTinybird(config);
    const store = new TinybirdBootstrapStorage(memory.client, config);
    await store.putCheckpoint('probe', 'all', 2, { value: 2 });
    await assert.rejects(store.checkpointPayloads('probe', 'all'), /gap/);
    const record = memory.rows.get('v1_bootstrap_checkpoints')![0];
    record.payload_json = canonicalJson({ value: 99 });
    assert.notEqual(await hash({ value: 99 }), record.payload_hash);
    await assert.rejects(store.checkpoint('probe', 'all'), /hash mismatch/);
});
test('all concrete bootstrap uploads match their declared datasource fields and numeric wire types', async () => {
    const { config, tables } = await bootstrapFixture();
    const memory = new MemoryTinybird(config, tables);
    await executeBootstrap(config, new TinybirdBootstrapStorage(memory.client, config));
    for (const write of memory.writes) {
        if (!write.table.startsWith('v1_bootstrap_'))
            continue;
        const schema = await readFile(new URL(`../datasources/${write.table}.datasource`, import.meta.url), 'utf8');
        const fields = [...schema.matchAll(/`(\w+)`\s+(\w+(?:\([^\n]*?\))?)/g)].map(match => [match[1], match[2]]);
        for (const row of write.rows) {
            assert.deepEqual(Object.keys(row).sort(), fields.map(([name]) => name).sort());
            for (const [name, type] of fields)
                if (/^UInt/.test(type))
                    assert.equal(typeof row[name], 'number');
        }
    }
});
