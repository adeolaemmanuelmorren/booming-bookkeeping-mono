import test from 'node:test';
import assert from 'node:assert/strict';
import { buildCompleteVisitorSeed, sessionSeedChunks, publishSessionSeedChunk, type CompleteVisitor, type BulkSessionPublisher, type SeedGroup } from '../worker/bootstrap/sessions.ts';
import { sourcePartition, sourcePageQuery, normalizeSourceRow, selectSourceHeads, buildSourceReplayPage, publishSourceReplayPage } from '../worker/bootstrap/replay.ts';
import { IdentityComponentIndex, buildIdentitySeedBatch } from '../worker/bootstrap/identity.ts';
import { sealBootstrap, type BootstrapPlan } from '../worker/bootstrap/manifest.ts';
import { hash } from '../worker/bootstrap/hash.ts';
import { BootstrapBaselineReader, type SealedBaselineStore } from '../worker/bootstrap/baseline.ts';
import { normalizeHistorical } from '../worker/browser/normalize.ts';
import { runIdentityEngine, type PendingIdentityFact } from '../worker/identity/engine.ts';
import { buildSessions, canonicalJson } from '../worker/sessions/session-engine.ts';
import type { PageRevision } from '../worker/sessions/page-revisions.ts';
import type { SnapshotManifest, SnapshotRecord } from '../worker/sessions/session-publication.ts';

const SEAL = 'a'.repeat(64);
const TIME = '2026-09-05T12:00:00.000000Z';
async function* stream<T>(rows: T[]) { for (const row of rows) yield row; }
async function collect<T>(rows: AsyncIterable<T>): Promise<T[]> { const result: T[] = []; for await (const row of rows) result.push(row); return result; }

function head(id: string, time: string | null, visitor = 'visitor'): PageRevision {
  return { page_view_id: id, source_priority: 2, source_revision: '10', page: {
    page_view_id: id, visitor_key: visitor, page_view_timestamp: time, utm_source: null,
  } };
}
async function visitor(key: string, heads: PageRevision[]): Promise<CompleteVisitor> {
  return { tenantId: 'boom', visitorKey: key, sourceSeal: SEAL, heads,
    expectedHeadCount: heads.length, expectedHeadHash: await hash([...heads].sort((a, b) => a.page_view_id.localeCompare(b.page_view_id))) };
}

test('bootstrap keeps microseconds, null-time page heads, and the exact live session semantics', async () => {
  const heads = [head('a', '2026-09-05T12:00:00.000001Z'), head('b', '2026-09-05T12:30:59.999999Z'), head('c', '2026-09-05T13:02:00.000000Z'), head('d', null)];
  const seed = await buildCompleteVisitorSeed(await visitor('visitor', heads), SEAL);
  const reference = buildSessions('visitor', heads.map(row => row.page!));
  assert.deepEqual(seed.snapshot.sessions, [...reference].sort((a, b) => a.session_id.localeCompare(b.session_id)));
  assert.equal(seed.snapshot.sessions.length, 2);
  assert.equal(seed.heads.length, 4);
  assert.equal(seed.snapshot.sessions.find(row => row.first_page_view_id === 'a')!.page_view_count, 2);
  assert.ok(seed.snapshot.sessions.every(row => row.utm_source === null));
});

test('incomplete visitor reads and conflicting selected page heads cannot seed sessions', async () => {
  const complete = await visitor('visitor', [head('a', TIME), head('b', TIME)]);
  await assert.rejects(buildCompleteVisitorSeed({ ...complete, heads: complete.heads.slice(0, 1) }, SEAL), /incomplete/);
  const repeated = await visitor('visitor', [head('a', TIME), head('a', TIME)]);
  await assert.rejects(buildCompleteVisitorSeed(repeated, SEAL), /one selected head/);
  await assert.rejects(buildCompleteVisitorSeed(complete, 'b'.repeat(64)), /another source seal/);
});

test('first live hydration verifies the whole baseline and retains tombstones without publication', async () => {
  const removed = { page_view_id: 'removed', source_priority: 3, source_revision: '20', page: null };
  const complete = await visitor('visitor', [head('a', TIME), removed]);
  const seed = await buildCompleteVisitorSeed(complete, SEAL);
  let actual = seed.records;
  const store: SealedBaselineStore = {
    async requireSeal(seal) { assert.equal(seal, SEAL); },
    async sourceHeads(keys) { return Promise.all(keys.map(async key => ({ key, heads: [], expectedCount: 0, expectedHash: await hash([]) }))); },
    async members() { return [{ visitor_key: 'visitor', revision: '1', publication_id: seed.snapshot.publication_id,
      records: seed.records.map(row => ({ session_id: row.session_id, payload_hash: row.payload_hash })) }]; },
    async visitorHeads() { return complete; },
    async sessionRecords() { return actual; },
  };
  const loader = new BootstrapBaselineReader('boom', SEAL, store);
  const result = await loader.loadVisitor('boom', 'visitor');
  assert.equal(result!.heads.length, 2);
  assert.equal(result!.heads[1].page, null);
  assert.equal(result!.snapshot.revision, '1');
  actual = [];
  await assert.rejects(loader.loadVisitor('boom', 'visitor'), /incomplete/);
  await assert.rejects(loader.loadVisitor('other', 'visitor'), /tenant mismatch/);
  store.sourceHeads = async () => [];
  await assert.rejects(loader.loadSourceHeads([{ event_kind: 'page_view', source_record_id: 'missing' }]), /omitted/);
});

test('bulk publication uses a constant number of operations for many visitors and retries safely', async () => {
  const inputs = await Promise.all(['a', 'b', 'c'].map(key => visitor(key, [head(key, TIME, key)])));
  const [chunk] = await collect(sessionSeedChunks(stream(inputs), { tenantId: 'boom', runId: 'run', sourceSeal: SEAL, startSequence: 1 }));
  const records: SnapshotRecord[] = [];
  const manifests: SnapshotManifest[] = [];
  let group: SeedGroup | null = null;
  const calls = { rows: 0, manifests: 0, group: 0 };
  const publisher: BulkSessionPublisher = {
    async appendRecords(rows) { calls.rows++; records.push(...rows, ...rows); },
    readRecords: () => stream(records),
    async appendManifests(rows) { calls.manifests++; manifests.push(...rows); },
    readManifests: () => stream(manifests),
    async appendGroup(value) { calls.group++; group = value; },
    async readGroup() { return group; },
  };
  await publishSessionSeedChunk(chunk, publisher);
  await publishSessionSeedChunk(chunk, publisher);
  assert.deepEqual(calls, { rows: 1, manifests: 1, group: 1 });
  assert.equal(chunk.group.members.length, 3);
  records[0] = { ...records[0], payload_json: '{}' };
  await assert.rejects(publishSessionSeedChunk(chunk, publisher), /conflicting bootstrap output/);
});

test('a missing bulk record prevents every manifest and group commit', async () => {
  const [chunk] = await collect(sessionSeedChunks(stream([await visitor('a', [head('a', TIME, 'a')])]), { tenantId: 'boom', runId: 'run', sourceSeal: SEAL, startSequence: 1 }));
  let commits = 0;
  await assert.rejects(publishSessionSeedChunk(chunk, {
    async appendRecords() {}, readRecords: () => stream([]),
    async appendManifests() { commits++; }, readManifests: () => stream([]),
    async appendGroup() { commits++; }, async readGroup() { return null; },
  }), /not fully visible/);
  assert.equal(commits, 0);
});

test('chunk boundaries retain whole visitors and reject a repeated visitor after pagination', async () => {
  const inputs = await Promise.all(['a', 'b', 'c'].map(key => visitor(key, [head(key, TIME, key)])));
  const chunks = await collect(sessionSeedChunks(stream(inputs), { tenantId: 'boom', runId: 'run', sourceSeal: SEAL, startSequence: 5, maxVisitors: 2 }));
  assert.deepEqual(chunks.map(row => [row.group.sequence, row.group.members.length]), [[5, 2], [6, 1]]);
  await assert.rejects(collect(sessionSeedChunks(stream([inputs[0], inputs[0]]), { tenantId: 'boom', runId: 'run', sourceSeal: SEAL, startSequence: 1 })), /once in/);
});

test('historical source cursors preserve event time and detect normalization drift', async () => {
  const source = sourcePartition('raw_jitsu_data_pages', ['message_id', 'anonymous_id', 'timestamp', 'received_at', '__tb_sort_1'], SEAL);
  const row = { message_id: 'original', anonymous_id: 'anon', timestamp: '2117-01-01 00:00:00.000001', received_at: '2026-09-05 12:00:00.123456',
    __bootstrap_id: 'original', __bootstrap_revision: '1788609600123457', __bootstrap_hash: 'F'.repeat(64) };
  const normalized = await normalizeSourceRow(source, row, 'boom', TIME);
  assert.equal(normalized.event.source.observed_at, '2117-01-01T00:00:00.000001Z');
  assert.equal(normalized.event.source.source_revision, '1788609600123457');
  assert.equal(JSON.parse(normalized.event.source.original_payload).__tb_sort_1, undefined);
  const sql = sourcePageQuery(source, normalized.cursor);
  assert.match(sql, /__bootstrap_id, __bootstrap_revision, __bootstrap_hash/);
  assert.doesNotMatch(sql, /timestamp\s*[<>]/);
  assert.throws(() => sourcePageQuery(source, { ...normalized.cursor, inputManifestHash: 'b'.repeat(64) }), /another frozen input/);
  await assert.rejects(normalizeSourceRow(source, { ...row, __bootstrap_revision: '10' }, 'boom', TIME), /differs/);
});

test('all five browser kinds retain identity evidence and source heads cross input pages', async () => {
  const suffixes = ['pages', 'identifies', 'form_submitted', 'order_completed', 'attr'];
  assert.deepEqual(suffixes.map(kind => sourcePartition(`raw_jitsu_data_${kind}`, ['message_id', 'timestamp'], SEAL).kind), ['page_view', 'identify', 'client_form', 'client_order', 'attribution']);
  const earlier = await normalizeHistorical({ tenantId: 'boom', source: 'jitsu_data', kind: 'identify', ingestedAt: TIME,
    record: { message_id: 'id', anonymous_id: 'a', email: 'x@example.test', timestamp: TIME, received_at: '2026-09-05T12:00:00.000001Z' } });
  const later = await normalizeHistorical({ tenantId: 'boom', source: 'jitsu_data', kind: 'identify', ingestedAt: TIME,
    record: { message_id: 'id', anonymous_id: 'a', email: 'y@example.test', timestamp: TIME, received_at: '2026-09-05T12:00:00.000002Z' } });
  const selected = await collect(selectSourceHeads(stream([earlier, earlier, later])));
  assert.equal(selected.length, 1);
  assert.ok(selected[0].identity!.evidenceKeys.includes('email:y@example.test'));
  await assert.rejects(collect(selectSourceHeads(stream([later, { ...later, source: { ...later.source, original_payload_hash: 'changed' } }]))), /Conflicting source/);
});

test('source checkpoints advance after verified output and tolerate interrupted acknowledgments', async () => {
  const source = sourcePartition('raw_boom_domains_pages', ['id', 'anonymous_id', 'timestamp', 'loaded_at'], SEAL);
  const row = { id: 'a', anonymous_id: 'visitor', timestamp: TIME, loaded_at: TIME,
    __bootstrap_id: 'a', __bootstrap_revision: '1788609600000001', __bootstrap_hash: 'A'.repeat(64) };
  const page = await buildSourceReplayPage(source, null, [row], { tenantId: 'boom', ingestedAt: TIME, limit: 2 });
  assert.equal(page.eof, true);
  let stored = page.events.slice(0, 0);
  let appends = 0;
  let attempts = 0;
  const publisher = {
    async appendSources(events: typeof page.events) { appends++; stored = [...events]; },
    readSources: () => stream(stored),
    async advanceCheckpoint() { if (++attempts === 1) throw new Error('Interrupted acknowledgment'); },
  };
  await assert.rejects(publishSourceReplayPage(page, publisher), /Interrupted/);
  await publishSourceReplayPage(page, publisher);
  assert.equal(appends, 1);
  assert.equal(attempts, 2);
  await assert.rejects(buildSourceReplayPage(source, page.next, [row], { tenantId: 'boom', ingestedAt: TIME, limit: 2 }), /did not advance/);
  stored = [];
  publisher.appendSources = async () => {};
  await assert.rejects(publishSourceReplayPage(page, publisher), /not fully visible/);
  assert.equal(attempts, 2);
});

function fact(key: string, keys: string[], observedAt: string | null = TIME): PendingIdentityFact {
  return { eventId: key, producerId: 'fixture', factKind: 'identify', factKey: key, observedAt, ingestedAt: TIME,
    sourcePriority: 2, sourceFactVersion: 1, factDeleted: false, factPayloadHash: key, factPayload: '{}', evidenceKeys: keys };
}

test('the global identity index connects facts across batches and exact engine parity survives component batching', async () => {
  const facts = [fact('a', ['anonymous_id:a', 'email:late@example.test']), fact('b', ['anonymous_id:b', 'email:late@example.test']), fact('c', ['anonymous_id:b', 'email:earlier@example.test'], null), fact('d', ['anonymous_id:d'])];
  const index = new IdentityComponentIndex(100);
  for (const item of facts) index.addSelectedFact(item);
  assert.deepEqual(index.seal(4), { facts: 4, identifiers: 5, components: 2 });
  assert.equal(index.componentKey(facts[0]), index.componentKey(facts[2]));
  assert.throws(() => index.addSelectedFact(facts[3]), /sealed/);
  const components = [facts.slice(0, 3), facts.slice(3)];
  const results = [];
  for (let position = 0; position < components.length; position++) {
    const selected = components[position];
    results.push(await buildIdentitySeedBatch({ tenantId: 'boom', batchId: `seed-${position}`, sourceSeal: SEAL, committedAt: TIME,
      components: [{ componentKey: index.componentKey(selected[0]), sourceSeal: SEAL, facts: selected, expectedFactCount: selected.length, expectedFactHash: await hash(selected) }] }, index));
  }
  const whole = await runIdentityEngine({ tenantId: 'boom', batchVersion: 1, batchId: 'all', committedAt: TIME, pendingFacts: facts,
    currentFacts: [], currentMappings: [], currentProfiles: [], checkpointIngestedAt: '', checkpointEventId: '' });
  const graph = (rows: typeof whole.rows) => rows.filter(row => row.state_kind === 'mapping').map(row => [row.identifier_key, row.profile_id, row.first_seen_at, row.last_seen_at]).sort();
  assert.deepEqual(graph(results.flatMap(result => result.rows)), graph(whole.rows));
  const partial = facts.slice(0, 2);
  await assert.rejects(buildIdentitySeedBatch({ tenantId: 'boom', batchId: 'bad', sourceSeal: SEAL, committedAt: TIME,
    components: [{ componentKey: index.componentKey(facts[0]), sourceSeal: SEAL, facts: partial, expectedFactCount: 2, expectedFactHash: await hash(partial) }] }, index), /omits facts/);
});

test('deleted and identifier-free facts remain in identity bootstrap without creating graph edges', async () => {
  const facts = [{ ...fact('a', ['anonymous_id:old']), factDeleted: true }, fact('b', [])];
  const index = new IdentityComponentIndex(1);
  facts.forEach(row => index.addSelectedFact(row));
  assert.deepEqual(index.seal(2), { facts: 2, identifiers: 0, components: 2 });
  const result = await buildIdentitySeedBatch({ tenantId: 'boom', batchId: 'seed', sourceSeal: SEAL, committedAt: TIME,
    components: await Promise.all(facts.map(async row => ({ componentKey: index.componentKey(row), sourceSeal: SEAL, facts: [row], expectedFactCount: 1, expectedFactHash: await hash([row]) }))) }, index);
  assert.equal(result.rows.filter(row => row.state_kind === 'fact').length, 2);
  assert.equal(result.rows.filter(row => row.state_kind === 'mapping').length, 0);
});

test('final activation rejects missing, overlapping, and unverified bootstrap output', async () => {
  const plan: BootstrapPlan = { runId: 'run', tenantId: 'boom', inputManifestHash: SEAL, sourceSeal: SEAL,
    inputPartitions: ['pages', 'identifies'], expectedSelectedPageHeads: 3, expectedVisitors: 2,
    expectedIdentityFacts: 3, expectedIdentityComponents: 1, firstSessionSequence: 1 };
  const common = { sourceSeal: SEAL, contentHash: SEAL, verifiedContentHash: SEAL };
  const receipts = {
    sources: ['pages', 'identifies'].map(partition => ({ ...common, partition, lastCursor: 'end', eof: true as const })),
    sessions: [{ ...common, sequence: 1, firstVisitor: 'a', lastVisitor: 'b', visitorCount: 2, pageHeadCount: 3, sessionCount: 2 }],
    identity: [{ ...common, batchId: 'identity', firstComponent: 'a', lastComponent: 'a', componentCount: 1, factCount: 3 }],
    pageHeadStore: common,
  };
  const complete = await sealBootstrap(plan, receipts);
  assert.equal(complete.finalSessionSequence, 1);
  assert.equal(complete.identityVersion, 1);
  assert.equal(complete.counts.pageHeads, 3);
  await assert.rejects(sealBootstrap(plan, { ...receipts, sources: receipts.sources.slice(0, 1) }), /incomplete source/);
  await assert.rejects(sealBootstrap(plan, { ...receipts, sessions: [{ ...receipts.sessions[0], pageHeadCount: 2 }] }), /totals/);
  await assert.rejects(sealBootstrap(plan, { ...receipts, sessions: [{ ...receipts.sessions[0], sequence: 2 }] }), /gap/);
  await assert.rejects(sealBootstrap(plan, { ...receipts, pageHeadStore: { ...common, verifiedContentHash: 'b'.repeat(64) } }), /not verified/);
});
