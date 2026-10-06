import { normalizeJitsu, type NormalizedBrowserEvent, type JitsuObservationInput } from '../browser/normalize.ts';
import { deriveVisitorKey, type PageRevision } from '../sessions/page-revisions.ts';
import { canonicalJson, compareStrings } from '../sessions/session-engine.ts';
import { IdentityStorage } from '../identity/storage.ts';
import { sqlString, waitForReadback } from '../storage/tinybird.ts';
import { sourcePartition, sourcePageQuery, buildSourceReplayPage, publishSourceReplayPage, selectSourceHeads, type SourcePartition, type SourceCursor, type SourceReplayPage } from './replay.ts';
import { sessionSeedChunks, type CompleteVisitor } from './sessions.ts';
import { IdentityComponentIndex, buildIdentitySeedBatch, type CompleteIdentityComponent } from './identity.ts';
import { sealBootstrap, type SourceReceipt, type SessionReceipt, type IdentityReceipt } from './manifest.ts';
import { TinybirdBootstrapStorage, type SourceProgress, type EventReference } from './tinybird-storage.ts';
import { hash } from './hash.ts';
import { buildMembershipProofs, type MembershipSeal } from './membership.ts';
import { boundedInputCardinality, boundedLiveCardinality } from './input-counts.ts';
import { restoreConversionIdentity, conversionPartition, validateConversionSnapshot, type ConversionIdentitySnapshot } from './conversion-identity.ts';
export interface BootstrapInput {
    partition: SourcePartition;
    landingTable: string;
    expectedPhysicalRows: number;
    /** Physical first sorting key, independently checked against the original ID. */
    landingRecordIdColumn?: string;
}
export interface BootstrapConfig {
    /** Larger byte-bounded transport is reserved for the one-time process. */
    bulk?: boolean;
    baselineId: string;
    tenantId: string;
    sourceSeal: string;
    inputManifestHash: string;
    algorithmVersion: string;
    workspaceId: string;
    workspaceName: string;
    startedAt: string;
    region: string;
    inputs: BootstrapInput[];
    live?: {
        table: string;
        cutoff: string;
        expectedPhysicalRows: number;
    };
    conversionIdentity?: ConversionIdentitySnapshot;
    pageSize: number;
    batchSize: number;
    maxVisitorsPerChunk: number;
    maxSessionRecordsPerChunk: number;
    maxIdentityFactsPerBatch: number;
    maxIdentityFactsPerComponent: number;
    maxIdentifiers: number;
}
type Key = [
    string,
    string
];
type StageState = {
    cursor: unknown;
    counters: Record<string, number>;
    chainHash: string;
};
type StageResult = StageState & {
    complete: true;
};
type Progress = (stage: string, counts: Record<string, number>) => void;
/** Run as one supervised process near Tinybird. Restart with the identical configuration. */
export async function executeBootstrap(config: BootstrapConfig, store: TinybirdBootstrapStorage, progress: Progress = () => { }): Promise<unknown> {
    validateConfig(config, store);
    const plan = await store.getManifest<BootstrapConfig>('plan', 'config');
    if (plan && canonicalJson(plan) !== canonicalJson(config))
        throw new Error('Bootstrap configuration changed; use the saved configuration');
    if (!plan) {
        const [existing] = await store.client.query<{
            rows: number;
        }>(`SELECT count() AS rows FROM v1_session_commits WHERE tenant_id = ${sqlString(config.tenantId)}`);
        const [identity] = await store.client.query<{
            rows: number;
        }>(`SELECT count() AS rows FROM v1_identity_commits WHERE tenant_id = ${sqlString(config.tenantId)}`);
        if (Number(existing.rows) || Number(identity.rows))
            throw new Error('A fresh bootstrap requires empty session and identity outputs for its tenant');
        await store.putManifest('plan', 'config', config);
    }
    const sealed = await store.getManifest<{
        membership?: MembershipSeal;
    }>('seal', 'complete');
    if (sealed) {
        if (!sealed.membership)
            throw new Error('Older baseline has no authenticated membership proof; use a fresh baseline ID');
        return sealed;
    }
    for (const input of config.inputs)
        await restoreHistorical(input, config, store, progress);
    if (config.live)
        await restoreLive(config, store, progress);
    const sources: string[] = [...new Set(config.inputs.map(input => input.partition.source))];
    if (config.live)
        sources.push('jitsu_events_api' as typeof sources[number]);
    for (const source of sources) {
        let expectedEvents = 0;
        for (const input of config.inputs.filter(input => input.partition.source === source)) {
            const receipt = await store.requiredManifest<SourceReceipt>('source', input.partition.table);
            const current = await waitForReadback(() => store.checkpoint<SourceProgress>('sources', input.partition.table), row => Boolean(row?.payload.eof && row.payload.chainHash === receipt.contentHash));
            expectedEvents += current!.payload.rows;
        }
        if (source === 'jitsu_events_api')
            expectedEvents = (await store.requiredManifest<StageResult>('stage', 'live-source:all')).counters.rows;
        await restoreSourceHeads(config, store, source, expectedEvents, progress);
    }
    const sourceHeads = await Promise.all(sources.map(source => store.requiredManifest<StageResult>('stage', `source-heads:${source}`)));
    const expectedSourceHeads = sourceHeads.reduce((total, result) => total + (result.counters.heads ?? 0), 0);
    const logical = await restoreLogicalHeads(config, store, expectedSourceHeads, progress);
    if (expectedSourceHeads !== (logical.counters.sourceHeads ?? 0))
        throw new Error('Logical selection did not consume every per-source head');
    const conversionFacts = config.conversionIdentity
        ? await restoreConversionIdentity(config.conversionIdentity, store, counts => progress('conversion-identity', counts))
        : 0;
    const expectedIdentityFacts = (logical.counters.facts ?? 0) + conversionFacts;
    const totals = await waitForReadback(() => store.totals(), value => value.pages === (logical.counters.pages ?? 0) && value.identityFacts === expectedIdentityFacts);
    await restoreSessions(config, store, totals.pages, totals.visitors, progress);
    let identitySummary = await store.getManifest<{
        facts: number;
        identifiers: number;
        components: number;
    }>('stage', 'identity-complete');
    if (!identitySummary) {
        const indexed = await waitForReadback(async () => {
            const index = new IdentityComponentIndex(config.maxIdentifiers);
            let count = 0;
            for await (const item of store.identityFacts('selected')) {
                index.addSelectedFact(item.fact);
                count++;
            }
            if (count > totals.identityFacts)
                throw new Error('Identity scan contains unexpected facts');
            return { index, count };
        }, result => result.count === totals.identityFacts);
        const index = indexed.index;
        identitySummary = index.seal(totals.identityFacts);
        progress('identity-index', identitySummary);
        const partitioned = await batchedStage(store, 'identity-partition', 'all', config.batchSize, cursor => store.identityFacts('selected', cursor ? { kind: (cursor as Key)[0], key: (cursor as Key)[1] } : undefined), item => [item.fact.factKind, item.fact.factKey], async (items) => {
            await store.writeIdentityFacts('component', items.map(item => ({ fact: item.fact, componentKey: index.componentKey(item.fact) })));
            return { facts: items.length };
        }, progress, { facts: totals.identityFacts });
        if ((partitioned.counters.facts ?? 0) !== totals.identityFacts)
            throw new Error('Identity partition did not include every selected fact');
        await restoreIdentity(config, store, index, totals.identityFacts, progress);
        await store.putManifest('stage', 'identity-complete', identitySummary);
    }
    // Counts and hashes come from verified output receipts, not an independent progress counter.
    const sourceReceipts: SourceReceipt[] = [];
    for (const partition of [...config.inputs.map(input => input.partition.table), ...(config.live ? ['live-jitsu'] : []),
        ...(config.conversionIdentity ? [conversionPartition(config.conversionIdentity)] : [])]) {
        const result = await store.requiredManifest<SourceReceipt>('source', partition);
        sourceReceipts.push(result);
    }
    const sessions = await store.checkpointPayloads<SessionReceipt>('sessions', 'all');
    const identity = await store.checkpointPayloads<IdentityReceipt>('identity', 'all');
    const final = await sealBootstrap({
        runId: config.baselineId, tenantId: config.tenantId, sourceSeal: config.sourceSeal, inputManifestHash: config.inputManifestHash,
        inputPartitions: sourceReceipts.map(row => row.partition), expectedSelectedPageHeads: totals.pages,
        expectedVisitors: totals.visitors, expectedIdentityFacts: totals.identityFacts,
        expectedIdentityComponents: identitySummary.components, firstSessionSequence: 1,
    }, { sources: sourceReceipts, sessions, identity, pageHeadStore: {
            sourceSeal: config.sourceSeal, contentHash: logical.chainHash, verifiedContentHash: logical.chainHash,
        } });
    const membership = await buildMembershipProofs(store, { source: logical.counters.logicalHeads ?? 0, visitor: totals.visitors });
    const authenticated = { ...final, membership };
    await store.putManifest('seal', 'complete', authenticated);
    progress('complete', final.counts);
    return authenticated;
}
async function restoreSourceHeads(config: BootstrapConfig, store: TinybirdBootstrapStorage, source: string, expectedEvents: number, progress: Progress): Promise<void> {
    if (await store.getManifest('stage', `source-heads:${source}`))
        return;
    const refs = await waitForReadback(async () => {
        let seen = 0;
        async function* counted() { for await (const event of store.eventVersions(source)) {
            seen++;
            yield event;
        } }
        const rows: EventReference[] = [];
        for await (const event of selectSourceHeads(counted()))
            rows.push(await eventReference(event));
        if (seen > expectedEvents)
            throw new Error('Source scan contains unexpected events');
        return { seen, rows };
    }, result => result.seen === expectedEvents);
    await batchedStage(store, 'source-heads', source, config.batchSize, cursor => referenceStream(refs.rows, cursor as Key | undefined, row => [row.kind, row.id]), row => [row.kind, row.id], async (rows) => { await store.writeHeads('source', await store.eventsByReferences(rows)); return { heads: rows.length }; }, progress, { heads: refs.rows.length });
}
interface LogicalReference {
    ref: EventReference;
    sourceHeadCount: number;
    sourceHeadsHash: string;
}
async function restoreLogicalHeads(config: BootstrapConfig, store: TinybirdBootstrapStorage, expectedSourceHeads: number, progress: Progress): Promise<StageResult> {
    const completed = await store.getManifest<StageResult>('stage', 'logical-heads:all');
    if (completed)
        return completed;
    const refs = await waitForReadback(async () => {
        let seen = 0;
        const rows: LogicalReference[] = [];
        for await (const group of logicalGroups(store.heads('source'))) {
            seen += group.length;
            rows.push({ ref: await eventReference(selectLogicalHead(group)), sourceHeadCount: group.length, sourceHeadsHash: await hash(group) });
        }
        if (seen > expectedSourceHeads)
            throw new Error('Logical scan contains unexpected source heads');
        return { seen, rows };
    }, result => result.seen === expectedSourceHeads);
    return batchedStage(store, 'logical-heads', 'all', config.batchSize, cursor => referenceStream(refs.rows, cursor as Key | undefined, row => [row.ref.kind, row.ref.id]), row => [row.ref.kind, row.ref.id], async (groups) => {
        const selected = await store.eventsByReferences(groups.map(group => group.ref));
        const pages = selected.filter(event => event.pageRevision !== null).map(event => ({
            visitorKey: event.pageRevision!.page?.visitor_key ?? deriveVisitorKey({
                page_view_id: event.source.source_record_id, anonymous_id: event.source.anonymous_id, user_id: event.source.user_id,
            }), head: event.pageRevision!,
        }));
        const facts = selected.flatMap(event => event.identity ? [{ fact: event.identity }] : []);
        await store.putManifests('logical-heads', groups.map(group => ({ key: canonicalJson({ event_kind: group.ref.kind, source_record_id: group.ref.id }), payload: { count: group.sourceHeadCount, hash: group.sourceHeadsHash } })));
        await store.writeHeads('logical', selected);
        await store.writePageHeads(pages);
        await store.writeIdentityFacts('selected', facts);
        return { sourceHeads: groups.reduce((total, group) => total + group.sourceHeadCount, 0), logicalHeads: selected.length, pages: pages.length, facts: facts.length };
    }, progress, { sourceHeads: expectedSourceHeads, logicalHeads: refs.rows.length });
}
async function eventReference(event: NormalizedBrowserEvent): Promise<EventReference> { return { source: event.source.source_system, kind: event.source.event_kind, id: event.source.source_record_id, hash: await hash(event) }; }
async function* referenceStream<T>(rows: T[], after: Key | undefined, key: (row: T) => Key): AsyncGenerator<T> {
    for (const row of rows) {
        const current = key(row);
        if (after && (compareStrings(current[0], after[0]) || compareStrings(current[1], after[1])) <= 0)
            continue;
        yield row;
    }
}
async function restoreHistorical(input: BootstrapInput, config: BootstrapConfig, store: TinybirdBootstrapStorage, progress: Progress): Promise<void> {
    const key = input.partition.table;
    if (await store.getManifest('source', key))
        return;
    await verifyHistoricalLanding(store, input);
    const cardinality = input.landingRecordIdColumn
        ? await boundedInputCardinality(store, key, input.landingTable, input.partition.columns, input.expectedPhysicalRows, input.landingRecordIdColumn)
        : await frozenCardinality(store, key, input.landingTable, input.partition.columns, input.expectedPhysicalRows);
    let checkpoint = await store.checkpoint<SourceProgress>('sources', key);
    let attempts = 0;
    for (;;) {
        while (!checkpoint?.payload.eof) {
            const page = await historicalReplayWave(input, config, store, checkpoint?.payload.cursor ?? null);
            const sequence = (checkpoint?.sequence ?? 0) + 1;
            const publisher = store.sourcePublisher(sequence, checkpoint?.payload ?? null);
            await publishSourceReplayPage(page, publisher);
            if (!publisher.progress) throw new Error('Source receipt was not published');
            checkpoint = { sequence, payload: publisher.progress };
            progress(key, { normalizedRows: checkpoint!.payload.rows });
        }
        if (checkpoint!.payload.rows === cardinality)
            break;
        if (checkpoint!.payload.rows > cardinality || attempts++ >= 3)
            throw new Error('Historical source scan did not match the frozen distinct-row count');
        const reset: SourceProgress = { cursor: null, eof: false, contentHash: '', rows: 0, chainHash: '' };
        const sequence = checkpoint!.sequence + 1;
        await store.putCheckpoint('sources', key, sequence, reset);
        checkpoint = { sequence, payload: reset };
    }
    await verifyHistoricalLanding(store, input);
    const receipt: SourceReceipt = { partition: key, lastCursor: canonicalJson(checkpoint!.payload.cursor), eof: true,
        sourceSeal: config.sourceSeal, contentHash: checkpoint!.payload.chainHash, verifiedContentHash: checkpoint!.payload.chainHash };
    await store.putManifest('source', key, receipt);
}
/** Read small physical ranges, then publish one byte-bounded wave and one durable cursor. */
async function historicalReplayWave(input: BootstrapInput, config: BootstrapConfig, store: TinybirdBootstrapStorage, after: SourceCursor | null): Promise<SourceReplayPage> {
    let cursor = after;
    let eof = false;
    let bytes = 0;
    const events: NormalizedBrowserEvent[] = [];
    do {
        let sql = sourcePageQuery(input.partition, cursor, config.pageSize, input.landingRecordIdColumn)
            .replace(`FROM ${input.partition.table}`, `FROM ${input.landingTable}`);
        let emptyRange = false;
        if (input.landingRecordIdColumn) {
            const column = input.landingRecordIdColumn;
            const lower = cursor ? `${column} >= ${sqlString(cursor.recordId)}` : '';
            const ids = await store.client.query<{ record_id: string }>(`SELECT DISTINCT ${column} AS record_id FROM ${input.landingTable}
                ${lower ? `WHERE ${lower}` : ''} ORDER BY ${column} LIMIT ${config.pageSize + 1}`);
            emptyRange = !ids.length;
            if (ids.length) {
                const bound = `${lower ? `${lower} AND ` : ''}${column} <= ${sqlString(ids.at(-1)!.record_id)}`;
                sql = sql.includes('WHERE tuple(') ? sql.replace('WHERE tuple(', `WHERE ${bound} AND tuple(`) : sql.replace('ORDER BY ', `WHERE ${bound}\nORDER BY `);
            }
        }
        const rows = emptyRange ? [] : await store.client.query<Record<string, unknown>>(sql);
        const page = await buildSourceReplayPage(input.partition, cursor, rows, { tenantId: config.tenantId, ingestedAt: config.startedAt, limit: config.pageSize });
        events.push(...page.events);
        cursor = page.next;
        eof = page.eof;
        bytes += new TextEncoder().encode(canonicalJson(page.events)).byteLength;
    } while (store.bulk && !eof && events.length < config.batchSize && bytes < store.maxWaveBytes);
    const page = { partition: input.partition, after, next: cursor, eof, events };
    return { ...page, contentHash: await hash(page) };
}
async function restoreLive(config: BootstrapConfig, store: TinybirdBootstrapStorage, progress: Progress): Promise<void> {
    const input = config.live!;
    if (await store.getManifest('source', 'live-jitsu'))
        return;
    const where = `tenant_id = ${sqlString(config.tenantId)} AND ingested_at <= toDateTime64(${sqlString(input.cutoff)},6)`;
    await verifyLandingCount(store, input.table, input.expectedPhysicalRows, where);
    const columns = ['producer_id', 'producer_sequence', 'delivery_event_id', 'ingested_at', 'tenant_id', 'message_id', 'event_kind', 'observed_at', 'source_fact_version', 'source_deleted', 'fact_payload'];
    const cardinality = await boundedLiveCardinality(store, input.table, columns, input.expectedPhysicalRows, where);
    const result = await batchedStage(store, 'live-source', 'all', config.batchSize, async function* (cursor) {
        // fact_payload carries the exact source microseconds lost by the old typed projections.
        const after = cursor ? `AND tuple(producer_id,producer_sequence,delivery_event_id,ingested_at) > tuple(
      ${sqlString((cursor as string[])[0])},toUInt64(${sqlString((cursor as string[])[1])}),
      ${sqlString((cursor as string[])[2])},toDateTime64(${sqlString((cursor as string[])[3])},6))` : '';
        for await (const row of store.scan<JitsuObservationInput & {
            producer_sequence: string;
        }>(input.table, ['producer_id', 'producer_sequence', 'delivery_event_id', 'ingested_at'], `${where} ${after}`, ['tenant_id', 'message_id', 'event_kind', 'observed_at', 'source_fact_version', 'source_deleted', 'fact_payload'])) {
            yield { cursor: [row.producer_id, String(row.producer_sequence), row.delivery_event_id, row.ingested_at], event: await normalizeJitsu(row) };
        }
    }, item => item.cursor, async (items) => { await store.appendEvents(items.map(item => item.event)); return { rows: items.length }; }, progress, { rows: cardinality });
    await verifyLandingCount(store, input.table, input.expectedPhysicalRows, where);
    await store.putManifest('source', 'live-jitsu', { partition: 'live-jitsu', lastCursor: canonicalJson(result.cursor), eof: true,
        sourceSeal: config.sourceSeal, contentHash: result.chainHash, verifiedContentHash: result.chainHash } satisfies SourceReceipt);
}
async function restoreSessions(config: BootstrapConfig, store: TinybirdBootstrapStorage, expectedPages: number, expectedVisitors: number, progress: Progress): Promise<void> {
    if (await store.getManifest('stage', 'sessions-complete'))
        return;
    const saved = await store.checkpoint<SessionReceipt>('sessions', 'all');
    const indexed = await waitForReadback(async () => {
        const entries: {
            visitorKey: string;
            count: number;
            hash: string;
        }[] = [];
        let pages = 0;
        for await (const visitor of store.visitors()) {
            pages += visitor.expectedHeadCount;
            entries.push({ visitorKey: visitor.visitorKey, count: visitor.expectedHeadCount, hash: visitor.expectedHeadHash });
        }
        if (pages > expectedPages || entries.length > expectedVisitors)
            throw new Error('Visitor scan contains unexpected heads');
        return { pages, entries };
    }, result => result.pages === expectedPages && result.entries.length === expectedVisitors);
    const visitorMetadata = new Map<string, {
        count: number;
        hash: string;
    }>();
    async function* visitors(): AsyncGenerator<CompleteVisitor> {
        const pending = indexed.entries.filter(entry => !saved || compareStrings(entry.visitorKey, saved.payload.lastVisitor) > 0);
        for await (const visitor of store.indexedVisitors(pending)) {
            visitorMetadata.set(visitor.visitorKey, { count: visitor.expectedHeadCount, hash: visitor.expectedHeadHash });
            yield visitor;
        }
    }
    for await (const chunk of sessionSeedChunks(visitors(), {
        tenantId: config.tenantId, runId: config.baselineId, sourceSeal: config.sourceSeal,
        startSequence: (saved?.sequence ?? 0) + 1, maxVisitors: config.maxVisitorsPerChunk, maxRecords: config.maxSessionRecordsPerChunk,
        maxBytes: store.maxWaveBytes,
    })) {
        await store.publishSessions(chunk);
        await store.writeMembers(chunk, visitorMetadata);
        const receipt: SessionReceipt = { sourceSeal: config.sourceSeal, sequence: chunk.group.sequence,
            firstVisitor: chunk.firstVisitor, lastVisitor: chunk.lastVisitor, visitorCount: chunk.group.members.length,
            pageHeadCount: chunk.pageHeadCount, sessionCount: chunk.records.length,
            contentHash: chunk.contentHash, verifiedContentHash: chunk.contentHash };
        await store.putCheckpoint('sessions', 'all', chunk.group.sequence, receipt);
        for (const member of chunk.group.members)
            visitorMetadata.delete(member.visitor_key);
        progress('sessions', { sequence: chunk.group.sequence, visitors: chunk.group.members.length, sessions: chunk.records.length });
    }
    await store.putManifest('stage', 'sessions-complete', { complete: true });
}
async function restoreIdentity(config: BootstrapConfig, store: TinybirdBootstrapStorage, index: IdentityComponentIndex, expectedFacts: number, progress: Progress): Promise<void> {
    const saved = await store.checkpoint<IdentityReceipt>('identity', 'all');
    let sequence = (saved?.sequence ?? 0) + 1;
    let components: CompleteIdentityComponent[] = [];
    let facts = 0;
    let bytes = 0;
    const publish = async () => {
        if (!components.length)
            return;
        const id = `bootstrap:${config.baselineId}:${sequence}`;
        const result = await buildIdentitySeedBatch({ tenantId: config.tenantId, batchId: id, sourceSeal: config.sourceSeal,
            committedAt: config.startedAt, components }, index);
        await new IdentityStorage(store.client, { readPageRows: store.scanRows }).publish({ tenantId: config.tenantId, version: 1, id,
            committedAt: config.startedAt, facts: components.flatMap(component => component.facts) }, result.rows);
        const contentHash = await hash(result.rows);
        await store.putCheckpoint('identity', 'all', sequence, {
            sourceSeal: config.sourceSeal, batchId: id, firstComponent: components[0].componentKey,
            lastComponent: components.at(-1)!.componentKey, componentCount: components.length, factCount: facts,
            contentHash, verifiedContentHash: contentHash,
        } satisfies IdentityReceipt);
        progress('identity', { sequence, facts, components: components.length });
        sequence++;
        components = [];
        facts = 0;
        bytes = 0;
    };
    const pending = index.components().filter(row => !saved || compareStrings(row.key, saved.payload.lastComponent) > 0);
    if (pending.some(row => row.count > config.maxIdentityFactsPerComponent))
        throw new Error('Identity component exceeds the configured bootstrap memory bound');
    for await (const item of store.indexedIdentityComponents(pending)) {
        const component: CompleteIdentityComponent = { componentKey: item.componentKey, sourceSeal: config.sourceSeal, facts: item.facts,
            expectedFactCount: index.expectedComponentFacts(item.componentKey), expectedFactHash: await hash(item.facts) };
        const componentBytes = new TextEncoder().encode(canonicalJson(component.facts)).byteLength;
        if (components.length && (facts + component.facts.length > config.maxIdentityFactsPerBatch || bytes + componentBytes > store.maxWaveBytes))
            await publish();
        components.push(component);
        facts += component.facts.length;
        bytes += componentBytes;
    }
    await publish();
}
async function batchedStage<T>(store: TinybirdBootstrapStorage, stage: string, partition: string, limit: number, input: (cursor: unknown) => AsyncIterable<T>, cursor: (row: T) => unknown, save: (rows: T[]) => Promise<Record<string, number>>, progress: Progress, expected?: Record<string, number>, attempt = 0): Promise<StageResult> {
    const done = await store.getManifest<StageResult>('stage', `${stage}:${partition}`);
    if (done)
        return done;
    const checkpoint = await store.checkpoint<StageState>(stage, partition);
    let state: StageState = checkpoint?.payload ?? { cursor: undefined, counters: {}, chainHash: await hash([]) };
    let sequence = (checkpoint?.sequence ?? 0) + 1;
    let batch: T[] = [];
    let bytes = 0;
    const flush = async () => {
        if (!batch.length)
            return;
        const counts = await save(batch);
        const counters = { ...state.counters };
        for (const [key, count] of Object.entries(counts))
            counters[key] = (counters[key] ?? 0) + count;
        state = { cursor: cursor(batch.at(-1)!), counters, chainHash: await hash([state.chainHash, await hash(batch)]) };
        await store.putCheckpoint(stage, partition, sequence++, state);
        progress(`${stage}:${partition}`, counters);
        batch = [];
        bytes = 0;
    };
    for await (const row of input(state.cursor)) {
        const size = new TextEncoder().encode(canonicalJson(row)).byteLength;
        if (batch.length && bytes + size > store.maxWaveBytes)
            await flush();
        batch.push(row);
        bytes += size;
        if (batch.length >= limit)
            await flush();
    }
    await flush();
    if (expected && Object.entries(expected).some(([key, value]) => (state.counters[key] ?? 0) !== value)) {
        if (attempt >= 3 || Object.entries(expected).some(([key, value]) => (state.counters[key] ?? 0) > value))
            throw new Error('Bootstrap stage did not consume its complete verified input');
        await store.putCheckpoint(stage, partition, sequence, { cursor: null, counters: {}, chainHash: await hash([]) });
        return batchedStage(store, stage, partition, limit, input, cursor, save, progress, expected, attempt + 1);
    }
    const result = { ...state, complete: true as const };
    await store.putManifest('stage', `${stage}:${partition}`, result);
    return result;
}
async function* logicalGroups(events: AsyncIterable<NormalizedBrowserEvent>): AsyncGenerator<NormalizedBrowserEvent[]> {
    let key: string | null = null;
    let group: NormalizedBrowserEvent[] = [];
    for await (const event of events) {
        const next = canonicalJson([event.source.event_kind, event.source.source_record_id]);
        if (key !== null && next !== key) {
            yield group;
            group = [];
        }
        key = next;
        group.push(event);
    }
    if (group.length)
        yield group;
}
function selectLogicalHead(events: NormalizedBrowserEvent[]): NormalizedBrowserEvent {
    if (!events.length)
        throw new Error('Logical source group is empty');
    return [...events].sort((left, right) => right.source.source_priority - left.source.source_priority
        || Number(BigInt(right.source.source_revision) - BigInt(left.source.source_revision)))[0];
}
async function verifyLandingCount(store: TinybirdBootstrapStorage, table: string, expected: number, where = '1'): Promise<void> {
    const [result] = await store.client.query<{
        rows: number;
    }>(`SELECT count() AS rows FROM ${table} WHERE ${where}`);
    if (Number(result.rows) !== expected)
        throw new Error(`Frozen landing row count changed for ${table}`);
}
async function frozenCardinality(store: TinybirdBootstrapStorage, key: string, table: string, columns: string[], expectedPhysical: number, where = '1'): Promise<number> {
    const saved = await store.getManifest<{
        distinct: number;
    }>('input-counts', key);
    if (saved)
        return saved.distinct;
    const result = await waitForReadback(async () => {
        const [row] = await store.client.query<{
            physical: number;
            distinct: number;
        }>(`SELECT count() AS physical,uniqExact(tuple(${columns.join(',')})) AS distinct FROM ${table} WHERE ${where}`);
        if (Number(row.physical) > expectedPhysical)
            throw new Error('Frozen source contains unexpected rows');
        return { physical: Number(row.physical), distinct: Number(row.distinct) };
    }, row => row.physical === expectedPhysical);
    await store.putManifest('input-counts', key, result);
    return result.distinct;
}
async function verifyHistoricalLanding(store: TinybirdBootstrapStorage, input: BootstrapInput): Promise<void> {
    await verifyLandingCount(store, input.landingTable, input.expectedPhysicalRows);
    if (!input.landingRecordIdColumn)
        return;
    const fields = ['message_id', 'id'].filter(key => input.partition.columns.includes(key));
    const id = fields.length === 1 ? fields[0] : `coalesce(${fields.join(',')})`;
    const [result] = await store.client.query<{
        rows: number;
    }>(`SELECT count() AS rows FROM ${input.landingTable}
    WHERE ${input.landingRecordIdColumn} != ifNull(${id}, '')`);
    if (Number(result.rows))
        throw new Error('Landing sorting key does not match every original record ID');
}
function validateConfig(config: BootstrapConfig, store: TinybirdBootstrapStorage): void {
    if (config.conversionIdentity) validateConversionSnapshot(config.conversionIdentity);
    if (Boolean(config.bulk) !== store.bulk)
        throw new Error('Bootstrap transport mode differs from the frozen configuration');
    if (store.baselineId !== config.baselineId || store.tenantId !== config.tenantId || store.sourceSeal !== config.sourceSeal)
        throw new Error('Bootstrap scope mismatch');
    if (!config.baselineId || !config.algorithmVersion || !config.workspaceId || !config.workspaceName || !config.region.startsWith('us-east'))
        throw new Error('Bootstrap execution identity, workspace, and nearby region are required');
    if (!/^[a-f0-9]{64}$/.test(config.inputManifestHash) || !/^[a-f0-9]{64}$/.test(config.sourceSeal))
        throw new Error('Bootstrap input hashes are required');
    const limits = [config.pageSize, config.batchSize, config.maxVisitorsPerChunk, config.maxSessionRecordsPerChunk,
        config.maxIdentityFactsPerBatch, config.maxIdentityFactsPerComponent, config.maxIdentifiers];
    if (limits.some(value => !Number.isSafeInteger(value) || value < 1) || config.maxVisitorsPerChunk > store.maxSessionVisitors || config.pageSize > 10000)
        throw new Error('Invalid bootstrap limits');
    if (new Set(config.inputs.map(input => input.partition.table)).size !== config.inputs.length)
        throw new Error('Duplicate historical input partition');
    const required = ['boom_domains', 'jitsu_data'].flatMap(source => ['pages', 'identifies', 'form_submitted', 'order_completed', 'attr'].map(kind => `raw_${source}_${kind}`)).sort();
    if (canonicalJson(config.inputs.map(input => input.partition.table).sort()) !== canonicalJson(required))
        throw new Error('Full browser bootstrap requires all ten historical sources');
    for (const input of config.inputs) {
        if (canonicalJson(sourcePartition(input.partition.table, input.partition.columns, config.inputManifestHash)) !== canonicalJson(input.partition))
            throw new Error('Historical source contract differs from the known raw schema');
        if (!/^[a-z][a-z0-9_]*$/.test(input.landingTable) || input.partition.inputManifestHash !== config.inputManifestHash)
            throw new Error('Invalid frozen landing input');
        if (input.landingRecordIdColumn && !/^[a-z_][a-z0-9_]*$/.test(input.landingRecordIdColumn))
            throw new Error('Invalid physical landing ID column');
        if (!Number.isSafeInteger(input.expectedPhysicalRows) || input.expectedPhysicalRows < 0)
            throw new Error('Frozen input count is required');
    }
    if (config.live && (!/^[a-z][a-z0-9_]*$/.test(config.live.table) || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$/.test(config.live.cutoff)
        || !Number.isSafeInteger(config.live.expectedPhysicalRows) || config.live.expectedPhysicalRows < 0))
        throw new Error('Invalid frozen live input');
}
