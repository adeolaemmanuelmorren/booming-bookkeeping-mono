import { Tinybird, sqlString, sqlStrings, uint64Number, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import { canonicalJson } from '../sessions/session-engine.ts';
import type { NormalizedBrowserEvent } from '../browser/normalize.ts';
import type { PendingIdentityFact } from '../identity/engine.ts';
import type { PageRevision } from '../sessions/page-revisions.ts';
import type { SessionSnapshot } from '../sessions/session-publication.ts';
import { TinybirdSessionPublisher } from '../sessions/tinybird-storage.ts';
import { TinybirdBrowserGroupPublisher } from '../browser/tinybird-storage.ts';
import type { SourceReplayPage, SourceReplayPublisher } from './replay.ts';
import type { CompleteVisitor, SessionSeedChunk, SeedMember } from './sessions.ts';
import type { SealedBaselineStore } from './baseline.ts';
import { hash, hashText } from './hash.ts';
import { proveMembership, validateMembershipSeal, type MembershipSeal } from './membership.ts';
export type EventRow = JsonRecord & {
    baseline_id: string;
    tenant_id: string;
    source_system: string;
    event_kind: string;
    source_record_id: string;
    source_priority: number;
    source_revision: number;
    event_hash: string;
    event_json: string;
};
export type Checkpoint<T = unknown> = {
    sequence: number;
    payload: T;
};
export interface SourceProgress {
    cursor: SourceReplayPage['next'];
    eof: boolean;
    contentHash: string;
    rows: number;
    chainHash: string;
}
export interface ScanCoverage {
    sql: string;
    expected: number;
}
export interface EventReference {
    source: string;
    kind: string;
    id: string;
    hash: string;
}
/** One supervised executor owns a baseline. Immutable receipts make retries idempotent. */
export class TinybirdBootstrapStorage {
    readonly client: Tinybird;
    readonly baselineId: string;
    readonly tenantId: string;
    readonly sourceSeal: string;
    readonly bulk: boolean;
    readonly scanRows: number;
    readonly writeBatchRows: number;
    readonly maxWaveBytes: number;
    readonly maxSessionVisitors: number;
    constructor(client: Tinybird, scope: {
        baselineId: string;
        tenantId: string;
        sourceSeal: string;
    }, options: { bulk?: boolean } = {}) {
        this.client = client;
        this.baselineId = scope.baselineId;
        this.tenantId = scope.tenantId;
        this.sourceSeal = scope.sourceSeal;
        this.bulk = options.bulk ?? false;
        this.scanRows = this.bulk ? 5000 : 500;
        this.writeBatchRows = this.bulk ? 5000 : 500;
        this.maxWaveBytes = this.bulk ? 32_000_000 : 4_000_000;
        this.maxSessionVisitors = this.bulk ? 5000 : 500;
    }
    scope(): string { return `baseline_id = ${sqlString(this.baselineId)} AND tenant_id = ${sqlString(this.tenantId)}`; }
    private base(): JsonRecord { return { baseline_id: this.baselineId, tenant_id: this.tenantId }; }
    async getManifest<T>(kind: string, key: string): Promise<T | null> {
        const rows = await this.client.query<{
            payload_json: string;
            payload_hash: string;
        }>(`
      SELECT DISTINCT payload_json, payload_hash FROM v1_bootstrap_manifests
      WHERE ${this.scope()} AND manifest_kind = ${sqlString(kind)} AND manifest_key = ${sqlString(key)} LIMIT 2`);
        if (!rows.length)
            return null;
        if (rows.length !== 1 || await hashText(rows[0].payload_json) !== rows[0].payload_hash)
            throw new Error('Conflicting bootstrap manifest');
        return JSON.parse(rows[0].payload_json) as T;
    }
    async putManifest(kind: string, key: string, payload: unknown): Promise<void> {
        const row = { ...this.base(), manifest_kind: kind, manifest_key: key,
            payload_json: canonicalJson(payload), payload_hash: await hash(payload) };
        await this.writeVerified('v1_bootstrap_manifests', [row], `${this.scope()} AND manifest_kind = ${sqlString(kind)} AND manifest_key = ${sqlString(key)}`);
    }
    async requiredManifest<T>(kind: string, key: string): Promise<T> {
        return (await waitForReadback(() => this.getManifest<T>(kind, key), value => value !== null))!;
    }
    async putManifests(kind: string, items: {
        key: string;
        payload: unknown;
    }[]): Promise<void> {
        let batch: JsonRecord[] = [];
        let bytes = 0;
        const flush = async () => {
            if (!batch.length)
                return;
            await this.writeVerified('v1_bootstrap_manifests', batch, `${this.scope()} AND manifest_kind = ${sqlString(kind)} AND manifest_key IN (${sqlStrings(batch.map(row => String(row.manifest_key)))})`);
            batch = [];
            bytes = 0;
        };
        for (const item of items) {
            const row = { ...this.base(), manifest_kind: kind, manifest_key: item.key, payload_json: canonicalJson(item.payload), payload_hash: await hash(item.payload) };
            const size = new TextEncoder().encode(canonicalJson(row)).byteLength;
            if (batch.length && bytes + size > (this.bulk ? 8_000_000 : 500_000))
                await flush();
            batch.push(row);
            bytes += size;
        }
        await flush();
    }
    async requiredManifests<T>(kind: string, keys: string[]): Promise<Map<string, T>> {
        if (keys.length > 200)
            throw new Error('Proof lookup exceeds 200 records');
        if (!keys.length)
            return new Map();
        const wanted = new Set(keys);
        const read = async () => {
            const rows = await this.client.query<{
                manifest_key: string;
                payload_json: string;
                payload_hash: string;
            }>(`
        SELECT DISTINCT manifest_key,payload_json,payload_hash FROM v1_bootstrap_manifests
        WHERE ${this.scope()} AND manifest_kind = ${sqlString(kind)} AND manifest_key IN (${sqlStrings(keys)}) LIMIT ${keys.length + 1}`);
            const result = new Map<string, T>();
            for (const row of rows) {
                if (!wanted.has(row.manifest_key) || result.has(row.manifest_key) || await hashText(row.payload_json) !== row.payload_hash)
                    throw new Error('Conflicting bootstrap proof record');
                result.set(row.manifest_key, JSON.parse(row.payload_json) as T);
            }
            return result;
        };
        return waitForReadback(read, result => result.size === wanted.size);
    }
    /** Coverage must be restricted to a bounded key set, never the full bootstrap history. */
    async coveredPage(sql: string, columns: string[], coverage: ScanCoverage): Promise<JsonRecord[]> {
        const read = async () => {
            const [result] = await this.client.query<{
                __coverage: number | string;
                __rows: string[];
            }>(`
        SELECT (${coverage.sql}) AS __coverage,
          (SELECT groupArray(toJSONString(tuple(${columns.join(',')}))) FROM (${sql})) AS __rows`);
            if (!result || !Array.isArray(result.__rows))
                throw new Error('Missing immutable scan coverage');
            const count = uint64Number(result.__coverage);
            if (count > coverage.expected)
                throw new Error('Immutable scan contains unexpected extra input');
            return { count, rows: result.__rows.map(text => {
                    const values = JSON.parse(text) as unknown[];
                    if (!Array.isArray(values) || values.length !== columns.length)
                        throw new Error('Invalid covered bootstrap page');
                    return Object.fromEntries(columns.map((name, index) => [name, values[index]]));
                }) };
        };
        return (await waitForReadback(read, result => result.count === coverage.expected)).rows;
    }
    countCoverage(table: string, keys: string[], where: string, expected: number): ScanCoverage {
        return { sql: `SELECT uniqExact(tuple(${keys.join(',')})) FROM ${table} WHERE ${where}`, expected };
    }
    async checkpoint<T>(stage: string, partition: string): Promise<Checkpoint<T> | null> {
        const rows = await this.client.query<{
            sequence: string;
            payload_json: string;
            payload_hash: string;
        }>(`
      SELECT DISTINCT sequence, payload_json, payload_hash
      FROM v1_bootstrap_checkpoints WHERE ${this.scope()} AND stage = ${sqlString(stage)}
        AND partition_key = ${sqlString(partition)} ORDER BY sequence DESC LIMIT 2`);
        if (!rows.length)
            return null;
        if (rows.length > 1 && rows[0].sequence === rows[1].sequence)
            throw new Error('Conflicting bootstrap checkpoint');
        if (await hashText(rows[0].payload_json) !== rows[0].payload_hash)
            throw new Error('Bootstrap checkpoint hash mismatch');
        return { sequence: uint64Number(rows[0].sequence), payload: JSON.parse(rows[0].payload_json) as T };
    }
    async putCheckpoint(stage: string, partition: string, sequence: number, payload: unknown): Promise<void> {
        const row = { ...this.base(), stage, partition_key: partition, sequence: uint64Number(sequence),
            payload_json: canonicalJson(payload), payload_hash: await hash(payload) };
        await this.writeVerified('v1_bootstrap_checkpoints', [row], `${this.scope()} AND stage = ${sqlString(stage)}
      AND partition_key = ${sqlString(partition)} AND sequence = ${sequence}`);
    }
    async checkpointPayloads<T>(stage: string, partition: string): Promise<T[]> {
        const values: T[] = [];
        let after = 0;
        for (;;) {
            const sql = `
        SELECT DISTINCT sequence, payload_json, payload_hash FROM v1_bootstrap_checkpoints
        WHERE ${this.scope()} AND stage = ${sqlString(stage)} AND partition_key = ${sqlString(partition)}
          AND sequence > ${after} ORDER BY sequence LIMIT ${this.scanRows}`;
            const rows = await this.client.query<{
                sequence: string;
                payload_json: string;
                payload_hash: string;
            }>(sql);
            if (!rows.length)
                return values;
            for (const row of rows) {
                const sequence = uint64Number(row.sequence);
                if (sequence !== after + 1 || await hashText(row.payload_json) !== row.payload_hash)
                    throw new Error('Bootstrap checkpoint chain has a gap or conflict');
                after = sequence;
                values.push(JSON.parse(row.payload_json) as T);
            }
        }
    }
    sourcePublisher(sequence: number, prior: SourceProgress | null): SourceReplayPublisher & { progress: SourceProgress | null } {
        let appended = false;
        const publisher: SourceReplayPublisher & { progress: SourceProgress | null } = {
            progress: prior,
            appendSources: async events => {
                if (this.bulk) await this.client.append('v1_bootstrap_events', await Promise.all(events.map(event => this.eventRow(event))));
                else await this.appendEvents(events);
                appended = true;
            },
            readSources: page => this.readEventsByContent(page.events, appended),
            advanceCheckpoint: async (page) => {
                const expected: SourceProgress = { cursor: page.next, eof: page.eof, contentHash: page.contentHash,
                    rows: (prior?.rows ?? 0) + page.events.length,
                    chainHash: await hash([prior?.chainHash ?? '', page.contentHash]) };
                if (canonicalJson(prior?.cursor ?? null) !== canonicalJson(page.after)) throw new Error('Source checkpoint cursor changed');
                // The immutable (stage, partition, sequence) row itself rejects a conflicting owner/retry.
                await this.putCheckpoint('sources', page.partition.table, sequence, expected);
                publisher.progress = expected;
            },
        };
        return publisher;
    }
    async appendEvents(events: NormalizedBrowserEvent[]): Promise<void> {
        const rows = await Promise.all(events.map(event => this.eventRow(event)));
        if (!rows.length)
            return;
        await this.writeVerified('v1_bootstrap_events', rows, `${this.scope()} AND ${sourceKeys(events)} AND event_hash IN (${sqlStrings(rows.map(row => row.event_hash))})`);
    }
    private async *readEventsByContent(events: NormalizedBrowserEvent[], wait = false): AsyncGenerator<NormalizedBrowserEvent> {
        if (!events.length)
            return;
        const hashes = await Promise.all(events.map(event => hash(event)));
        if (this.bulk) {
            const expected = await Promise.all(events.map(event => this.eventRow(event)));
            const read = await this.rowVerifier('v1_bootstrap_events', expected,
                `${this.scope()} AND ${sourceKeys(events)} AND event_hash IN (${sqlStrings(hashes)})`);
            const complete = wait ? await waitForReadback(read, value => value) : await read();
            if (complete) yield* events;
            return;
        }
        for await (const row of this.scan<{
            event_json: string;
        }>('v1_bootstrap_events', ['event_hash'], `${this.scope()} AND ${sourceKeys(events)} AND event_hash IN (${sqlStrings(hashes)})`, ['event_json']))
            yield JSON.parse(row.event_json) as NormalizedBrowserEvent;
    }
    async eventsByReferences(refs: EventReference[]): Promise<NormalizedBrowserEvent[]> {
        if (!refs.length)
            return [];
        const wanted = new Map(refs.map(ref => [ref.hash, ref]));
        if (wanted.size !== refs.length)
            throw new Error('Duplicate bootstrap event reference');
        const keys = refs.map(ref => `tuple(${sqlString(ref.source)},${sqlString(ref.kind)},${sqlString(ref.id)})`).join(',');
        const read = async () => {
            const rows = await this.client.query<{
                event_hash: string;
                event_json: string;
            }>(`SELECT DISTINCT event_hash,event_json FROM v1_bootstrap_events
        WHERE ${this.scope()} AND tuple(source_system,event_kind,source_record_id) IN (${keys}) AND event_hash IN (${sqlStrings([...wanted.keys()])}) LIMIT ${refs.length + 1}`);
            const result = new Map<string, NormalizedBrowserEvent>();
            for (const row of rows) {
                if (!wanted.has(row.event_hash) || result.has(row.event_hash) || await hashText(row.event_json) !== row.event_hash)
                    throw new Error('Bootstrap reference payload did not verify');
                result.set(row.event_hash, JSON.parse(row.event_json));
            }
            return result;
        };
        const found = await waitForReadback(read, rows => rows.size === wanted.size);
        return refs.map(ref => found.get(ref.hash)!);
    }
    async *indexedVisitors(entries: {
        visitorKey: string;
        count: number;
        hash: string;
    }[]): AsyncGenerator<CompleteVisitor> {
        for (const batch of completeGroups(entries, row => row.count, this.bulk ? 5000 : 200, this.bulk ? 10000 : 2000)) {
            const where = `${this.scope()} AND visitor_key IN (${sqlStrings(batch.map(row => row.visitorKey))})`;
            const heads = new Map(batch.map(row => [row.visitorKey, [] as PageRevision[]]));
            for await (const row of this.scan<{
                visitor_key: string;
                head_json: string;
            }>('v1_bootstrap_pages', ['visitor_key', 'page_id'], where, ['head_json', 'head_hash'], this.scanRows, this.countCoverage('v1_bootstrap_pages', ['visitor_key', 'page_id'], where, batch.reduce((sum, row) => sum + row.count, 0)))) {
                heads.get(row.visitor_key)!.push(JSON.parse(row.head_json));
            }
            for (const entry of batch)
                yield { tenantId: this.tenantId, visitorKey: entry.visitorKey, sourceSeal: this.sourceSeal,
                    heads: heads.get(entry.visitorKey)!, expectedHeadCount: entry.count, expectedHeadHash: entry.hash };
        }
    }
    async *indexedIdentityComponents(entries: {
        key: string;
        count: number;
    }[]): AsyncGenerator<{
        componentKey: string;
        facts: PendingIdentityFact[];
    }> {
        for (const batch of completeGroups(entries, row => row.count, this.bulk ? 5000 : 100, this.bulk ? 10000 : 2000)) {
            const where = `${this.scope()} AND phase = 'component' AND component_key IN (${sqlStrings(batch.map(row => row.key))})`;
            const facts = new Map(batch.map(row => [row.key, [] as PendingIdentityFact[]]));
            for await (const row of this.scan<{
                component_key: string;
                fact_json: string;
            }>('v1_bootstrap_identity_facts', ['component_key', 'fact_kind', 'fact_key'], where, ['fact_json', 'fact_hash'], this.scanRows, this.countCoverage('v1_bootstrap_identity_facts', ['fact_kind', 'fact_key'], where, batch.reduce((sum, row) => sum + row.count, 0))))
                facts.get(row.component_key)!.push(JSON.parse(row.fact_json));
            for (const entry of batch)
                yield { componentKey: entry.key, facts: facts.get(entry.key)! };
        }
    }
    async *eventVersions(source: string, afterKey?: [
        string,
        string
    ]): AsyncGenerator<NormalizedBrowserEvent> {
        const after = afterKey ? `AND tuple(event_kind, source_record_id) > tuple(${afterKey.map(sqlString).join(',')})` : '';
        for await (const row of this.scan<{
            event_json: string;
        }>('v1_bootstrap_events', ['event_kind', 'source_record_id', 'source_revision', 'event_hash'], `${this.scope()} AND source_system = ${sqlString(source)} ${after}`, ['event_json'])) {
            yield JSON.parse(row.event_json) as NormalizedBrowserEvent;
        }
    }
    async writeHeads(selection: 'source' | 'logical', events: NormalizedBrowserEvent[]): Promise<void> {
        const rows = await Promise.all(events.map(async (event) => ({ ...await this.eventRow(event), selection })));
        if (!rows.length)
            return;
        const keys = rows.map(row => `tuple(${sqlString(row.event_kind)},${sqlString(row.source_record_id)},${sqlString(row.source_system)})`).join(',');
        await this.writeVerified('v1_bootstrap_heads', rows, `${this.scope()} AND selection = ${sqlString(selection)}
      AND tuple(event_kind,source_record_id,source_system) IN (${keys})`);
    }
    async writeLogicalManifests(groups: NormalizedBrowserEvent[][]): Promise<void> {
        if (!groups.length)
            return;
        const rows = await Promise.all(groups.map(async (heads) => {
            const source = heads[0].source;
            const payload = { count: heads.length, hash: await hash(heads) };
            return { ...this.base(), manifest_kind: 'logical-heads',
                manifest_key: canonicalJson({ event_kind: source.event_kind, source_record_id: source.source_record_id }),
                payload_json: canonicalJson(payload), payload_hash: await hash(payload) };
        }));
        await this.writeVerified('v1_bootstrap_manifests', rows, `${this.scope()} AND manifest_kind = 'logical-heads'
      AND manifest_key IN (${sqlStrings(rows.map(row => row.manifest_key))})`);
    }
    async *heads(selection: 'source' | 'logical', afterKey?: [
        string,
        string
    ]): AsyncGenerator<NormalizedBrowserEvent> {
        const after = afterKey ? `AND tuple(event_kind,source_record_id) > tuple(${afterKey.map(sqlString).join(',')})` : '';
        for await (const row of this.scan<{
            event_json: string;
        }>('v1_bootstrap_heads', ['event_kind', 'source_record_id', 'source_system'], `${this.scope()} AND selection = ${sqlString(selection)} ${after}`, ['event_json', 'event_hash'])) {
            yield JSON.parse(row.event_json) as NormalizedBrowserEvent;
        }
    }
    async writePageHeads(rows: {
        visitorKey: string;
        head: PageRevision;
    }[]): Promise<void> {
        if (!rows.length)
            return;
        const records = await Promise.all(rows.map(async (row) => ({ ...this.base(), visitor_key: row.visitorKey,
            page_id: row.head.page_view_id, head_json: canonicalJson(row.head), head_hash: await hash(row.head) })));
        const keys = rows.map(row => `tuple(${sqlString(row.visitorKey)},${sqlString(row.head.page_view_id)})`).join(',');
        await this.writeVerified('v1_bootstrap_pages', records, `${this.scope()} AND tuple(visitor_key,page_id) IN (${keys})`);
    }
    async *visitors(afterVisitor?: string): AsyncGenerator<CompleteVisitor> {
        const after = afterVisitor ? `AND visitor_key > ${sqlString(afterVisitor)}` : '';
        let visitorKey: string | null = null;
        let heads: PageRevision[] = [];
        for await (const row of this.scan<{
            visitor_key: string;
            page_id: string;
            head_json: string;
        }>('v1_bootstrap_pages', ['visitor_key', 'page_id'], `${this.scope()} ${after}`, ['head_json', 'head_hash'])) {
            if (visitorKey !== null && visitorKey !== row.visitor_key) {
                yield await this.completeVisitor(visitorKey, heads);
                heads = [];
            }
            visitorKey = row.visitor_key;
            heads.push(JSON.parse(row.head_json) as PageRevision);
        }
        if (visitorKey !== null)
            yield await this.completeVisitor(visitorKey, heads);
    }
    async writeIdentityFacts(phase: 'selected' | 'component', facts: {
        fact: PendingIdentityFact;
        componentKey?: string;
    }[]): Promise<void> {
        if (!facts.length)
            return;
        const rows = await Promise.all(facts.map(async ({ fact, componentKey }) => ({ ...this.base(), phase,
            component_key: componentKey ?? '', fact_kind: fact.factKind, fact_key: fact.factKey,
            fact_json: canonicalJson(fact), fact_hash: await hash(fact) })));
        const keys = rows.map(row => `tuple(${sqlString(row.component_key)},${sqlString(row.fact_kind)},${sqlString(row.fact_key)})`).join(',');
        await this.writeVerified('v1_bootstrap_identity_facts', rows, `${this.scope()} AND phase = ${sqlString(phase)}
      AND tuple(component_key,fact_kind,fact_key) IN (${keys})`);
    }
    async *identityFacts(phase: 'selected' | 'component', after?: {
        kind?: string;
        key?: string;
        component?: string;
    }): AsyncGenerator<{
        fact: PendingIdentityFact;
        componentKey: string;
    }> {
        let condition = '';
        if (after?.component !== undefined)
            condition = `AND component_key > ${sqlString(after.component)}`;
        else if (after?.kind !== undefined && after.key !== undefined)
            condition = `AND tuple(fact_kind,fact_key) > tuple(${sqlString(after.kind)},${sqlString(after.key)})`;
        for await (const row of this.scan<{
            fact_json: string;
            component_key: string;
        }>('v1_bootstrap_identity_facts', ['component_key', 'fact_kind', 'fact_key'], `${this.scope()} AND phase = ${sqlString(phase)} ${condition}`, ['fact_json', 'fact_hash'])) {
            yield { fact: JSON.parse(row.fact_json) as PendingIdentityFact, componentKey: row.component_key };
        }
    }
    async writeMembers(chunk: SessionSeedChunk, visitors: Map<string, {
        count: number;
        hash: string;
    }>): Promise<void> {
        const rows = await Promise.all(chunk.group.members.map(async (member) => {
            const heads = visitors.get(member.visitor_key);
            if (!heads)
                throw new Error('Missing complete visitor metadata during bootstrap');
            return { ...this.base(), visitor_key: member.visitor_key, head_count: heads.count, head_hash: heads.hash,
                member_json: canonicalJson(member), member_hash: await hash(member) };
        }));
        await this.writeVerified('v1_bootstrap_members', rows, `${this.scope()} AND visitor_key IN (${sqlStrings(chunk.group.members.map(row => row.visitor_key))})`);
    }
    async publishSessions(chunk: SessionSeedChunk): Promise<void> {
        const records = new Map<string, typeof chunk.records>();
        for (const row of chunk.records) {
            const group = records.get(row.publication_id) ?? [];
            group.push(row);
            records.set(row.publication_id, group);
        }
        const snapshots: SessionSnapshot[] = chunk.group.members.map(member => ({
            tenant_id: this.tenantId, visitor_key: member.visitor_key, revision: member.revision,
            publication_id: member.publication_id,
            sessions: (records.get(member.publication_id) ?? []).map(row => JSON.parse(row.payload_json)),
        }));
        await new TinybirdSessionPublisher(this.client, { readPageRows: this.scanRows, maxVisitors: this.maxSessionVisitors }).publishSnapshots(snapshots);
        const groups = new TinybirdBrowserGroupPublisher(this.client, this.tenantId, { readPageRows: this.scanRows });
        const current = await groups.readGroup(this.tenantId, chunk.group.group_id);
        if (current && canonicalJson(current) !== canonicalJson(chunk.group))
            throw new Error('Bootstrap browser group conflicts');
        if (!current)
            await groups.appendGroup(chunk.group);
        await waitForReadback(() => groups.readGroup(this.tenantId, chunk.group.group_id), row => {
            if (row && canonicalJson(row) !== canonicalJson(chunk.group))
                throw new Error('Bootstrap browser group conflicts');
            return row !== null;
        });
    }
    baselineStore(): SealedBaselineStore {
        let verifiedSeal: Promise<MembershipSeal> | undefined;
        const readSeal = async (): Promise<MembershipSeal> => {
            if (!verifiedSeal)
                verifiedSeal = (async () => {
                    const seal = await this.requiredManifest<{
                        sourceSeal: string;
                        membership?: MembershipSeal;
                    }>('seal', 'complete');
                    if (seal.sourceSeal !== this.sourceSeal)
                        throw new Error('Bootstrap baseline seal mismatch');
                    validateMembershipSeal(seal.membership);
                    return seal.membership;
                })();
            try {
                return await verifiedSeal;
            }
            catch (error) {
                verifiedSeal = undefined;
                throw error;
            }
        };
        const provenMembers = async (visitorKeys: string[]) => {
            const result: Awaited<ReturnType<TinybirdBootstrapStorage['memberRows']>> = [];
            for (let position = 0; position < visitorKeys.length; position += 200) {
                const present = await proveMembership(this, await readSeal(), 'visitor', visitorKeys.slice(position, position + 200));
                if (!present.size)
                    continue;
                result.push(...await waitForReadback(() => this.memberRows([...present]), rows => rows.length === present.size));
            }
            return result;
        };
        return {
            requireSeal: async (sourceSeal) => {
                if (sourceSeal !== this.sourceSeal)
                    throw new Error('Bootstrap baseline seal mismatch');
                await readSeal();
            },
            sourceHeads: async (keys) => {
                if (!keys.length)
                    return [];
                const present = await proveMembership(this, await readSeal(), 'source', keys.map(canonicalJson));
                const result = new Map(keys.map(key => [canonicalJson(key), { key, heads: [] as NormalizedBrowserEvent[] }]));
                if (!present.size)
                    return Promise.all([...result.values()].map(async (row) => ({ ...row, expectedCount: 0, expectedHash: await hash([]) })));
                const metadata = await this.requiredManifests<{
                    count: number;
                    hash: string;
                }>('logical-heads', [...present]);
                const values = keys.filter(key => present.has(canonicalJson(key))).map(key => `tuple(${sqlString(key.event_kind)},${sqlString(key.source_record_id)})`).join(',');
                const where = `${this.scope()} AND selection = 'source' AND tuple(event_kind,source_record_id) IN (${values})`;
                const expected = [...metadata.values()].reduce((total, row) => total + row.count, 0);
                for await (const row of this.scan<{
                    event_kind: string;
                    source_record_id: string;
                    event_json: string;
                }>('v1_bootstrap_heads', ['event_kind', 'source_record_id', 'source_system'], where, ['event_json', 'event_hash'], 500, this.countCoverage('v1_bootstrap_heads', ['event_kind', 'source_record_id', 'source_system'], where, expected))) {
                    result.get(canonicalJson({ event_kind: row.event_kind, source_record_id: row.source_record_id }))!.heads.push(JSON.parse(row.event_json));
                }
                return Promise.all([...result.values()].map(async (row) => {
                    const expected = metadata.get(canonicalJson(row.key));
                    if (!expected && row.heads.length)
                        throw new Error('Missing source head manifest');
                    return { ...row, expectedCount: expected?.count ?? 0, expectedHash: expected?.hash ?? await hash([]) };
                }));
            },
            members: async (visitorKeys) => {
                const rows = await provenMembers(visitorKeys);
                return rows.map(row => JSON.parse(row.member_json) as SeedMember);
            },
            visitorHeads: async (tenantId, visitorKey) => {
                if (tenantId !== this.tenantId)
                    throw new Error('Bootstrap baseline tenant mismatch');
                const members = await provenMembers([visitorKey]);
                if (!members.length)
                    return null;
                if (members.length !== 1)
                    throw new Error('Conflicting baseline visitor metadata');
                const heads: PageRevision[] = [];
                for await (const row of this.scan<{
                    head_json: string;
                }>('v1_bootstrap_pages', ['page_id'], `${this.scope()} AND visitor_key = ${sqlString(visitorKey)}`, ['head_json', 'head_hash'], 500, this.countCoverage('v1_bootstrap_pages', ['page_id'], `${this.scope()} AND visitor_key = ${sqlString(visitorKey)}`, Number(members[0].head_count))))
                    heads.push(JSON.parse(row.head_json));
                return { tenantId, visitorKey, sourceSeal: this.sourceSeal, heads,
                    expectedHeadCount: Number(members[0].head_count), expectedHeadHash: members[0].head_hash };
            },
            sessionRecords: async (member) => new TinybirdSessionPublisher(this.client).readRecords({
                tenant_id: this.tenantId, visitor_key: member.visitor_key, revision: member.revision,
                publication_id: member.publication_id, row_count: member.records.length, content_hash: '',
            }),
        };
    }
    private async memberRows(visitorKeys: string[]): Promise<{
        visitor_key: string;
        head_count: number;
        head_hash: string;
        member_json: string;
    }[]> {
        if (!visitorKeys.length)
            return [];
        const result = [];
        for await (const row of this.scan<{
            visitor_key: string;
            head_count: number;
            head_hash: string;
            member_json: string;
            member_hash: string;
        }>('v1_bootstrap_members', ['visitor_key'], `${this.scope()} AND visitor_key IN (${sqlStrings(visitorKeys)})`, ['head_count', 'head_hash', 'member_json', 'member_hash'])) {
            if (await hashText(row.member_json) !== row.member_hash)
                throw new Error('Bootstrap member hash mismatch');
            result.push(row);
        }
        return result;
    }
    async totals(): Promise<{
        pages: number;
        visitors: number;
        identityFacts: number;
    }> {
        const [pages] = await this.client.query<{
            pages: number;
            visitors: number;
        }>(`
      SELECT uniqExact(tuple(visitor_key,page_id)) AS pages, uniqExact(visitor_key) AS visitors
      FROM v1_bootstrap_pages WHERE ${this.scope()}`);
        const [identity] = await this.client.query<{
            facts: number;
        }>(`
      SELECT uniqExact(tuple(fact_kind,fact_key)) AS facts FROM v1_bootstrap_identity_facts
      WHERE ${this.scope()} AND phase = 'selected'`);
        return { pages: Number(pages.pages), visitors: Number(pages.visitors), identityFacts: Number(identity.facts) };
    }
    async writeVerified(table: string, expected: JsonRecord[], where: string): Promise<void> {
        if (!expected.length)
            return;
        const read = await this.rowVerifier(table, expected, where);
        if (await read()) return;
        await this.client.append(table, expected);
        await waitForReadback(read, matches => matches);
    }
    private async rowVerifier(table: string, expected: JsonRecord[], where: string): Promise<() => Promise<boolean>> {
        const payloads = new Set(this.bulk ? Object.keys(expected[0]).filter(name => name.endsWith('_json')) : []);
        const proof = async (row: JsonRecord) => Object.fromEntries(await Promise.all(Object.entries(row).map(async ([name, value]) =>
            [name, payloads.has(name) ? (await hashText(String(value))).toUpperCase() : value])));
        const texts = new Set((await Promise.all(expected.map(proof))).map(canonicalJson));
        if (texts.size !== expected.length)
            throw new Error('Bootstrap write contains duplicate records');
        const read = async () => {
            // Hash the actual stored payload in the query. A separately uploaded hash is not proof.
            const actual = await this.client.query<JsonRecord>(`SELECT DISTINCT ${Object.keys(expected[0]).map(name => payloads.has(name) ? `hex(SHA256(\`${name}\`)) AS ${name}` : `\`${name}\``).join(',')}
        FROM ${table} WHERE ${where} LIMIT ${expected.length + 1}`);
            return actual.map(row => normalizeNumbers(row, expected[0])).map(canonicalJson);
        };
        const matches = (actual: string[]) => {
            if (actual.some(row => !texts.has(row)))
                throw new Error(`Conflicting immutable bootstrap rows in ${table}`);
            return new Set(actual).size === texts.size;
        };
        return async () => matches(await read());
    }
    private async eventRow(event: NormalizedBrowserEvent): Promise<EventRow> {
        if (event.source.tenant_id !== this.tenantId)
            throw new Error('Bootstrap source tenant mismatch');
        return { baseline_id: this.baselineId, tenant_id: this.tenantId, source_system: event.source.source_system,
            event_kind: event.source.event_kind, source_record_id: event.source.source_record_id,
            source_priority: event.source.source_priority, source_revision: uint64Number(event.source.source_revision),
            event_hash: await hash(event), event_json: canonicalJson(event) };
    }
    private async completeVisitor(visitorKey: string, heads: PageRevision[]): Promise<CompleteVisitor> {
        return { tenantId: this.tenantId, visitorKey, sourceSeal: this.sourceSeal,
            heads, expectedHeadCount: heads.length, expectedHeadHash: await hash(heads) };
    }
    /** Stable scalar keys permit keyset pagination. A conflicting duplicate logical key stops the scan. */
    async *scan<T>(table: string, keys: string[], where: string, values: string[] = [], limit = this.scanRows, coverage?: ScanCoverage): AsyncGenerator<T> {
        let cursor: unknown[] | null = null;
        for (;;) {
            const after = cursor ? `AND tuple(${keys.join(',')}) > tuple(${cursor.map((value, index) => {
                if (['sequence', 'source_revision', 'producer_sequence', 'bucket'].includes(keys[index]))
                    return `toUInt64(${sqlString(String(value))})`;
                if (keys[index] === 'ingested_at')
                    return `toDateTime64(${sqlString(String(value))},6)`;
                return sqlString(String(value));
            }).join(',')})` : '';
            const fields = [...new Set([...keys, ...values])];
            const sql = `SELECT DISTINCT ${fields.join(',')} FROM ${table} WHERE ${where} ${after} ORDER BY ${keys.join(',')} LIMIT ${limit + 1}`;
            const rows: JsonRecord[] = coverage ? await this.coveredPage(sql, fields, coverage) : await this.client.query<JsonRecord>(sql);
            if (!rows.length)
                return;
            if (this.bulk && limit > 1) {
                const bytes = new TextEncoder().encode(JSON.stringify(rows)).byteLength;
                if (bytes > this.maxWaveBytes) {
                    limit = Math.max(1, Math.floor(limit * this.maxWaveBytes / bytes));
                    continue;
                }
            }
            if (rows.length > limit && canonicalJson(keys.map(key => rows[limit - 1][key])) === canonicalJson(keys.map(key => rows[limit][key]))) {
                throw new Error('Conflicting logical key at a bootstrap query page boundary');
            }
            let prior: unknown[] | null = cursor;
            for (const row of rows.slice(0, limit)) {
                const key = keys.map(name => row[name]);
                if (prior && canonicalJson(prior) === canonicalJson(key))
                    throw new Error('Conflicting logical key in bootstrap scan');
                prior = key;
                yield row as T;
            }
            if (rows.length <= limit)
                return;
            cursor = prior;
        }
    }
}
function* completeGroups<T>(entries: T[], size: (entry: T) => number, maxKeys: number, maxRows: number): Generator<T[]> {
    let batch: T[] = [];
    let rows = 0;
    for (const entry of entries) {
        if (batch.length && (batch.length >= maxKeys || rows + size(entry) > maxRows)) {
            yield batch;
            batch = [];
            rows = 0;
        }
        batch.push(entry);
        rows += size(entry);
    }
    if (batch.length) yield batch;
}
function sourceKeys(events: NormalizedBrowserEvent[]): string {
    return `tuple(source_system,event_kind,source_record_id) IN (${events.map(event =>
        `tuple(${sqlString(event.source.source_system)},${sqlString(event.source.event_kind)},${sqlString(event.source.source_record_id)})`).join(',')})`;
}
function normalizeNumbers(row: JsonRecord, template: JsonRecord): JsonRecord {
    return Object.fromEntries(Object.entries(row).map(([key, value]) => [key, typeof template[key] === 'number' ? uint64Number(value as string | number) : value]));
}
