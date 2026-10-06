import { canonicalJson, compareStrings } from '../sessions/session-engine.ts';
import { sqlString, sqlStrings, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import type { TinybirdBootstrapStorage } from './tinybird-storage.ts';
import { hash, hashText } from './hash.ts';
const BUCKETS = 16384;
const PAGE_SIZE = 256;
const MAX_BUCKET_BYTES = 128000;
type Index = 'source' | 'visitor';
interface Descriptor {
    count: number;
    hash: string;
}
interface DescriptorPage {
    index: Index;
    page: number;
    buckets: Descriptor[];
}
export interface MembershipSeal {
    version: 1;
    buckets: number;
    pageSize: number;
    pages: {
        source: string[];
        visitor: string[];
    };
}
export async function membershipBucket(key: string): Promise<number> {
    return parseInt((await hashText(key)).slice(0, 4), 16) % BUCKETS;
}
/** One metadata pass at bootstrap. Runtime lookups never scan historical membership. */
export async function buildMembershipProofs(store: TinybirdBootstrapStorage, expected: Record<Index, number>): Promise<MembershipSeal> {
    const pages = { source: [] as string[], visitor: [] as string[] };
    for (const index of ['source', 'visitor'] as const) {
        await indexKeys(store, index, expected[index]);
        pages[index] = await descriptorPages(store, index, expected[index]);
    }
    return { version: 1, buckets: BUCKETS, pageSize: PAGE_SIZE, pages };
}
async function indexKeys(store: TinybirdBootstrapStorage, index: Index, expected: number, attempt = 0): Promise<void> {
    if (await store.getManifest('stage', `membership-index:${index}`))
        return;
    if (store.bulk) {
        await bulkIndexKeys(store, index, expected);
        return;
    }
    const saved = await store.checkpoint<{
        cursor: string[];
        rows: number;
    }>('membership-index', index);
    const source = index === 'source';
    const table = source ? 'v1_bootstrap_heads' : 'v1_bootstrap_members';
    const columns = source ? ['event_kind', 'source_record_id'] : ['visitor_key'];
    const where = `${store.scope()}${source ? " AND selection = 'logical'" : ''}`;
    const after = saved?.payload.cursor.length ? ` AND tuple(${columns.join(',')}) > tuple(${saved.payload.cursor.map(sqlString).join(',')})` : '';
    let rows = saved?.payload.rows ?? 0;
    let sequence = (saved?.sequence ?? 0) + 1;
    let batch: JsonRecord[] = [];
    let cursor: string[] = [];
    const flush = async () => {
        if (!batch.length)
            return;
        await store.writeVerified('v1_bootstrap_membership_keys', batch, `${store.scope()} AND index_kind = ${sqlString(index)}
      AND member_key IN (${sqlStrings(batch.map(row => String(row.member_key)))})`);
        rows += batch.length;
        await store.putCheckpoint('membership-index', index, sequence++, { cursor, rows });
        batch = [];
    };
    for await (const row of store.scan<JsonRecord>(table, columns, where + after)) {
        cursor = columns.map(column => String(row[column]));
        const key = source ? canonicalJson({ event_kind: cursor[0], source_record_id: cursor[1] }) : cursor[0];
        batch.push({ baseline_id: store.baselineId, tenant_id: store.tenantId, index_kind: index, bucket: await membershipBucket(key), member_key: key });
        if (batch.length === store.writeBatchRows)
            await flush();
    }
    await flush();
    if (rows !== expected) {
        if (rows > expected || attempt >= 3)
            throw new Error('Membership index is incomplete');
        await store.putCheckpoint('membership-index', index, sequence, { cursor: [], rows: 0 });
        return indexKeys(store, index, expected, attempt + 1);
    }
    await store.putManifest('stage', `membership-index:${index}`, { keys: rows });
}
/** Group the one-time writes by the physical bucket prefix, so readback avoids scattered historical scans. */
async function bulkIndexKeys(store: TinybirdBootstrapStorage, index: Index, expected: number): Promise<void> {
    const source = index === 'source';
    const columns = source ? ['event_kind', 'source_record_id'] : ['visitor_key'];
    const table = source ? 'v1_bootstrap_heads' : 'v1_bootstrap_members';
    const where = `${store.scope()}${source ? " AND selection = 'logical'" : ''}`;
    const groups = await waitForReadback(async () => {
        const buckets = new Map<number, string[]>();
        let count = 0;
        for await (const row of store.scan<JsonRecord>(table, columns, where)) {
            const key = source ? canonicalJson({ event_kind: row.event_kind, source_record_id: row.source_record_id }) : String(row.visitor_key);
            const bucket = await membershipBucket(key);
            const keys = buckets.get(bucket) ?? [];
            keys.push(key);
            buckets.set(bucket, keys);
            count++;
        }
        if (count > expected) throw new Error('Membership source has unexpected keys');
        return { buckets, count };
    }, result => result.count === expected);
    const saved = await store.checkpoint<{ bucket: number; key: string; rows: number }>('membership-index', index);
    let rows = saved?.payload.rows ?? 0;
    let sequence = (saved?.sequence ?? 0) + 1;
    let batch: JsonRecord[] = [];
    const flush = async () => {
        if (!batch.length) return;
        const keys = batch.map(row => `tuple(${row.bucket},${sqlString(String(row.member_key))})`).join(',');
        await store.writeVerified('v1_bootstrap_membership_keys', batch,
            `${store.scope()} AND index_kind = ${sqlString(index)} AND tuple(bucket,member_key) IN (${keys})`);
        rows += batch.length;
        const last = batch.at(-1)!;
        await store.putCheckpoint('membership-index', index, sequence++, { bucket: last.bucket, key: last.member_key, rows });
        batch = [];
    };
    for (const bucket of [...groups.buckets.keys()].sort((a, b) => a - b)) {
        for (const key of groups.buckets.get(bucket)!.sort(compareStrings)) {
            if (saved && (bucket < saved.payload.bucket || (bucket === saved.payload.bucket && compareStrings(key, saved.payload.key) <= 0))) continue;
            batch.push({ baseline_id: store.baselineId, tenant_id: store.tenantId, index_kind: index, bucket, member_key: key });
            if (batch.length >= store.writeBatchRows) await flush();
        }
    }
    await flush();
    if (rows !== expected) throw new Error('Membership index receipt is incomplete');
    await store.putManifest('stage', `membership-index:${index}`, { keys: rows });
}
async function descriptorPages(store: TinybirdBootstrapStorage, index: Index, expected: number): Promise<string[]> {
    const saved = await store.getManifest<{
        keys: number;
        hashes: string[];
    }>('membership-root', index);
    if (saved) {
        if (saved.keys !== expected)
            throw new Error('Membership root count changed');
        return saved.hashes;
    }
    const where = `${store.scope()} AND index_kind = ${sqlString(index)}`;
    const complete = await waitForReadback(async () => {
        const groups = new Map<number, string[]>();
        let count = 0;
        for await (const row of buckets(store, index, where)) {
            groups.set(row.bucket, row.keys);
            count += row.keys.length;
        }
        if (count > expected)
            throw new Error('Membership scan contains unexpected keys');
        return { groups, count };
    }, result => result.count === expected);
    const emptyHash = await hash([]);
    const descriptors: {
        key: string;
        payload: DescriptorPage;
    }[] = [];
    const hashes: string[] = [];
    for (let page = 0; page < BUCKETS / PAGE_SIZE; page++) {
        const descriptor: DescriptorPage = { index, page, buckets: [] };
        const leaves: {
            key: string;
            payload: {
                keys: string[];
            };
        }[] = [];
        for (let offset = 0; offset < PAGE_SIZE; offset++) {
            const bucket = page * PAGE_SIZE + offset;
            const members = complete.groups.get(bucket);
            if (!members) {
                descriptor.buckets.push({ count: 0, hash: emptyHash });
                continue;
            }
            descriptor.buckets.push({ count: members.length, hash: await hash(members) });
            leaves.push({ key: `${index}:${bucket}`, payload: { keys: members } });
        }
        await store.putManifests('membership-bucket', leaves);
        descriptors.push({ key: `${index}:${page}`, payload: descriptor });
        hashes.push(await hash(descriptor));
    }
    await store.putManifests('membership-page', descriptors);
    await store.putManifest('membership-root', index, { keys: expected, hashes });
    return hashes;
}
async function* buckets(store: TinybirdBootstrapStorage, index: Index, where: string): AsyncGenerator<{
    bucket: number;
    keys: string[];
}> {
    let current: number | null = null;
    let keys: string[] = [];
    let bytes = 2;
    for await (const row of store.scan<{
        bucket: number;
        member_key: string;
    }>('v1_bootstrap_membership_keys', ['bucket', 'member_key'], where)) {
        const bucket = Number(row.bucket);
        if (current !== null && current !== bucket) {
            yield { bucket: current, keys };
            keys = [];
            bytes = 2;
        }
        current = bucket;
        if (await membershipBucket(row.member_key) !== bucket)
            throw new Error('Membership key is misbucketed');
        bytes += new TextEncoder().encode(JSON.stringify(row.member_key)).byteLength + 1;
        if (bytes > MAX_BUCKET_BYTES)
            throw new Error('Membership bucket exceeds its bounded-reader size; increase the bucket count in a new baseline');
        keys.push(row.member_key);
    }
    if (current !== null)
        yield { bucket: current, keys };
}
/** The final seal authenticates every descriptor, which authenticates a full bucket. */
export async function proveMembership(store: TinybirdBootstrapStorage, seal: MembershipSeal, index: Index, keys: string[]): Promise<Set<string>> {
    validateMembershipSeal(seal);
    if (keys.length > 200)
        throw new Error('Baseline membership lookup exceeds 200 keys');
    const requested = new Set(keys);
    if (requested.size !== keys.length)
        throw new Error('Duplicate membership lookup key');
    const bucketsByKey = new Map(await Promise.all(keys.map(async (key) => [key, await membershipBucket(key)] as const)));
    const wantedBuckets = [...new Set(bucketsByKey.values())].sort((a, b) => a - b);
    const pages = [...new Set(wantedBuckets.map(bucket => Math.floor(bucket / PAGE_SIZE)))];
    const descriptors = new Map<number, Descriptor>();
    for (let position = 0; position < pages.length; position += 16) {
        const group = pages.slice(position, position + 16);
        const records = await store.requiredManifests<DescriptorPage>('membership-page', group.map(page => `${index}:${page}`));
        for (const page of group) {
            const record = records.get(`${index}:${page}`)!;
            if (record.index !== index || record.page !== page || record.buckets.length !== PAGE_SIZE || await hash(record) !== seal.pages[index][page])
                throw new Error('Membership descriptor is not authenticated by the final seal');
            for (const bucket of wantedBuckets)
                if (Math.floor(bucket / PAGE_SIZE) === page)
                    descriptors.set(bucket, record.buckets[bucket % PAGE_SIZE]);
        }
    }
    const positiveBuckets = wantedBuckets.filter(bucket => {
        const descriptor = descriptors.get(bucket)!;
        if (!Number.isSafeInteger(descriptor.count) || descriptor.count < 0 || !/^[a-f0-9]{64}$/.test(descriptor.hash))
            throw new Error('Invalid membership descriptor');
        return descriptor.count > 0;
    });
    const found = new Set<string>();
    // At most 16 * 128 KB of bucket payloads are retained, even for 200 unrelated keys.
    for (let position = 0; position < positiveBuckets.length; position += 16) {
        const group = positiveBuckets.slice(position, position + 16);
        const records = await store.requiredManifests<{
            keys: string[];
        }>('membership-bucket', group.map(bucket => `${index}:${bucket}`));
        for (const bucket of group) {
            const record = records.get(`${index}:${bucket}`)!;
            const descriptor = descriptors.get(bucket)!;
            if (!Array.isArray(record.keys) || record.keys.length !== descriptor.count || await hash(record.keys) !== descriptor.hash
                || new Set(record.keys).size !== record.keys.length || new TextEncoder().encode(canonicalJson(record.keys)).byteLength > MAX_BUCKET_BYTES)
                throw new Error('Membership bucket is incomplete or conflicts with its descriptor');
            for (const key of record.keys)
                if (requested.has(key)) {
                    if (bucketsByKey.get(key) !== bucket)
                        throw new Error('Membership proof returned a misplaced key');
                    found.add(key);
                }
        }
    }
    return found;
}
export function validateMembershipSeal(seal: MembershipSeal | undefined): asserts seal is MembershipSeal {
    if (!seal || seal.version !== 1 || seal.buckets !== BUCKETS || seal.pageSize !== PAGE_SIZE
        || (['source', 'visitor'] as const).some(index => seal.pages[index]?.length !== BUCKETS / PAGE_SIZE || seal.pages[index].some(hash => !/^[a-f0-9]{64}$/.test(hash)))) {
        throw new Error('Bootstrap baseline lacks authenticated membership proofs');
    }
}
