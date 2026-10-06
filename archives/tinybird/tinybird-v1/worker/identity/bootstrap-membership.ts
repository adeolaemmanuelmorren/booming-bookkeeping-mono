import { canonicalJson, sha256 } from '../storage/json.ts';
import { sqlString, sqlStrings, uint64Number, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import { compareStrings } from '../sessions/session-engine.ts';
import { IdentityBootstrapStore, MANIFESTS_TABLE } from './bootstrap-store.ts';
import type { IdentityRecord } from './storage.ts';
import type { PendingIdentityFact } from './engine.ts';

const BUCKETS = 65536;
const PAGE_SIZE = 256;
const MAX_BUCKET_BYTES = 128_000;
export const SCOPES_TABLE = 'v1_identity_bootstrap_scopes';
export interface LookupProof { key: string; count: number; hash: string }
interface Descriptor { count: number; hash: string }
interface DescriptorPage { page: number; buckets: Descriptor[] }
export interface IdentityMembership { version: 1; buckets: 65536; pageSize: 256; pageHashes: string[] }
export interface ScopeRecord extends JsonRecord { scope_id: string; fact_kind: string; fact_key: string; fact_json: string; fact_hash: string }

export function lookupKey(kind: string, key: string): string { return canonicalJson([kind, key]); }
export async function lookupBucket(key: string): Promise<number> { return parseInt((await sha256(key)).slice(0, 4), 16); }
export async function recordsHash(rows: JsonRecord[]): Promise<string> {
  return sha256([...new Set(rows.map(canonicalJson))].sort(compareStrings).join('\n'));
}

/** Every lookup in a complete component is wholly contained in its publication batch. */
export async function recordLookupProofs(store: IdentityBootstrapStore, records: IdentityRecord[]): Promise<number> {
  const groups = new Map<string, IdentityRecord[]>();
  for (const row of records) {
    const key = lookupKey(row.state_kind, row.lookup_key);
    const group = groups.get(key) ?? [];
    group.push(row); groups.set(key, group);
  }
  const proofs = await Promise.all([...groups].map(async ([key, rows]) => ({ key, count: rows.length, hash: await recordsHash(rows) })));
  await writeLookupProofs(store, proofs);
  return proofs.length;
}

export async function writeLookupProofs(store: IdentityBootstrapStore, proofs: LookupProof[]): Promise<void> {
  const values = await Promise.all(proofs.map(async proof => ({
    key: `${String(await lookupBucket(proof.key)).padStart(5, '0')}:${proof.key}`, payload: proof,
  })));
  for (let offset = 0; offset < values.length; offset += 20000) await store.putMany('lookup', values.slice(offset, offset + 20000));
}

/** One ordered pass, holding at most one bucket plus its descriptor page. */
export async function buildIdentityMembership(store: IdentityBootstrapStore, expectedLookups: number): Promise<IdentityMembership> {
  const existing = await store.get<{ expectedLookups: number; membership: IdentityMembership }>('membership', 'complete');
  if (existing) {
    if (existing.payload.expectedLookups !== expectedLookups) throw new Error('Identity membership count changed');
    return existing.payload.membership;
  }
  const emptyHash = await sha256(canonicalJson([]));
  const descriptors: Descriptor[] = Array.from({ length: BUCKETS }, () => ({ count: 0, hash: emptyHash }));
  let current = -1;
  let entries: LookupProof[] = [];
  let count = 0;
  let leaves: { key: string; payload: { entries: LookupProof[] } }[] = [];
  const flushBucket = async () => {
    if (current < 0) return;
    entries.sort((a, b) => compareStrings(a.key, b.key));
    if (new Set(entries.map(row => row.key)).size !== entries.length || bytes(entries) > MAX_BUCKET_BYTES) throw new Error('Identity proof bucket exceeds its bound or has duplicate keys');
    descriptors[current] = { count: entries.length, hash: await sha256(canonicalJson(entries)) };
    leaves.push({ key: `${current}:${descriptors[current].hash}`, payload: { entries } });
    if (leaves.length >= 128) { await store.putMany('membership-bucket', leaves); leaves = []; }
  };
  for await (const row of store.scan<JsonRecord>(MANIFESTS_TABLE, ['manifest_key'], `${store.scope()} AND manifest_kind='lookup' AND sequence=0`, 'manifest_key,payload_json,payload_hash')) {
    const proof = JSON.parse(String(row.payload_json)) as LookupProof;
    if (await sha256(String(row.payload_json)) !== row.payload_hash) throw new Error('Identity lookup proof hash mismatch');
    validateLookup(proof);
    const bucket = await lookupBucket(proof.key);
    if (String(row.manifest_key) !== `${String(bucket).padStart(5, '0')}:${proof.key}`) throw new Error('Identity lookup proof is misbucketed');
    if (bucket !== current) { await flushBucket(); current = bucket; entries = []; }
    entries.push(proof); count++;
    if (count > expectedLookups) throw new Error('Unexpected identity proof keys');
  }
  await flushBucket();
  if (count !== expectedLookups) throw new Error('Identity lookup proofs are not fully visible; retry the stage');
  await store.putMany('membership-bucket', leaves);
  const pages = [];
  const pageHashes: string[] = [];
  for (let page = 0; page < BUCKETS / PAGE_SIZE; page++) {
    const payload: DescriptorPage = { page, buckets: descriptors.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE) };
    const pageHash = await sha256(canonicalJson(payload));
    pages.push({ key: `${page}:${pageHash}`, payload }); pageHashes.push(pageHash);
  }
  for (let offset = 0; offset < pages.length; offset += 128) await store.putMany('membership-page', pages.slice(offset, offset + 128));
  const membership: IdentityMembership = { version: 1, buckets: BUCKETS, pageSize: PAGE_SIZE, pageHashes };
  await store.put('membership', 'complete', { expectedLookups, membership });
  return membership;
}

export function validateIdentityMembership(value: IdentityMembership): void {
  if (!value || value.version !== 1 || value.buckets !== BUCKETS || value.pageSize !== PAGE_SIZE
    || value.pageHashes?.length !== BUCKETS / PAGE_SIZE || value.pageHashes.some(hash => !/^[a-f0-9]{64}$/.test(hash))) throw new Error('Identity membership seal is invalid');
}

/** Missing metadata retries. Only a complete authenticated bucket can prove absence. */
export async function proveIdentityLookups(store: IdentityBootstrapStore, membership: IdentityMembership, keys: string[]): Promise<Map<string, LookupProof | null>> {
  validateIdentityMembership(membership);
  if (keys.length > 200 || new Set(keys).size !== keys.length) throw new Error('Identity lookup proof request exceeds its bound or repeats keys');
  const result = new Map<string, LookupProof | null>();
  const buckets = new Map(await Promise.all(keys.map(async key => [key, await lookupBucket(key)] as const)));
  const pageIds = [...new Set([...buckets.values()].map(bucket => Math.floor(bucket / PAGE_SIZE)))];
  const descriptors = new Map<number, Descriptor>();
  for (let offset = 0; offset < pageIds.length; offset += 16) {
    const ids = pageIds.slice(offset, offset + 16);
    const pages = await store.requireMany<DescriptorPage>('membership-page', ids.map(page => `${page}:${membership.pageHashes[page]}`));
    for (const page of ids) {
      const value = pages.get(`${page}:${membership.pageHashes[page]}`)!;
      if (value.page !== page || value.buckets.length !== PAGE_SIZE || await sha256(canonicalJson(value)) !== membership.pageHashes[page]) throw new Error('Identity descriptor is not authenticated by its seal');
      for (const bucket of buckets.values()) if (Math.floor(bucket / PAGE_SIZE) === page) descriptors.set(bucket, value.buckets[bucket % PAGE_SIZE]);
    }
  }
  const positive: number[] = [];
  const emptyHash = await sha256(canonicalJson([]));
  for (const bucket of new Set(buckets.values())) {
    const descriptor = descriptors.get(bucket)!;
    if (!Number.isSafeInteger(descriptor.count) || descriptor.count < 0 || !/^[a-f0-9]{64}$/.test(descriptor.hash)) throw new Error('Invalid identity bucket descriptor');
    if (descriptor.count === 0) {
      if (descriptor.hash !== emptyHash) throw new Error('Invalid empty identity bucket');
      for (const [key, value] of buckets) if (value === bucket) result.set(key, null);
    } else positive.push(bucket);
  }
  for (let offset = 0; offset < positive.length; offset += 16) {
    const ids = positive.slice(offset, offset + 16);
    const leaves = await store.requireMany<{ entries: LookupProof[] }>('membership-bucket', ids.map(bucket => `${bucket}:${descriptors.get(bucket)!.hash}`));
    for (const bucket of ids) {
      const entries = leaves.get(`${bucket}:${descriptors.get(bucket)!.hash}`)!.entries;
      const descriptor = descriptors.get(bucket)!;
      if (!Array.isArray(entries) || entries.length !== descriptor.count || bytes(entries) > MAX_BUCKET_BYTES
        || new Set(entries.map(row => row.key)).size !== entries.length || await sha256(canonicalJson(entries)) !== descriptor.hash) throw new Error('Identity membership bucket is incomplete');
      for (const entry of entries) if (await lookupBucket(entry.key) !== bucket) throw new Error('Identity membership entry is misbucketed');
      const found = new Map(entries.map(entry => { validateLookup(entry); return [entry.key, entry] as const; }));
      for (const [key, value] of buckets) if (value === bucket) result.set(key, found.get(key) ?? null);
    }
  }
  return result;
}

/** Baseline rows can be loaded even before their commit row is visible on a read replica. */
export async function readIdentityBaselineRecords(store: IdentityBootstrapStore, membership: IdentityMembership, kind: string, keys: string[]): Promise<IdentityRecord[]> {
  const proofs = await proveIdentityLookups(store, membership, keys.map(key => lookupKey(kind, key)));
  const wanted = keys.filter(key => proofs.get(lookupKey(kind, key)) !== null);
  if (!wanted.length) return [];
  requireBoundedRows([...proofs.values()]);
  return waitForReadback(async () => {
    const rows = await store.client.query<IdentityRecord>(`SELECT DISTINCT tenant_id,state_kind,state_key,lookup_key,sub_key,
      toString(records.batch_version) AS batch_version,batch_id,is_deleted,payload_json FROM v1_identity_records AS records
      WHERE tenant_id=${sqlString(store.tenantId)} AND records.batch_version=1 AND state_kind=${sqlString(kind)} AND lookup_key IN (${sqlStrings(wanted)})`);
    for (const row of rows) row.is_deleted = uint64Number(row.is_deleted);
    const groups = new Map(wanted.map(key => [key, [] as IdentityRecord[]]));
    for (const row of rows) {
      if (!groups.has(row.lookup_key)) throw new Error('Unexpected identity baseline lookup');
      groups.get(row.lookup_key)!.push(row);
    }
    let complete = true;
    for (const [key, records] of groups) {
      const proof = proofs.get(lookupKey(kind, key))!;
      if (records.length > proof.count || (records.length === proof.count && await recordsHash(records) !== proof.hash)) throw new Error('Identity baseline records conflict with the sealed proof');
      complete &&= records.length === proof.count;
    }
    return { rows, complete };
  }, result => result.complete).then(result => result.rows);
}

/** Every requested scope gets a value. Only its sealed membership proof permits []. */
export async function readIdentityBaselineScopes(store: IdentityBootstrapStore, membership: IdentityMembership, scopeIds: string[]): Promise<Map<string, PendingIdentityFact[]>> {
  const proofs = await proveIdentityLookups(store, membership, scopeIds.map(id => lookupKey('scope', id)));
  const result = new Map(scopeIds.map(id => [id, [] as PendingIdentityFact[]]));
  const wanted = scopeIds.filter(id => proofs.get(lookupKey('scope', id)) !== null);
  if (!wanted.length) return result;
  requireBoundedRows([...proofs.values()]);
  const loaded = await waitForReadback(async () => {
    const groups = new Map(wanted.map(id => [id, [] as ScopeRecord[]]));
    for await (const row of store.scan<ScopeRecord>(SCOPES_TABLE, ['scope_id','fact_kind','fact_key'],
      `${store.scope()} AND scope_id IN (${sqlStrings(wanted)})`, 'scope_id,fact_kind,fact_key,fact_json,fact_hash', 1000)) {
      const rows = groups.get(row.scope_id);
      if (!rows) throw new Error('Unexpected identity baseline scope');
      rows.push(row);
      if (rows.length > proofs.get(lookupKey('scope', row.scope_id))!.count) throw new Error('Conflicting identity baseline scope');
    }
    let complete = true;
    for (const [scopeId, rows] of groups) {
      const proof = proofs.get(lookupKey('scope', scopeId))!;
      if (rows.length === proof.count && await recordsHash(rows) !== proof.hash) throw new Error('Identity baseline scope differs from sealed proof');
      complete &&= rows.length === proof.count;
    }
    return { groups, complete };
  }, value => value.complete);
  for (const [scopeId, rows] of loaded.groups) {
    const facts: PendingIdentityFact[] = [];
    for (const row of rows) {
      if (await sha256(row.fact_json) !== row.fact_hash) throw new Error('Identity baseline fact hash mismatch');
      const fact = JSON.parse(row.fact_json) as PendingIdentityFact;
      if (fact.factKind !== row.fact_kind || fact.factKey !== row.fact_key) throw new Error('Identity baseline scope key mismatch');
      facts.push(fact);
    }
    result.set(scopeId, facts);
  }
  return result;
}

function requireBoundedRows(proofs: (LookupProof | null)[]): void {
  if (proofs.reduce((sum, proof) => sum + (proof?.count ?? 0), 0) > 20_000) throw new Error('Identity baseline read exceeds 20,000 rows; split the requested keys');
}

function validateLookup(value: LookupProof): void {
  if (!value || typeof value.key !== 'string' || !Number.isSafeInteger(value.count) || value.count < 1 || !/^[a-f0-9]{64}$/.test(value.hash)) throw new Error('Invalid identity lookup proof');
}
function bytes(value: unknown): number { return new TextEncoder().encode(canonicalJson(value)).byteLength; }
