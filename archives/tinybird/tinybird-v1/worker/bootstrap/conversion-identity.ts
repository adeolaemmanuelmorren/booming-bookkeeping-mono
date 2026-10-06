// Used only by the one-time Node executor. Live Worker readers do not import this module.
import { createHash } from 'node:crypto';
import type { PendingIdentityFact } from '../identity/engine.ts';
import { canonicalJson, compareStrings, timestampMicros } from '../sessions/session-engine.ts';
import { sqlString, uint64Number, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import { hashText } from './hash.ts';
import type { SourceReceipt } from './manifest.ts';
import type { TinybirdBootstrapStorage } from './tinybird-storage.ts';

export const CONVERSION_FACTS_TABLE = 'v1_bootstrap_conversion_identity_facts';
export const CONVERSION_MANIFESTS_TABLE = 'v1_bootstrap_conversion_identity_manifests';

export interface ConversionIdentitySnapshot {
  snapshotId: string;
  snapshotAt: string;
  expectedDistinctFactCount: number;
  canonicalHash: string;
}

interface FactReference { kind: string; key: string; hash: string }
interface CopyProgress { lastKind: string; lastKey: string; copied: number }
interface SnapshotManifest extends JsonRecord {
  snapshot_at: string;
  expected_distinct_fact_count: string | number;
  canonical_hash: string;
}

const COLUMNS = ['fact_kind', 'fact_key', 'event_id', 'producer_id', 'observed_at', 'ingested_at', 'source_priority',
  'source_fact_version', 'fact_deleted', 'fact_payload_hash', 'fact_payload', 'evidence_keys', 'canonical_fact_json', 'row_hash'];
const FACT_KINDS = new Set(['stripe', 'stripe_kajabi', 'activecampaign']);

export function conversionPartition(snapshot: ConversionIdentitySnapshot): string {
  return `conversion-identity:${snapshot.snapshotId}`;
}

export function validateConversionSnapshot(snapshot: ConversionIdentitySnapshot): void {
  if (!snapshot.snapshotId || typeof snapshot.snapshotId !== 'string'
    || !Number.isSafeInteger(snapshot.expectedDistinctFactCount) || snapshot.expectedDistinctFactCount < 0
    || !/^[a-f0-9]{64}$/.test(snapshot.canonicalHash)) throw new Error('Invalid frozen conversion identity snapshot');
  timestampMicros(snapshot.snapshotAt);
}

/** Verify the complete fixed snapshot before copying any facts into the identity input. */
export async function restoreConversionIdentity(
  snapshot: ConversionIdentitySnapshot, store: TinybirdBootstrapStorage,
  progress: (counts: Record<string, number>) => void = () => {},
): Promise<number> {
  validateConversionSnapshot(snapshot);
  const partition = conversionPartition(snapshot);
  const where = `tenant_id = ${sqlString(store.tenantId)} AND snapshot_id = ${sqlString(snapshot.snapshotId)}`;
  await requireSnapshotManifest(store, snapshot, where);
  const done = await store.getManifest<SourceReceipt>('source', partition);
  if (done) {
    if (done.sourceSeal !== store.sourceSeal || done.contentHash !== snapshot.canonicalHash
      || done.verifiedContentHash !== snapshot.canonicalHash || done.eof !== true) {
      throw new Error('Conversion snapshot receipt differs from the frozen input');
    }
    return snapshot.expectedDistinctFactCount;
  }

  // SHA-256 is streamed over canonical text. Only compact known-key references remain in memory.
  const verified = await waitForReadback(async () => {
    const digest = createHash('sha256');
    const refs: FactReference[] = [];
    for await (const row of store.scan<JsonRecord>(CONVERSION_FACTS_TABLE, ['fact_kind', 'fact_key'], where, COLUMNS)) {
      const fact = await validateFactRow(row);
      if (refs.length) digest.update('\n');
      digest.update(String(row.canonical_fact_json));
      refs.push({ kind: fact.factKind, key: fact.factKey, hash: String(row.row_hash) });
      if (refs.length > snapshot.expectedDistinctFactCount) throw new Error('Conversion snapshot has unexpected extra facts');
    }
    const complete = refs.length === snapshot.expectedDistinctFactCount;
    if (complete && digest.digest('hex') !== snapshot.canonicalHash) throw new Error('Conversion snapshot content hash differs from its manifest');
    return { refs, complete };
  }, result => result.complete);

  const saved = await store.checkpoint<CopyProgress>('conversion-identity', snapshot.snapshotId);
  let copied = saved?.payload.copied ?? 0;
  let sequence = (saved?.sequence ?? 0) + 1;
  const remaining = verified.refs.filter(ref => !saved || comparePair(ref.kind, ref.key, saved.payload.lastKind, saved.payload.lastKey) > 0);
  if (copied + remaining.length !== verified.refs.length) throw new Error('Conversion snapshot copy cursor does not cover its verified input');
  for (let offset = 0; offset < remaining.length; offset += store.writeBatchRows) {
    const refs = remaining.slice(offset, offset + store.writeBatchRows);
    const facts = await readKnownFacts(store, where, refs);
    await store.writeIdentityFacts('selected', facts.map(fact => ({ fact })));
    copied += facts.length;
    const last = refs.at(-1)!;
    await store.putCheckpoint('conversion-identity', snapshot.snapshotId, sequence++, { lastKind: last.kind, lastKey: last.key, copied });
    progress({ copiedFacts: copied, expectedFacts: snapshot.expectedDistinctFactCount });
  }
  if (copied !== snapshot.expectedDistinctFactCount) throw new Error('Conversion snapshot copy is incomplete');
  const receipt: SourceReceipt = {
    partition, sourceSeal: store.sourceSeal, lastCursor: canonicalJson(verified.refs.at(-1) ?? null), eof: true,
    contentHash: snapshot.canonicalHash, verifiedContentHash: snapshot.canonicalHash,
  };
  await store.putManifest('source', partition, receipt);
  return copied;
}

async function requireSnapshotManifest(store: TinybirdBootstrapStorage, snapshot: ConversionIdentitySnapshot, where: string): Promise<void> {
  await waitForReadback(async () => {
    // Identical reseals may coexist until storage merges them. Their content must agree.
    const rows = await store.client.query<SnapshotManifest>(`SELECT DISTINCT snapshot_at,expected_distinct_fact_count,canonical_hash
      FROM ${CONVERSION_MANIFESTS_TABLE} WHERE ${where} LIMIT 2`);
    if (!rows.length) return false;
    if (rows.length !== 1) throw new Error('Conversion snapshot has conflicting manifests');
    const row = rows[0];
    if (timestampMicros(row.snapshot_at) !== timestampMicros(snapshot.snapshotAt)
      || uint64Number(row.expected_distinct_fact_count) !== snapshot.expectedDistinctFactCount
      || row.canonical_hash !== snapshot.canonicalHash) throw new Error('Conversion snapshot manifest differs from the frozen configuration');
    return true;
  }, value => value);
}

async function readKnownFacts(store: TinybirdBootstrapStorage, where: string, refs: FactReference[]): Promise<PendingIdentityFact[]> {
  const wanted = new Map(refs.map(ref => [canonicalJson([ref.kind, ref.key]), ref]));
  const keys = refs.map(ref => `tuple(${sqlString(ref.kind)},${sqlString(ref.key)})`).join(',');
  const rows = await waitForReadback(async () => {
    const result = new Map<string, PendingIdentityFact>();
    const actual = await store.client.query<JsonRecord>(`SELECT DISTINCT ${COLUMNS.join(',')} FROM ${CONVERSION_FACTS_TABLE}
      WHERE ${where} AND tuple(fact_kind,fact_key) IN (${keys}) LIMIT ${refs.length + 1}`);
    for (const row of actual) {
      const key = canonicalJson([row.fact_kind, row.fact_key]);
      const ref = wanted.get(key);
      if (!ref || result.has(key) || row.row_hash !== ref.hash) throw new Error('Conversion snapshot fact changed after verification');
      result.set(key, await validateFactRow(row));
    }
    return result;
  }, value => value.size === wanted.size);
  return refs.map(ref => rows.get(canonicalJson([ref.kind, ref.key]))!);
}

async function validateFactRow(row: JsonRecord): Promise<PendingIdentityFact> {
  if (typeof row.canonical_fact_json !== 'string' || typeof row.row_hash !== 'string'
    || await hashText(row.canonical_fact_json) !== row.row_hash) throw new Error('Conversion canonical fact hash did not verify');
  const fact = JSON.parse(row.canonical_fact_json) as PendingIdentityFact;
  const keys = ['eventId', 'producerId', 'observedAt', 'ingestedAt', 'factKind', 'factKey', 'sourcePriority',
    'sourceFactVersion', 'factDeleted', 'factPayloadHash', 'factPayload', 'evidenceKeys'];
  if (!fact || canonicalJson(Object.keys(fact).sort()) !== canonicalJson(keys.sort()) || canonicalJson(fact) !== row.canonical_fact_json) {
    throw new Error('Conversion snapshot does not contain a canonical PendingIdentityFact');
  }
  if (!FACT_KINDS.has(fact.factKind) || typeof fact.factKey !== 'string'
    || !fact.factKey.startsWith(`${fact.factKind}:`) || fact.factKey === `${fact.factKind}:`
    || fact.producerId !== `source:${fact.factKind}` || !fact.eventId || typeof fact.eventId !== 'string'
    || fact.sourcePriority !== 1 || !Number.isSafeInteger(fact.sourceFactVersion) || fact.sourceFactVersion < 0
    || typeof fact.factDeleted !== 'boolean' || typeof fact.factPayload !== 'string'
    || !Array.isArray(fact.evidenceKeys) || fact.evidenceKeys.some(key => typeof key !== 'string')
    || canonicalJson(fact.evidenceKeys) !== canonicalJson([...new Set(fact.evidenceKeys)].sort(compareStrings))) {
    throw new Error('Conversion identity source fields are invalid');
  }
  if (fact.observedAt !== null) timestampMicros(fact.observedAt);
  timestampMicros(fact.ingestedAt);
  if (await hashText(fact.factPayload) !== fact.factPayloadHash) throw new Error('Conversion identity payload hash did not verify');
  const actual = { kind: row.fact_kind, key: row.fact_key, event: row.event_id, producer: row.producer_id,
    observed: row.observed_at === null ? null : String(timestampMicros(String(row.observed_at))), ingested: String(timestampMicros(String(row.ingested_at))),
    priority: Number(row.source_priority), version: uint64Number(row.source_fact_version as string | number),
    deleted: row.fact_deleted === true || row.fact_deleted === 1, payloadHash: row.fact_payload_hash, payload: row.fact_payload, evidence: row.evidence_keys };
  const expected = { kind: fact.factKind, key: fact.factKey, event: fact.eventId, producer: fact.producerId,
    observed: fact.observedAt === null ? null : String(timestampMicros(fact.observedAt)), ingested: String(timestampMicros(fact.ingestedAt)),
    priority: fact.sourcePriority, version: fact.sourceFactVersion, deleted: fact.factDeleted,
    payloadHash: fact.factPayloadHash, payload: fact.factPayload, evidence: fact.evidenceKeys };
  if (![true, false, 0, 1].includes(row.fact_deleted as boolean | number) || canonicalJson(actual) !== canonicalJson(expected)) {
    throw new Error('Conversion snapshot columns disagree with the canonical fact');
  }
  return fact;
}

function comparePair(kind: string, key: string, otherKind: string, otherKey: string): number {
  return compareStrings(kind, otherKind) || compareStrings(key, otherKey);
}
