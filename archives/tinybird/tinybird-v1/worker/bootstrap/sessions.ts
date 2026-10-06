import { applyPageRevisions, pagesForVisitor, type PageRevision } from '../sessions/page-revisions.ts';
import { buildSessions, canonicalJson, compareStrings } from '../sessions/session-engine.ts';
import { createSnapshot, type SessionSnapshot, type SnapshotRecord, type SnapshotManifest } from '../sessions/session-publication.ts';
import { hash, hashText } from './hash.ts';

export interface CompleteVisitor {
  tenantId: string;
  visitorKey: string;
  sourceSeal: string;
  heads: PageRevision[];
  expectedHeadCount: number;
  expectedHeadHash: string;
}

export interface VisitorSeed {
  tenantId: string;
  visitorKey: string;
  heads: PageRevision[];
  snapshot: SessionSnapshot;
  records: SnapshotRecord[];
  manifest: SnapshotManifest;
}

export interface SeedMember {
  visitor_key: string;
  revision: string;
  publication_id: string;
  records: { session_id: string; payload_hash: string }[];
}

export interface SeedGroup {
  tenant_id: string;
  group_id: string;
  sequence: number;
  members: SeedMember[];
}

export interface SessionSeedChunk {
  sourceSeal: string;
  group: SeedGroup;
  firstVisitor: string;
  lastVisitor: string;
  pageHeadCount: number;
  records: SnapshotRecord[];
  manifests: SnapshotManifest[];
  contentHash: string;
}

/** Heads must be complete for one visitor at the sealed source cutoff. */
export async function buildCompleteVisitorSeed(input: CompleteVisitor, sourceSeal: string): Promise<VisitorSeed> {
  if (!input.tenantId || !input.visitorKey || !sourceSeal) throw new Error('Visitor seed scope is required');
  if (input.sourceSeal !== sourceSeal) throw new Error('Visitor heads belong to another source seal');
  const ordered = [...input.heads].sort((a, b) => compareStrings(a.page_view_id, b.page_view_id));
  if (ordered.length !== input.expectedHeadCount || await hash(ordered) !== input.expectedHeadHash) {
    throw new Error('Visitor head read is incomplete or changed');
  }
  const selected = applyPageRevisions(new Map(), ordered).heads;
  if (selected.size !== ordered.length) throw new Error('Visitor seed must contain one selected head per page');
  for (const head of selected.values()) {
    if (head.page && head.page.visitor_key !== input.visitorKey) throw new Error('Visitor seed contains another visitor');
  }
  const sessions = buildSessions(input.visitorKey, pagesForVisitor(selected, input.visitorKey));
  const snapshot = createSnapshot(input.tenantId, input.visitorKey, '1', sessions);
  const records: SnapshotRecord[] = [];
  for (const session of snapshot.sessions) {
    const payload = canonicalJson(session);
    records.push({
      tenant_id: input.tenantId, visitor_key: input.visitorKey, revision: '1',
      publication_id: snapshot.publication_id, session_id: session.session_id,
      payload_json: payload, payload_hash: await hashText(payload),
    });
  }
  const manifest: SnapshotManifest = {
    tenant_id: input.tenantId, visitor_key: input.visitorKey, revision: '1',
    publication_id: snapshot.publication_id, row_count: records.length, content_hash: await hash(records),
  };
  return { tenantId: input.tenantId, visitorKey: input.visitorKey, heads: ordered, snapshot, records, manifest };
}

/** Batching never divides a visitor or drops its carry-over at a query page boundary. */
export async function* sessionSeedChunks(
  visitors: AsyncIterable<CompleteVisitor>,
  options: { tenantId: string; runId: string; sourceSeal: string; startSequence: number; maxVisitors?: number; maxRecords?: number; maxBytes?: number },
): AsyncGenerator<SessionSeedChunk> {
  const maxVisitors = options.maxVisitors ?? 250;
  const maxRecords = options.maxRecords ?? 2000;
  const maxBytes = options.maxBytes ?? 32_000_000;
  if (![maxVisitors, maxRecords, maxBytes, options.startSequence].every(value => Number.isSafeInteger(value) && value > 0)) {
    throw new Error('Bootstrap limits and sequence must be positive safe integers');
  }
  let batch: VisitorSeed[] = [];
  let recordCount = 0;
  let bytes = 0;
  let previousVisitor: string | null = null;
  let sequence = options.startSequence;
  for await (const visitor of visitors) {
    if (visitor.tenantId !== options.tenantId) throw new Error('Bootstrap visitor tenant mismatch');
    if (previousVisitor !== null && compareStrings(visitor.visitorKey, previousVisitor) <= 0) {
      throw new Error('Complete visitors must arrive once in Unicode code-point order');
    }
    previousVisitor = visitor.visitorKey;
    const seed = await buildCompleteVisitorSeed(visitor, options.sourceSeal);
    const size = new TextEncoder().encode(canonicalJson(seed)).byteLength;
    if (batch.length && (batch.length >= maxVisitors || recordCount + seed.records.length > maxRecords || bytes + size > maxBytes)) {
      yield await chunk(batch, sequence++, options);
      batch = [];
      recordCount = 0;
      bytes = 0;
    }
    batch.push(seed);
    recordCount += seed.records.length;
    bytes += size;
  }
  if (batch.length) yield await chunk(batch, sequence, options);
}

async function chunk(
  visitors: VisitorSeed[], sequence: number,
  scope: { tenantId: string; runId: string; sourceSeal: string },
): Promise<SessionSeedChunk> {
  if (!Number.isSafeInteger(sequence)) throw new Error('Bootstrap sequence overflow');
  const records = visitors.flatMap(visitor => visitor.records);
  const manifests = visitors.map(visitor => visitor.manifest);
  const group: SeedGroup = {
    tenant_id: scope.tenantId,
    group_id: await hash([scope.runId, 'sessions', sequence]),
    sequence,
    members: visitors.map(visitor => ({
      visitor_key: visitor.visitorKey, revision: '1', publication_id: visitor.snapshot.publication_id,
      records: visitor.records.map(row => ({ session_id: row.session_id, payload_hash: row.payload_hash })),
    })),
  };
  return {
    sourceSeal: scope.sourceSeal, group,
    firstVisitor: visitors[0].visitorKey, lastVisitor: visitors.at(-1)!.visitorKey,
    pageHeadCount: visitors.reduce((sum, visitor) => sum + visitor.heads.length, 0),
    records, manifests, contentHash: await hash({ sourceSeal: scope.sourceSeal, group, records, manifests }),
  };
}

/** Production adapters paginate each read and split uploads by bytes, not by visitor. */
export interface BulkSessionPublisher {
  appendRecords(rows: SnapshotRecord[]): Promise<void>;
  readRecords(chunk: SessionSeedChunk): AsyncIterable<SnapshotRecord>;
  appendManifests(rows: SnapshotManifest[]): Promise<void>;
  readManifests(chunk: SessionSeedChunk): AsyncIterable<SnapshotManifest>;
  appendGroup(group: SeedGroup): Promise<void>;
  readGroup(group: SeedGroup): Promise<SeedGroup | null>;
}

/** Persist the deterministic chunk before calling this. Advance its cursor only after return. */
export async function publishSessionSeedChunk(chunk: SessionSeedChunk, publisher: BulkSessionPublisher): Promise<void> {
  if (await hash({ sourceSeal: chunk.sourceSeal, group: chunk.group, records: chunk.records, manifests: chunk.manifests }) !== chunk.contentHash) {
    throw new Error('Saved bootstrap chunk has changed');
  }
  const recordKey = (row: SnapshotRecord) => `${row.publication_id}:${row.session_id}`;
  if (!await verify(chunk.records, publisher.readRecords(chunk), recordKey)) {
    await publisher.appendRecords(chunk.records);
    if (!await verify(chunk.records, publisher.readRecords(chunk), recordKey)) throw new Error('Bootstrap session rows are not fully visible');
  }
  const manifestKey = (row: SnapshotManifest) => row.publication_id;
  if (!await verify(chunk.manifests, publisher.readManifests(chunk), manifestKey)) {
    await publisher.appendManifests(chunk.manifests);
    if (!await verify(chunk.manifests, publisher.readManifests(chunk), manifestKey)) throw new Error('Bootstrap visitor manifests are not fully visible');
  }
  let actual = await publisher.readGroup(chunk.group);
  if (actual && canonicalJson(actual) !== canonicalJson(chunk.group)) throw new Error('Bootstrap group conflicts');
  if (!actual) {
    await publisher.appendGroup(chunk.group);
    actual = await publisher.readGroup(chunk.group);
  }
  if (canonicalJson(actual) !== canonicalJson(chunk.group)) throw new Error('Bootstrap group is not fully visible');
}

async function verify<T>(expected: T[], actual: AsyncIterable<T>, key: (value: T) => string): Promise<boolean> {
  const wanted = new Map(expected.map(row => [key(row), canonicalJson(row)]));
  if (wanted.size !== expected.length) throw new Error('Duplicate bootstrap output key');
  const seen = new Set<string>();
  for await (const row of actual) {
    const id = key(row);
    if (wanted.get(id) !== canonicalJson(row)) throw new Error('Unexpected or conflicting bootstrap output');
    seen.add(id);
  }
  return seen.size === wanted.size;
}
