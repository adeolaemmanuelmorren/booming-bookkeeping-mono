import { canonicalJson } from '../sessions/session-engine.ts';
import type { NormalizedBrowserEvent } from '../browser/normalize.ts';
import type { SnapshotRecord } from '../sessions/session-publication.ts';
import { buildCompleteVisitorSeed, type CompleteVisitor, type SeedMember } from './sessions.ts';
import { hash } from './hash.ts';

export interface LogicalKey { event_kind: string; source_record_id: string }
export interface SourceHeadRead {
  key: LogicalKey;
  heads: NormalizedBrowserEvent[];
  expectedCount: number;
  expectedHash: string;
}

/** Each method reads one immutable sealed generation, including authoritative absent keys. */
export interface SealedBaselineStore {
  requireSeal(sourceSeal: string): Promise<void>;
  sourceHeads(keys: LogicalKey[]): Promise<SourceHeadRead[]>;
  members(visitorKeys: string[]): Promise<SeedMember[]>;
  visitorHeads(tenantId: string, visitorKey: string): Promise<CompleteVisitor | null>;
  sessionRecords(member: SeedMember): Promise<SnapshotRecord[]>;
}

/** Structural implementation of BrowserBaselineReader and SessionBaselineReader. */
export class BootstrapBaselineReader {
  private sourceSeal: string;
  private tenantId: string;
  private store: SealedBaselineStore;

  constructor(tenantId: string, sourceSeal: string, store: SealedBaselineStore) {
    this.tenantId = tenantId;
    this.sourceSeal = sourceSeal;
    this.store = store;
  }

  async loadSourceHeads(keys: LogicalKey[]): Promise<{ key: LogicalKey; heads: NormalizedBrowserEvent[] }[]> {
    await this.store.requireSeal(this.sourceSeal);
    const wanted = new Set(keys.map(canonicalJson));
    if (wanted.size !== keys.length) throw new Error('Duplicate baseline lookup key');
    const rows = await this.store.sourceHeads(keys);
    const seen = new Set<string>();
    for (const row of rows) {
      const key = canonicalJson(row.key);
      if (!wanted.has(key) || seen.has(key)) throw new Error('Unexpected baseline source key');
      seen.add(key);
      if (row.heads.length !== row.expectedCount || await hash(row.heads) !== row.expectedHash) throw new Error('Baseline source heads are incomplete');
      const sourceNames = new Set<string>();
      for (const head of row.heads) {
        if (head.source.tenant_id !== this.tenantId || head.source.event_kind !== row.key.event_kind || head.source.source_record_id !== row.key.source_record_id) {
          throw new Error('Misrouted baseline source head');
        }
        if (sourceNames.has(head.source.source_system)) throw new Error('Baseline must retain one selected head per source');
        sourceNames.add(head.source.source_system);
      }
    }
    if (seen.size !== wanted.size) throw new Error('Baseline source lookup omitted requested keys');
    return rows.map(row => ({ key: row.key, heads: row.heads }));
  }

  async loadMembers(visitorKeys: string[]): Promise<SeedMember[]> {
    await this.store.requireSeal(this.sourceSeal);
    const wanted = new Set(visitorKeys);
    const members = await this.store.members(visitorKeys);
    const seen = new Set<string>();
    for (const member of members) {
      if (!wanted.has(member.visitor_key) || seen.has(member.visitor_key) || member.revision !== '1') throw new Error('Unexpected baseline visitor member');
      if (new Set(member.records.map(row => row.session_id)).size !== member.records.length) throw new Error('Duplicate baseline session ID');
      seen.add(member.visitor_key);
    }
    return members;
  }

  async loadVisitor(tenantId: string, visitorKey: string) {
    if (tenantId !== this.tenantId) throw new Error('Baseline tenant mismatch');
    await this.store.requireSeal(this.sourceSeal);
    const visitor = await this.store.visitorHeads(tenantId, visitorKey);
    if (!visitor) return null;
    if (visitor.tenantId !== tenantId || visitor.visitorKey !== visitorKey) throw new Error('Misrouted visitor baseline');
    const seed = await buildCompleteVisitorSeed(visitor, this.sourceSeal);
    const members = await this.loadMembers([visitorKey]);
    const expectedMember: SeedMember = {
      visitor_key: visitorKey, revision: '1', publication_id: seed.snapshot.publication_id,
      records: seed.records.map(row => ({ session_id: row.session_id, payload_hash: row.payload_hash })),
    };
    if (members.length !== 1 || canonicalJson(members[0]) !== canonicalJson(expectedMember)) throw new Error('Visitor baseline does not reproduce its published session snapshot');
    const expected = new Map(seed.records.map(row => [row.session_id, canonicalJson(row)]));
    const actual = await this.store.sessionRecords(members[0]);
    const seen = new Set<string>();
    for (const row of actual) {
      if (expected.get(row.session_id) !== canonicalJson(row)) throw new Error('Visitor baseline session records conflict');
      seen.add(row.session_id);
    }
    if (seen.size !== expected.size) throw new Error('Visitor baseline session records are incomplete');
    return { tenantId, visitorKey, heads: seed.heads, snapshot: seed.snapshot };
  }
}
