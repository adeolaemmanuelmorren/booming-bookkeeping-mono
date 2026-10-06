import { DurableObject } from "cloudflare:workers";
import { canonicalJson } from "../sessions/session-engine.ts";
import type { PageRevision } from "../sessions/page-revisions.ts";
import type { PrepareSessionGroup } from "../sessions/visitor-sessions.ts";
import { type SessionPublisher, type SessionSnapshot } from "../sessions/session-publication.ts";
import { readChunkedJson, writeChunkedJson } from "../storage/chunked-json.ts";
import type { BrowserBaselineReader, BrowserLogicalKey } from "./baseline.ts";
import { MAX_BROWSER_BATCH_BYTES } from "./batches.ts";
import type { BrowserIdentityFact, BrowserSourceFact, NormalizedBrowserEvent } from "./normalize.ts";
import { commitGroup, publishSessionBatch, snapshotMember, publishSources, sha256, type BrowserGroupPublisher, type BrowserSourcePublisher, type RetainedBrowserFact, type BrowserReadPlan, type GroupMember } from "./publication.ts";

export interface BrowserRouterEnv {
  TENANT_ID: string;
  BROWSER_SOURCE_PUBLISHER: BrowserSourcePublisher;
  GROUP_PUBLISHER: BrowserGroupPublisher;
  SESSION_PUBLISHER: SessionPublisher;
  IDENTITY: { getByName(name: string): { enqueue(facts: BrowserIdentityFact[]): Promise<unknown> } };
  VISITORS: { getByName(name: string): { prepareGroup(input: PrepareSessionGroup): Promise<SessionSnapshot> } };
  BROWSER_FLUSH_DELAY_MS?: string;
  BROWSER_BASELINE?: BrowserBaselineReader;
  BROWSER_BASELINE_ID?: string;
  BROWSER_BASELINE_SEQUENCE?: string;
}

type SavedHead = { fingerprint: string; event_json: string };
type SourceHead = { fingerprint: string; source_priority: number; source_revision: string };
type LogicalHead = {
  source: Pick<BrowserSourceFact, "tenant_id" | "source_system" | "event_kind" | "source_record_id" | "source_priority" | "source_revision">;
  pageRevision: PageRevision | null;
  identity: BrowserIdentityFact | null;
};
type BaselineInput = { key: string; heads: { event: NormalizedBrowserEvent; fingerprint: string }[] };
type Route = { visitorKey: string; revisions: PageRevision[] };
type Delivery = {
  id: string;
  sequence: number;
  sources: RetainedBrowserFact[];
  identities: BrowserIdentityFact[];
  routes: Route[];
  members: GroupMember[];
  sources_done: boolean;
  identity_done: boolean;
};
type Outbox = {
  sequence: number; lease_id: string | null; lease_until: number;
  next_attempt: number; attempts: number; last_error: string | null;
};

const PRIORITY = { boom_domains: 1, jitsu_data: 2, jitsu_events_api: 3 };
const LEASE_MS = 240_000;

/** One object per tenant. Ingress reads only touched keys; the outbox serializes publication. */
export class BrowserRouter extends DurableObject<BrowserRouterEnv> {
  constructor(ctx: DurableObjectState, env: BrowserRouterEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS browser_meta (
          singleton INTEGER PRIMARY KEY CHECK(singleton = 1), tenant_id TEXT NOT NULL,
          sequence INTEGER NOT NULL, published_sequence INTEGER NOT NULL,
          baseline_id TEXT NOT NULL, baseline_sequence INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS browser_receipts (fact_id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS browser_source_heads (
          source_key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
          source_priority INTEGER NOT NULL, source_revision TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS browser_heads (
          logical_key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, event_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS browser_outbox (
          sequence INTEGER PRIMARY KEY,
          lease_id TEXT, lease_until INTEGER NOT NULL DEFAULT 0,
          next_attempt INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS browser_outbox_chunks (
          owner_key TEXT NOT NULL, chunk_index INTEGER NOT NULL, payload TEXT NOT NULL,
          PRIMARY KEY(owner_key, chunk_index)
        );
        CREATE TABLE IF NOT EXISTS browser_baseline_keys (logical_key TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS browser_published_visitors (visitor_key TEXT PRIMARY KEY, member_json TEXT);
      `);
      const baselineSequence = Number(env.BROWSER_BASELINE_SEQUENCE ?? "0");
      const baselineId = env.BROWSER_BASELINE_ID ?? "empty";
      if (!Number.isSafeInteger(baselineSequence) || baselineSequence < 0) throw new Error("Invalid baseline sequence");
      if (baselineSequence > 0 && (!env.BROWSER_BASELINE || baselineId === "empty")) throw new Error("Historical baseline reader and ID are required");
      const meta = this.meta();
      if (meta && (meta.tenant_id !== env.TENANT_ID || meta.baseline_id !== baselineId || meta.baseline_sequence !== baselineSequence)) throw new Error("Router baseline or tenant changed");
      ctx.storage.sql.exec("INSERT OR IGNORE INTO browser_meta VALUES (1, ?, ?, ?, ?, ?)", env.TENANT_ID, baselineSequence, baselineSequence, baselineId, baselineSequence);
      await this.schedule();
    });
  }

  async receive(events: NormalizedBrowserEvent[]): Promise<{ accepted: number; quarantined: number; sequence: number }> {
    if (events.length > 200) throw new Error("Browser batch exceeds 200 observations");
    if (new TextEncoder().encode(JSON.stringify(events)).byteLength > MAX_BROWSER_BATCH_BYTES) throw new Error("Browser batch exceeds 512000 bytes; split it without dropping observations");
    const inputs: { event: NormalizedBrowserEvent; fingerprint: string; factId: string }[] = [];
    for (const event of events) {
      validateEvent(event, this.env.TENANT_ID);
      const fingerprint = await sha256(eventFingerprint(event));
      inputs.push({ event, fingerprint, factId: fingerprint });
    }
    const baseline = await this.loadBaseline(events);
    return this.ctx.storage.transaction(async () => {
      const sql = this.ctx.storage.sql;
      this.seedBaselineKeys(baseline);
      const meta = this.meta();
      if (meta && meta.tenant_id !== this.env.TENANT_ID) throw new Error("Router belongs to another tenant");
      const original = new Map<string, LogicalHead | null>();
      const sources: RetainedBrowserFact[] = [];
      let quarantined = 0;
      for (const { event, fingerprint, factId } of inputs) {
        if (sql.exec("SELECT fact_id FROM browser_receipts WHERE fact_id = ?", factId).toArray().length) continue;
        const key = canonicalJson([event.source.event_kind, event.source.source_record_id]);
        const sourceKey = canonicalJson([event.source.source_system, event.source.event_kind, event.source.source_record_id]);
        const sourceHead = sql.exec<SourceHead>("SELECT * FROM browser_source_heads WHERE source_key = ?", sourceKey).toArray()[0];
        const order = sourceHead ? compareSource(event, { source: sourceHead }) : 1;
        const rejection = order === 0 && sourceHead!.fingerprint !== fingerprint ? "Conflicting content at the same source revision" : null;
        const record = { fact_id: factId, source: event.source, rejection };
        sources.push(record);
        // The outbox holds the full raw fact until verified publication. Keep its receipt forever.
        sql.exec("INSERT INTO browser_receipts VALUES (?)", factId);
        if (rejection) { quarantined++; continue; }
        if (order <= 0) continue;
        sql.exec("INSERT INTO browser_source_heads VALUES (?, ?, ?, ?) ON CONFLICT(source_key) DO UPDATE SET fingerprint = excluded.fingerprint, source_priority = excluded.source_priority, source_revision = excluded.source_revision",
          sourceKey, fingerprint, event.source.source_priority, event.source.source_revision);
        const selectedRow = sql.exec<SavedHead>("SELECT fingerprint, event_json FROM browser_heads WHERE logical_key = ?", key).toArray()[0];
        const selected = selectedRow ? JSON.parse(selectedRow.event_json) as LogicalHead : null;
        if (selected && compareSource(event, selected) <= 0) continue;
        if (!original.has(key)) original.set(key, selected);
        sql.exec("INSERT INTO browser_heads VALUES (?, ?, ?) ON CONFLICT(logical_key) DO UPDATE SET fingerprint = excluded.fingerprint, event_json = excluded.event_json",
          key, fingerprint, JSON.stringify(logicalHead(event)));
      }
      if (!sources.length) return { accepted: 0, quarantined: 0, sequence: meta?.sequence ?? 0 };
      const sequence = (meta?.sequence ?? 0) + 1;
      if (!Number.isSafeInteger(sequence)) throw new Error("Browser sequence exceeds integer precision");
      const delivery: Delivery = {
        id: crypto.randomUUID(), sequence, sources, identities: [], routes: [], members: [], sources_done: false, identity_done: false,
      };
      const routes = new Map<string, PageRevision[]>();
      for (const [key, previous] of original) {
        const current = JSON.parse(sql.exec<SavedHead>("SELECT fingerprint, event_json FROM browser_heads WHERE logical_key = ?", key).one().event_json) as LogicalHead;
        routeChange(previous, current, delivery, routes);
      }
      delivery.routes = [...routes].sort(([a], [b]) => a.localeCompare(b)).map(([visitorKey, revisions]) => ({ visitorKey, revisions }));
      sql.exec("UPDATE browser_meta SET sequence = ? WHERE singleton = 1", sequence);
      sql.exec("INSERT INTO browser_outbox (sequence, next_attempt) VALUES (?, ?)", sequence, Date.now() + this.flushDelay());
      writeChunkedJson(this.ctx.storage, "browser_outbox_chunks", String(sequence), delivery);
      await this.schedule();
      return { accepted: sources.length, quarantined, sequence };
    });
  }

  async alarm(): Promise<void> {
    const claimed = await this.ctx.storage.transaction(async () => {
      const row = this.oldest();
      if (!row) { await this.schedule(); return null; }
      if (row.lease_until > Date.now() || row.next_attempt > Date.now()) { await this.schedule(); return null; }
      const lease = crypto.randomUUID();
      this.ctx.storage.sql.exec("UPDATE browser_outbox SET lease_id = ?, lease_until = ?, attempts = attempts + 1 WHERE sequence = ?", lease, Date.now() + LEASE_MS, row.sequence);
      await this.schedule();
      return { ...row, lease_id: lease };
    });
    if (!claimed) return;
    const delivery = readChunkedJson<Delivery>(this.ctx.storage, "browser_outbox_chunks", String(claimed.sequence));
    try {
      if (!delivery.sources_done) {
        await publishSources(delivery.sources, this.env.BROWSER_SOURCE_PUBLISHER);
        delivery.sources_done = true;
        await this.saveProgress(claimed, delivery);
      }
      if (!delivery.identity_done) {
        if (delivery.identities.length) await this.env.IDENTITY.getByName(this.env.TENANT_ID).enqueue(delivery.identities);
        delivery.identity_done = true;
        await this.saveProgress(claimed, delivery);
      }
      let snapshots: SessionSnapshot[] = [];
      let snapshotBytes = 0;
      const flushSnapshots = async () => {
        if (!snapshots.length) return;
        await publishSessionBatch(snapshots, this.env.SESSION_PUBLISHER);
        for (const snapshot of snapshots) delivery.members.push(await snapshotMember(snapshot));
        await this.saveProgress(claimed, delivery);
        snapshots = [];
        snapshotBytes = 0;
      };
      for (const route of delivery.routes) {
        if (delivery.members.some((member) => member.visitor_key === route.visitorKey)) continue;
        const snapshot = await this.env.VISITORS.getByName(visitorObjectName(this.env.TENANT_ID, route.visitorKey)).prepareGroup({
          groupId: delivery.id, sequence: delivery.sequence, visitorKey: route.visitorKey, revisions: route.revisions,
        });
        const bytes = new TextEncoder().encode(JSON.stringify(snapshot)).byteLength;
        if (snapshotBytes + bytes > 2_000_000) await flushSnapshots();
        snapshots.push(snapshot);
        snapshotBytes += bytes;
        // Oversized single visitors publish alone. No snapshot is truncated.
        if (snapshotBytes >= 2_000_000) await flushSnapshots();
      }
      await flushSnapshots();
      await commitGroup({ tenant_id: this.env.TENANT_ID, group_id: delivery.id, sequence: delivery.sequence, members: delivery.members }, this.env.GROUP_PUBLISHER);
      await this.ctx.storage.transaction(async () => {
        this.assertLease(claimed);
        for (const member of delivery.members) this.ctx.storage.sql.exec("INSERT INTO browser_published_visitors VALUES (?, ?) ON CONFLICT(visitor_key) DO UPDATE SET member_json = excluded.member_json", member.visitor_key, JSON.stringify(member));
        this.ctx.storage.sql.exec("UPDATE browser_meta SET published_sequence = ? WHERE singleton = 1", delivery.sequence);
        this.ctx.storage.sql.exec("DELETE FROM browser_outbox_chunks WHERE owner_key = ?", String(delivery.sequence));
        this.ctx.storage.sql.exec("DELETE FROM browser_outbox WHERE sequence = ? AND lease_id = ?", delivery.sequence, claimed.lease_id);
        await this.schedule();
      });
    } catch (error) {
      await this.ctx.storage.transaction(async () => {
        this.ctx.storage.sql.exec("UPDATE browser_outbox SET lease_id = NULL, lease_until = 0, next_attempt = ?, last_error = ? WHERE sequence = ? AND lease_id = ?",
          Date.now() + 10_000, error instanceof Error ? error.message : "Browser delivery failed", delivery.sequence, claimed.lease_id);
        await this.schedule();
      });
    }
  }

  async status() {
    const meta = this.meta();
    const pending = this.oldest();
    return {
      sequence: meta?.sequence ?? 0, publishedSequence: meta?.published_sequence ?? 0,
      pending: this.ctx.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM browser_outbox").one().count,
      retainedFacts: this.ctx.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM browser_receipts").one().count,
      lastError: pending?.last_error ?? null, alarmAt: await this.ctx.storage.getAlarm(),
    };
  }

  /** Fetch all requested snapshots against this one plan; never mix independently fetched plans. */
  async readPlan(visitorKeys: string[]): Promise<BrowserReadPlan> {
    const keys = [...new Set(visitorKeys)].sort();
    if (keys.length > 500) throw new Error("Read plan exceeds 500 visitors");
    const missing = keys.filter(key => !this.ctx.storage.sql.exec("SELECT visitor_key FROM browser_published_visitors WHERE visitor_key = ?", key).toArray().length);
    const baseline = missing.length && this.env.BROWSER_BASELINE ? await this.env.BROWSER_BASELINE.loadMembers(missing) : [];
    if (baseline.some(member => !missing.includes(member.visitor_key))) throw new Error("Unexpected baseline visitor member");
    if (new Set(baseline.map(member => member.visitor_key)).size !== baseline.length) throw new Error("Duplicate baseline visitor member");
    return this.ctx.storage.transactionSync(() => {
      for (const key of missing) {
        const member = baseline.find(item => item.visitor_key === key);
        this.ctx.storage.sql.exec("INSERT OR IGNORE INTO browser_published_visitors VALUES (?, ?)", key, member ? JSON.stringify(member) : null);
      }
      const members: GroupMember[] = [];
      for (const key of keys) {
        const row = this.ctx.storage.sql.exec<{ member_json: string | null }>("SELECT member_json FROM browser_published_visitors WHERE visitor_key = ?", key).one();
        if (row.member_json) members.push(JSON.parse(row.member_json) as GroupMember);
      }
      return { tenant_id: this.env.TENANT_ID, sequence: this.meta()!.published_sequence, visitor_keys: keys, members };
    });
  }

  private async loadBaseline(events: NormalizedBrowserEvent[]): Promise<BaselineInput[]> {
    if (!this.env.BROWSER_BASELINE) return [];
    const keys = new Map<string, BrowserLogicalKey>();
    for (const event of events) {
      const { event_kind, source_record_id } = event.source;
      const key = canonicalJson([event_kind, source_record_id]);
      if (this.ctx.storage.sql.exec("SELECT logical_key FROM browser_baseline_keys WHERE logical_key = ?", key).toArray().length) continue;
      keys.set(key, { event_kind, source_record_id });
    }
    if (!keys.size) return [];
    const loaded = await this.env.BROWSER_BASELINE.loadSourceHeads([...keys.values()]);
    const result: BaselineInput[] = [];
    const found = new Set<string>();
    for (const row of loaded) {
      const key = canonicalJson([row.key.event_kind, row.key.source_record_id]);
      if (!keys.has(key) || found.has(key)) throw new Error("Unexpected or duplicate baseline source key");
      found.add(key);
      const heads = [];
      const sources = new Set<string>();
      for (const event of row.heads) {
        validateEvent(event, this.env.TENANT_ID);
        if (event.source.event_kind !== row.key.event_kind || event.source.source_record_id !== row.key.source_record_id || sources.has(event.source.source_system)) throw new Error("Conflicting baseline source heads");
        sources.add(event.source.source_system);
        heads.push({ event, fingerprint: await sha256(eventFingerprint(event)) });
      }
      result.push({ key, heads });
    }
    if (found.size !== keys.size) throw new Error("Baseline source lookup is incomplete");
    return result;
  }

  private seedBaselineKeys(inputs: BaselineInput[]): void {
    const sql = this.ctx.storage.sql;
    for (const input of inputs) {
      if (sql.exec("SELECT logical_key FROM browser_baseline_keys WHERE logical_key = ?", input.key).toArray().length) continue;
      for (const { event, fingerprint } of input.heads) {
        const source = event.source;
        const sourceKey = canonicalJson([source.source_system, source.event_kind, source.source_record_id]);
        sql.exec("INSERT OR IGNORE INTO browser_source_heads VALUES (?, ?, ?, ?)", sourceKey, fingerprint, source.source_priority, source.source_revision);
        const current = sql.exec<SavedHead>("SELECT fingerprint, event_json FROM browser_heads WHERE logical_key = ?", input.key).toArray()[0];
        if (current && compareSource(event, JSON.parse(current.event_json) as LogicalHead) <= 0) continue;
        sql.exec("INSERT INTO browser_heads VALUES (?, ?, ?) ON CONFLICT(logical_key) DO UPDATE SET fingerprint = excluded.fingerprint, event_json = excluded.event_json", input.key, fingerprint, JSON.stringify(logicalHead(event)));
      }
      sql.exec("INSERT INTO browser_baseline_keys VALUES (?)", input.key);
    }
  }

  private async saveProgress(row: Outbox, delivery: Delivery): Promise<void> {
    await this.ctx.storage.transaction(async () => {
      this.assertLease(row);
      writeChunkedJson(this.ctx.storage, "browser_outbox_chunks", String(row.sequence), delivery);
      this.ctx.storage.sql.exec("UPDATE browser_outbox SET lease_until = ? WHERE sequence = ? AND lease_id = ?", Date.now() + LEASE_MS, row.sequence, row.lease_id);
      await this.schedule();
    });
  }

  private assertLease(row: Outbox): void {
    if (!this.ctx.storage.sql.exec("SELECT sequence FROM browser_outbox WHERE sequence = ? AND lease_id = ?", row.sequence, row.lease_id).toArray().length) {
      throw new Error("Browser delivery lease was lost");
    }
  }

  private async schedule(): Promise<void> {
    const row = this.oldest();
    if (!row) { await this.ctx.storage.deleteAlarm(); return; }
    await this.ctx.storage.setAlarm(Math.max(Date.now() + 1, row.next_attempt, row.lease_until));
  }

  private oldest(): Outbox | undefined {
    return this.ctx.storage.sql.exec<Outbox>("SELECT * FROM browser_outbox ORDER BY sequence LIMIT 1").toArray()[0];
  }

  private meta() {
    return this.ctx.storage.sql.exec<{ tenant_id: string; sequence: number; published_sequence: number; baseline_id: string; baseline_sequence: number }>("SELECT * FROM browser_meta WHERE singleton = 1").toArray()[0];
  }

  private flushDelay(): number {
    const delay = Number(this.env.BROWSER_FLUSH_DELAY_MS ?? "1000");
    if (!Number.isSafeInteger(delay) || delay < 0) throw new Error("Invalid BROWSER_FLUSH_DELAY_MS");
    return delay;
  }
}

export function visitorObjectName(tenantId: string, visitorKey: string): string {
  return canonicalJson([tenantId, visitorKey]);
}

function routeChange(previous: LogicalHead | null, current: LogicalHead, delivery: Delivery, routes: Map<string, PageRevision[]>): void {
  const oldPage = previous?.pageRevision?.page;
  const newPage = current.pageRevision?.page;
  const revision = {
    page_view_id: current.source.source_record_id,
    source_priority: current.source.source_priority,
    source_revision: current.source.source_revision,
  };
  if (oldPage && oldPage.visitor_key !== newPage?.visitor_key) addRoute(routes, oldPage.visitor_key, { ...revision, page: null });
  if (newPage) addRoute(routes, newPage.visitor_key, { ...revision, page: newPage });

  const newIdentity = current.identity;
  if (newIdentity) delivery.identities.push({ ...newIdentity, eventId: `${delivery.id}:${newIdentity.factKind}:${newIdentity.factKey}:head` });
}

function addRoute(routes: Map<string, PageRevision[]>, visitor: string, revision: PageRevision): void {
  const revisions = routes.get(visitor) ?? [];
  revisions.push(revision);
  routes.set(visitor, revisions);
}

function compareSource(left: { source: { source_priority: number; source_revision: string } }, right: { source: { source_priority: number; source_revision: string } }): number {
  const priority = left.source.source_priority - right.source.source_priority;
  if (priority) return priority;
  const leftVersion = BigInt(left.source.source_revision);
  const rightVersion = BigInt(right.source.source_revision);
  return leftVersion < rightVersion ? -1 : leftVersion > rightVersion ? 1 : 0;
}

function eventFingerprint(event: NormalizedBrowserEvent): string {
  const { ingested_at: _time, delivery_event_id: _id, ...source } = event.source;
  const identity = event.identity;
  const semanticIdentity = identity ? {
    observedAt: identity.observedAt, factKind: identity.factKind, factKey: identity.factKey,
    sourcePriority: identity.sourcePriority, sourceFactVersion: identity.sourceFactVersion,
    factDeleted: identity.factDeleted, factPayloadHash: identity.factPayloadHash,
    factPayload: identity.factPayload, evidenceKeys: identity.evidenceKeys,
  } : null;
  return canonicalJson({ source, pageRevision: event.pageRevision, identity: semanticIdentity });
}

function validateEvent(event: NormalizedBrowserEvent, tenantId: string): void {
  const source = event.source;
  if (!tenantId || source.tenant_id !== tenantId) throw new Error("Browser tenant mismatch");
  if (!source.source_record_id || PRIORITY[source.source_system] !== source.source_priority) throw new Error("Invalid browser source identity or priority");
  if (!/^(0|[1-9]\d*)$/.test(source.source_revision) || !Number.isSafeInteger(Number(source.source_revision))) throw new Error("Invalid browser source revision");
  if (event.pageRevision && (event.pageRevision.page_view_id !== source.source_record_id || event.pageRevision.source_priority !== source.source_priority || event.pageRevision.source_revision !== source.source_revision)) {
    throw new Error("Page revision does not match its source");
  }
  if (event.identity && (event.identity.factKey !== `${event.identity.factKind}:${source.source_record_id}` || event.identity.sourcePriority !== source.source_priority || event.identity.sourceFactVersion !== Number(source.source_revision))) {
    throw new Error("Identity revision does not match its source");
  }
}

function logicalHead(event: NormalizedBrowserEvent): LogicalHead {
  const { tenant_id, source_system, event_kind, source_record_id, source_priority, source_revision } = event.source;
  return { source: { tenant_id, source_system, event_kind, source_record_id, source_priority, source_revision }, pageRevision: event.pageRevision, identity: event.identity };
}
