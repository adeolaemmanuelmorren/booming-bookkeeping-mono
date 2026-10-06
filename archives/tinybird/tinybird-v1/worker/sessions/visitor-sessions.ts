import { DurableObject } from "cloudflare:workers";
import { applyPageRevisions, pagesForVisitor, type PageRevision } from "./page-revisions.ts";
import { buildSessions, canonicalJson, type SessionFact } from "./session-engine.ts";
import { createSnapshot, publishSnapshot, type SessionPublisher, type SessionSnapshot } from "./session-publication.ts";
import { sha256 } from "../storage/json.ts";
import type { SessionBaselineReader, VisitorBaseline } from "../browser/baseline.ts";
import { readChunkedJson, writeChunkedJson } from "../storage/chunked-json.ts";

export interface SessionEnv {
  TENANT_ID: string;
  SESSION_PUBLISHER: SessionPublisher;
  SESSION_FLUSH_DELAY_MS?: string;
  SESSION_BASELINE?: SessionBaselineReader;
}

type Metadata = {
  tenant_id: string;
  visitor_key: string;
  revision: number;
  published_revision: number;
}

type OutboxRow = {
  revision: number;
  snapshot_json: string;
  attempts: number;
  next_attempt_at: number;
  lease_id: string | null;
  lease_until: number;
  last_error: string | null;
}

export interface PrepareSessionGroup {
  groupId: string;
  sequence: number;
  visitorKey: string;
  revisions: PageRevision[];
}

export interface ReceiveResult {
  revision: string;
  changedPageIds: string[];
  sessionCount: number;
  publicationQueued: boolean;
}

const LEASE_MS = 60_000;
const RETRY_BASE_MS = 10_000;
const RETRY_MAX_MS = 300_000;

export class VisitorSessions extends DurableObject<SessionEnv> {
  constructor(ctx: DurableObjectState, env: SessionEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS session_metadata (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          tenant_id TEXT NOT NULL,
          visitor_key TEXT NOT NULL,
          revision INTEGER NOT NULL,
          published_revision INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS page_heads (
          page_id TEXT PRIMARY KEY,
          revision_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS current_sessions (
          session_id TEXT PRIMARY KEY,
          session_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prepared_session_groups (
          group_id TEXT PRIMARY KEY,
          sequence INTEGER NOT NULL UNIQUE,
          fingerprint TEXT NOT NULL,
          snapshot_json TEXT
        );
        CREATE TABLE IF NOT EXISTS session_baseline (
          singleton INTEGER PRIMARY KEY CHECK(singleton = 1), fingerprint TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prepared_session_chunks (
          owner_key TEXT NOT NULL, chunk_index INTEGER NOT NULL, payload TEXT NOT NULL,
          PRIMARY KEY(owner_key, chunk_index)
        );
        CREATE TABLE IF NOT EXISTS session_outbox (
          revision INTEGER PRIMARY KEY,
          snapshot_json TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          next_attempt_at INTEGER NOT NULL,
          lease_id TEXT,
          lease_until INTEGER NOT NULL DEFAULT 0,
          last_error TEXT
        );
      `);
      if (this.oldestOutbox() && await ctx.storage.getAlarm() === null) {
        await this.scheduleNextAlarm();
      }
    });
  }

  /** Bootstrap uses this same method with all current source pages and revisions. */
  async receive(visitorKey: string, revisions: PageRevision[]): Promise<ReceiveResult> {
    if (this.groupMode()) throw new Error("This visitor is controlled by BrowserRouter");
    return this.apply(visitorKey, revisions);
  }

  /** Only BrowserRouter publishes these snapshots through its complete group commit. */
  async prepareGroup(input: PrepareSessionGroup): Promise<SessionSnapshot> {
    if (!input.groupId || !Number.isSafeInteger(input.sequence) || input.sequence < 1) {
      throw new Error("Invalid session group identity");
    }
    if (!this.metadata() && this.env.SESSION_BASELINE) {
      const seed = await this.env.SESSION_BASELINE.loadVisitor(this.env.TENANT_ID, input.visitorKey);
      if (seed) await this.bootstrap(seed);
    }
    const fingerprint = await sha256(canonicalJson(input));
    const result = await this.apply(input.visitorKey, input.revisions, { ...input, fingerprint });
    if (!result.snapshot) throw new Error("Session group did not produce a snapshot");
    return result.snapshot;
  }

  /** Hydrate once, on the first live event. Bulk bootstrap never creates visitor objects. */
  async bootstrap(seed: VisitorBaseline): Promise<void> {
    if (seed.tenantId !== this.env.TENANT_ID || seed.visitorKey !== seed.snapshot.visitor_key || seed.tenantId !== seed.snapshot.tenant_id) {
      throw new Error("Session baseline identity mismatch");
    }
    const revision = Number(seed.snapshot.revision);
    if (!Number.isSafeInteger(revision) || revision < 1 || String(revision) !== seed.snapshot.revision) throw new Error("Invalid baseline revision");
    for (const head of seed.heads) {
      if (head.page && head.page.visitor_key !== seed.visitorKey) throw new Error("Misrouted baseline page");
    }
    const heads = applyPageRevisions(new Map(), seed.heads).heads;
    const sessions = buildSessions(seed.visitorKey, pagesForVisitor(heads, seed.visitorKey));
    const expected = createSnapshot(seed.tenantId, seed.visitorKey, seed.snapshot.revision, sessions);
    if (canonicalJson(expected) !== canonicalJson(seed.snapshot)) throw new Error("Baseline pages do not match verified snapshot");
    const orderedHeads = [...heads.values()].sort((left, right) => left.page_view_id.localeCompare(right.page_view_id));
    const fingerprint = await sha256(canonicalJson({ ...seed, heads: orderedHeads }));
    this.ctx.storage.transactionSync(() => {
      const saved = this.ctx.storage.sql.exec<{ fingerprint: string }>("SELECT fingerprint FROM session_baseline").toArray()[0];
      if (saved) {
        if (saved.fingerprint !== fingerprint) throw new Error("Conflicting session baseline retry");
        return;
      }
      if (this.metadata()) throw new Error("Cannot bootstrap an initialized visitor");
      for (const [id, head] of heads) this.ctx.storage.sql.exec("INSERT INTO page_heads VALUES (?, ?)", id, JSON.stringify(head));
      for (const session of sessions) this.ctx.storage.sql.exec("INSERT INTO current_sessions VALUES (?, ?)", session.session_id, JSON.stringify(session));
      this.ctx.storage.sql.exec("INSERT INTO session_metadata VALUES (1, ?, ?, ?, ?)", seed.tenantId, seed.visitorKey, revision, revision);
      this.ctx.storage.sql.exec("INSERT INTO session_baseline VALUES (1, ?)", fingerprint);
    });
  }

  private async apply(visitorKey: string, revisions: PageRevision[], group?: PrepareSessionGroup & { fingerprint: string }): Promise<ReceiveResult & { snapshot?: SessionSnapshot }> {
    if (!visitorKey) throw new Error("Visitor key is required");
    if (!this.env.TENANT_ID) throw new Error("TENANT_ID is required");
    for (const revision of revisions) {
      if (revision.page && revision.page.visitor_key !== visitorKey) {
        throw new Error("Misrouted page revision belongs to another visitor");
      }
    }

    // The async storage transaction includes SQL and the alarm. No external I/O occurs here.
    return this.ctx.storage.transaction(async () => {
      const sql = this.ctx.storage.sql;
      const existingMetadata = this.metadata();
      if (group) {
        const saved = sql.exec<{ fingerprint: string; snapshot_json: string | null }>(
          "SELECT fingerprint, snapshot_json FROM prepared_session_groups WHERE group_id = ?", group.groupId,
        ).toArray()[0];
        if (saved) {
          if (saved.fingerprint !== group.fingerprint) throw new Error("Conflicting session group retry");
          if (saved.snapshot_json === null) throw new Error("Session group was superseded after publication");
          const snapshot = readChunkedJson<SessionSnapshot>(this.ctx.storage, "prepared_session_chunks", group.groupId);
          return { revision: snapshot.revision, changedPageIds: [], sessionCount: snapshot.sessions.length, publicationQueued: false, snapshot };
        }
        const latest = sql.exec<{ sequence: number }>("SELECT sequence FROM prepared_session_groups ORDER BY sequence DESC LIMIT 1").toArray()[0];
        if (latest && group.sequence <= latest.sequence) throw new Error("Stale session group sequence");
        if (existingMetadata && !latest && !this.hasBaseline()) throw new Error("Cannot mix independent and grouped session publication");
      } else if (this.groupMode()) {
        throw new Error("This visitor is controlled by BrowserRouter");
      }
      if (existingMetadata && existingMetadata.visitor_key !== visitorKey) {
        throw new Error("Durable Object is already bound to another visitor");
      }
      if (existingMetadata && existingMetadata.tenant_id !== this.env.TENANT_ID) {
        throw new Error("Durable Object is already bound to another tenant");
      }
      const previousHeads = this.pageHeads();
      const result = applyPageRevisions(previousHeads, revisions);
      const previousSessions = this.sessions();
      const currentSessions = buildSessions(visitorKey, pagesForVisitor(result.heads, visitorKey));
      const sessionsChanged = canonicalJson(previousSessions) !== canonicalJson(currentSessions);
      const firstSnapshot = existingMetadata === undefined;
      const publicationQueued = sessionsChanged || firstSnapshot;
      const revision = (existingMetadata?.revision ?? 0) + Number(publicationQueued);
      if (!Number.isSafeInteger(revision)) throw new Error("Session revision exceeded integer precision");

      for (const [pageId, head] of result.heads) {
        if (canonicalJson(previousHeads.get(pageId)) === canonicalJson(head)) continue;
        sql.exec(
          "INSERT INTO page_heads (page_id, revision_json) VALUES (?, ?) ON CONFLICT(page_id) DO UPDATE SET revision_json = excluded.revision_json",
          pageId, JSON.stringify(head),
        );
      }
      sql.exec(
        "INSERT INTO session_metadata (singleton, tenant_id, visitor_key, revision, published_revision) VALUES (1, ?, ?, ?, 0) ON CONFLICT(singleton) DO UPDATE SET revision = excluded.revision",
        this.env.TENANT_ID, visitorKey, revision,
      );
      if (publicationQueued) {
        sql.exec("DELETE FROM current_sessions");
        for (const session of currentSessions) {
          sql.exec("INSERT INTO current_sessions (session_id, session_json) VALUES (?, ?)", session.session_id, JSON.stringify(session));
        }
        const snapshot = createSnapshot(this.env.TENANT_ID, visitorKey, String(revision), currentSessions);
        if (!group) {
          sql.exec(
            "INSERT INTO session_outbox (revision, snapshot_json, next_attempt_at) VALUES (?, ?, ?)",
            revision, JSON.stringify(snapshot), Date.now() + this.flushDelay(),
          );
        }
      }
      const snapshot = group ? createSnapshot(this.env.TENANT_ID, visitorKey, String(revision), currentSessions) : undefined;
      if (group) {
        // The serial router prepares a later group only after earlier groups have committed.
        // Keep their receipts, but their complete snapshots now live in Tinybird.
        sql.exec("UPDATE prepared_session_groups SET snapshot_json = NULL WHERE snapshot_json IS NOT NULL");
        sql.exec("DELETE FROM prepared_session_chunks");
        sql.exec("INSERT INTO prepared_session_groups VALUES (?, ?, ?, ?)",
          group.groupId, group.sequence, group.fingerprint, "chunks");
        writeChunkedJson(this.ctx.storage, "prepared_session_chunks", group.groupId, snapshot);
      }
      await this.scheduleNextAlarm();
      return {
        snapshot,
        revision: String(revision),
        changedPageIds: result.changedPageIds,
        sessionCount: currentSessions.length,
        publicationQueued,
      };
    });
  }

  async alarm(): Promise<void> {
    const leased = await this.ctx.storage.transaction(async () => {
      const next = this.oldestOutbox();
      if (!next) return undefined;
      if (next.next_attempt_at > Date.now() || next.lease_until > Date.now()) {
        await this.scheduleNextAlarm();
        return undefined;
      }
      const leaseId = crypto.randomUUID();
      this.ctx.storage.sql.exec(
        "UPDATE session_outbox SET lease_id = ?, lease_until = ?, attempts = attempts + 1 WHERE revision = ?",
        leaseId, Date.now() + LEASE_MS, next.revision,
      );
      // If this execution is interrupted, a persisted alarm reclaims the expired lease.
      await this.scheduleNextAlarm();
      return { ...next, lease_id: leaseId, attempts: next.attempts + 1 };
    });
    if (!leased) return;

    try {
      await publishSnapshot(JSON.parse(leased.snapshot_json) as SessionSnapshot, this.env.SESSION_PUBLISHER);
      await this.finishPublication(leased);
    } catch (error) {
      await this.retryPublication(leased, error);
    }
  }

  async status(): Promise<{
    visitorKey: string | null;
    revision: string;
    publishedRevision: string;
    pendingSnapshots: number;
    pageHeads: number;
    sessions: number;
    oldestPendingRevision: string | null;
    attempts: number;
    lastError: string | null;
    alarmAt: number | null;
  }> {
    const metadata = this.metadata();
    const oldest = this.oldestOutbox();
    return {
      visitorKey: metadata?.visitor_key ?? null,
      revision: String(metadata?.revision ?? 0),
      publishedRevision: String(metadata?.published_revision ?? 0),
      pendingSnapshots: this.countRows("session_outbox"),
      pageHeads: this.countRows("page_heads"),
      sessions: this.countRows("current_sessions"),
      oldestPendingRevision: oldest ? String(oldest.revision) : null,
      attempts: oldest?.attempts ?? 0,
      lastError: oldest?.last_error ?? null,
      alarmAt: await this.ctx.storage.getAlarm(),
    };
  }

  private async finishPublication(leased: OutboxRow): Promise<void> {
    await this.ctx.storage.transaction(async () => {
      const ownsLease = this.ctx.storage.sql.exec(
        "SELECT revision FROM session_outbox WHERE revision = ? AND lease_id = ?",
        leased.revision, leased.lease_id,
      ).toArray().length > 0;
      if (!ownsLease) return;
      this.ctx.storage.sql.exec(
        "UPDATE session_metadata SET published_revision = MAX(published_revision, ?) WHERE singleton = 1", leased.revision,
      );
      this.ctx.storage.sql.exec("DELETE FROM session_outbox WHERE revision = ? AND lease_id = ?", leased.revision, leased.lease_id);
      await this.scheduleNextAlarm();
    });
  }

  private async retryPublication(leased: OutboxRow, error: unknown): Promise<void> {
    const delay = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** Math.min(leased.attempts - 1, 5));
    await this.ctx.storage.transaction(async () => {
      this.ctx.storage.sql.exec(
        "UPDATE session_outbox SET lease_id = NULL, lease_until = 0, next_attempt_at = ?, last_error = ? WHERE revision = ? AND lease_id = ?",
        Date.now() + delay, error instanceof Error ? error.message : "Snapshot publication failed", leased.revision, leased.lease_id,
      );
      await this.scheduleNextAlarm();
    });
  }

  private async scheduleNextAlarm(): Promise<void> {
    const next = this.oldestOutbox();
    if (!next) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    await this.ctx.storage.setAlarm(Math.max(Date.now() + 1, next.next_attempt_at, next.lease_until));
  }

  private groupMode(): boolean {
    return this.hasBaseline() || this.ctx.storage.sql.exec("SELECT group_id FROM prepared_session_groups LIMIT 1").toArray().length > 0;
  }

  private hasBaseline(): boolean {
    return this.ctx.storage.sql.exec("SELECT singleton FROM session_baseline").toArray().length > 0;
  }

  private metadata(): Metadata | undefined {
    return this.ctx.storage.sql.exec<Metadata>("SELECT tenant_id, visitor_key, revision, published_revision FROM session_metadata WHERE singleton = 1").toArray()[0];
  }

  private oldestOutbox(): OutboxRow | undefined {
    return this.ctx.storage.sql.exec<OutboxRow>("SELECT * FROM session_outbox ORDER BY revision LIMIT 1").toArray()[0];
  }

  private pageHeads(): Map<string, PageRevision> {
    const heads = new Map<string, PageRevision>();
    for (const row of this.ctx.storage.sql.exec<{ revision_json: string }>("SELECT revision_json FROM page_heads")) {
      const revision = JSON.parse(row.revision_json) as PageRevision;
      heads.set(revision.page_view_id, revision);
    }
    return heads;
  }

  private sessions(): SessionFact[] {
    const sessions = this.ctx.storage.sql.exec<{ session_json: string }>("SELECT session_json FROM current_sessions").toArray()
      .map((row) => JSON.parse(row.session_json) as SessionFact);
    // The engine returns chronological session order, while SQLite's natural row order is unspecified.
    return sessions.sort((left, right) => left.session_start_timestamp.localeCompare(right.session_start_timestamp));
  }

  private countRows(table: "page_heads" | "current_sessions" | "session_outbox"): number {
    return this.ctx.storage.sql.exec<{ count: number }>(`SELECT COUNT(*) AS count FROM ${table}`).one().count;
  }

  private flushDelay(): number {
    const value = Number(this.env.SESSION_FLUSH_DELAY_MS ?? "1000");
    if (!Number.isSafeInteger(value) || value < 0) throw new Error("Invalid SESSION_FLUSH_DELAY_MS");
    return value;
  }
}
