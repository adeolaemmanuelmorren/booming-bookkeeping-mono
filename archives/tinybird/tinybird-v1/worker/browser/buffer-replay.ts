import { DurableObject } from "cloudflare:workers";
import type { BrowserIngressReceipt, BrowserQueueEnvelope } from "./ingress.ts";
import { sha256 } from "../storage/json.ts";

interface RouterProgress { sequence: number; publishedSequence: number }
export interface BrowserBufferReplayEnv {
  TENANT_ID: string;
  BROWSER_INGRESS_MODE?: string;
  BROWSER_BASELINE_ID?: string;
  BROWSER_BASELINE_SEQUENCE?: string;
  BROWSER_BUFFER: R2Bucket;
  BROWSER_INGRESS: { receive(envelope: BrowserQueueEnvelope): Promise<BrowserIngressReceipt> };
  BROWSER_ROUTER: { getByName(name: string): { status(): Promise<RouterProgress> } };
}

type Meta = {
  enabled: number; baseline_id: string | null; baseline_sequence: number | null; cursor: string | null; cycle: number; cycle_started_at: number;
  last_cycle_completed_at: number; last_cycle_ms: number; next_scan_at: number;
  lease_id: string | null; lease_until: number; last_error: string | null;
};
type Item = {
  object_key: string; state: "pending" | "awaiting" | "verified";
  required_sequence: number | null; uploaded_at: number; first_seen_at: number;
  next_attempt: number; attempts: number; last_error: string | null; cleaned_at: number | null;
};

const PREFIX = "jitsu/envelopes/";
const PAGE_SIZE = 100;
const ITEMS_PER_RUN = 25;
const LEASE_MS = 120_000;
const ACTIVE_DELAY_MS = 5_000;
const SCAN_DELAY_MS = 30_000;
const RUN_BUDGET_MS = 20_000;

/** One maintenance object per tenant. It never sits on the ingress request path. */
export class BrowserBufferReplay extends DurableObject<BrowserBufferReplayEnv> {
  constructor(ctx: DurableObjectState, env: BrowserBufferReplayEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS buffer_replay_meta (
          singleton INTEGER PRIMARY KEY CHECK(singleton = 1), tenant_id TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 0, baseline_id TEXT, baseline_sequence INTEGER, cursor TEXT, cycle INTEGER NOT NULL DEFAULT 0,
          cycle_started_at INTEGER NOT NULL DEFAULT 0, last_cycle_completed_at INTEGER NOT NULL DEFAULT 0,
          last_cycle_ms INTEGER NOT NULL DEFAULT 0, next_scan_at INTEGER NOT NULL DEFAULT 0,
          lease_id TEXT, lease_until INTEGER NOT NULL DEFAULT 0, last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS buffer_replay_items (
          object_key TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'pending', required_sequence INTEGER,
          uploaded_at INTEGER NOT NULL, first_seen_at INTEGER NOT NULL, next_attempt INTEGER NOT NULL DEFAULT 0,
          attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, cleaned_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS buffer_replay_due ON buffer_replay_items(next_attempt) WHERE cleaned_at IS NULL;
      `);
      ctx.storage.sql.exec("INSERT OR IGNORE INTO buffer_replay_meta(singleton, tenant_id) VALUES(1, ?)", env.TENANT_ID);
      const tenant = ctx.storage.sql.exec<{ tenant_id: string }>("SELECT tenant_id FROM buffer_replay_meta").one().tenant_id;
      if (!env.TENANT_ID || tenant !== env.TENANT_ID) throw new Error("Replay tenant mismatch");
      if (this.meta().enabled) await this.schedule();
    });
  }

  async start() {
    this.requireLiveBaseline();
    this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET enabled = 1, baseline_id = ?, baseline_sequence = ? WHERE singleton = 1", this.env.BROWSER_BASELINE_ID!, Number(this.env.BROWSER_BASELINE_SEQUENCE));
    await this.schedule();
    return this.status();
  }

  async pause() {
    this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET enabled = 0, lease_id = NULL, lease_until = 0 WHERE singleton = 1");
    await this.ctx.storage.deleteAlarm();
    return this.status();
  }

  async wake() { await this.alarm(); return this.status(); }

  async alarm(): Promise<void> {
    const lease = await this.claim();
    if (!lease) return;
    let error: string | null = null;
    try {
      this.requireLiveBaseline();
      const deadline = Date.now() + RUN_BUDGET_MS;
      await this.scan(lease);
      const rows = this.ctx.storage.sql.exec<Item>(
        "SELECT * FROM buffer_replay_items WHERE cleaned_at IS NULL AND next_attempt <= ? ORDER BY next_attempt, object_key LIMIT ?",
        Date.now(), ITEMS_PER_RUN,
      ).toArray();
      for (const row of rows) {
        if (Date.now() >= deadline) break;
        await this.refresh(lease);
        await this.process(row, lease);
      }
    } catch {
      error = "Replay run failed; buffered objects remain retained";
    } finally {
      await this.ctx.storage.transaction(async () => {
        if (!this.owns(lease)) return;
        this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET lease_id = NULL, lease_until = 0, last_error = ? WHERE singleton = 1", error);
        await this.schedule();
      });
    }
  }

  async status() {
    const meta = this.meta();
    const counts = this.ctx.storage.sql.exec<{ state: string; count: number }>("SELECT state, COUNT(*) AS count FROM buffer_replay_items GROUP BY state").toArray();
    const pending = this.ctx.storage.sql.exec<{ count: number; oldest: number | null }>("SELECT COUNT(*) AS count, MIN(uploaded_at) AS oldest FROM buffer_replay_items WHERE cleaned_at IS NULL").one();
    const failures = this.ctx.storage.sql.exec<{ object_key: string; attempts: number; last_error: string }>("SELECT object_key, attempts, last_error FROM buffer_replay_items WHERE last_error IS NOT NULL ORDER BY first_seen_at LIMIT 10").toArray();
    return {
      enabled: meta.enabled === 1, baselineId: meta.baseline_id, baselineSequence: meta.baseline_sequence, cycle: meta.cycle, cursor: meta.cursor,
      lastCycleCompletedAt: meta.last_cycle_completed_at, lastCycleMs: meta.last_cycle_ms,
      pending: pending.count, oldestPendingAgeMs: pending.oldest === null ? 0 : Math.max(0, Date.now() - pending.oldest),
      counts: Object.fromEntries(counts.map(row => [row.state, row.count])), failures,
      lastError: meta.last_error, alarmAt: await this.ctx.storage.getAlarm(),
    };
  }

  private async scan(lease: string) {
    const meta = this.meta();
    if (meta.next_scan_at > Date.now()) return;
    let listing: R2Objects;
    try {
      listing = await this.env.BROWSER_BUFFER.list({ prefix: PREFIX, limit: PAGE_SIZE, ...(meta.cursor ? { cursor: meta.cursor } : {}) });
    } catch {
      // Repeating the prefix is safe. Do not get stuck forever on a rejected opaque cursor.
      this.mutate(lease, () => this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET cursor = NULL, cycle_started_at = 0, next_scan_at = ? WHERE singleton = 1", Date.now() + SCAN_DELAY_MS));
      throw new Error("Buffer listing failed; restart the scan");
    }
    this.mutate(lease, () => {
      const now = Date.now();
      for (const object of listing.objects) {
        // A previously verified hash can reappear after an old request finishes. Clean it again.
        this.ctx.storage.sql.exec(`INSERT INTO buffer_replay_items(object_key, uploaded_at, first_seen_at)
          VALUES(?, ?, ?) ON CONFLICT(object_key) DO UPDATE SET cleaned_at = NULL`, object.key, object.uploaded.getTime(), now);
      }
      const started = meta.cycle_started_at || now;
      if (listing.truncated) {
        this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET cursor = ?, cycle_started_at = ?, next_scan_at = 0 WHERE singleton = 1", listing.cursor, started);
        return;
      }
      this.ctx.storage.sql.exec(`UPDATE buffer_replay_meta SET cursor = NULL, cycle = cycle + 1,
        cycle_started_at = 0, last_cycle_completed_at = ?, last_cycle_ms = ?, next_scan_at = ? WHERE singleton = 1`, now, now - started, now + SCAN_DELAY_MS);
    });
  }

  private async process(row: Item, lease: string): Promise<void> {
    try {
      if (row.state === "awaiting") {
        const progress = await this.env.BROWSER_ROUTER.getByName(this.env.TENANT_ID).status();
        this.check(lease);
        if (!validProgress(progress) || row.required_sequence === null) throw new Error("Invalid router progress");
        if (progress.publishedSequence < row.required_sequence) {
          this.mutate(lease, () => this.ctx.storage.sql.exec("UPDATE buffer_replay_items SET next_attempt = ?, last_error = NULL WHERE object_key = ?", Date.now() + ACTIVE_DELAY_MS, row.object_key));
          return;
        }
        this.mutate(lease, () => this.ctx.storage.sql.exec("UPDATE buffer_replay_items SET state = 'verified', last_error = NULL WHERE object_key = ?", row.object_key));
        row.state = "verified";
      }
      const object = await this.env.BROWSER_BUFFER.get(row.object_key);
      this.check(lease);
      if (!object) {
        if (row.state !== "verified") throw new Error("Unpublished object disappeared");
        this.markClean(row, lease);
        return;
      }
      const body = await object.text();
      const hash = await sha256(body);
      this.check(lease);
      if (row.object_key !== `${PREFIX}${hash}.json`) throw new Error("Buffered object does not match its hash key");
      if (row.state === "verified") {
        // The durable publication receipt is saved before deletion. Ambiguous deletes retry safely.
        await this.env.BROWSER_BUFFER.delete(row.object_key);
        this.markClean(row, lease);
        return;
      }
      const receipt = await this.env.BROWSER_INGRESS.receive(JSON.parse(body) as BrowserQueueEnvelope);
      this.check(lease);
      if (receipt.status !== "accepted" || receipt.key !== row.object_key || receipt.sha256 !== hash) throw new Error("Ingress did not accept this buffered object");
      // This fixed fence includes every batch accepted before the status call, even on a retry.
      const progress = await this.env.BROWSER_ROUTER.getByName(this.env.TENANT_ID).status();
      if (!validProgress(progress)) throw new Error("Invalid router progress");
      this.mutate(lease, () => this.ctx.storage.sql.exec(`UPDATE buffer_replay_items SET state = 'awaiting',
        required_sequence = ?, next_attempt = ?, last_error = NULL WHERE object_key = ?`, progress.sequence, Date.now() + ACTIVE_DELAY_MS, row.object_key));
    } catch {
      this.mutate(lease, () => this.ctx.storage.sql.exec(`UPDATE buffer_replay_items SET attempts = attempts + 1,
        next_attempt = ?, last_error = 'Replay or cleanup failed; durable progress is retained' WHERE object_key = ?`, Date.now() + SCAN_DELAY_MS, row.object_key));
    }
  }

  private markClean(row: Item, lease: string) {
    this.mutate(lease, () => this.ctx.storage.sql.exec("UPDATE buffer_replay_items SET cleaned_at = ?, last_error = NULL WHERE object_key = ?", Date.now(), row.object_key));
  }

  private async claim(): Promise<string | null> {
    return this.ctx.storage.transaction(async () => {
      const meta = this.meta();
      if (!meta.enabled) return null;
      if (meta.lease_until > Date.now()) {
        await this.ctx.storage.setAlarm(meta.lease_until);
        return null;
      }
      const lease = crypto.randomUUID();
      const until = Date.now() + LEASE_MS;
      this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET lease_id = ?, lease_until = ? WHERE singleton = 1", lease, until);
      await this.ctx.storage.setAlarm(until);
      return lease;
    });
  }

  private async refresh(lease: string) {
    await this.ctx.storage.transaction(async () => {
      this.check(lease);
      const until = Date.now() + LEASE_MS;
      this.ctx.storage.sql.exec("UPDATE buffer_replay_meta SET lease_until = ? WHERE singleton = 1", until);
      await this.ctx.storage.setAlarm(until);
    });
  }

  private mutate(lease: string, change: () => unknown) {
    this.ctx.storage.transactionSync(() => { this.check(lease); change(); });
  }

  private owns(lease: string) { const meta = this.meta(); return meta.enabled === 1 && meta.lease_id === lease; }
  private check(lease: string) { if (!this.owns(lease) || this.meta().lease_until <= Date.now()) throw new Error("Replay lease expired"); }
  private meta() { return this.ctx.storage.sql.exec<Meta>("SELECT * FROM buffer_replay_meta WHERE singleton = 1").one(); }

  private async schedule() {
    const meta = this.meta();
    if (!meta.enabled) { await this.ctx.storage.deleteAlarm(); return; }
    if (meta.lease_until > Date.now()) { await this.ctx.storage.setAlarm(meta.lease_until); return; }
    const due = this.ctx.storage.sql.exec<{ next: number | null }>("SELECT MIN(next_attempt) AS next FROM buffer_replay_items WHERE cleaned_at IS NULL").one().next;
    const next = Math.min(meta.next_scan_at, due ?? Number.POSITIVE_INFINITY);
    await this.ctx.storage.setAlarm(Math.max(Date.now() + ACTIVE_DELAY_MS, next));
  }

  private requireLiveBaseline() {
    if (this.env.BROWSER_INGRESS_MODE !== "live") throw new Error("Buffer replay requires live ingress");
    const sequence = Number(this.env.BROWSER_BASELINE_SEQUENCE ?? "0");
    if (!this.env.BROWSER_BASELINE_ID || this.env.BROWSER_BASELINE_ID === "empty" || !Number.isSafeInteger(sequence) || sequence < 1) throw new Error("Buffer replay requires a sealed historical baseline");
    const meta = this.meta();
    if (meta.baseline_id !== null && (meta.baseline_id !== this.env.BROWSER_BASELINE_ID || meta.baseline_sequence !== sequence)) throw new Error("Replay baseline cannot change after activation");
  }
}

function validProgress(value: RouterProgress): boolean {
  return Number.isSafeInteger(value.sequence) && value.sequence >= 0
    && Number.isSafeInteger(value.publishedSequence) && value.publishedSequence >= 0 && value.publishedSequence <= value.sequence;
}
