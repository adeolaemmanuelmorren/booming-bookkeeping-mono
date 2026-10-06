import { DurableObject } from "cloudflare:workers";
import type { BrowserQueueEnvelope } from "../browser/ingress.ts";
import { normalizeJitsu } from "../browser/normalize.ts";
import { sha256 } from "../storage/json.ts";
import type { PendingIdentityFact } from "./engine.ts";

export interface IdentityTouchpointReplayEnv {
  TENANT_ID: string; IDENTITY_INGESTION_ENABLED?: string; IDENTITY_BASELINE_ID: string;
  BROWSER_BUFFER: R2Bucket;
  IDENTITY: { getByName(name: string): { status(): Promise<{ publishedVersion: number; baseline: { baselineId: string; identityVersion: number } | null }>;
    enqueue(facts: PendingIdentityFact[]): Promise<unknown> } };
}
interface State extends Record<string, SqlStorageValue> { tenant_id: string; enabled: number; cursor: string | null; lease: string | null;
  lease_until: number; next_scan_at: number; scans: number; last_error: string | null }
interface Item extends Record<string, SqlStorageValue> { object_key: string; event_index: number; event_count: number | null }
const PREFIX = "jitsu/envelopes/";

/** Serial, restart-safe replay. It never deletes the immutable source envelope. */
export class IdentityTouchpointReplay extends DurableObject<IdentityTouchpointReplayEnv> {
  constructor(ctx: DurableObjectState, env: IdentityTouchpointReplayEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_replay_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),tenant_id TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 0,cursor TEXT,lease TEXT,lease_until INTEGER NOT NULL DEFAULT 0,next_scan_at INTEGER NOT NULL DEFAULT 0,
        scans INTEGER NOT NULL DEFAULT 0,last_error TEXT);
        CREATE TABLE IF NOT EXISTS identity_replay_items(object_key TEXT PRIMARY KEY,event_index INTEGER NOT NULL DEFAULT -1,event_count INTEGER,next_attempt INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS identity_replay_pending ON identity_replay_items(next_attempt,object_key);`);
      ctx.storage.sql.exec("INSERT OR IGNORE INTO identity_replay_state(singleton,tenant_id) VALUES(1,?)", env.TENANT_ID);
      if (this.state().tenant_id !== env.TENANT_ID) throw new Error("Identity replay tenant changed");
      if (this.state().enabled && await ctx.storage.getAlarm() === null) await ctx.storage.setAlarm(Date.now() + 1_000);
    });
  }

  async stage(keys: string[]) {
    if (!Array.isArray(keys) || keys.length > 100 || new Set(keys).size !== keys.length) throw new Error("Identity replay stage is invalid");
    this.ctx.storage.transactionSync(() => {
      for (const key of keys) {
        if (!/^jitsu\/envelopes\/[a-f0-9]{64}\.json$/.test(key)) throw new Error("Identity replay key is invalid");
        this.ctx.storage.sql.exec("INSERT OR IGNORE INTO identity_replay_items(object_key) VALUES(?)", key);
      }
    });
    if (this.state().enabled && await this.ctx.storage.getAlarm() === null) await this.ctx.storage.setAlarm(Date.now() + 1_000);
    return { staged: keys.length };
  }
  async start() { await this.requireReady(); this.ctx.storage.sql.exec("UPDATE identity_replay_state SET enabled=1"); await this.ctx.storage.setAlarm(Date.now() + 1_000); return this.status(); }
  async pause() { this.ctx.storage.sql.exec("UPDATE identity_replay_state SET enabled=0,lease=NULL,lease_until=0"); await this.ctx.storage.deleteAlarm(); return this.status(); }
  async status() {
    const state = this.state();
    const counts = this.ctx.storage.sql.exec<{ objects: number; events: number; pending: number }>(
      "SELECT count(*) AS objects,coalesce(sum(event_index+1),0) AS events,coalesce(sum(event_count IS NULL OR event_index+1<event_count),0) AS pending FROM identity_replay_items",
    ).one();
    return { enabled: state.enabled === 1, scans: state.scans, ...counts, lastError: state.last_error, alarmAt: await this.ctx.storage.getAlarm() };
  }
  async alarm() {
    const lease = await this.claim();
    if (!lease) return;
    let error: string | null = null;
    try {
      await this.requireReady();
      await this.scan(lease);
      const deadline = Date.now() + 20_000;
      while (Date.now() < deadline) {
        const item = this.ctx.storage.sql.exec<Item>("SELECT object_key,event_index,event_count FROM identity_replay_items WHERE next_attempt<=? AND (event_count IS NULL OR event_index+1<event_count) ORDER BY object_key LIMIT 1", Date.now()).toArray()[0];
        if (!item) break;
        try { await this.process(item, lease); }
        catch {
          this.checkLease(lease);
          this.ctx.storage.sql.exec("UPDATE identity_replay_items SET next_attempt=? WHERE object_key=?", Date.now() + 60_000, item.object_key);
        }
      }
    } catch { error = "Identity replay failed; source envelopes and event progress are retained"; }
    finally {
      if (this.state().lease === lease) {
        this.ctx.storage.sql.exec("UPDATE identity_replay_state SET lease=NULL,lease_until=0,last_error=?", error);
        await this.ctx.storage.setAlarm(Date.now() + (error ? 60_000 : 5_000));
      }
    }
  }
  private async process(item: Item, lease: string) {
    const object = await this.env.BROWSER_BUFFER.get(item.object_key);
    if (!object) throw new Error("Staged identity envelope is missing");
    const body = await object.text();
    if (item.object_key !== `${PREFIX}${await sha256(body)}.json`) throw new Error("Identity envelope hash mismatch");
    const envelope = JSON.parse(body) as BrowserQueueEnvelope;
    const start = item.event_index + 1;
    if (envelope.schema_version !== "jitsu_events_api_v1" || !envelope.events.length || start >= envelope.events.length) throw new Error("Identity envelope is invalid");
    const end = Math.min(envelope.events.length, start + 200);
    const facts: PendingIdentityFact[] = [];
    for (const event of envelope.events.slice(start, end)) {
      if (event.tenant_id !== this.env.TENANT_ID || event.producer_id !== envelope.producer_id) throw new Error("Identity envelope scope mismatch");
      const fact = (await normalizeJitsu(event)).identity;
      if (fact) facts.push(fact);
    }
    if (facts.length) await this.env.IDENTITY.getByName(this.env.TENANT_ID).enqueue(facts);
    this.checkLease(lease);
    this.ctx.storage.sql.exec("UPDATE identity_replay_items SET event_index=?,event_count=?,next_attempt=0 WHERE object_key=?", end - 1, envelope.events.length, item.object_key);
  }
  private async scan(lease: string) {
    const state = this.state();
    if (state.next_scan_at > Date.now()) return;
    let listing: R2Objects;
    try { listing = await this.env.BROWSER_BUFFER.list({ prefix: PREFIX, limit: 100, ...(state.cursor ? { cursor: state.cursor } : {}) }); }
    catch {
      this.checkLease(lease);
      this.ctx.storage.sql.exec("UPDATE identity_replay_state SET cursor=NULL,next_scan_at=0");
      throw new Error("Identity replay listing failed");
    }
    this.checkLease(lease);
    this.ctx.storage.transactionSync(() => {
      for (const object of listing.objects) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO identity_replay_items(object_key) VALUES(?)", object.key);
      this.ctx.storage.sql.exec("UPDATE identity_replay_state SET cursor=?,scans=scans+?,next_scan_at=?", listing.truncated ? listing.cursor : null,
        listing.truncated ? 0 : 1, listing.truncated ? 0 : Date.now() + 600_000);
    });
  }
  private async requireReady() {
    if (this.env.IDENTITY_INGESTION_ENABLED !== "true") throw new Error("Identity ingestion is disabled");
    const status = await this.env.IDENTITY.getByName(this.env.TENANT_ID).status();
    if (!status.baseline || status.baseline.baselineId !== this.env.IDENTITY_BASELINE_ID || status.baseline.identityVersion !== 1 || status.publishedVersion < 1) {
      throw new Error("Identity baseline version 1 is not active");
    }
  }
  private async claim() {
    return this.ctx.storage.transaction(async () => {
      const state = this.state();
      if (!state.enabled) return null;
      if (state.lease_until > Date.now()) { await this.ctx.storage.setAlarm(state.lease_until + 1); return null; }
      const lease = crypto.randomUUID();
      this.ctx.storage.sql.exec("UPDATE identity_replay_state SET lease=?,lease_until=?", lease, Date.now() + 120_000);
      await this.ctx.storage.setAlarm(Date.now() + 120_000);
      return lease;
    });
  }
  private state() { return this.ctx.storage.sql.exec<State>("SELECT * FROM identity_replay_state").one(); }
  private checkLease(lease: string) { const state = this.state(); if (!state.enabled || state.lease !== lease || state.lease_until <= Date.now()) throw new Error("Identity replay lease expired"); }
}
