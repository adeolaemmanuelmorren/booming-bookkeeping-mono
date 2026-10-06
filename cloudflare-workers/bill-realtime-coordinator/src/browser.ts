import { DurableObject } from "cloudflare:workers";
import { normalizeJitsu, type JitsuObservationInput } from "../../../tinybird-v1/worker/browser/normalize.ts";
import { sha256 } from "../../../tinybird-v1/worker/storage/json.ts";
import type { Publication } from "./publication.ts";
import { fingerprint, type BrowserReplacement } from "./replacements.ts";

interface Env {
  BROWSER_BUFFER: R2Bucket;
  PUBLICATION: DurableObjectNamespace<Publication>;
  PUBLICATION_NAME?: string;
}
type Meta = { enabled: number; cursor: string | null; cycle: number; completed_at: number;
  lease: string | null; lease_until: number; last_error: string | null };
type Item = { object_key: string; event_offset: number };

export async function browserReplacement(observation: JitsuObservationInput): Promise<BrowserReplacement> {
  if (observation.tenant_id !== "boom") throw new Error("Browser tenant mismatch.");
  const browser = await normalizeJitsu(observation);
  const contract: BrowserReplacement = {
    source: "browser", source_account: "default",
    scope_id: `${browser.source.event_kind}:${browser.source.source_record_id}`,
    replacement_id: "pending", observation_sequence: Number(browser.source.source_revision),
    observed_at: observation.ingested_at, rows: [], evidence_inbox_ids: [], source_evidence: {}, browser,
  };
  contract.replacement_id = `browser:${await sha256(fingerprint(contract))}`;
  return contract;
}

// Read the existing immutable buffer. Never delete or rewrite source objects.
// Hash-named keys require repeated full scans to discover insertions before the cursor.
export class BrowserSource extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS browser_scan (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), enabled INTEGER NOT NULL DEFAULT 0,
        cursor TEXT, cycle INTEGER NOT NULL DEFAULT 0, completed_at INTEGER NOT NULL DEFAULT 0,
        lease TEXT, lease_until INTEGER NOT NULL DEFAULT 0, last_error TEXT);
        INSERT OR IGNORE INTO browser_scan(singleton) VALUES (1);
        CREATE TABLE IF NOT EXISTS browser_objects (
          object_key TEXT PRIMARY KEY, event_offset INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS browser_pending ON browser_objects(done, object_key);`);
      const columns = ctx.storage.sql.exec<{ name: string }>("PRAGMA table_info(browser_objects)").toArray();
      if (!columns.some(column => column.name === "priority")) {
        ctx.storage.sql.exec("ALTER TABLE browser_objects ADD COLUMN priority INTEGER NOT NULL DEFAULT 0");
      }
      if (!columns.some(column => column.name === "notified_at")) {
        ctx.storage.sql.exec("ALTER TABLE browser_objects ADD COLUMN notified_at INTEGER NOT NULL DEFAULT 0");
      }
      ctx.storage.sql.exec("CREATE INDEX IF NOT EXISTS browser_recent_pending ON browser_objects(done, priority DESC, notified_at DESC, object_key)");
    });
  }

  async start() {
    this.ctx.storage.sql.exec("UPDATE browser_scan SET enabled = 1");
    return this.wake();
  }
  async pause() {
    this.ctx.storage.sql.exec("UPDATE browser_scan SET enabled = 0");
    await this.ctx.storage.deleteAlarm();
    return this.status();
  }
  status() {
    const meta = this.meta();
    const counts = this.ctx.storage.sql.exec(`SELECT COUNT(*) objects,
      COALESCE(SUM(done), 0) completedObjects, COALESCE(SUM(event_offset), 0) acceptedEvents FROM browser_objects`).one();
    return { ...counts, enabled: Boolean(meta.enabled), cycle: meta.cycle,
      lastCycleCompletedAt: meta.completed_at || null, lastError: meta.last_error,
      running: meta.lease_until > Date.now() };
  }
  async alarm() { await this.wake(); }
  async notify(keys: string[]) {
    if (keys.length > 100 || keys.some(key => !/^jitsu\/envelopes\/[a-f0-9]{64}\.json$/.test(key))) {
      throw new Error("Invalid browser notification keys.");
    }
    this.ctx.storage.transactionSync(() => {
      for (const key of keys) {
        this.ctx.storage.sql.exec(`INSERT INTO browser_objects(object_key, priority, notified_at) VALUES (?, 1, ?)
          ON CONFLICT(object_key) DO UPDATE SET priority = 1, notified_at = excluded.notified_at`, key, Date.now());
      }
    });
    if (this.meta().enabled) await this.ctx.storage.setAlarm(Date.now() + 1000);
    return { accepted: keys.length };
  }
  async wake() {
    const meta = this.meta();
    if (!meta.enabled || meta.lease_until > Date.now()) return this.status();
    const lease = crypto.randomUUID();
    this.ctx.storage.sql.exec("UPDATE browser_scan SET lease = ?, lease_until = ?", lease, Date.now() + 120_000);
    await this.ctx.storage.setAlarm(Date.now() + 60_000);
    try {
      const pending = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) n FROM browser_objects WHERE done = 0").one().n;
      if (pending < 100) await this.scan();
      const items = this.ctx.storage.sql.exec<Item>(
        "SELECT object_key, event_offset FROM browser_objects WHERE done = 0 ORDER BY priority DESC, notified_at DESC, object_key LIMIT 10").toArray();
      const deadline = Date.now() + 20_000;
      for (const item of items) {
        if (Date.now() >= deadline || !this.meta().enabled) break;
        await this.process(item);
      }
      this.ctx.storage.sql.exec("UPDATE browser_scan SET last_error = NULL WHERE lease = ?", lease);
    } catch {
      this.ctx.storage.sql.exec("UPDATE browser_scan SET last_error = 'Browser ingestion incomplete; retained for retry' WHERE lease = ?", lease);
    } finally {
      this.ctx.storage.sql.exec("UPDATE browser_scan SET lease = NULL, lease_until = 0 WHERE lease = ?", lease);
      if (!this.meta().enabled) await this.ctx.storage.deleteAlarm();
    }
    return this.status();
  }
  private async scan() {
    const meta = this.meta();
    const page = await this.env.BROWSER_BUFFER.list({ prefix: "jitsu/envelopes/", limit: 100,
      ...(meta.cursor ? { cursor: meta.cursor } : {}) });
    this.ctx.storage.transactionSync(() => {
      for (const object of page.objects) {
        this.ctx.storage.sql.exec("INSERT OR IGNORE INTO browser_objects(object_key) VALUES (?)", object.key);
      }
      this.ctx.storage.sql.exec("UPDATE browser_scan SET cursor = ?, cycle = cycle + ?, completed_at = ?",
        page.truncated ? page.cursor : null, page.truncated ? 0 : 1, page.truncated ? meta.completed_at : Date.now());
    });
  }
  private async process(item: Item) {
    const object = await this.env.BROWSER_BUFFER.get(item.object_key);
    if (!object || object.size > 2_000_000) throw new Error("Buffered object missing or oversized.");
    const body = await object.text();
    if (item.object_key !== `jitsu/envelopes/${await sha256(body)}.json`) throw new Error("Buffered object hash mismatch.");
    const envelope = JSON.parse(body);
    if (envelope.schema_version !== "jitsu_events_api_v1" || !Array.isArray(envelope.events)) {
      throw new Error("Invalid browser envelope.");
    }
    const end = Math.min(item.event_offset + 25, envelope.events.length);
    const contracts = [];
    for (const observation of envelope.events.slice(item.event_offset, end)) {
      contracts.push(await browserReplacement(observation));
    }
    if (contracts.length) await this.env.PUBLICATION.getByName(this.env.PUBLICATION_NAME ?? "boom").enqueue(contracts);
    // A crash after enqueue replays the same replacement IDs safely.
    this.ctx.storage.sql.exec("UPDATE browser_objects SET event_offset = ?, done = ? WHERE object_key = ?",
      end, end === envelope.events.length ? 1 : 0, item.object_key);
  }
  private meta(): Meta { return this.ctx.storage.sql.exec<Meta>("SELECT * FROM browser_scan").one(); }
}
