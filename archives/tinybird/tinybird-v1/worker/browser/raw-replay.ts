import { DurableObject } from 'cloudflare:workers';
import type { BrowserIngressReceipt, BrowserQueueEnvelope } from './ingress.ts';
import { sha256 } from '../storage/json.ts';

export interface RawReplayEnv {
  TENANT_ID: string;
  BROWSER_INGRESS_MODE?: string;
  BROWSER_BUFFER: R2Bucket;
  BROWSER_INGRESS: {
    receiveBatch(envelopes: BrowserQueueEnvelope[]): Promise<BrowserIngressReceipt[]>;
  };
}

interface ReplayState extends Record<string, SqlStorageValue> {
  enabled: number;
  cursor: string | null;
  scans: number;
  next_scan_at: number;
  lease: string | null;
  lease_until: number;
  last_verified_at: number;
  last_error: string | null;
}

const PREFIX = 'jitsu/envelopes/';
const LEASE_MS = 120_000;

/** Raw delivery has its own progress. It never waits for identity or deletes source envelopes. */
export class RawTouchpointReplay extends DurableObject<RawReplayEnv> {
  constructor(ctx: DurableObjectState, env: RawReplayEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS raw_replay_state (
          singleton INTEGER PRIMARY KEY CHECK(singleton=1), tenant_id TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 0, cursor TEXT, scans INTEGER NOT NULL DEFAULT 0,
          next_scan_at INTEGER NOT NULL DEFAULT 0, lease TEXT, lease_until INTEGER NOT NULL DEFAULT 0,
          last_verified_at INTEGER NOT NULL DEFAULT 0, last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS raw_replay_items (
          object_key TEXT PRIMARY KEY, done INTEGER NOT NULL DEFAULT 0,
          event_count INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS raw_replay_pending ON raw_replay_items(object_key) WHERE done=0;
      `);
      ctx.storage.sql.exec('INSERT OR IGNORE INTO raw_replay_state(singleton,tenant_id) VALUES(1,?)', env.TENANT_ID);
      const tenant = ctx.storage.sql.exec<{ tenant_id: string }>('SELECT tenant_id FROM raw_replay_state').one().tenant_id;
      if (!env.TENANT_ID || tenant !== env.TENANT_ID) throw new Error('Raw replay tenant mismatch');
      if (this.state().enabled && await ctx.storage.getAlarm() === null) await ctx.storage.setAlarm(Date.now() + 1_000);
    });
  }

  async start() {
    this.requireCollectMode();
    this.ctx.storage.sql.exec('UPDATE raw_replay_state SET enabled=1');
    if (await this.ctx.storage.getAlarm() === null) await this.ctx.storage.setAlarm(Date.now() + 1_000);
    return this.status();
  }

  async pause() {
    this.ctx.storage.sql.exec('UPDATE raw_replay_state SET enabled=0,lease=NULL,lease_until=0');
    await this.ctx.storage.deleteAlarm();
    return this.status();
  }

  async status() {
    const state = this.state();
    const counts = this.ctx.storage.sql.exec<{ envelopes: number; pending: number; verified_events: number }>(
      'SELECT count(*) AS envelopes,coalesce(sum(done=0),0) AS pending,coalesce(sum(event_count),0) AS verified_events FROM raw_replay_items',
    ).one();
    return { enabled: state.enabled === 1, scans: state.scans, ...counts,
      lastVerifiedAt: state.last_verified_at, lastError: state.last_error, alarmAt: await this.ctx.storage.getAlarm() };
  }

  async alarm() {
    const lease = await this.claim();
    if (!lease) return;
    let error: string | null = null;
    let activeKeys: string[] = [];
    try {
      this.requireCollectMode();
      const deadline = Date.now() + 20_000;
      await this.scan(lease);
      while (Date.now() < deadline) {
        const pending = this.ctx.storage.sql.exec<{ object_key: string }>(
          'SELECT object_key FROM raw_replay_items WHERE done=0 AND next_attempt<=? ORDER BY object_key LIMIT 32', Date.now(),
        ).toArray();
        if (!pending.length) break;
        activeKeys = pending.map(row => row.object_key);
        // Four independent batches keep historical replay from waiting on one remote request at a time.
        const deliveries: Promise<void>[] = [];
        for (let offset = 0; offset < activeKeys.length; offset += 8) {
          deliveries.push(this.deliver(activeKeys.slice(offset, offset + 8), lease));
        }
        const results = await Promise.allSettled(deliveries);
        if (results.some(result => result.status === 'rejected')) throw new Error('A raw replay group failed');
        activeKeys = [];
      }
    } catch {
      error = 'Raw replay failed; source envelopes and progress are retained';
      if (this.state().lease === lease) {
        for (const key of activeKeys) this.ctx.storage.sql.exec(
          'UPDATE raw_replay_items SET next_attempt=? WHERE object_key=? AND done=0', Date.now() + 60_000, key,
        );
      }
    } finally {
      if (this.state().lease === lease) {
        this.ctx.storage.sql.exec('UPDATE raw_replay_state SET lease=NULL,lease_until=0,last_error=?', error);
        await this.ctx.storage.setAlarm(Date.now() + (error ? 30_000 : 5_000));
      }
    }
  }

  private async deliver(keys: string[], lease: string): Promise<void> {
    const envelopes = await Promise.all(keys.map(async key => {
      const object = await this.env.BROWSER_BUFFER.get(key);
      if (!object) throw new Error('An undelivered envelope is missing');
      const body = await object.text();
      if (key !== `${PREFIX}${await sha256(body)}.json`) throw new Error('Envelope hash mismatch');
      return JSON.parse(body) as BrowserQueueEnvelope;
    }));
    this.checkLease(lease);
    const receipts = await this.env.BROWSER_INGRESS.receiveBatch(envelopes);
    this.checkLease(lease);
    if (receipts.length !== keys.length || receipts.some((receipt, index) =>
      receipt.status !== 'stored' || receipt.key !== keys[index]
      || receipt.eventCount !== envelopes[index].events.length)) {
      throw new Error('Raw replay was not completely verified');
    }
    this.ctx.storage.transactionSync(() => {
      for (const receipt of receipts) this.ctx.storage.sql.exec(
        'UPDATE raw_replay_items SET done=1,event_count=? WHERE object_key=?', receipt.eventCount, receipt.key,
      );
      this.ctx.storage.sql.exec('UPDATE raw_replay_state SET last_verified_at=?', Date.now());
    });
  }

  private async scan(lease: string) {
    const state = this.state();
    if (state.next_scan_at > Date.now()) return;
    let listing: R2Objects;
    try {
      listing = await this.env.BROWSER_BUFFER.list({ prefix: PREFIX, limit: 100,
        ...(state.cursor ? { cursor: state.cursor } : {}) });
    } catch (error) {
      this.checkLease(lease);
      this.ctx.storage.sql.exec('UPDATE raw_replay_state SET cursor=NULL,next_scan_at=0');
      throw error;
    }
    this.checkLease(lease);
    this.ctx.storage.transactionSync(() => {
      for (const object of listing.objects) this.ctx.storage.sql.exec(
        'INSERT OR IGNORE INTO raw_replay_items(object_key) VALUES(?)', object.key,
      );
      this.ctx.storage.sql.exec(
        'UPDATE raw_replay_state SET cursor=?,scans=scans+?,next_scan_at=?',
        listing.truncated ? listing.cursor : null, listing.truncated ? 0 : 1,
        listing.truncated ? 0 : Date.now() + 30_000,
      );
    });
  }

  private async claim() {
    return this.ctx.storage.transaction(async () => {
      const state = this.state();
      if (!state.enabled) return null;
      if (state.lease_until > Date.now()) {
        await this.ctx.storage.setAlarm(state.lease_until + 1);
        return null;
      }
      const lease = crypto.randomUUID();
      const until = Date.now() + LEASE_MS;
      this.ctx.storage.sql.exec('UPDATE raw_replay_state SET lease=?,lease_until=?', lease, until);
      await this.ctx.storage.setAlarm(until);
      return lease;
    });
  }

  private state() {
    return this.ctx.storage.sql.exec<ReplayState>('SELECT * FROM raw_replay_state').one();
  }

  private checkLease(lease: string) {
    const state = this.state();
    if (!state.enabled || state.lease !== lease || state.lease_until <= Date.now()) throw new Error('Raw replay lease expired');
  }

  private requireCollectMode() {
    if (this.env.BROWSER_INGRESS_MODE !== 'collect') throw new Error('Raw replay requires collect mode');
  }
}
