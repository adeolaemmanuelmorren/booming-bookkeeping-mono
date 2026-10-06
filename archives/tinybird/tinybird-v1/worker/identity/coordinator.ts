import { DurableObject } from 'cloudflare:workers';
import { canonicalJson } from '../storage/json.ts';
import { Tinybird } from '../storage/tinybird.ts';
import type { TinybirdEnv } from '../storage/tinybird.ts';
import { readIdentityBootstrapSeal, type IdentityBaselineReceipt as BaselineReceipt } from './bootstrap-seal.ts';
import type { IdentityJournalRow, PendingIdentityFact } from './engine.ts';
import { computeIdentityBatch, type IdentityBatch } from './state.ts';
import { IdentityStorage, identityRecord, type IdentityRecord } from './storage.ts';

interface Env extends TinybirdEnv {
  TENANT_ID: string;
  IDENTITY_BASELINE_ID: string;
  IDENTITY_BASELINE_SEAL: string;
  IDENTITY_BASELINE: { readRecords(input: { kind: string; keys: string[] }): Promise<import('./storage.ts').IdentityRecord[]> };
}
type Job = {
  version: number; id: string; committed_at: string; input_cutoff: number;
  computed: number; lease_id: string | null; lease_until: number;
};

/** One serial graph writer per tenant; source ingestion can continue during publication. */
export class IdentityCoordinator extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL)`);
      ctx.storage.sql.exec(`INSERT OR IGNORE INTO identity_meta VALUES ('published_version', 0)`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_inbox (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
        fingerprint TEXT NOT NULL, payload TEXT NOT NULL
      )`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_job (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1), version INTEGER NOT NULL,
        id TEXT NOT NULL, committed_at TEXT NOT NULL, input_cutoff INTEGER NOT NULL,
        computed INTEGER NOT NULL DEFAULT 0, lease_id TEXT, lease_until INTEGER NOT NULL DEFAULT 0
      )`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_outbox (
        state_key TEXT PRIMARY KEY, payload TEXT NOT NULL
      )`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_baseline (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), receipt_json TEXT NOT NULL)`);
      ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_current_overlay (
        state_kind TEXT NOT NULL, state_key TEXT NOT NULL, lookup_key TEXT NOT NULL,
        batch_version INTEGER NOT NULL, batch_id TEXT NOT NULL, is_deleted INTEGER NOT NULL,
        record_json TEXT NOT NULL, PRIMARY KEY(state_kind,state_key)
      )`);
      ctx.storage.sql.exec('CREATE INDEX IF NOT EXISTS identity_overlay_lookup ON identity_current_overlay(state_kind,lookup_key,batch_version)');
      const pending = ctx.storage.sql.exec('SELECT sequence FROM identity_inbox LIMIT 1').toArray().length;
      const active = ctx.storage.sql.exec('SELECT singleton FROM identity_job LIMIT 1').toArray().length;
      if (pending || active) await this.wake();
    });
  }

  async activateBaseline(): Promise<BaselineReceipt> {
    // Check again after the network read. Input may arrive while Tinybird is responding.
    this.assertCanActivate();
    const client = new Tinybird(this.env);
    const { receipt } = await readIdentityBootstrapSeal({
      tenantId: this.env.TENANT_ID,
      baselineId: this.env.IDENTITY_BASELINE_ID,
      expectedSealHash: this.env.IDENTITY_BASELINE_SEAL,
      reader: {
        read: async (tenantId, baselineId) => {
          const rows = await client.query<{ payload_json: string; payload_hash: string }>(`
            SELECT DISTINCT payload_json, payload_hash
            FROM v1_identity_bootstrap_manifests
            WHERE tenant_id = '${escapeSql(tenantId)}'
              AND baseline_id = '${escapeSql(baselineId)}'
              AND manifest_kind = 'seal' AND manifest_key = 'complete'
            ORDER BY payload_hash LIMIT 2
          `);
          if (rows.length > 1) throw new Error('Identity bootstrap seal is ambiguous');
          if (!rows[0]) return null;
          return { payload: JSON.parse(rows[0].payload_json), payloadHash: rows[0].payload_hash };
        },
      },
    });
    return this.ctx.storage.transactionSync(() => {
      const previous = this.baseline();
      if (previous) {
        if (canonicalJson(previous) !== canonicalJson(receipt)) throw new Error('Identity baseline cannot change after activation');
        return previous;
      }
      this.assertCanActivate();
      this.ctx.storage.sql.exec('INSERT INTO identity_baseline VALUES (1, ?)', canonicalJson(receipt));
      this.ctx.storage.sql.exec("UPDATE identity_meta SET value = ? WHERE key = 'published_version'", receipt.identityVersion);
      return receipt;
    });
  }

  async enqueue(facts: PendingIdentityFact[]): Promise<{ accepted: number }> {
    this.assertBaselineReady();
    if (facts.length > 500) throw new Error('Identity input batch exceeds 500 facts');
    return this.ctx.storage.transaction(async () => {
      let accepted = 0;
      for (const fact of facts) {
        if (!fact.eventId || !fact.factKey || !fact.factKind) throw new Error('Identity fact keys are required');
        if (!Number.isSafeInteger(fact.sourceFactVersion) || fact.sourceFactVersion < 0) {
          throw new Error('Invalid identity source version');
        }
        const fingerprint = eventFingerprint(fact);
        const previous = this.ctx.storage.sql.exec<{ fingerprint: string }>(
          'SELECT fingerprint FROM identity_inbox WHERE event_id = ?', fact.eventId,
        ).toArray()[0];
        if (previous) {
          if (previous.fingerprint !== fingerprint) throw new Error('Conflicting identity event retry');
          continue;
        }
        this.ctx.storage.sql.exec(
          'INSERT INTO identity_inbox (event_id, fingerprint, payload) VALUES (?, ?, ?)',
          fact.eventId, fingerprint, JSON.stringify(fact),
        );
        accepted++;
      }
      await this.wake();
      return { accepted };
    });
  }

  async wake(): Promise<void> {
    const alarm = await this.ctx.storage.getAlarm();
    const next = Date.now() + 60_000;
    if (alarm === null || alarm > next) await this.ctx.storage.setAlarm(next);
  }

  async status(): Promise<{ pending: number; publishedVersion: number; activeBatch: string | null; baseline: BaselineReceipt | null }> {
    const pending = this.ctx.storage.sql.exec<{ count: number }>('SELECT count(*) AS count FROM identity_inbox').one().count;
    const publishedVersion = this.publishedVersion();
    const activeBatch = this.ctx.storage.sql.exec<Job>('SELECT * FROM identity_job').toArray()[0]?.id ?? null;
    return { pending, publishedVersion, activeBatch, baseline: this.baseline() };
  }

  async alarm(): Promise<void> {
    const lease = crypto.randomUUID();
    const job = await this.claim(lease);
    if (!job) {
      const active = this.ctx.storage.sql.exec<Job>('SELECT * FROM identity_job').toArray()[0];
      if (active) await this.ctx.storage.setAlarm(Math.max(Date.now() + 1_000, active.lease_until + 10));
      return;
    }
    try {
      const storage = new IdentityStorage(new Tinybird(this.env), {
        baselineReader: this.env.IDENTITY_BASELINE,
        overlayReader: { readRecords: input => this.readOverlay(input) },
      });
      const batch = this.readBatch(job);
      if (!job.computed) {
        const result = await computeIdentityBatch(batch, storage.reader(this.env.TENANT_ID, job.version - 1));
        this.ctx.storage.transactionSync(() => {
          this.assertLease(lease);
          for (const row of result.rows) {
            this.ctx.storage.sql.exec('INSERT OR REPLACE INTO identity_outbox VALUES (?, ?)',
              `${row.state_kind}:${row.state_key}`, JSON.stringify(row));
          }
          this.ctx.storage.sql.exec('UPDATE identity_job SET computed = 1 WHERE lease_id = ?', lease);
        });
      }
      const rows = this.ctx.storage.sql.exec<{ payload: string }>('SELECT payload FROM identity_outbox ORDER BY state_key')
        .toArray().map(row => JSON.parse(row.payload) as IdentityJournalRow);
      await storage.publish(batch, rows);
      this.ctx.storage.transactionSync(() => {
        this.assertLease(lease);
        for (const row of rows.map(identityRecord)) {
          const previous = this.ctx.storage.sql.exec<{ batch_version: number; record_json: string }>(
            'SELECT batch_version,record_json FROM identity_current_overlay WHERE state_kind=? AND state_key=?', row.state_kind, row.state_key,
          ).toArray()[0];
          if (previous?.batch_version === Number(row.batch_version) && previous.record_json !== canonicalJson(row)) {
            throw new Error('Identity overlay contains a conflicting batch version');
          }
          this.ctx.storage.sql.exec(`INSERT INTO identity_current_overlay
            (state_kind,state_key,lookup_key,batch_version,batch_id,is_deleted,record_json)
            VALUES(?,?,?,?,?,?,?) ON CONFLICT(state_kind,state_key) DO UPDATE SET
              lookup_key=excluded.lookup_key,batch_version=excluded.batch_version,batch_id=excluded.batch_id,
              is_deleted=excluded.is_deleted,record_json=excluded.record_json
            WHERE excluded.batch_version > identity_current_overlay.batch_version`,
          row.state_kind, row.state_key, row.lookup_key, Number(row.batch_version), row.batch_id, row.is_deleted, canonicalJson(row));
        }
        this.ctx.storage.sql.exec("UPDATE identity_meta SET value = ? WHERE key = 'published_version'", job.version);
        this.ctx.storage.sql.exec('DELETE FROM identity_inbox WHERE sequence <= ?', job.input_cutoff);
        this.ctx.storage.sql.exec('DELETE FROM identity_outbox');
        this.ctx.storage.sql.exec('DELETE FROM identity_job WHERE lease_id = ?', lease);
      });
    } catch (error) {
      this.ctx.storage.sql.exec('UPDATE identity_job SET lease_id = NULL, lease_until = 0 WHERE lease_id = ?', lease);
      await this.ctx.storage.setAlarm(Date.now() + 60_000);
      throw error;
    }
    const remaining = this.ctx.storage.sql.exec('SELECT sequence FROM identity_inbox LIMIT 1').toArray().length;
    if (remaining) await this.ctx.storage.setAlarm(Date.now() + 1_000);
    else await this.ctx.storage.deleteAlarm();
  }

  private async claim(lease: string): Promise<Job | null> {
    this.assertBaselineReady();
    return this.ctx.storage.transaction(async () => {
      let job = this.ctx.storage.sql.exec<Job>('SELECT * FROM identity_job').toArray()[0];
      if (job?.lease_until > Date.now()) return null;
      if (!job) {
        const inputs = this.ctx.storage.sql.exec<{ sequence: number }>(
          'SELECT sequence FROM identity_inbox ORDER BY sequence LIMIT 200',
        ).toArray();
        if (!inputs.length) return null;
        this.ctx.storage.sql.exec(`INSERT INTO identity_job
          (singleton, version, id, committed_at, input_cutoff) VALUES (1, ?, ?, ?, ?)`,
        this.publishedVersion() + 1, crypto.randomUUID(), new Date().toISOString(), inputs.at(-1)!.sequence);
        job = this.ctx.storage.sql.exec<Job>('SELECT * FROM identity_job').one();
      }
      this.ctx.storage.sql.exec('UPDATE identity_job SET lease_id = ?, lease_until = ? WHERE singleton = 1',
        lease, Date.now() + 240_000);
      await this.ctx.storage.setAlarm(Date.now() + 240_000);
      return job;
    });
  }

  private readBatch(job: Job): IdentityBatch {
    const facts = this.ctx.storage.sql.exec<{ payload: string }>(
      'SELECT payload FROM identity_inbox WHERE sequence <= ? ORDER BY sequence', job.input_cutoff,
    ).toArray().map(row => JSON.parse(row.payload) as PendingIdentityFact);
    return { tenantId: this.env.TENANT_ID, version: job.version, id: job.id, committedAt: job.committed_at, facts };
  }

  private baseline(): BaselineReceipt | null {
    const row = this.ctx.storage.sql.exec<{ receipt_json: string }>('SELECT receipt_json FROM identity_baseline WHERE singleton = 1').toArray()[0];
    return row ? JSON.parse(row.receipt_json) as BaselineReceipt : null;
  }

  private assertCanActivate(): void {
    if (this.baseline()) return;
    const inbox = this.ctx.storage.sql.exec('SELECT sequence FROM identity_inbox LIMIT 1').toArray().length;
    const job = this.ctx.storage.sql.exec('SELECT singleton FROM identity_job LIMIT 1').toArray().length;
    const outbox = this.ctx.storage.sql.exec('SELECT state_key FROM identity_outbox LIMIT 1').toArray().length;
    const sequence = this.ctx.storage.sql.exec<{ seq: number }>("SELECT seq FROM sqlite_sequence WHERE name = 'identity_inbox'").toArray()[0]?.seq ?? 0;
    if (this.publishedVersion() !== 0 || inbox || job || outbox || sequence) throw new Error('Identity baseline must activate before any live input or job');
  }

  private assertBaselineReady(): void {
    const active = this.baseline();
    if (!active) throw new Error('Identity baseline has not been activated');
    if (active.identityVersion < 1 || this.publishedVersion() < 1) throw new Error('Identity version 0 cannot process live input');
    if (active.tenantId !== this.env.TENANT_ID || active.baselineId !== this.env.IDENTITY_BASELINE_ID || active.sealHash !== this.env.IDENTITY_BASELINE_SEAL) {
      throw new Error('Identity baseline configuration changed');
    }
  }

  private publishedVersion(): number {
    return this.ctx.storage.sql.exec<{ value: number }>(
      "SELECT value FROM identity_meta WHERE key = 'published_version'",
    ).one().value;
  }

  private assertLease(lease: string): void {
    const owned = this.ctx.storage.sql.exec('SELECT 1 FROM identity_job WHERE lease_id = ?', lease).toArray().length;
    if (!owned) throw new Error('Identity publication lease was lost');
  }

  private async readOverlay(input: { kind: string; keys: string[]; throughVersion: number }): Promise<IdentityRecord[]> {
    if (!input.keys.length) return [];
    const placeholders = input.keys.map(() => '?').join(',');
    return this.ctx.storage.sql.exec<{ record_json: string }>(`SELECT record_json FROM identity_current_overlay
      WHERE state_kind=? AND lookup_key IN (${placeholders}) AND batch_version<=? ORDER BY state_key`,
    input.kind, ...input.keys, input.throughVersion).toArray().map(row => JSON.parse(row.record_json) as IdentityRecord);
  }
}

function escapeSql(value: string): string {
  return value.replaceAll('\\', '\\\\').replaceAll("'", "\\'");
}

function eventFingerprint(fact: PendingIdentityFact): string {
  // Delivery times can change on retry. The original observation and content cannot.
  const { ingestedAt: _deliveryTime, producerId: _deliveryProducer, ...content } = fact;
  return canonicalJson(content);
}
