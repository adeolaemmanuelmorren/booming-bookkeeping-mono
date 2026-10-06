import { createHash } from 'node:crypto';
import { Tinybird, sqlString, waitForReadback, type TinybirdEnv, type JsonRecord } from '../../../tinybird-v1/worker/storage/tinybird.ts';
import type { Manifest } from './publication.ts';

export type ExportEnv = Partial<TinybirdEnv>;
type StateRow = { state_kind: string; state_key: string; lookup_key: string; is_deleted: number; payload: string };
type Batch = { version: number; batch_id: string; created_at: string; next_position: number; row_count: number };
const digest = (value: string) => createHash('sha256').update(value).digest('hex');

/** Delivers the existing worker's committed results. Never computes identity. */
export class IdentityExport {
  private ctx: DurableObjectState;
  private env: ExportEnv;
  constructor(ctx: DurableObjectState, env: ExportEnv) { this.ctx = ctx; this.env = env; }
  initialize() {
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS identity_tb_lease (
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),owner TEXT,expires INTEGER NOT NULL);
      INSERT OR IGNORE INTO identity_tb_lease VALUES(1,NULL,0);
      CREATE TABLE IF NOT EXISTS identity_tb_config (
      singleton INTEGER PRIMARY KEY CHECK(singleton=1), enabled INTEGER NOT NULL,
      seed_version INTEGER NOT NULL, exported_version INTEGER NOT NULL, last_error TEXT);
      CREATE TABLE IF NOT EXISTS identity_tb_batches (
        version INTEGER PRIMARY KEY,batch_id TEXT NOT NULL,created_at TEXT NOT NULL,
        next_position INTEGER NOT NULL DEFAULT 0,row_count INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS identity_tb_rows (
        version INTEGER,position INTEGER,table_name TEXT,payload TEXT,row_hash TEXT,
        PRIMARY KEY(version,position)) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS identity_tb_history (
        old_id TEXT,profile_id TEXT,PRIMARY KEY(old_id,profile_id)) WITHOUT ROWID;
      CREATE INDEX IF NOT EXISTS identity_tb_history_profile ON identity_tb_history(profile_id);
      CREATE TABLE IF NOT EXISTS identity_tb_dirty (old_id TEXT PRIMARY KEY) WITHOUT ROWID;`);
  }
  status() {
    const config = this.ctx.storage.sql.exec('SELECT * FROM identity_tb_config').toArray()[0];
    const pending = this.ctx.storage.sql.exec('SELECT count(*) batches,coalesce(sum(row_count-next_position),0) rows FROM identity_tb_batches').one();
    return { enabled: Boolean(config?.enabled), seedVersion: config?.seed_version ?? null,
      exportedVersion: config?.exported_version ?? null, lastError: config?.last_error ?? null, pending };
  }
  async start(version: number) {
    this.client();
    if (version < 1) throw new Error('The original identity worker has no committed state');
    this.ctx.storage.transactionSync(() => {
      if (this.ctx.storage.sql.exec('SELECT 1 FROM identity_tb_config').toArray().length) {
        this.ctx.storage.sql.exec('UPDATE identity_tb_config SET enabled=1');
        return;
      }
      this.ctx.storage.sql.exec('INSERT INTO identity_tb_config VALUES(1,1,?,0,NULL)', version);
      const rows = this.ctx.storage.sql.exec<StateRow>(`SELECT state_kind,state_key,lookup_key,is_deleted,payload
        FROM identity_state WHERE state_kind IN ('mapping','profile') ORDER BY state_kind,state_key`);
      this.stage(version, crypto.randomUUID(), new Date().toISOString(), rows);
    });
    await this.ctx.storage.setAlarm(Date.now() + 1000);
    return this.status();
  }
  enqueue(manifest: Manifest, values: {kind: string; payload: JsonRecord}[]) {
    if (!this.ctx.storage.sql.exec('SELECT 1 FROM identity_tb_config').toArray().length) return;
    const rows = values.filter(row => row.kind === 'identity').map(({payload}) => ({
      state_kind: String(payload.state_kind), state_key: String(payload.state_key), lookup_key: String(payload.lookup_key),
      is_deleted: Number(payload.is_deleted), payload: String(payload.payload_json),
    }));
    this.stage(manifest.version, manifest.batchId, manifest.createdAt, rows);
  }
  private stage(version: number, batchId: string, createdAt: string, rows: Iterable<StateRow>) {
    this.ctx.storage.sql.exec('INSERT INTO identity_tb_batches(version,batch_id,created_at) VALUES(?,?,?)', version, batchId, createdAt);
    let position = 0;
    const emit = (table: string, key: string, fields: JsonRecord) => {
      const value = { tenant_id: 'boom', snapshot_id: `identity-worker:${version}:${batchId}`,
        bucket: parseInt(digest(key).slice(0,2),16), ...fields, exported_at: createdAt };
      const rowHash = digest(JSON.stringify(value));
      this.ctx.storage.sql.exec('INSERT INTO identity_tb_rows VALUES(?,?,?,?,?)', version, position++, table, JSON.stringify({...value,row_hash:rowHash}), rowHash);
    };
    for (const row of rows) {
      const value = JSON.parse(row.payload);
      if (row.state_kind === 'mapping') {
        emit('identifiers', value.identifierKey, { profile_id: value.profileId,
          id_type: value.identifierType, id_value_norm: value.identifierValue, valid: null, validation_meta: null,
          source: 'bill-realtime-coordinator', action: row.is_deleted ? 'removed' : 'reassigned', changed_at: createdAt,
          first_seen_at: value.firstSeenAt, last_seen_at: value.lastSeenAt, source_stub: 'boom' });
        continue;
      }
      if (row.state_kind !== 'profile') continue;
      emit('profiles', value.profileId, { profile_id: value.profileId, winner_identifier: value.winnerIdentifierKey,
        identifier_count: row.is_deleted ? 0 : value.memberIdentifierKeys.length,
        first_seen_at: value.firstSeenAt, last_seen_at: value.lastSeenAt });
      this.ctx.storage.sql.exec(`INSERT OR IGNORE INTO identity_tb_dirty SELECT old_id FROM identity_tb_history WHERE profile_id=?`, value.profileId);
      this.ctx.storage.sql.exec('DELETE FROM identity_tb_history WHERE profile_id=?', value.profileId);
      for (const oldId of new Set<string>([value.profileId, ...value.historicalProfileIds])) {
        this.ctx.storage.sql.exec('INSERT OR IGNORE INTO identity_tb_dirty VALUES(?)', oldId);
        if (!row.is_deleted) this.ctx.storage.sql.exec('INSERT OR IGNORE INTO identity_tb_history VALUES(?,?)', oldId, value.profileId);
      }
    }
    // Redirects describe existing profile history; ambiguous splits have no single destination.
    const redirects = this.ctx.storage.sql.exec<{old_id:string; targets:number; target:string|null}>(`
      SELECT d.old_id,count(h.profile_id) targets,min(h.profile_id) target FROM identity_tb_dirty d
      LEFT JOIN identity_tb_history h ON h.old_id=d.old_id GROUP BY d.old_id ORDER BY d.old_id`);
    for (const row of redirects) emit('profile_redirects', row.old_id, { old_profile_id: row.old_id,
      new_profile_id: row.targets === 1 && row.target !== row.old_id ? row.target : '', merged_at: createdAt, source_stub: 'boom' });
    this.ctx.storage.sql.exec('DELETE FROM identity_tb_dirty');
    this.ctx.storage.sql.exec('UPDATE identity_tb_batches SET row_count=? WHERE version=?', position, version);
  }
  async alarm() {
    const config = this.ctx.storage.sql.exec('SELECT enabled FROM identity_tb_config').toArray()[0];
    if (!config?.enabled) return;
    await this.ctx.storage.setAlarm(Date.now() + 60_000);
    const lease = crypto.randomUUID();
    const claimed = this.ctx.storage.transactionSync(() => {
      const current = this.ctx.storage.sql.exec<{expires:number}>('SELECT expires FROM identity_tb_lease').one();
      if (current.expires > Date.now()) return false;
      this.ctx.storage.sql.exec('UPDATE identity_tb_lease SET owner=?,expires=?',lease,Date.now()+300_000);
      return true;
    });
    if (!claimed) return;
    try {
      const client = this.client();
      const batch = this.ctx.storage.sql.exec<Batch>('SELECT * FROM identity_tb_batches ORDER BY version LIMIT 1').toArray()[0];
      if (!batch) return;
      const rows = this.ctx.storage.sql.exec<{table_name:string;payload:string;row_hash:string}>(
        'SELECT table_name,payload,row_hash FROM identity_tb_rows WHERE version=? AND position>=? ORDER BY position LIMIT 1000', batch.version,batch.next_position).toArray();
      for (const table of ['identifiers','profiles','profile_redirects']) {
        const values = rows.filter(row => row.table_name === table).map(row => JSON.parse(row.payload));
        if (!values.length) continue;
        await client.append(table, values);
        const hashes = values.map(row => sqlString(row.row_hash)).join(',');
        await waitForReadback(() => client.query<JsonRecord>(`SELECT DISTINCT * FROM ${table} WHERE tenant_id='boom'
          AND snapshot_id=${sqlString(`identity-worker:${batch.version}:${batch.batch_id}`)} AND row_hash IN (${hashes}) LIMIT ${values.length+1}`), saved => {
          if (saved.length < values.length) return false;
          verifyStoredRows(values, saved);
          return true;
        });
      }
      const next = batch.next_position + rows.length;
      this.ctx.storage.sql.exec('UPDATE identity_tb_batches SET next_position=max(next_position,?) WHERE version=?',next,batch.version);
      this.ctx.storage.sql.exec('UPDATE identity_tb_config SET last_error=NULL');
      if (next < batch.row_count) { await this.ctx.storage.setAlarm(Date.now()+1000); return; }
      const hash = createHash('sha256');
      for (const row of this.ctx.storage.sql.exec<{row_hash:string}>('SELECT row_hash FROM identity_tb_rows WHERE version=? ORDER BY position',batch.version)) hash.update(row.row_hash+'\n');
      const contentHash=hash.digest('hex');
      await client.append('v1_identity_commits',[{tenant_id:'boom',batch_version:batch.version,batch_id:batch.batch_id,
        committed_at:batch.created_at,row_count:batch.row_count,content_hash:contentHash}]);
      await waitForReadback(() => client.query<JsonRecord>(`SELECT DISTINCT row_count,content_hash FROM v1_identity_commits WHERE tenant_id='boom'
        AND batch_version=${batch.version} AND batch_id=${sqlString(batch.batch_id)}`), commits => {
        if (!commits.length) return false;
        if(commits.length!==1 || Number(commits[0].row_count)!==batch.row_count || commits[0].content_hash!==contentHash) throw new Error('Identity export commit did not verify');
        return true;
      });
      this.ctx.storage.transactionSync(()=>{
        this.ctx.storage.sql.exec('DELETE FROM identity_tb_rows WHERE version=?',batch.version);
        this.ctx.storage.sql.exec('DELETE FROM identity_tb_batches WHERE version=?',batch.version);
        this.ctx.storage.sql.exec('UPDATE identity_tb_config SET exported_version=max(exported_version,?),last_error=NULL',batch.version);
      });
      await this.ctx.storage.setAlarm(Date.now()+1000);
    } catch (error) {
      const message = error instanceof Error ? error.message : '';
      const known = ['Missing exported column','Invalid exported timestamp','Incomplete identity export','Duplicate identity export',
        'Identity export data differs','Identity export commit did not verify','Tinybird did not accept every publication row',
        'Invalid Tinybird query response'];
      const reason = known.includes(message) || /^Identity export data differs: [a-z_]+$/.test(message) || /^Tinybird request failed with HTTP [0-9]{3}$/.test(message)
        ? message : `${error instanceof Error ? error.name : 'Error'}: ${message.slice(0, 160)}`;
      this.ctx.storage.sql.exec('UPDATE identity_tb_config SET last_error=?', reason);
      await this.ctx.storage.setAlarm(Date.now()+60_000);
    } finally {
      this.ctx.storage.sql.exec('UPDATE identity_tb_lease SET owner=NULL,expires=0 WHERE owner=?',lease);
    }
  }
  private client() {
    if (!this.env.TINYBIRD_TOKEN || this.env.TINYBIRD_URL !== 'https://api.us-east.tinybird.co') throw new Error('Identity export destination is not configured');
    return new Tinybird({TINYBIRD_URL:this.env.TINYBIRD_URL,TINYBIRD_TOKEN:this.env.TINYBIRD_TOKEN});
  }
}
export function verifyStoredRows(expected: JsonRecord[], actual: JsonRecord[]) {
  const normalize = (row: JsonRecord, keys: string[]) => JSON.stringify(keys.map(key => {
    const value=row[key];
    if (value===undefined) throw new Error('Missing exported column');
    if(value!==null && key.endsWith('_at')) {
      const match=String(value).replace(' ','T').replace(/Z$/,'').match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?$/);
      if(!match) throw new Error('Invalid exported timestamp');
      return match[1]+'.'+(match[2]??'').padEnd(6,'0');
    }
    return ['bucket','identifier_count'].includes(key)?Number(value):value;
  }));
  if(actual.length!==expected.length) throw new Error('Incomplete identity export');
  const byHash=new Map(actual.map(row=>[String(row.row_hash),row]));
  if(byHash.size!==expected.length) throw new Error('Duplicate identity export');
  for(const row of expected) {
    const saved=byHash.get(String(row.row_hash));
    if(!saved) throw new Error('Identity export data differs');
    for (const key of Object.keys(row)) if(normalize(saved,[key])!==normalize(row,[key])) throw new Error('Identity export data differs: '+key);
  }
}
