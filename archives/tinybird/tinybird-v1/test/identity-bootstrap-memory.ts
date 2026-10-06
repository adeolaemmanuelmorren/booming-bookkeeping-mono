import { IdentityBootstrapStore, FACTS_TABLE, MANIFESTS_TABLE, type FactRow, type Saved } from '../worker/identity/bootstrap-store.ts';
import { SCOPES_TABLE } from '../worker/identity/bootstrap-membership.ts';
import { Tinybird, type JsonRecord } from '../worker/storage/tinybird.ts';
import { canonicalJson } from '../worker/storage/json.ts';
import { compareStrings } from '../worker/sessions/session-engine.ts';

export class JournalClient extends Tinybird {
  tables = new Map<string, JsonRecord[]>();
  constructor() { super({ TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: 'fixture' }); }
  async append(table: string, rows: readonly JsonRecord[]) { this.tables.set(table, [...(this.tables.get(table) ?? []), ...structuredClone(rows)]); }
  async query<T>(sql: string): Promise<T[]> {
    const table = sql.match(/FROM (v1_[a-z0-9_]+)/)?.[1];
    if (!table) throw new Error(`Unexpected test SQL: ${sql}`);
    let rows = this.tables.get(table) ?? [];
    const batchId = sql.match(/batch_id\s*=\s*'([^']*)'/)?.[1];
    if (batchId) rows = rows.filter(row => row.batch_id === batchId);
    if (sql.includes('tuple(state_kind,lookup_key,state_key) IN')) {
      const keys = new Set([...sql.matchAll(/tuple\('([^']*)','([^']*)','([^']*)'\)/g)].map(match => canonicalJson(match.slice(1))));
      rows = rows.filter(row => keys.has(canonicalJson([row.state_kind,row.lookup_key,row.state_key])));
    }
    const kind = sql.match(/state_kind\s*=\s*'([^']*)'/)?.[1];
    if (kind) rows = rows.filter(row => row.state_kind === kind);
    const wanted = sql.match(/lookup_key IN \(([^)]+)\)/)?.[1];
    if (wanted) { const keys = [...wanted.matchAll(/'([^']*)'/g)].map(match => match[1]); rows = rows.filter(row => keys.includes(String(row.lookup_key))); }
    rows = [...new Map(rows.map(row => [canonicalJson(row),row])).values()];
    return structuredClone(rows.map(row => ({ ...row, batch_version: String(row.batch_version) }))) as T[];
  }
}

export class MemoryStore extends IdentityBootstrapStore {
  manifests = new Map<string, Saved<unknown>[]>();
  rows = new Map<string, JsonRecord[]>();
  missingManifest: string | null = null;
  lostCheckpointAck = false;
  omitCandidateOrigin: string | null = null;
  constructor(client = new JournalClient()) { super(client, 'test', 'fixture'); }
  async get<T>(kind: string, key: string): Promise<Saved<T> | null> {
    const rows = this.manifests.get(`${kind}:${key}`) ?? [];
    return structuredClone(rows.at(-1) ?? null) as Saved<T> | null;
  }
  async put(kind: string, key: string, payload: unknown, sequence = 0): Promise<void> {
    const id = `${kind}:${key}`;
    const rows = this.manifests.get(id) ?? [];
    const existing = rows.find(row => row.sequence === sequence);
    if (existing && canonicalJson(existing.payload) !== canonicalJson(payload)) throw new Error('Conflicting immutable test receipt');
    if (!existing) rows.push({ sequence, payload: structuredClone(payload) });
    rows.sort((a,b) => a.sequence-b.sequence); this.manifests.set(id, rows);
    if (kind === 'source-cursor' && this.lostCheckpointAck) { this.lostCheckpointAck = false; throw new Error('lost checkpoint acknowledgement'); }
  }
  async putMany(kind: string, values: { key: string; payload: unknown; sequence?: number }[]) {
    for (const value of values) await this.put(kind, value.key, value.payload, value.sequence);
  }
  async requireMany<T>(kind: string, keys: string[]): Promise<Map<string,T>> {
    const result = new Map<string,T>();
    for (const key of keys) {
      if (this.missingManifest === `${kind}:${key}`) throw new Error('proof is not visible');
      const saved = await this.get<T>(kind,key);
      if (!saved) throw new Error('proof is not visible');
      result.set(key,saved.payload);
    }
    return result;
  }
  async write(table: string, rows: JsonRecord[], _where: string | ((rows: JsonRecord[]) => string)): Promise<void> {
    const existing = this.rows.get(table) ?? [];
    const key = (row: JsonRecord) => table === FACTS_TABLE ? canonicalJson([row.phase,row.origin_key]) : canonicalJson([row.scope_id,row.fact_kind,row.fact_key]);
    for (const row of rows) {
      const found = existing.find(value => key(value) === key(row));
      if (found && canonicalJson(found) !== canonicalJson(row)) throw new Error('Conflicting immutable test row');
      if (!found) existing.push(structuredClone(row));
    }
    this.rows.set(table,existing);
  }
  async *facts(phase: string, after?: [string,string]): AsyncGenerator<FactRow> {
    const rows = (this.rows.get(FACTS_TABLE) ?? []).filter(row => row.phase === phase && row.origin_key !== this.omitCandidateOrigin)
      .filter(row => !after || compareStrings(String(row.fact_kind),after[0]) > 0 || row.fact_kind === after[0] && compareStrings(String(row.fact_key),after[1]) > 0);
    rows.sort((a,b) => compareStrings(String(a.fact_kind),String(b.fact_kind)) || compareStrings(String(a.fact_key),String(b.fact_key))
      || Number(a.source_priority)-Number(b.source_priority) || Number(a.source_version)-Number(b.source_version) || compareStrings(String(a.origin_key),String(b.origin_key)));
    for (const row of rows) yield structuredClone(row) as FactRow;
  }
  async *scan<T extends JsonRecord>(table: string, keys: string[], where: string, columns = '*'): AsyncGenerator<T> {
    let rows: JsonRecord[];
    if (table === MANIFESTS_TABLE) {
      const kind = where.match(/manifest_kind='([^']+)'/)![1];
      rows = [];
      for (const [id, values] of this.manifests) if (id.startsWith(kind+':')) {
        const value = values.find(row => row.sequence === 0)!;
        const json = canonicalJson(value.payload);
        const { sha256 } = await import('../worker/storage/json.ts');
        rows.push({ manifest_key: id.slice(kind.length+1), payload_json: json, payload_hash: await sha256(json) });
      }
    } else {
      rows = this.rows.get(table) ?? [];
      const phase = where.match(/phase='([^']+)'/)?.[1];
      if (phase) rows = rows.filter(row => row.phase === phase);
      for (const field of ['component_key','scope_id']) {
        const part = where.match(new RegExp(field+' IN \\(([^)]+)\\)'))?.[1];
        if (part) { const wanted = [...part.matchAll(/'([^']*)'/g)].map(match => match[1]); rows = rows.filter(row => wanted.includes(String(row[field]))); }
      }
    }
    rows = [...rows].sort((a,b) => { for (const key of keys) { const order = compareStrings(String(a[key]),String(b[key])); if (order) return order; } return 0; });
    for (const row of rows) yield structuredClone(columns === '*' ? row : Object.fromEntries(columns.split(',').map(key => [key,row[key]]))) as T;
  }
}
