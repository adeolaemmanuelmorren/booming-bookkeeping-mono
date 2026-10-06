import { canonicalJson, sha256 } from '../storage/json.ts';
import { Tinybird, sqlString, sqlStrings, uint64Number, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import type { PendingIdentityFact } from './engine.ts';

export const FACTS_TABLE = 'v1_identity_bootstrap_facts';
export const MANIFESTS_TABLE = 'v1_identity_bootstrap_manifests';
export interface FactRow extends JsonRecord {
  tenant_id: string; baseline_id: string; phase: string; component_key: string; input_key: string;
  origin_key: string; scope_id: string; fact_kind: string; fact_key: string;
  source_priority: number; source_version: number; fact_json: string; fact_hash: string;
}
export interface Saved<T> { sequence: number; payload: T }

/** Only compact facts and immutable receipts are written here. */
export class IdentityBootstrapStore {
  readonly client: Tinybird;
  readonly tenantId: string;
  readonly baselineId: string;
  constructor(client: Tinybird, tenantId: string, baselineId: string) {
    this.client = client; this.tenantId = tenantId; this.baselineId = baselineId;
  }
  scope(): string { return `tenant_id=${sqlString(this.tenantId)} AND baseline_id=${sqlString(this.baselineId)}`; }

  async get<T>(kind: string, key: string): Promise<Saved<T> | null> {
    const rows = await this.client.query<JsonRecord>(`SELECT DISTINCT sequence,payload_json,payload_hash FROM ${MANIFESTS_TABLE}
      WHERE ${this.scope()} AND manifest_kind=${sqlString(kind)} AND manifest_key=${sqlString(key)} ORDER BY sequence DESC LIMIT 2`);
    if (!rows.length) return null;
    if (rows[1] && rows[0].sequence === rows[1].sequence) throw new Error('Conflicting identity bootstrap receipt');
    if (await sha256(String(rows[0].payload_json)) !== rows[0].payload_hash) throw new Error('Identity bootstrap receipt hash mismatch');
    return { sequence: uint64Number(rows[0].sequence as string | number), payload: JSON.parse(String(rows[0].payload_json)) as T };
  }

  async put(kind: string, key: string, payload: unknown, sequence = 0): Promise<void> {
    await this.putMany(kind, [{ key, payload, sequence }]);
  }

  async putMany(kind: string, values: { key: string; payload: unknown; sequence?: number }[]): Promise<void> {
    if (!values.length) return;
    const rows = await Promise.all(values.map(async value => ({ tenant_id: this.tenantId, baseline_id: this.baselineId,
      manifest_kind: kind, manifest_key: value.key, sequence: value.sequence ?? 0,
      payload_json: canonicalJson(value.payload), payload_hash: await sha256(canonicalJson(value.payload)) })));
    await this.write(MANIFESTS_TABLE, rows, group => `${this.scope()} AND manifest_kind=${sqlString(kind)} AND tuple(manifest_key,sequence) IN (${group.map(row => `tuple(${sqlString(String(row.manifest_key))},${row.sequence})`).join(',')})`);
  }

  async requireMany<T>(kind: string, keys: string[]): Promise<Map<string, T>> {
    if (!keys.length) return new Map();
    if (keys.length > 200) throw new Error('Bootstrap proof read is too large');
    return waitForReadback(async () => {
      const rows = await this.client.query<JsonRecord>(`SELECT DISTINCT manifest_key,payload_json,payload_hash FROM ${MANIFESTS_TABLE}
        WHERE ${this.scope()} AND manifest_kind=${sqlString(kind)} AND sequence=0 AND manifest_key IN (${sqlStrings(keys)}) LIMIT ${keys.length + 1}`);
      const result = new Map<string, T>();
      for (const row of rows) {
        const key = String(row.manifest_key);
        if (!keys.includes(key) || result.has(key) || await sha256(String(row.payload_json)) !== row.payload_hash) throw new Error('Conflicting identity proof');
        result.set(key, JSON.parse(String(row.payload_json)));
      }
      return result;
    }, rows => rows.size === keys.length);
  }

  async factRow(fact: PendingIdentityFact, inputKey: string, originKey: string, scopeId = '', phase = 'candidate', componentKey = ''): Promise<FactRow> {
    return { tenant_id: this.tenantId, baseline_id: this.baselineId, phase, component_key: componentKey,
      input_key: inputKey, origin_key: originKey, scope_id: scopeId, fact_kind: fact.factKind, fact_key: fact.factKey,
      source_priority: fact.sourcePriority ?? 0, source_version: fact.sourceFactVersion,
      fact_json: canonicalJson(fact), fact_hash: await sha256(canonicalJson(fact)) };
  }

  async writeFacts(rows: FactRow[]): Promise<void> {
    if (!rows.length) return;
    const phases = new Set(rows.map(row => row.phase));
    if (phases.size !== 1) throw new Error('Mixed identity write phases');
    if (rows.some(row => (row.phase === 'candidate' || row.phase.startsWith('selected:')) && row.component_key !== '')) {
      throw new Error('Unpartitioned identity facts cannot have a component key');
    }
    if (new Set(rows.map(row => row.origin_key)).size !== rows.length) throw new Error('Duplicate identity origin in a write batch');
    await this.write(FACTS_TABLE, rows, group => {
      const keys = group.map(row => `tuple(${[
        sqlString(String(row.component_key)), sqlString(String(row.fact_kind)), sqlString(String(row.fact_key)),
        uint64Number(row.source_priority as number), uint64Number(row.source_version as number), sqlString(String(row.origin_key)),
      ].join(',')})`);
      return `${this.scope()} AND phase=${sqlString(rows[0].phase)}
        AND tuple(component_key,fact_kind,fact_key,source_priority,source_version,origin_key) IN (${keys.join(',')})`;
    });
  }

  async *scan<T extends JsonRecord>(table: string, keys: string[], where: string, columns = '*', size = 20000): AsyncGenerator<T> {
    let cursor: unknown[] | null = null;
    for (;;) {
      const after = cursor ? ` AND tuple(${keys.join(',')}) > tuple(${cursor.map((value, index) =>
        ['source_priority', 'source_version', 'bucket'].includes(keys[index]) ? String(uint64Number(value as number | string)) : sqlString(String(value))).join(',')})` : '';
      const rows: T[] = await this.client.query<T>(`SELECT DISTINCT ${columns} FROM ${table} WHERE ${where}${after} ORDER BY ${keys.join(',')} LIMIT ${size + 1}`);
      for (const row of rows.slice(0, size)) {
        const next = keys.map(key => row[key]);
        if (cursor && canonicalJson(cursor) === canonicalJson(next)) throw new Error('Conflicting identity scan key');
        cursor = next;
        yield row;
      }
      if (rows.length <= size) return;
      if (canonicalJson(cursor) === canonicalJson(keys.map(key => rows[size][key]))) throw new Error('Conflicting identity page boundary');
    }
  }

  facts(phase: string, afterFact?: [string, string]): AsyncGenerator<FactRow> {
    const after = afterFact ? ` AND tuple(fact_kind,fact_key)>tuple(${afterFact.map(sqlString).join(',')})` : '';
    const unpartitioned = phase === 'candidate' || phase.startsWith('selected:') ? " AND component_key=''" : '';
    return this.scan(FACTS_TABLE, ['fact_kind', 'fact_key', 'source_priority', 'source_version', 'origin_key'], `${this.scope()} AND phase=${sqlString(phase)}${unpartitioned}${after}`);
  }

  async write(table: string, expected: JsonRecord[], where: string | ((rows: JsonRecord[]) => string)): Promise<void> {
    if (!expected.length) return;
    const fields = Object.keys(expected[0]);
    for (const row of expected) {
      if (Object.keys(row).length !== fields.length || fields.some(field => !(field in row))) throw new Error('Inconsistent identity write columns');
      if (Object.values(row).some(value => value !== null && !['string', 'number', 'boolean'].includes(typeof value))) {
        throw new Error('Identity storage columns must be primitive values');
      }
    }
    // JSON payloads are already serialized. Compare them directly instead of escaping them again.
    const keyFields = fields.filter(field => field !== 'fact_json' && field !== 'payload_json');
    const verificationKey = (row: JsonRecord) => canonicalJson(keyFields.map(field => row[field]));
    const wanted = new Map(expected.map(row => [verificationKey(row), row]));
    if (wanted.size !== expected.length) throw new Error('Duplicate bootstrap write');
    const numeric = Object.keys(expected[0]).filter(key => typeof expected[0][key] === 'number');
    const columns = fields.join(',');
    const queries: string[] = [];
    const addQuery = (rows: JsonRecord[]): void => {
      const predicate = typeof where === 'string' ? where : where(rows);
      const query = `SELECT DISTINCT ${columns} FROM ${table} WHERE ${predicate} LIMIT ${rows.length + 1}`;
      if (new TextEncoder().encode(`${query}\nFORMAT JSON`).byteLength <= 200_000) {
        queries.push(query);
        return;
      }
      if (typeof where === 'string' || rows.length === 1) throw new Error('Identity verification SQL exceeds 200KB');
      const middle = Math.ceil(rows.length / 2);
      addQuery(rows.slice(0, middle));
      addQuery(rows.slice(middle));
    };
    // Measure the actual request. Common columns and scope are sent once per query.
    addQuery(expected);
    // These rows are immutable. Keep exact receipts when another replica lags on the next read.
    const found = new Set<string>();
    const read = async () => {
      for (let offset = 0; offset < queries.length; offset += 4) {
        const pages = await Promise.all(queries.slice(offset, offset + 4).map(query => this.client.query<JsonRecord>(query)));
        for (const row of pages.flat()) {
          for (const key of numeric) row[key] = uint64Number(row[key] as string | number);
          const key = verificationKey(row);
          const original = wanted.get(key);
          if (!original || fields.some(field => row[field] !== original[field])) throw new Error(`Conflicting immutable identity rows in ${table}`);
          found.add(key);
        }
      }
      return found;
    };
    await read();
    if (found.size === wanted.size) return;
    let appendError: unknown;
    try { await this.client.append(table, [...wanted].filter(([key]) => !found.has(key)).map(([, row]) => row)); }
    catch (error) { appendError = error; }
    try { await waitForReadback(read, rows => rows.size === wanted.size); }
    catch (error) { throw appendError ?? error; }
  }
}
