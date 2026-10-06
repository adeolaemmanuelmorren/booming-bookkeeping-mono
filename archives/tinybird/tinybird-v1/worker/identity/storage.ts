import { canonicalJson, sha256 } from '../storage/json.ts';
import { Tinybird, sqlString, sqlStrings, uint64Number, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';
import type { IdentityJournalRow } from './engine.ts';
import type { IdentityBatch, IdentityStateReader } from './state.ts';

export interface IdentityRecord extends JsonRecord {
  tenant_id: string;
  state_kind: string;
  state_key: string;
  lookup_key: string;
  sub_key: string;
  batch_version: string;
  batch_id: string;
  is_deleted: number;
  payload_json: string;
}

export interface IdentityCommit extends JsonRecord {
  tenant_id: string;
  batch_version: string;
  batch_id: string;
  committed_at: string;
  row_count: number;
  content_hash: string;
}

export interface IdentityBaselineReader {
  readRecords(input: { kind: string; keys: string[] }): Promise<IdentityRecord[]>;
}
export interface IdentityOverlayReader {
  readRecords(input: { kind: string; keys: string[]; throughVersion: number }): Promise<IdentityRecord[]>;
}

export class IdentityStorage {
  private client: Tinybird;
  private readPageRows: number;
  private baselineReader: IdentityBaselineReader | null;
  private overlayReader: IdentityOverlayReader | null;
  constructor(client: Tinybird, options: { readPageRows?: number; baselineReader?: IdentityBaselineReader; overlayReader?: IdentityOverlayReader } = {}) {
    this.client = client;
    this.readPageRows = options.readPageRows ?? 500;
    this.baselineReader = options.baselineReader ?? null;
    this.overlayReader = options.overlayReader ?? null;
  }

  /** Freeze this reader before a batch starts. Uncommitted writes remain invisible. */
  reader(tenantId: string, version: number): IdentityStateReader {
    return {
      facts: keys => this.current(tenantId, version, 'fact', keys, true),
      mappings: keys => this.current(tenantId, version, 'mapping', keys),
      profiles: keys => this.current(tenantId, version, 'profile', keys),
      evidence: keys => this.current(tenantId, version, 'evidence', keys),
    };
  }

  async publish(batch: IdentityBatch, rows: IdentityJournalRow[]): Promise<void> {
    const records = rows.map(identityRecord);
    const expected = distinctRecordText(records);
    const expectedSet = new Set(expected);
    const manifest: IdentityCommit = {
      tenant_id: batch.tenantId, batch_version: String(batch.version), batch_id: batch.id,
      committed_at: batch.committedAt, row_count: expected.length,
      content_hash: await sha256(expected.join('\n')),
    };
    // Read the stored payloads themselves. A separately written count/hash is not proof.
    const readRecords = async () => {
      const result: IdentityRecord[] = [];
      if (this.readPageRows > 500) {
        // The physical prefix is tenant, state kind, lookup key, state key.
        // Scoping only by batch ID would scan earlier bootstrap batches repeatedly.
        const keys = [...new Set(records.map(row => `tuple(${sqlString(row.state_kind)},${sqlString(row.lookup_key)},${sqlString(row.state_key)})`))];
        const groups: string[][] = [];
        const encoder = new TextEncoder();
        let group: string[] = [];
        let bytes = 0;
        for (const key of keys) {
          const size = encoder.encode(key).byteLength + 1;
          if (size > 160_000) throw new Error('An identity publication key exceeds the read limit');
          if (group.length === 2000 || bytes + size > 160_000) {
            groups.push(group); group = []; bytes = 0;
          }
          group.push(key); bytes += size;
        }
        if (group.length) groups.push(group);
        for (let offset = 0; offset < groups.length; offset += 4) {
          const pages = await Promise.allSettled(groups.slice(offset, offset + 4).map(group => this.client.query<IdentityRecord>(`
            SELECT DISTINCT tenant_id, state_kind, state_key, lookup_key, sub_key,
              toString(batch_version) AS batch_version, batch_id, is_deleted, payload_json
            FROM v1_identity_records
            WHERE tenant_id = ${sqlString(batch.tenantId)} AND batch_id = ${sqlString(batch.id)}
              AND tuple(state_kind,lookup_key,state_key) IN (${group.join(',')})
            LIMIT ${group.length + 1}
          `)));
          for (const page of pages) {
            if (page.status === 'rejected') throw page.reason;
            result.push(...page.value);
          }
        }
        return result;
      }
      for (let offset = 0; ; offset += this.readPageRows) {
        const page = await this.client.query<IdentityRecord>(`
          SELECT DISTINCT tenant_id, state_kind, state_key, lookup_key, sub_key,
            toString(batch_version) AS batch_version, batch_id, is_deleted, payload_json
          FROM v1_identity_records
          WHERE tenant_id = ${sqlString(batch.tenantId)} AND batch_id = ${sqlString(batch.id)}
          ORDER BY state_kind,state_key,payload_json LIMIT ${this.readPageRows} OFFSET ${offset}
        `);
        result.push(...page);
        if (page.length < this.readPageRows) return result;
      }
    };
    const verifyRecords = (actual: IdentityRecord[]) => {
      const received = distinctRecordText(actual);
      if (received.some(record => !expectedSet.has(record))) {
        throw new Error('Identity publication records did not verify');
      }
      return received.length === expected.length;
    };
    const existing = await readRecords();
    if (!verifyRecords(existing)) {
      const present = new Set(distinctRecordText(existing));
      await this.client.append('v1_identity_records', records.filter(record => !present.has(canonicalJson(record)))
        .map(record => ({ ...record, batch_version: uint64Number(record.batch_version) })));
      await waitForReadback(readRecords, verifyRecords);
    }
    await this.client.append('v1_identity_commits', [{
      ...manifest, batch_version: uint64Number(manifest.batch_version),
    }]);
    await waitForReadback(() => this.client.query<IdentityCommit>(`
      SELECT tenant_id, toString(batch_version) AS batch_version, batch_id,
        toString(committed_at) AS committed_at, row_count, content_hash
      FROM v1_identity_commits
      WHERE tenant_id = ${sqlString(batch.tenantId)} AND batch_id = ${sqlString(batch.id)}
    `), commits => {
      if (commits.some(commit =>
        commit.batch_version !== manifest.batch_version || Number(commit.row_count) !== manifest.row_count
        || commit.content_hash !== manifest.content_hash)) {
        throw new Error('Identity commit contains conflicting values');
      }
      return commits.length > 0;
    });
  }

  private async current<T>(tenantId: string, version: number, kind: string, keys: string[], keepDeleted = false): Promise<T[]> {
    if (!keys.length) return [];
    if (!Number.isSafeInteger(version) || version < 0) throw new Error('Invalid identity read version');
    if (this.baselineReader && this.overlayReader) {
      return this.currentFromVerifiedState<T>(version, kind, keys, keepDeleted);
    }
    const values = await this.client.query<{ state_key: string; batch_version: string; payload_json: string; is_deleted: number }>(`
      SELECT state_key, toString(tupleElement(latest, 1)) AS batch_version,
        tupleElement(latest, 2) AS payload_json, tupleElement(latest, 3) AS is_deleted
      FROM (
        SELECT state_key, argMax(tuple(batch_version, payload_json, is_deleted), batch_version) AS latest
        FROM v1_identity_records
        WHERE tenant_id = ${sqlString(tenantId)} AND state_kind = ${sqlString(kind)}
          AND lookup_key IN (${sqlStrings(keys)}) AND batch_version <= ${version}
          AND batch_id IN (
            SELECT batch_id FROM v1_identity_commits
            WHERE tenant_id = ${sqlString(tenantId)} AND batch_version <= ${version}
          )
        GROUP BY state_key
      )
    `);
    const merged = new Map(values.map(row => [row.state_key, row]));
    if (this.baselineReader && version >= 1) {
      const baseline: IdentityRecord[] = [];
      for (let offset = 0; offset < keys.length; offset += 200) {
        baseline.push(...await this.baselineReader.readRecords({ kind, keys: keys.slice(offset, offset + 200) }));
      }
      for (const row of baseline) {
        if (row.state_kind !== kind || !keys.includes(row.lookup_key) || row.batch_version !== '1') throw new Error('Identity baseline returned an invalid record');
        const current = merged.get(row.state_key);
        if (current && Number(current.batch_version) === 1 &&
          (current.payload_json !== row.payload_json || Number(current.is_deleted) !== Number(row.is_deleted))) {
          throw new Error('Identity baseline conflicts with visible version 1 state');
        }
        if (!current || Number(current.batch_version) < 1) merged.set(row.state_key, row);
      }
    }
    return [...merged.values()].filter(row => keepDeleted || Number(row.is_deleted) === 0).map(row => JSON.parse(row.payload_json) as T);
  }

  private async currentFromVerifiedState<T>(version: number, kind: string, keys: string[], keepDeleted: boolean): Promise<T[]> {
    const merged = new Map<string, IdentityRecord>();
    for (let offset = 0; offset < keys.length; offset += 200) {
      const group = keys.slice(offset, offset + 200);
      const baseline = await this.baselineReader!.readRecords({ kind, keys: group });
      const overlay = await this.overlayReader!.readRecords({ kind, keys: group, throughVersion: version });
      for (const row of [...baseline, ...overlay]) {
        if (row.state_kind !== kind || !group.includes(row.lookup_key)) throw new Error('Verified identity state returned an invalid record');
        const rowVersion = uint64Number(row.batch_version);
        if (rowVersion < 1 || rowVersion > version) throw new Error('Verified identity state crossed its version fence');
        const previous = merged.get(row.state_key);
        if (!previous || uint64Number(previous.batch_version) < rowVersion) merged.set(row.state_key, row);
        else if (uint64Number(previous.batch_version) === rowVersion && canonicalJson(previous) !== canonicalJson(row)) {
          throw new Error('Verified identity state contains a conflicting version');
        }
      }
    }
    return [...merged.values()].filter(row => keepDeleted || Number(row.is_deleted) === 0)
      .map(row => JSON.parse(row.payload_json) as T);
  }
}

export function identityRecord(row: IdentityJournalRow): IdentityRecord {
  return {
    tenant_id: row.tenant_id, state_kind: row.state_kind, state_key: row.state_key,
    lookup_key: row.lookup_key, sub_key: row.sub_key, batch_version: String(row.batch_version),
    batch_id: row.batch_id, is_deleted: row.is_deleted, payload_json: canonicalJson(identityValue(row)),
  };
}

function identityValue(row: IdentityJournalRow): Record<string, unknown> {
  if (row.state_kind === 'redirect') return { oldProfileId: row.lookup_key, newProfileId: row.profile_id };
  if (row.state_kind === 'fact') {
    const source = row.fact_payload ? JSON.parse(row.fact_payload) : {};
    return {
      producerId: row.producer_id, factKind: row.fact_kind, factKey: row.fact_key,
      sourcePriority: row.source_priority,
      sourceFactVersion: row.source_fact_version, factDeleted: Boolean(row.fact_deleted),
      factObservedAt: row.fact_observed_at, factPayloadHash: row.fact_payload_hash,
      evidenceKeys: row.evidence_keys, firstName: source.first_name ?? '', lastName: source.last_name ?? '',
      isDeleted: Boolean(row.is_deleted),
    };
  }
  if (row.state_kind === 'evidence') return { identifierKey: row.lookup_key, factKey: row.fact_key };
  if (row.state_kind === 'mapping') return {
    identifierType: row.identifier_type, identifierValue: row.identifier_value,
    identifierKey: row.identifier_key, profileId: row.profile_id,
    firstSeenAt: row.first_seen_at, lastSeenAt: row.last_seen_at,
  };
  if (row.state_kind === 'profile') return {
    profileId: row.profile_id, profileKey: row.profile_key, winnerIdentifierKey: row.winner_identifier_key,
    memberIdentifierKeys: row.member_identifier_keys, historicalProfileIds: row.historical_profile_ids,
    firstName: row.first_name, lastName: row.last_name, firstSeenAt: row.first_seen_at, lastSeenAt: row.last_seen_at,
  };
  throw new Error('Unexpected identity output kind');
}

function distinctRecordText(records: IdentityRecord[]): string[] {
  const byKey = new Map<string, string>();
  for (const record of records) {
    const key = `${record.state_kind}\0${record.state_key}`;
    const text = canonicalJson(record);
    const previous = byKey.get(key);
    if (previous && previous !== text) throw new Error('Conflicting identity publication records');
    byKey.set(key, text);
  }
  return [...byKey.values()].sort();
}
