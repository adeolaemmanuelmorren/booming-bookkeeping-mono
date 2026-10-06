import type { BrowserQueueEnvelope } from './ingress.ts';
import { sha256 } from '../storage/json.ts';
import { sqlString, sqlStrings, waitForReadback, type JsonRecord } from '../storage/tinybird.ts';

export const RAW_TOUCHPOINT_TABLE = 'v1_jitsu_observations';

export interface ArchivedEnvelope {
  envelope: BrowserQueueEnvelope;
  hash: string;
  receivedAt: string;
}

export interface RawObservation extends JsonRecord {
  tenant_id: string;
  envelope_hash: string;
  event_index: number;
  delivery_event_id: string;
  message_id: string;
  event_kind: string;
  received_at: string;
  observation_json: string;
  observation_hash: string;
}

export interface RawStorageClient {
  query<T>(sql: string): Promise<T[]>;
  append(table: string, rows: readonly JsonRecord[]): Promise<void>;
}

interface StoredObservation {
  envelope_hash: string;
  event_index: number;
  stored_hash: string;
  variants: number;
  invalid_hashes: number;
}

/** Preserve the complete original observation, including fields unknown to the normalizer. */
export async function rawObservations(archives: readonly ArchivedEnvelope[]): Promise<RawObservation[]> {
  const rows: RawObservation[] = [];
  for (const archive of archives) {
    for (const [index, event] of archive.envelope.events.entries()) {
      const json = JSON.stringify(event);
      rows.push({
        tenant_id: event.tenant_id,
        envelope_hash: archive.hash,
        event_index: index,
        delivery_event_id: event.delivery_event_id,
        message_id: event.message_id,
        event_kind: event.event_kind,
        received_at: archive.receivedAt,
        observation_json: json,
        observation_hash: await sha256(json),
      });
    }
  }
  return rows;
}

/** A lost append acknowledgement is recovered by reading immutable keys before retrying. */
export async function publishRawObservations(client: RawStorageClient, rows: readonly RawObservation[]): Promise<void> {
  if (!rows.length) return;
  const tenant = rows[0].tenant_id;
  const expected = new Map<string, RawObservation>();
  for (const row of rows) {
    if (row.tenant_id !== tenant) throw new Error('Raw touchpoint batch spans tenants');
    const key = rowKey(row);
    const previous = expected.get(key);
    if (previous && previous.observation_hash !== row.observation_hash) throw new Error('Conflicting raw touchpoint input');
    expected.set(key, row);
  }
  const read = async () => {
    const hashes = [...new Set(rows.map(row => row.envelope_hash))];
    const stored = await client.query<StoredObservation>(`
      SELECT envelope_hash, event_index, any(raw.observation_hash) AS stored_hash,
        uniqExact(tuple(raw.observation_hash, raw.observation_json)) AS variants,
        countIf(raw.observation_hash != lower(hex(SHA256(raw.observation_json)))) AS invalid_hashes
      FROM ${RAW_TOUCHPOINT_TABLE} AS raw
      WHERE tenant_id = ${sqlString(tenant)} AND envelope_hash IN (${sqlStrings(hashes)})
      GROUP BY envelope_hash, event_index`);
    const found = new Set<string>();
    for (const row of stored) {
      const key = rowKey(row);
      const target = expected.get(key);
      if (!target || Number(row.variants) !== 1 || Number(row.invalid_hashes) !== 0
        || row.stored_hash !== target.observation_hash) {
        throw new Error('Stored raw touchpoint conflicts with its original envelope');
      }
      found.add(key);
    }
    return found;
  };
  const found = await read();
  const missing = [...expected].filter(([key]) => !found.has(key)).map(([, row]) => row);
  if (!missing.length) return;
  try {
    await client.append(RAW_TOUCHPOINT_TABLE, missing);
  } catch (error) {
    // A response can be lost after a successful write. Allow replica visibility to catch up.
    try {
      await waitForReadback(read, stored => stored.size === expected.size);
      return;
    } catch {
      throw error;
    }
  }
  await waitForReadback(read, stored => stored.size === expected.size);
}

function rowKey(row: { envelope_hash: string; event_index: number }): string {
  return `${row.envelope_hash}:${row.event_index}`;
}
