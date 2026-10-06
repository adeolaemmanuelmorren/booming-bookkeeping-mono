import { sqlString, uint64Number } from '../storage/tinybird.ts';
import type { TinybirdBootstrapStorage } from './tinybird-storage.ts';

interface RangeProgress<Key> {
  after: Key | null;
  physical: number;
  distinct: number;
}

export function inputRangeKeysQuery(table: string, column: string, after: string | null, limit: number): string {
  return `SELECT DISTINCT ${column} AS record_id FROM ${table}
    ${after === null ? '' : `WHERE ${column} > ${sqlString(after)}`}
    ORDER BY ${column} LIMIT ${limit}`;
}

export function inputRangeCountQuery(table: string, columns: string[], column: string, after: string | null, upper: string): string {
  const lower = after === null ? '' : `${column} > ${sqlString(after)} AND `;
  return `SELECT count() AS physical,uniqExact(tuple(${columns.join(',')})) AS distinct
    FROM ${table} WHERE ${lower}${column} <= ${sqlString(upper)}`;
}

/** Wide exact counts stay inside disjoint physical ID ranges and resume after each verified range. */
export async function boundedInputCardinality(
  store: TinybirdBootstrapStorage, key: string, table: string, columns: string[], expectedPhysical: number,
  column: string, rangeIds = 5000,
): Promise<number> {
  return countRanges<string>(store, key, expectedPhysical, {
    keys: async after => (await store.client.query<{ record_id: string }>(inputRangeKeysQuery(table, column, after, rangeIds))).map(row => row.record_id),
    count: (after, upper) => inputRangeCountQuery(table, columns, column, after, upper),
  });
}

interface ProducerKey { producer_id: string; producer_sequence: string }

export async function boundedLiveCardinality(
  store: TinybirdBootstrapStorage, table: string, columns: string[], expectedPhysical: number, where: string,
): Promise<number> {
  const tuple = (key: ProducerKey) => `tuple(${sqlString(key.producer_id)},toUInt64(${sqlString(String(key.producer_sequence))}))`;
  return countRanges<ProducerKey>(store, 'live-jitsu', expectedPhysical, {
    keys: after => store.client.query<ProducerKey>(`SELECT DISTINCT producer_id,producer_sequence FROM ${table}
      WHERE ${where} ${after ? `AND tuple(producer_id,producer_sequence) > ${tuple(after)}` : ''}
      ORDER BY producer_id,producer_sequence LIMIT 5000`),
    count: (after, upper) => `SELECT count() AS physical,uniqExact(tuple(${columns.join(',')})) AS distinct
      FROM ${table} WHERE ${where} ${after ? `AND tuple(producer_id,producer_sequence) > ${tuple(after)}` : ''}
      AND tuple(producer_id,producer_sequence) <= ${tuple(upper)}`,
  });
}

async function countRanges<Key>(store: TinybirdBootstrapStorage, key: string, expectedPhysical: number, queries: {
  keys: (after: Key | null) => Promise<Key[]>;
  count: (after: Key | null, upper: Key) => string;
}): Promise<number> {
  const saved = await store.getManifest<{ physical: number; distinct: number }>('input-counts', key);
  if (saved) {
    if (saved.physical !== expectedPhysical) throw new Error('Frozen source count changed');
    return saved.distinct;
  }
  const checkpoint = await store.checkpoint<RangeProgress<Key>>('input-cardinality', key);
  let state = checkpoint?.payload ?? { after: null, physical: 0, distinct: 0 };
  let sequence = (checkpoint?.sequence ?? 0) + 1;
  let retries = 0;
  for (;;) {
    const ids = await queries.keys(state.after);
    if (!ids.length) {
      if (state.physical === expectedPhysical) break;
      if (state.physical > expectedPhysical || retries++ >= 3) throw new Error('Frozen source range count is incomplete');
      state = { after: null, physical: 0, distinct: 0 };
      await store.putCheckpoint('input-cardinality', key, sequence++, state);
      continue;
    }
    const upper = ids.at(-1)!;
    if (JSON.stringify(upper) === JSON.stringify(state.after)) throw new Error('Frozen source range did not advance');
    const [row] = await store.client.query<{ physical: string | number; distinct: string | number }>(
      queries.count(state.after, upper),
    );
    if (!row) throw new Error('Missing frozen source range count');
    const physical = uint64Number(row.physical);
    const distinct = uint64Number(row.distinct);
    if (physical < ids.length || distinct < ids.length || distinct > physical) {
      throw new Error('Frozen source range is not fully visible');
    }
    state = { after: upper, physical: state.physical + physical, distinct: state.distinct + distinct };
    if (state.physical > expectedPhysical) throw new Error('Frozen source contains unexpected rows');
    await store.putCheckpoint('input-cardinality', key, sequence++, state);
  }
  await store.putManifest('input-counts', key, { physical: state.physical, distinct: state.distinct });
  return state.distinct;
}
