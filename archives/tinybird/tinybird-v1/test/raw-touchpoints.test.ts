import assert from 'node:assert/strict';
import { test } from 'node:test';
import { sha256 } from '../worker/storage/json.ts';
import { rawObservations, publishRawObservations, type RawObservation } from '../worker/browser/raw-storage.ts';
import type { BrowserQueueEnvelope } from '../worker/browser/ingress.ts';

async function observations() {
  const envelope = {
    schema_version: 'jitsu_events_api_v1', producer_id: 'jitsu:boom',
    events: [0, 1].map(index => ({
      tenant_id: 'boom', producer_id: 'jitsu:boom', producer_sequence: index,
      message_id: `message-${index}`, delivery_event_id: `delivery-${index}`, event_kind: 'page_view',
      observed_at: '2026-09-05T01:02:03.123456Z', ingested_at: '2026-09-05T01:02:04.456789Z',
      source_fact_version: 1788570123123456, source_deleted: 0,
      fact_payload: JSON.stringify({ unknown_field: { keep: true } }), extra_field: ['retain', index],
    })),
  } as BrowserQueueEnvelope;
  return { envelope, rows: await rawObservations([{ envelope, hash: await sha256(JSON.stringify(envelope)), receivedAt: '2026-09-05T01:03:00.000Z' }]) };
}

function storage() {
  const stored: RawObservation[] = [];
  let writes = 0;
  let failAfterWrite = false;
  return {
    stored,
    get writes() { return writes; },
    set failAfterWrite(value: boolean) { failAfterWrite = value; },
    async query<T>() {
      const groups = new Map<string, RawObservation[]>();
      for (const row of stored) {
        const key = `${row.envelope_hash}:${row.event_index}`;
        groups.set(key, [...groups.get(key) ?? [], row]);
      }
      return await Promise.all([...groups.values()].map(async rows => ({
        envelope_hash: rows[0].envelope_hash, event_index: rows[0].event_index,
        stored_hash: rows[0].observation_hash,
        variants: new Set(rows.map(row => JSON.stringify([row.observation_hash, row.observation_json]))).size,
        invalid_hashes: (await Promise.all(rows.map(async row => row.observation_hash !== await sha256(row.observation_json)))).filter(Boolean).length,
      }))) as T[];
    },
    async append(_table: string, rows: readonly Record<string, unknown>[]) {
      writes++;
      stored.push(...rows as RawObservation[]);
      if (failAfterWrite) throw new Error('Response lost after storage');
    },
  };
}

test('raw observations retain every original field and microsecond timestamp', async () => {
  const { envelope, rows } = await observations();
  assert.deepEqual(rows.map(row => JSON.parse(row.observation_json)), envelope.events);
  assert.equal(rows[0].received_at, '2026-09-05T01:03:00.000Z');
  assert.equal(rows[0].observation_hash, await sha256(JSON.stringify(envelope.events[0])));
});

test('lost append response is recovered by exact readback without another append', async () => {
  const { rows } = await observations();
  const client = storage();
  client.failAfterWrite = true;
  await publishRawObservations(client, rows);
  client.failAfterWrite = false;
  await publishRawObservations(client, rows);
  assert.equal(client.writes, 1);
  assert.equal(client.stored.length, 2);
});

test('partial publication appends only missing logical events', async () => {
  const { rows } = await observations();
  const client = storage();
  client.stored.push(rows[0]);
  await publishRawObservations(client, [...rows, ...rows]);
  assert.equal(client.stored.length, 2);
  assert.equal(client.writes, 1);
});

test('identical physical retries remain one verified logical event', async () => {
  const { rows } = await observations();
  const client = storage();
  client.stored.push(...rows, ...rows);
  await publishRawObservations(client, rows);
  assert.equal(client.writes, 0);
});

test('changed payload or forged hash cannot acknowledge a stored event', async () => {
  const { rows } = await observations();
  for (const changed of [{ ...rows[0], observation_json: '{}' }, { ...rows[0], observation_hash: 'bad' }]) {
    const client = storage();
    client.stored.push(changed, rows[1]);
    await assert.rejects(publishRawObservations(client, rows), /conflicts/);
    assert.equal(client.writes, 0);
  }
});

test('input with conflicting keys or mixed tenants is rejected before writing', async () => {
  const { rows } = await observations();
  const client = storage();
  await assert.rejects(publishRawObservations(client, [rows[0], { ...rows[0], observation_hash: 'changed' }]), /Conflicting/);
  await assert.rejects(publishRawObservations(client, [rows[0], { ...rows[1], tenant_id: 'other' }]), /spans tenants/);
  assert.equal(client.writes, 0);
});
