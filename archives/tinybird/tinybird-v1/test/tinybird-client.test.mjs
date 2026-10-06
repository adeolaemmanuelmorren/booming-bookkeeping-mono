import assert from 'node:assert/strict';
import test from 'node:test';
import { Tinybird, ndjsonBatches, sqlString, uint64Number, waitForReadback } from '../worker/storage/tinybird.ts';

const env = { TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: 'private-test-token' };

test('UInt64 uploads retain exact integers and reject unsafe or malformed values', () => {
  assert.equal(uint64Number('1788639468000001'), 1788639468000001);
  assert.equal(uint64Number(0), 0);
  for (const value of ['9007199254740993', '', '-1', '1.2', '1e3', Infinity]) {
    assert.throws(() => uint64Number(value), /integer/);
  }
});

test('readback retries delayed visibility but never accepts missing or conflicting content', async () => {
  let calls = 0;
  assert.deepEqual(await waitForReadback(async () => ++calls < 3 ? [] : ['verified'], rows => rows.length === 1, [0, 0, 0]), ['verified']);
  await assert.rejects(waitForReadback(async () => [], rows => rows.length > 0, [0]), /not yet fully visible/);
  await assert.rejects(waitForReadback(async () => ['corrupt'], () => { throw new Error('Conflicting content'); }, [0]), /Conflicting content/);
});

test('append rejects partial ingestion, including HTTP success with quarantined rows', async () => {
  const client = new Tinybird(env, async () => Response.json({ successful_rows: 1, quarantined_rows: 1 }));
  await assert.rejects(client.append('v1_test', [{ id: 1 }, { id: 2 }]), /every publication row/);
});

test('append counts exact rows and sends credentials only in the authorization header', async () => {
  let calls = 0;
  const client = new Tinybird(env, async (url, init) => {
    calls++;
    assert.equal(new URL(url).searchParams.get('wait'), 'true');
    assert.ok(!String(url).includes(env.TINYBIRD_TOKEN));
    assert.equal(init.headers.Authorization, `Bearer ${env.TINYBIRD_TOKEN}`);
    assert.equal(init.body, '{"id":1}\n{"id":2}\n');
    return Response.json({ successful_rows: 2, quarantined_rows: 0 });
  });
  await client.append('v1_test', [{ id: 1 }, { id: 2 }]);
  assert.equal(calls, 1);
});

test('UTF-8 upload bounds retain every row without cutting a JSON record', () => {
  const rows = [{ text: 'ééé' }, { text: 'hello' }, { text: '😀' }];
  const batches = [...ndjsonBatches(rows, 22)];
  assert.ok(batches.every(body => Buffer.byteLength(body) <= 22));
  assert.deepEqual(batches.join('').trim().split('\n').map(JSON.parse), rows);
  assert.throws(() => [...ndjsonBatches([{ text: 'x'.repeat(100) }], 22)], /exceeds/);
});

test('query errors do not echo source data or authentication', async () => {
  const client = new Tinybird(env, async () => new Response('private provider payload', { status: 429 }));
  await assert.rejects(client.query('SELECT 1'), error => error.message === 'Tinybird request failed with HTTP 429');
  assert.equal(sqlString("a\\b'c"), "'a\\\\b\\'c'");
});
