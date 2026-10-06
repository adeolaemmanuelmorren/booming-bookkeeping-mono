import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { changedIdentityFacts } from '../worker/identity/engine.ts';
import { computeIdentityBatch } from '../worker/identity/state.ts';
import { identityRecord, IdentityStorage } from '../worker/identity/storage.ts';

const timestamp = '2026-09-01T00:00:00.000000Z';

function fact(key, evidenceKeys, version = 1, deleted = false) {
  const payload = JSON.stringify({ evidenceKeys, first_name: 'Casey', last_name: '', deleted });
  return {
    eventId: `${key}:${version}`, producerId: 'fixture', observedAt: timestamp, ingestedAt: timestamp,
    factKind: 'fixture', factKey: key, sourceFactVersion: version, factDeleted: deleted,
    factPayloadHash: createHash('sha256').update(payload).digest('hex'), factPayload: payload, evidenceKeys,
  };
}

function memoryState() {
  const records = new Map();
  const reads = [];
  const current = (kind, keys, keepDeleted = false) => {
    reads.push({ kind, keys: [...keys] });
    return [...records.values()]
      .filter(row => row.state_kind === kind && keys.includes(row.lookup_key) && (keepDeleted || !row.is_deleted))
      .map(row => JSON.parse(row.payload_json));
  };
  const reader = {
    facts: async keys => current('fact', keys, true),
    mappings: async keys => current('mapping', keys),
    profiles: async keys => current('profile', keys),
    evidence: async keys => current('evidence', keys),
  };
  return {
    reader, reads, records,
    apply(rows) { for (const row of rows.map(identityRecord)) records.set(`${row.state_kind}:${row.state_key}`, row); },
    mappings() {
      return [...records.values()].filter(row => row.state_kind === 'mapping' && !row.is_deleted)
        .map(row => JSON.parse(row.payload_json)).map(row => [row.identifierKey, row.profileId]).sort();
    },
  };
}

function batch(facts, version = 1) {
  return { tenantId: 'test', version, id: `batch-${version}`, committedAt: timestamp, facts };
}

test('incremental merge and split match a fresh complete graph while leaving unrelated profiles alone', async () => {
  const state = memoryState();
  const left = fact('left', ['email:a@example.com', 'anonymous_id:a']);
  const right = fact('right', ['email:b@example.com', 'anonymous_id:b']);
  const unrelated = fact('unrelated', ['email:z@example.com']);
  state.apply((await computeIdentityBatch(batch([left, right, unrelated]), state.reader)).rows);
  state.reads.length = 0;

  const bridge = fact('bridge', ['anonymous_id:a', 'anonymous_id:b']);
  const merged = await computeIdentityBatch(batch([bridge], 2), state.reader);
  state.apply(merged.rows);
  assert.ok(!state.reads.some(read => read.keys.includes('unrelated') || read.keys.includes('email:z@example.com')));
  const freshMerged = memoryState();
  freshMerged.apply((await computeIdentityBatch(batch([left, right, unrelated, bridge]), freshMerged.reader)).rows);
  assert.deepEqual(state.mappings(), freshMerged.mappings());

  const removed = fact('bridge', [], 2, true);
  state.apply((await computeIdentityBatch(batch([removed], 3), state.reader)).rows);
  const freshSplit = memoryState();
  freshSplit.apply((await computeIdentityBatch(batch([left, right, unrelated]), freshSplit.reader)).rows);
  assert.deepEqual(state.mappings(), freshSplit.mappings());
  const replay = await computeIdentityBatch(batch([bridge], 4), state.reader);
  assert.equal(replay.changedFacts.length, 0);
});

test('same source revision cannot silently switch between conflicting identity payloads', async () => {
  const state = memoryState();
  state.apply((await computeIdentityBatch(batch([fact('one', ['email:a@example.com'])]), state.reader)).rows);
  const stable = await state.reader.facts(['one']);
  assert.throws(() => changedIdentityFacts([fact('one', ['email:b@example.com'])], stable), /Conflicting identity/);
});

test('a missing reverse-index fact blocks identity publication', async () => {
  const state = memoryState();
  state.reader.evidence = async () => [{ identifierKey: 'email:a@example.com', factKey: 'missing' }];
  await assert.rejects(computeIdentityBatch(batch([fact('one', ['email:a@example.com'])]), state.reader), /missing fact/);
});

test('publication verifies actual stored payloads before committing, including retries', async () => {
  const state = memoryState();
  const input = batch([fact('one', ['email:a@example.com'])]);
  const result = await computeIdentityBatch(input, state.reader);
  let records = [];
  let commits = [];
  let corrupt = true;
  const client = {
    async append(table, rows) {
      if (table === 'v1_identity_records') records = rows;
      else commits.push(...rows);
    },
    async query(sql) {
      if (sql.includes('FROM v1_identity_commits')) return commits.map(row => ({
        ...row, batch_version: String(row.batch_version),
      }));
      const actual = structuredClone(records).map(row => ({ ...row, batch_version: String(row.batch_version) }));
      if (corrupt && actual.length) actual[0].payload_json = '{"corrupted":true}';
      return [...actual, ...actual];
    },
  };
  const storage = new IdentityStorage(client);
  await assert.rejects(storage.publish(input, result.rows), /records did not verify/);
  assert.equal(commits.length, 0);
  corrupt = false;
  await storage.publish(input, result.rows);
  assert.equal(commits.length, 1);
  assert.equal(commits[0].row_count, records.length);
});
