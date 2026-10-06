import assert from 'node:assert/strict';
import test from 'node:test';
import { publishSourceReplacement, publishSourceReplacements, sourceRecords } from '../../worker/conversions/publication.ts';
import { conversionIdentityFacts } from '../../worker/conversions/identity.ts';

function replacement(overrides = {}) {
  return {
    source: 'stripe', source_account: 'main', scope_id: 'stripe:main:charge:ch_1',
    replacement_id: 'stripe:main:charge:ch_1:observation:1', observed_at: '2026-09-05T00:00:00Z',
    observation_sequence: 1, evidence_inbox_ids: ['event:1'],
    source_evidence: { canonical_charge: { id: 'ch_1', amount: 1234 }, event: { id: 'evt_1' } },
    rows: [{ source: 'stripe', source_account: 'main', source_fact_id: 'stripe:main:charge:ch_1',
      charge_id: 'ch_1', occurred_at: '2026-01-01T00:00:00Z', email: 'BUYER@example.com',
      name: 'Ada Buyer', phone: '4152221234', is_deleted: false, amount_minor: 1234 }],
    ...overrides,
  };
}

function remote() {
  const tables = { v1_source_records: [], v1_source_commits: [] };
  return {
    tables, corruptRead: false, loseCommitResponse: false,
    async append(table, rows) {
      tables[table].push(...structuredClone(rows));
      if (table === 'v1_source_commits' && this.loseCommitResponse) {
        this.loseCommitResponse = false;
        throw new Error('Lost reply after write');
      }
    },
    async query(sql) {
      const table = sql.includes('FROM v1_source_records') ? 'v1_source_records' : 'v1_source_commits';
      const rows = structuredClone(tables[table]);
      if (table === 'v1_source_records' && this.corruptRead && rows.length) rows[0].payload_json = '{"wrong":true}';
      return rows;
    },
  };
}

test('source publication retries lost commit replies without duplicating stored records', async () => {
  const client = remote();
  const facts = [];
  const identity = { async enqueue(input) { facts.push(...input); } };
  client.loseCommitResponse = true;
  await assert.rejects(publishSourceReplacement('boom', replacement(), client, identity), /Lost reply/);
  await publishSourceReplacement('boom', replacement(), client, identity);
  assert.equal(client.tables.v1_source_records.length, 4);
  assert.equal(client.tables.v1_source_commits.length, 1);
  assert.equal(facts.length, 2);
  assert.deepEqual(facts[0], facts[1]);
  assert.equal(facts[0].factKey, 'stripe:ch_1');
});

test('corrupted source payload prevents a commit even when row counts match', async () => {
  const client = remote();
  client.corruptRead = true;
  let enqueued = false;
  await assert.rejects(publishSourceReplacement('boom', replacement(), client,
    { async enqueue() { enqueued = true; } }), /Conflicting source publication/);
  assert.equal(client.tables.v1_source_commits.length, 0);
  assert.equal(enqueued, false);
});

test('a contact with zero registrations still publishes original contact and tag evidence', async () => {
  const input = replacement({ source: 'activecampaign', source_account: 'default',
    scope_id: 'activecampaign:contact:1', replacement_id: 'contact:1:observation:1', rows: [],
    source_evidence: { contact: { id: '1', email: 'contact@example.invalid' }, assignments: [], tags: [] } });
  const client = remote();
  await publishSourceReplacement('boom', input, client, { async enqueue() { throw Error('No identity expected'); } });
  assert.equal(client.tables.v1_source_commits.length, 1);
  assert.equal(client.tables.v1_source_records.length, 4);
  assert.ok(client.tables.v1_source_records.every(row => row.record_kind === 'evidence'));
  assert.deepEqual(await conversionIdentityFacts(input), []);
});

test('identity facts isolate Stripe accounts and retract deleted AC registration evidence', async () => {
  const main = replacement();
  const kajabi = replacement({ source_account: 'kajabi', rows: main.rows.map(row => ({ ...row, source_account: 'kajabi' })) });
  const [left] = await conversionIdentityFacts(main);
  const [right] = await conversionIdentityFacts(kajabi);
  assert.notEqual(left.factKey, right.factKey);
  assert.ok(left.evidenceKeys.includes('user_id:buyer@example.com'));
  assert.ok(left.evidenceKeys.includes('phone:+14152221234'));
  const [deleted] = await conversionIdentityFacts(replacement({ source: 'activecampaign', source_account: 'default',
    rows: [{ form_submission_id: 'assignment-7', email: 'contact@example.invalid', is_deleted: true }] }));
  assert.equal(deleted.factKey, 'activecampaign:assignment-7');
  assert.deepEqual(deleted.evidenceKeys, []);
  assert.equal(deleted.factDeleted, true);
});

test('raw evidence is mandatory and rows cannot silently cross source accounts', async () => {
  await assert.rejects(sourceRecords('boom', replacement({ source_evidence: null })), /Original source evidence/);
  await assert.rejects(sourceRecords('boom', replacement({ source_account: 'kajabi' })), /another source account/);
});

test('many source replacements share writes without losing independent scope commits', async () => {
  const client = remote();
  const appends = [];
  const append = client.append.bind(client);
  client.append = async (table, rows) => { appends.push({ table, rows: rows.length }); await append(table, rows); };
  const inputs = Array.from({ length: 25 }, (_, index) => replacement({
    scope_id: `scope:${index}`, replacement_id: `replacement:${index}`,
  }));
  await publishSourceReplacements('boom', inputs, client, { async enqueue() {} });
  assert.deepEqual(appends, [{ table: 'v1_source_records', rows: 100 }, { table: 'v1_source_commits', rows: 25 }]);
  assert.equal(new Set(client.tables.v1_source_commits.map(row => row.scope_id)).size, 25);
});
