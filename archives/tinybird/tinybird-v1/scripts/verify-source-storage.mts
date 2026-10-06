import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { Tinybird, sqlString, waitForReadback } from '../worker/storage/tinybird.ts';
import { publishSourceReplacements, type SourceReplacement } from '../worker/conversions/publication.ts';
import type { PendingIdentityFact } from '../worker/identity/engine.ts';
import { tinybirdRequest } from './tinybird.mjs';

const { environments } = await (await tinybirdRequest('/v1/environments')).json();
const branch = environments.find((row: { name: string }) => row.name === 'v1_facts_validation');
if (branch?.main !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af') throw new Error('Wrong validation branch');
const client = new Tinybird({ TINYBIRD_URL: 'https://api.us-east.tinybird.co', TINYBIRD_TOKEN: branch.token });
const tenant = `verification-source-${crypto.randomUUID()}`;
const now = new Date().toISOString();
const facts: PendingIdentityFact[] = [];
const identity = { async enqueue(input: PendingIdentityFact[]) { facts.push(...input); } };

function stripe(account: 'main' | 'kajabi', sequence: number, refunded = 0): SourceReplacement {
  const scope = `stripe:${account}:charge:ch_synthetic`;
  return {
    source: 'stripe', source_account: account, scope_id: scope,
    replacement_id: `${scope}:${sequence}`, observed_at: now, observation_sequence: sequence,
    evidence_inbox_ids: [`${scope}:event:${sequence}`],
    source_evidence: { canonical_charge: { id: 'ch_synthetic', amount: 1000, amount_refunded: refunded } },
    rows: [{ source: 'stripe', source_account: account, source_fact_id: scope, charge_id: 'ch_synthetic',
      occurred_at: now, is_deleted: false, amount_minor: 1000, net_amount_minor: 1000 - refunded,
      amount_refunded_minor: refunded, email: 'synthetic@example.invalid', name: 'Synthetic Fixture' }],
  };
}

function activeCampaign(sequence: number, deleted = false): SourceReplacement {
  return {
    source: 'activecampaign', source_account: 'default', scope_id: 'activecampaign:contact:synthetic',
    replacement_id: `activecampaign:contact:synthetic:${sequence}`, observed_at: now, observation_sequence: sequence,
    evidence_inbox_ids: [`ac-event:${sequence}`],
    source_evidence: { contact: { id: 'synthetic' }, assignments: deleted ? [] : [{ id: 'assignment-synthetic' }] },
    rows: [{ source: 'activecampaign', source_account: 'default', source_fact_id: 'activecampaign:assignment:synthetic',
      form_submission_id: 'assignment-synthetic', occurred_at: now, email: 'synthetic@example.invalid',
      first_name: 'Synthetic', last_name: 'Fixture', is_deleted: deleted }],
  };
}

async function currentConversions() {
  const rows = await client.query<{ payload_json: string }>(`
    SELECT DISTINCT records.payload_json AS payload_json
    FROM v1_source_records AS records
    INNER JOIN (
      SELECT scope_id, argMax(replacement_id, observation_sequence) AS current_replacement
      FROM v1_source_commits WHERE tenant_id = ${sqlString(tenant)} GROUP BY scope_id
    ) AS current ON records.scope_id = current.scope_id AND records.replacement_id = current.current_replacement
    WHERE records.tenant_id = ${sqlString(tenant)} AND records.record_kind = 'conversion'
      AND JSONExtractBool(records.payload_json, 'is_deleted') = 0
  `);
  return rows.map(row => JSON.parse(row.payload_json));
}

await publishSourceReplacements(tenant, [stripe('main', 1), stripe('kajabi', 1), activeCampaign(1)], client, identity);
await waitForReadback(currentConversions, rows => rows.length === 3);
await publishSourceReplacements(tenant, [stripe('main', 2, 500), activeCampaign(2, true)], client, identity);
const current = await waitForReadback(currentConversions, rows => rows.length === 2
  && rows.find(row => row.source_account === 'main')?.net_amount_minor === 500);
assert.equal(current.find(row => row.source_account === 'kajabi')?.net_amount_minor, 1000);
assert.ok(facts.some(fact => fact.factKind === 'activecampaign' && fact.factDeleted));
assert.ok(facts.some(fact => fact.factKey === 'stripe:ch_synthetic'));
assert.ok(facts.some(fact => fact.factKey === 'stripe_kajabi:ch_synthetic'));
const evidence = await client.query<{ rows: number }>(`SELECT count() AS rows FROM v1_source_records
  WHERE tenant_id = ${sqlString(tenant)} AND source = 'activecampaign' AND record_kind = 'evidence'`);
assert.ok(Number(evidence[0].rows) >= 6);
const result = { checked_at: new Date().toISOString(), branch_id: branch.id, tenant_id: tenant,
  verified: ['batch_publication', 'original_source_evidence', 'separate_stripe_accounts', 'refund_replacement',
    'registration_removal', 'current_fact_query', 'identity_inputs'], current_conversion_count: current.length };
await writeFile(new URL('../evidence/source-storage-verified.json', import.meta.url), JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify(result));
