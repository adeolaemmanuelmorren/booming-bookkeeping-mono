import assert from 'node:assert/strict';
import test from 'node:test';
import { projectStripeIdentity, projectActiveCampaignIdentity, replaceIdentityScope, identityObservationVersion } from '../worker/identity/fivetran-facts.ts';
import type { JsonRecord } from '../worker/storage/tinybird.ts';

const T = '2026-09-05T22:28:00.000000Z';
const observed = (row: JsonRecord) => ({ _fivetran_synced: '2025-01-01T00:00:00Z', _v1_observed_at: T, ...row });
const stripe = (charge: JsonRecord, account: 'main'|'kajabi' = 'main') => ({ account, chargeId: 'ch',
  chargeVersions: [observed({ id: 'ch', paid: true, status: 'succeeded', customer_id: 'cu', payment_intent_id: 'pi', created: null, ...charge })],
  customerVersions: [observed({ id: 'cu', email: 'customer@example.com', name: 'Ada Lovelace' })],
  paymentIntentVersions: [observed({ id: 'pi', receipt_email: 'intent@example.com' })], evidenceRecordIds: [] });

test('successful refunded charges retain identity, exact observation version and null event time', async () => {
  const [fact] = await projectStripeIdentity(stripe({ refunded: true, amount_refunded: 100, billing_detail_email: ' BILLING@EXAMPLE.COM ' }), T);
  assert.equal(fact.observedAt, null);
  assert.equal(fact.sourceFactVersion, 1788647280000001);
  assert.equal(fact.sourcePriority, 1);
  assert.ok(fact.evidenceKeys.includes('email:billing@example.com'));
  assert.ok(!fact.evidenceKeys.includes('email:customer@example.com'));
  assert.deepEqual(await projectStripeIdentity(stripe({ paid: false }), T), []);
  assert.deepEqual(await projectStripeIdentity(stripe({ status: 'failed' }), T), []);
});

test('Stripe email fallback, name and account-specific phone rules match the source staging rules', async () => {
  const [main] = await projectStripeIdentity(stripe({ billing_detail_email: '', receipt_email: ' receipt@example.com ', metadata: '{"phone":"4155551212"}' }), T);
  assert.deepEqual(JSON.parse(main.factPayload), { anonymous_id: null, user_id: 'receipt@example.com', email: 'receipt@example.com',
    phone: '4155551212', first_name: 'Ada', last_name: 'Lovelace' });
  const [kajabi] = await projectStripeIdentity(stripe({ metadata: '{"phone":"4155551212"}' }, 'kajabi'), T);
  assert.equal(JSON.parse(kajabi.factPayload).phone, null);
  assert.equal(kajabi.factKey, 'stripe_kajabi:ch');
});

function ac(assignments: JsonRecord[], contact: JsonRecord = {}) {
  return { contactId: '42', contactVersions: [observed({ id: 42, email: ' PERSON@GMAIL.COM ', first_name: ' Ada ', ...contact })],
    assignmentVersions: assignments.map(observed), referencedTagVersions: [
      observed({ id: 1, tags: '[KRC] Registered for Challenge' }),
      observed({ id: 2, tags: '[KRC] Registered for Challenge - September' }),
      observed({ id: 3, tags: '[CW] Registered for Webinar' }),
    ], evidenceRecordIds: [] };
}
test('ActiveCampaign primary suppresses fallback only for its registration type; assignment IDs remain stable', async () => {
  const rows = [ { id: 10, contact: 42, tags: 1, c_date: null }, { id: 11, contact: 42, tags: 2, c_date: T }, { id: 12, contact: 42, tags: 3, c_date: null } ];
  const facts = await projectActiveCampaignIdentity(ac(rows), T);
  assert.deepEqual(facts.map(f => f.factKey), ['activecampaign:11','activecampaign:12']);
  assert.equal(facts[1].observedAt, null);
  assert.ok(facts[0].evidenceKeys.includes('email:person@gmail.com'));
  assert.deepEqual(await projectActiveCampaignIdentity(ac(rows, { _fivetran_deleted: true }), T), []);
  assert.equal((await projectActiveCampaignIdentity(ac(rows, { deleted: true }), T)).length, 2);
  const withoutPrimary = await projectActiveCampaignIdentity(ac([{ ...rows[0] }, { ...rows[1], _fivetran_deleted: true }]), T);
  assert.deepEqual(withoutPrimary.map(f => f.factKey), ['activecampaign:10']);
});

test('physical deletes and changed scope qualification retract prior facts using observation order', async () => {
  const [previous] = await projectStripeIdentity(stripe({}), T);
  const later = '2026-09-05T22:28:00.000001Z';
  const raw = stripe({});
  raw.chargeVersions.push(observed({ id: 'ch', _v1_deleted: true, _v1_observed_at: later }));
  assert.deepEqual(await projectStripeIdentity(raw, later), []);
  const [deleted] = await replaceIdentityScope([], [previous], later);
  assert.equal(deleted.sourceFactVersion, previous.sourceFactVersion + 1);
  assert.equal(deleted.factDeleted, true);
  assert.deepEqual(deleted.evidenceKeys, []);
  assert.equal(deleted.observedAt, null);
  assert.equal(identityObservationVersion(later), previous.sourceFactVersion + 1);
});
