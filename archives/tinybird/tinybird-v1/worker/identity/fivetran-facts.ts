import { evidenceKeys, type BrowserIdentifiers } from '../browser/identity.ts';
import { canonicalJson, sha256 } from '../storage/json.ts';
import { timestampMicros } from '../sessions/session-engine.ts';
import { collapseFivetranVersions, booleanValue, isDeleted, optionalString } from '../fivetran/raw-collapse.ts';
import { matchActiveCampaignRegistrationTag } from '../conversions/activecampaign.mjs';
import type { ActiveCampaignRawScope, StripeRawScope } from '../fivetran/contracts.ts';
import type { PendingIdentityFact } from './engine.ts';

/** Shared by the fixed snapshot and the ordered raw-receipt consumer. */
export function identityObservationVersion(observedAt: string): number {
  const version = Number(timestampMicros(observedAt) + 1n);
  if (!Number.isSafeInteger(version) || version < 1) throw new Error('Invalid identity observation version');
  return version;
}

export async function projectStripeIdentity(raw: StripeRawScope, observedAt: string): Promise<PendingIdentityFact[]> {
  const charge = collapseFivetranVersions({ rows: raw.chargeVersions, throughInclusive: observedAt }).currentById.get(raw.chargeId);
  if (!charge || !booleanValue(charge.paid) || charge.status !== 'succeeded') return [];
  const customers = collapseFivetranVersions({ rows: raw.customerVersions, throughInclusive: observedAt }).currentById;
  const intents = collapseFivetranVersions({ rows: raw.paymentIntentVersions, throughInclusive: observedAt }).currentById;
  const customer = customers.get(String(charge.customer_id)) ?? {};
  const intent = intents.get(String(charge.payment_intent_id)) ?? {};
  const email = first(charge.billing_detail_email, charge.receipt_email, customer.email, intent.receipt_email)?.toLowerCase() ?? null;
  const name = first(charge.billing_detail_name, customer.name);
  let metadata: Record<string, unknown> = {};
  if (typeof charge.metadata === 'string') {
    try { metadata = JSON.parse(charge.metadata); } catch { /* GoogleSQL JSON_VALUE returns NULL for invalid JSON. */ }
  } else if (charge.metadata && typeof charge.metadata === 'object') metadata = charge.metadata as Record<string, unknown>;
  const identifiers: BrowserIdentifiers = {
    anonymous_id: null, user_id: email, email,
    phone: first(charge.billing_detail_phone, customer.phone, raw.account === 'main' ? metadata?.phone : null),
    first_name: name?.split(' ')[0] ?? null, last_name: name?.split(' ').at(-1) ?? null,
  };
  return [await identityFact(raw.account === 'main' ? 'stripe' : 'stripe_kajabi', raw.chargeId,
    nullableTime(charge.created), observedAt, identifiers)];
}

/** Registrations retain assignment IDs. A primary registration suppresses that type's fallback. */
export async function projectActiveCampaignIdentity(raw: ActiveCampaignRawScope, observedAt: string): Promise<PendingIdentityFact[]> {
  const contact = collapseFivetranVersions({ rows: raw.contactVersions, throughInclusive: observedAt }).currentById.get(raw.contactId);
  if (!contact || isDeleted(contact._fivetran_deleted)) return [];
  const tags = collapseFivetranVersions({ rows: raw.referencedTagVersions, throughInclusive: observedAt }).currentById;
  const assignments = collapseFivetranVersions({ rows: raw.assignmentVersions, throughInclusive: observedAt }).currentById;
  const matched = [...assignments.values()].flatMap(row => {
    const tag = tags.get(String(row.tags ?? row.tag));
    if (String(row.contact) !== raw.contactId || isDeleted(row._fivetran_deleted) || !tag || isDeleted(tag._fivetran_deleted)) return [];
    const match = matchActiveCampaignRegistrationTag(optionalString(tag.tags));
    return match ? [{ row, match }] : [];
  });
  const primary = new Set(matched.filter(item => item.match.matchType === 'primary').map(item => item.match.registrationType));
  const email = optionalString(contact.email)?.toLowerCase() ?? null;
  const identifiers: BrowserIdentifiers = {
    anonymous_id: null, user_id: email, email, phone: optionalString(contact.phone),
    first_name: optionalString(contact.first_name), last_name: optionalString(contact.last_name),
  };
  return Promise.all(matched.filter(item => item.match.matchType === 'primary' || !primary.has(item.match.registrationType))
    .sort((a, b) => String(a.row.id).localeCompare(String(b.row.id)))
    .map(item => identityFact('activecampaign', String(item.row.id), nullableTime(item.row.c_date), observedAt, identifiers)));
}

/** The caller retains previous scope keys so disappeared registrations can retract evidence. */
export async function replaceIdentityScope(current: PendingIdentityFact[], previous: PendingIdentityFact[], observedAt: string): Promise<PendingIdentityFact[]> {
  const present = new Set(current.map(fact => fact.factKey));
  const deleted = previous.filter(fact => !fact.factDeleted && !present.has(fact.factKey));
  return [...current, ...deleted.map(fact => ({ ...fact,
    eventId: `identity:${fact.factKey}:${identityObservationVersion(observedAt)}:deleted`,
    ingestedAt: observedAt, sourceFactVersion: identityObservationVersion(observedAt), factDeleted: true, evidenceKeys: [],
  }))];
}

async function identityFact(kind: string, id: string, occurredAt: string | null, observedAt: string, identifiers: BrowserIdentifiers): Promise<PendingIdentityFact> {
  if (!id) throw new Error('Identity source ID is required');
  const factKey = `${kind}:${id}`;
  const payload = canonicalJson(identifiers);
  const factPayloadHash = await sha256(payload);
  const version = identityObservationVersion(observedAt);
  return { eventId: `identity:${factKey}:${version}:${factPayloadHash}`, producerId: `source:${kind}`,
    factKind: kind, factKey, observedAt: occurredAt, ingestedAt: observedAt, sourcePriority: 1,
    sourceFactVersion: version, factDeleted: false, factPayload: payload, factPayloadHash, evidenceKeys: evidenceKeys(identifiers) };
}

function first(...values: unknown[]): string | null { return values.map(optionalString).find(value => value !== null) ?? null; }
function nullableTime(value: unknown): string | null {
  if (value === null || value === undefined || value === '') return null;
  const micros = timestampMicros(String(value));
  const seconds = micros / 1_000_000n;
  return new Date(Number(seconds * 1000n)).toISOString().replace('.000Z', `.${String(micros % 1_000_000n).padStart(6, '0')}Z`);
}
