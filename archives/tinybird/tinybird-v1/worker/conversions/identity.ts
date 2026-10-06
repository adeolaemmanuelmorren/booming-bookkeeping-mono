import { evidenceKeys, type BrowserIdentifiers } from '../browser/identity.ts';
import { canonicalJson, sha256 } from '../storage/json.ts';
import type { PendingIdentityFact } from '../identity/engine.ts';
import type { SourceReplacement } from './publication.ts';

/** Dataform identity evidence comes from payments and qualifying registrations. */
export async function conversionIdentityFacts(replacement: SourceReplacement): Promise<PendingIdentityFact[]> {
  return Promise.all(replacement.rows.map(async row => {
    const stripe = replacement.source === 'stripe';
    const kind = stripe ? replacement.source_account === 'main' ? 'stripe' : 'stripe_kajabi' : 'activecampaign';
    const recordId = requiredString(stripe ? row.charge_id : row.form_submission_id);
    const name = nullableString(row.name);
    const parts = name?.split(' ') ?? [];
    const identifiers: BrowserIdentifiers = {
      anonymous_id: null,
      user_id: email(row.email),
      email: email(row.email),
      phone: nullableString(row.phone),
      first_name: stripe ? parts[0] ?? null : nullableString(row.first_name),
      last_name: stripe ? parts.at(-1) ?? null : nullableString(row.last_name),
    };
    const factKey = `${kind}:${recordId}`;
    const payload = canonicalJson(identifiers);
    const deleted = row.is_deleted === true;
    return {
      eventId: `${replacement.replacement_id}:identity:${factKey}`,
      producerId: `source:${kind}`,
      observedAt: nullableString(row.occurred_at),
      ingestedAt: replacement.observed_at,
      factKind: kind,
      factKey,
      sourcePriority: 1,
      sourceFactVersion: replacement.observation_sequence,
      factDeleted: deleted,
      factPayloadHash: await sha256(payload),
      factPayload: payload,
      evidenceKeys: deleted ? [] : evidenceKeys(identifiers),
    };
  }));
}

function email(value: unknown): string | null { return nullableString(value)?.trim().toLowerCase() || null; }
function nullableString(value: unknown): string | null { return typeof value === 'string' ? value : null; }
function requiredString(value: unknown): string {
  if (typeof value !== 'string' || !value) throw new Error('Conversion source record ID is required');
  return value;
}
