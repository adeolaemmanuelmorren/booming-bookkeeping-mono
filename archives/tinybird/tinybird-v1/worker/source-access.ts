import type { StripeSource } from '../../../../cloudflare-workers/source-gateways/stripe.ts';
import type { ActiveCampaignSource } from '../../../../cloudflare-workers/source-gateways/activecampaign.ts';

export interface SourceAccessEnv {
  STRIPE_SOURCE: Pick<StripeSource, 'read'>;
  ACTIVECAMPAIGN_SOURCE: Pick<ActiveCampaignSource, 'read'>;
}

type ProviderResult = Awaited<ReturnType<StripeSource['read']>>;

export async function checkSourceAccess(env: SourceAccessEnv) {
  const stripe = await checkStripe(env, 'stripe');
  const kajabi = await checkStripe(env, 'stripe_kajabi');
  if (stripe.account_id === kajabi.account_id) throw new Error('Both Stripe bindings resolve to the same account');
  const contacts = providerObject(await env.ACTIVECAMPAIGN_SOURCE.read('/contacts', { limit: '1' }), 'ActiveCampaign');
  if (!Array.isArray(contacts.contacts)) throw new Error('ActiveCampaign contact response is invalid');
  const meta = contacts.meta as { total?: string | number } | undefined;
  const total = Number(meta?.total);
  if (!Number.isSafeInteger(total) || total < 0) throw new Error('ActiveCampaign contact count is unavailable');
  return { stripe, stripe_kajabi: kajabi, activecampaign: { connected: true, contacts: total } };
}

/** Verify that the provider's date filters really support minute polling. */
export async function checkActiveCampaignPolling(env: SourceAccessEnv) {
  const page = providerObject(await env.ACTIVECAMPAIGN_SOURCE.read('/contacts', {
    limit: '1', 'orders[id]': 'ASC',
  }), 'ActiveCampaign');
  const contacts = contactList(page);
  const contact = contacts[0];
  if (!contact || typeof contact.id !== 'string' || typeof contact.udate !== 'string') {
    throw new Error('ActiveCampaign returned no dated contact for the filter probe');
  }
  const id = Number(contact.id);
  const updatedAt = Date.parse(contact.udate);
  if (!Number.isSafeInteger(id) || id < 1 || !Number.isFinite(updatedAt)) throw new Error('Invalid contact probe metadata');
  const parameters = { limit: '1', id_greater: String(id - 1), id_less: String(id + 1), 'orders[id]': 'ASC' };
  const includes = async (field: string, time: number) => {
    const result = providerObject(await env.ACTIVECAMPAIGN_SOURCE.read('/contacts', {
      ...parameters, [field]: new Date(time).toISOString(),
    }), 'ActiveCampaign');
    return contactList(result).some(row => row.id === contact.id);
  };
  const afterEarlier = await includes('filters[updated_after]', updatedAt - 2_000);
  const afterLater = await includes('filters[updated_after]', updatedAt + 2_000);
  const beforeEarlier = await includes('filters[updated_before]', updatedAt - 2_000);
  const beforeLater = await includes('filters[updated_before]', updatedAt + 2_000);
  const current = providerObject(await env.ACTIVECAMPAIGN_SOURCE.read(`/contacts/${contact.id}`), 'ActiveCampaign').contact;
  if (!current || typeof current !== 'object' || (current as Record<string, unknown>).udate !== contact.udate) {
    throw new Error('Contact changed during filter verification; retry the probe');
  }
  return {
    subday_filters_verified: afterEarlier && !afterLater && !beforeEarlier && beforeLater,
    after_earlier_includes: afterEarlier, after_later_includes: afterLater,
    before_earlier_includes: beforeEarlier, before_later_includes: beforeLater,
  };
}

function contactList(body: Record<string, unknown>): Record<string, unknown>[] {
  if (!Array.isArray(body.contacts)) throw new Error('ActiveCampaign contact list is invalid');
  return body.contacts as Record<string, unknown>[];
}

async function checkStripe(env: SourceAccessEnv, account: 'stripe' | 'stripe_kajabi') {
  const details = providerObject(await env.STRIPE_SOURCE.read(account, '/account'), account);
  const charges = providerObject(await env.STRIPE_SOURCE.read(account, '/charges', { limit: '1' }), account);
  if (typeof details.id !== 'string' || !Array.isArray(charges.data)) throw new Error('Stripe source response is invalid');
  return { connected: true, account_id: details.id, charge_read_verified: true };
}

function providerObject(response: ProviderResult, name: string): Record<string, unknown> {
  if (response.status !== 200) throw new Error(`${name} returned HTTP ${response.status}`);
  const body = response.body;
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error(`${name} returned invalid JSON`);
  return body as Record<string, unknown>;
}
