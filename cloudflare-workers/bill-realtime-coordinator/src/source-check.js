// Return access checks only. Provider response bodies can contain customer data.
export async function checkSources(env) {
  async function stripe(account) {
    try {
      const identity = await env.STRIPE_SOURCE.read(account, "/account");
      const charges = await env.STRIPE_SOURCE.read(account, "/charges", { limit: "1" });
      const expansions = [];
      const chargeId = charges.body?.data?.[0]?.id;
      if (typeof chargeId === "string" && /^ch_[A-Za-z0-9_]+$/.test(chargeId)) {
        for (const field of ["customer", "payment_intent", "invoice", "balance_transaction", "payment_method"]) {
          const result = await env.STRIPE_SOURCE.read(account, `/charges/${chargeId}`, { "expand[0]": field });
          expansions.push({ field, status: result.status, errorCode: result.body?.error?.code ?? null });
        }
      }
      return { ok: charges.status === 200 && Array.isArray(charges.body?.data),
        expansions,
        status: charges.status, accountStatus: identity.status,
        errorCode: typeof charges.body?.error?.code === "string" ? charges.body.error.code : null,
        accountId: identity.status === 200 ? identity.body?.id ?? null : null };
    } catch { return { ok: false, error: "Private Stripe gateway is unavailable." }; }
  }
  async function activeCampaign() {
    const paths = [["/contacts", "contacts"], ["/contactTags", "contactTags"], ["/tags", "tags"]];
    try {
      for (const [path, field] of paths) {
        const result = await env.ACTIVECAMPAIGN_SOURCE.read(path, { limit: "1" });
        if (result.status !== 200 || !Array.isArray(result.body?.[field])) {
          return { ok: false, status: result.status, path };
        }
      }
      return { ok: true, status: 200 };
    } catch { return { ok: false, error: "Private ActiveCampaign gateway is unavailable." }; }
  }
  const main = await stripe("stripe");
  const kajabi = await stripe("stripe_kajabi");
  const activecampaign = await activeCampaign();
  const distinctStripeAccounts = main.accountId && kajabi.accountId ? main.accountId !== kajabi.accountId : null;
  return { ok: main.ok && kajabi.ok && activecampaign.ok && distinctStripeAccounts !== false,
    stripeMain: main, stripeKajabi: kajabi, activecampaign, distinctStripeAccounts };
}
