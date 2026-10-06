export type ProviderParameters = Record<string, string>;

export function stripeReadRequest(
  secret: string,
  path: string,
  parameters: ProviderParameters = {},
): Request {
  const allowed = /^\/(account|events|charges|refunds|customers|invoices|subscriptions|products|prices|payment_intents|payment_links|checkout\/sessions)(\/[A-Za-z0-9_]+)?(\/(lines|line_items))?$/;
  if (!allowed.test(path)) throw new Error("Unsupported Stripe source path");
  if (!secret) throw new Error("Stripe source credentials are missing");
  const url = new URL(`https://api.stripe.com/v1${path}`);
  url.search = new URLSearchParams(parameters).toString();
  return new Request(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${secret}`,
      "Stripe-Version": "2024-06-20",
    },
  });
}

export function activeCampaignReadRequest(
  apiUrl: string,
  secret: string,
  path: string,
  parameters: ProviderParameters = {},
): Request {
  const allowed = /^\/(contacts|contactTags|tags|fieldValues|fields)(\/\d+)?(\/(contactTags|fieldValues))?$/;
  if (!allowed.test(path)) throw new Error("Unsupported ActiveCampaign source path");
  if (!apiUrl || !secret) throw new Error("ActiveCampaign source credentials are missing");
  const base = new URL(apiUrl);
  if (base.protocol !== "https:") throw new Error("ActiveCampaign requires HTTPS");
  const url = new URL(`/api/3${path}`, base.origin);
  url.search = new URLSearchParams(parameters).toString();
  return new Request(url, { method: "GET", headers: { "Api-Token": secret, Accept: "application/json" } });
}

export async function readProvider(request: Request): Promise<{
  status: number;
  retryAfter: string | null;
  body: unknown;
}> {
  const response = await fetch(request, { signal: AbortSignal.timeout(20_000), redirect: "manual" });
  return {
    status: response.status,
    retryAfter: response.headers.get("retry-after"),
    body: await response.json(),
  };
}
