import test from "node:test";
import assert from "node:assert/strict";
import gateway from "../index.js";
import { handleAdsRequest } from "../ads.js";
import { createGoogleAdsClient } from "../../../bill-ad-reports-site/server/reporting/src/sources/google/google-ads-client.js";
import { createPerformanceLoader as googlePerformance } from "../../../bill-ad-reports-site/server/reporting/src/sources/google/performance.js";
import { createHourlyLoader } from "../../../bill-ad-reports-site/server/reporting/src/sources/google/hourly.js";
import { createRequestHandler as googleHandler } from "../../../bill-ad-reports-site/server/reporting/src/sources/google/app.js";
import { createMetaClient } from "../../../bill-ad-reports-site/server/reporting/src/sources/meta/meta-client.js";
import { createPerformanceLoader as metaPerformance } from "../../../bill-ad-reports-site/server/reporting/src/sources/meta/performance.js";
import { createRequestHandler as metaHandler } from "../../../bill-ad-reports-site/server/reporting/src/sources/meta/app.js";

const env = { ADMIN_TOKEN: "private", GOOGLE_ADS_DEVELOPER_TOKEN: "developer",
  GOOGLE_ADS_LOGIN_CUSTOMER_ID: "3855055503", META_ADS_ACCESS_TOKEN: "meta" };
const rawGoogle = {
  segments: { date: "2026-10-01", hour: 13 },
  customer: { id: "1941192637", descriptiveName: "Boom", currencyCode: "USD", timeZone: "America/Los_Angeles" },
  campaign: { id: "10", name: "Search", advertisingChannelType: "SEARCH" },
  adGroup: { id: "20", name: "Group" }, adGroupAd: { ad: { id: "30", name: "Ad" } },
  metrics: { costMicros: "12340000", impressions: "100", clicks: "7", conversions: 2.5, conversionsValue: 300 },
};
const rawMeta = { date_start: "2026-10-01", account_id: "10", account_name: "Boom", account_currency: "USD",
  campaign_id: "20", campaign_name: "Campaign", adset_id: "30", adset_name: "Set", ad_id: "40", ad_name: "Ad",
  impressions: "100", inline_link_clicks: "8", spend: "12.50", reach: "60",
  hourly_stats_aggregated_by_advertiser_time_zone: "13:00:00 - 13:59:59",
  actions: [{ action_type: "offsite_conversion.fb_pixel_purchase", value: "2" }],
  action_values: [{ action_type: "offsite_conversion.fb_pixel_purchase", value: "198" }],
};
async function provider(url, init) {
  if (String(url).includes("googleads.googleapis.com")) {
    assert.equal(new Headers(init.headers).get("developer-token"), "developer");
    assert.equal(new Headers(init.headers).get("authorization"), "Bearer google");
    const q = JSON.parse(init.body).query;
    const row = q.includes("PERFORMANCE_MAX")
      ? { ...rawGoogle, campaign: { ...rawGoogle.campaign, advertisingChannelType: "PERFORMANCE_MAX" } } : rawGoogle;
    return Response.json([{ results: [row] }]);
  }
  assert.equal(new Headers(init.headers).get("authorization"), "Bearer meta");
  return Response.json({ data: [rawMeta] });
}
const request = path => new Request("https://worker.example" + path + "?start_date=2026-10-01&end_date=2026-10-01",
  { headers: { Authorization: "Bearer private", "X-Google-Ads-Access-Token": "google" } });

test("Worker returns exactly the same nonempty payloads as both Cloud Run adapters", async t => {
  t.mock.timers.enable({ apis: ["Date"], now: new Date("2026-10-02T12:00:00Z") });
  const adsClient = createGoogleAdsClient({ customerId: "1941192637", loginCustomerId: "3855055503",
    developerToken: "developer", apiVersion: "v25", fetchImpl: provider,
    auth: { getClient: async () => ({ getRequestHeaders: async () => new Headers({ Authorization: "Bearer google" }) }) } });
  const google = googleHandler({ loadPerformance: googlePerformance({ adsClient, cacheSeconds: 300 }),
    loadHourly: createHourlyLoader({ adsClient, cacheSeconds: 300 }), maxRangeDays: 366, cacheSeconds: 300 });
  const meta = metaHandler({ loaders: metaPerformance({ metaClient: createMetaClient({
    accessToken: "meta", accountId: "10151088695453802", apiVersion: "v25.0", fetchImpl: provider,
  }), cacheSeconds: 300 }), maxRangeDays: 366, cacheSeconds: 300 });
  for (const path of ["/v1/google-ads/performance", "/v1/google-ads/hourly-performance",
    "/v1/meta-ads/performance", "/v1/meta-ads/hourly-performance", "/v1/meta-ads/delivery", "/v1/meta-ads/dashboard"]) {
    const expected = await (path.includes("google-ads") ? google : meta)(request(path));
    const actual = await handleAdsRequest(request(path), env, null, { fetchImpl: provider, cache: null });
    assert.equal(actual.status, 200, path);
    const body = await actual.json();
    assert.ok((body.rows ?? body.hourlyRows).length > 0, path);
    assert.deepEqual(body, JSON.parse(expected.body), path);
  }
});

test("unauthenticated requests cannot reach cached ads or existing sources", async () => {
  const response = await gateway.fetch(new Request("https://worker.example/v1/meta-ads/performance"), env);
  assert.equal(response.status, 401);
});
test("Google request without short-lived credentials is rejected", async () => {
  const response = await handleAdsRequest(new Request("https://worker.example/v1/google-ads/performance"), env);
  assert.equal(response.status, 401);
});
test("existing Stripe RPC contract remains intact", async () => {
  const response = await gateway.fetch(new Request("https://worker.example/read", {
    method: "POST", headers: { Authorization: "Bearer private" },
    body: JSON.stringify({ source: "stripe", account: "main", path: "/charges", parameters: { limit: "100" } }),
  }), { ...env, STRIPE_SOURCE: { read: async (account, path, params) => {
    assert.deepEqual([account, path, params], ["stripe", "/charges", { limit: "100" }]);
    return { status: 200, body: { data: [{ id: "ch_1" }] } };
  } } });
  assert.deepEqual(await response.json(), { status: 200, body: { data: [{ id: "ch_1" }] } });
});
