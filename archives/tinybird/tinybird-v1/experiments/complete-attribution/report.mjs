import { dimensions, metricCount } from "./engine.mjs";
import { namedMetrics } from "./named-metrics.mjs";

const adKeys = [
  "date",
  "source",
  "account",
  "campaign",
  "adset",
  "ad",
  "currency",
];
const adMetrics = [
  "spend",
  "impressions",
  "clicks",
  "unique_clicks",
  "unique_inline_link_clicks",
  "platform_conversions",
  "platform_conversion_value",
];
const key = (row) => JSON.stringify(adKeys.map((name) => row[name] ?? null));
const zeros = () => Object.fromEntries(adMetrics.map((name) => [name, 0]));

export function mergeAdPerformance(contributions, adRows) {
  const result = new Map();
  for (const row of contributions) {
    const values = JSON.parse(row.key);
    const dims = Object.fromEntries(
      dimensions.map((name, i) => [name, values[i]]),
    );
    const group = key(dims);
    if (!result.has(group)) {
      result.set(group, {
        ...Object.fromEntries(adKeys.map((name) => [name, dims[name]])),
        campaignName: dims.campaignName,
        adsetName: dims.adsetName,
        adName: dims.adName,
        ...zeros(),
        metrics: Array(metricCount * 6).fill(0),
      });
    }
    const target = result.get(group);
    for (let i = 0; i < row.values.length; i++)
      target.metrics[i] += row.values[i];
  }

  // The provider must return one complete row per requested ad grain.
  const seen = new Set();
  for (const ad of adRows) {
    const group = key(ad);
    if (seen.has(group)) throw new Error("Duplicate ad-performance grain");
    seen.add(group);
    const prior = result.get(group) ?? {
      ...ad,
      metrics: Array(metricCount * 6).fill(0),
    };
    const measures = Object.fromEntries(
      adMetrics.map((name) => [name, ad[name] ?? 0]),
    );
    if (Object.values(measures).some((value) => !Number.isFinite(value)))
      throw new Error("Invalid ad performance");
    result.set(group, {
      ...prior,
      ...measures,
      campaignName: ad.campaignName ?? prior.campaignName ?? "unknown",
      adsetName: ad.adsetName ?? prior.adsetName ?? "unknown",
      adName: ad.adName ?? prior.adName ?? "unknown",
    });
  }
  return [...result.values()];
}

// Separate paid-ad report. Full detail retains all attribution dimensions.
// Account and currency deliberately isolate distinct accounts and currencies.
export function paidAdReport(contributions, adRows) {
  const paid = contributions.filter((row) => {
    const values = JSON.parse(row.key);
    return (
      ["google", "meta"].includes(values[1]) &&
      ["cpc", "paid", "paid_social"].includes(values[2])
    );
  });
  return mergeAdPerformance(paid, adRows).map(({ metrics, ...row }) => {
    const named = namedMetrics(metrics, true);
    const roas = {};
    for (const product of [
      "vip",
      "book",
      "mentorship",
      "kajabi",
      "catalog",
      "unknown",
    ]) {
      roas[`roas_${product}_mt`] = row.spend
        ? named[`revenue_${product}_server_side_mt`] / row.spend
        : null;
    }
    roas.roas_all_payments_mt = row.spend
      ? named.revenue_server_side_mt / row.spend
      : null;
    return { ...row, medium: "cpc", ...named, ...roas };
  });
}
