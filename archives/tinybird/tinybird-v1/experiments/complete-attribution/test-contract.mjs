import assert from "node:assert/strict";
import { attributeProfile, conversionMetrics, metricCount } from "./engine.mjs";
import { touch, conversion } from "./fixtures.mjs";
import { detailReport, namedMetrics } from "./named-metrics.mjs";
import { paidAdReport } from "./report.mjs";
import { CurrentInputs } from "./current-inputs.mjs";
const labeled = namedMetrics(Array.from({ length: 144 }, (_, i) => i));
for (let i = 0; i < metricCount; i++) {
  assert.equal(labeled[`${conversionMetrics[i]}_ft`], i);
  assert.equal(labeled[`${conversionMetrics[i]}_lt`], 24 + i);
  assert.equal(labeled[`${conversionMetrics[i]}_mt`], 48 + i);
  assert.equal(labeled[`${conversionMetrics[i]}_paid_ft`], 72 + i);
  assert.equal(labeled[`${conversionMetrics[i]}_paid_lt`], 96 + i);
  assert.equal(labeled[`${conversionMetrics[i]}_paid_mt`], 120 + i);
}
const touches = [
  touch("first", 1, { source: "direct", medium: "none", ad: null }),
  touch("middle", 2),
  touch("last", 3, { ad: "last" }),
  touch("excluded", 4, { source: "direct", medium: "none", ad: "excluded" }),
];
const rows = attributeProfile(touches, [
  conversion("c", 5, { metrics: Array(24).fill(100) }),
]);
const details = detailReport(rows);
for (const [ad, ft, lt, mt, paid] of [
  [null, 100, 0, 40, false],
  ["ad:1", 0, 0, 20, true],
  ["last", 0, 100, 40, true],
  ["excluded", 0, 0, 0, false],
]) {
  const row = details.find((r) => r.ad === ad);
  assert.ok(row);
  assert.equal(row.sessions, 1);
  for (const metric of conversionMetrics) {
    assert.equal(row[`${metric}_ft`], ft);
    assert.equal(row[`${metric}_lt`], lt);
    assert.equal(row[`${metric}_mt`], mt);
    assert.equal(row[`${metric}_paid_mt`], paid ? mt : 0);
  }
}
const ads = paidAdReport(rows, []);
assert.equal(ads.length, 2);
assert.equal(
  ads.reduce((n, r) => n + r.revenue_server_side_mt, 0),
  60,
);
const state = new CurrentInputs();
state.applyIdentity(new Map([["a", "p"]]), 2);
assert.equal(state.applyIdentity(new Map([["a", "old"]]), 1).size, 0);
assert.equal(state.identities.get("a"), "p");
assert.equal(state.applyIdentity(new Map([["a", "p"]]), 2).size, 0);
assert.throws(
  () => state.applyIdentity(new Map([["a", "other"]]), 2),
  /Conflicting/,
);
console.log(
  "Named 144 metrics, manual direct weights, paid report, and identity revisions pass",
);
// The paid mart's case-sensitive filter is separate from case-insensitive weights.
const mixed = attributeProfile(
  [
    touch("null-ad", 1, { ad: null }),
    touch("paid-medium", 2, { medium: "paid" }),
    touch("paid-social-medium", 3, { medium: "paid_social" }),
    touch("upper-case", 4, { source: "GOOGLE", medium: "CPC" }),
  ],
  [conversion("mixed", 5, { metrics: Array(24).fill(100) })],
);
const mixedAds = paidAdReport(mixed, []);
assert.equal(mixedAds.length, 2);
assert.equal(mixedAds.find((row) => row.ad === null).revenue_server_side_mt, 0);
assert.equal(
  mixedAds.find((row) => row.ad === "ad:1").revenue_server_side_mt,
  20,
);
assert.ok(
  mixedAds.every((row) => row.medium === "cpc" && row.source === "google"),
);
