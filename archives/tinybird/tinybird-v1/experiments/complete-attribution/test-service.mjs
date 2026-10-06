import assert from "node:assert/strict";
import { attributeProfile } from "./engine.mjs";
import { touch, conversion } from "./fixtures.mjs";
import { requestReport } from "./report-service.mjs";
const rows = attributeProfile([touch("t", 1)], [conversion("c", 2)]);
const store = { report: async () => ({ version: 7, rows }) };
const complete = {
  start: "2026-09-01",
  end: "2026-09-02",
  timeZone: "America/Los_Angeles",
  asOf: "2026-09-02T00:00:00Z",
  complete: true,
  rows: [
    {
      date: "2026-09-01",
      source: "google",
      account: "account:1",
      campaign: "campaign:1",
      adset: "adset:1",
      ad: "ad:1",
      currency: "USD",
      spend: 100,
      clicks: 2,
      impressions: 10,
    },
  ],
};
const request = (response) =>
  requestReport({
    store,
    providers: [{ fetch: async () => response }],
    start: complete.start,
    end: complete.end,
  });
const report = await request(complete);
assert.equal(report.attributionVersion, 7);
assert.equal(report.paidAds.length, 1);
assert.equal(report.paidAds[0].spend, 100);
assert.equal(report.paidAds[0].roas_all_payments_mt, 8);
assert.equal(
  Object.keys(report.attribution[0]).filter(
    (k) => k.endsWith("_mt") || k.endsWith("_ft") || k.endsWith("_lt"),
  ).length,
  144,
);
await assert.rejects(request({ ...complete, complete: false }), /incomplete/);
await assert.rejects(
  request({ ...complete, timeZone: "UTC" }),
  /reporting window/,
);
await assert.rejects(
  request({ ...complete, rows: [...complete.rows, ...complete.rows] }),
  /Duplicate/,
);
await assert.rejects(
  requestReport({
    store,
    providers: [
      {
        fetch: async () => {
          throw new Error("Provider failed");
        },
      },
    ],
    start: complete.start,
    end: complete.end,
  }),
  /Provider failed/,
);
console.log(
  "Complete ad-response contract, 144-column detail, paid report, ROAS, and provider failures pass",
);
