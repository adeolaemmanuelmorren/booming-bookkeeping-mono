import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { CurrentInputs } from "./current-inputs.mjs";
import { referenceProfile } from "./reference.mjs";
import { conversionMetrics, metricCount } from "./engine.mjs";
import { touch, conversion, epoch } from "./fixtures.mjs";
import { connect, hash } from "./tinybird-store.mjs";
import { mergeAdPerformance } from "./report.mjs";
const directory = new URL("./results/", import.meta.url);
await mkdir(directory, { recursive: true });
const scenario = `complete-attribution:${crypto.randomUUID()}`;
const store = await connect(scenario),
  inputs = new CurrentInputs();
const evidence = {
  scenario,
  branchId: store.branchId,
  checks: [],
  reader: "single-commit-scan",
  liveAdApiTested: false,
};
const save = () =>
  writeFile(
    new URL("persisted.json", directory),
    JSON.stringify(evidence, null, 2),
  );
let version = 0;
function expected(start = "", end = "9999") {
  const groups = new Map();
  for (const profile of inputs.byProfile.keys()) {
    const source = inputs.inputs(profile);
    for (const row of referenceProfile(source.touches, source.conversions)) {
      const date = JSON.parse(row.key)[0];
      if (date < start || date >= end) continue;
      if (!groups.has(row.key))
        groups.set(row.key, {
          key: row.key,
          values: Array(metricCount * 6).fill(0),
          sessions: 0,
          sessionIds: new Set(),
        });
      const target = groups.get(row.key);
      row.sessionIds.forEach((id) => target.sessionIds.add(id));
      target.sessions = target.sessionIds.size;
      row.values.forEach((value, i) => (target.values[i] += value));
    }
  }
  return [...groups.values()];
}
function compare(actual, wanted) {
  assert.equal(actual.length, wanted.length);
  const reference = new Map(wanted.map((row) => [row.key, row]));
  for (const row of actual) {
    const target = reference.get(row.key);
    assert.ok(target, `Unexpected group ${row.key}`);
    assert.equal(row.sessions, target.sessions);
    row.values.forEach((value, i) =>
      assert.ok(
        Math.abs(value - target.values[i]) < 1e-6,
        `Metric ${i}: ${value} != ${target.values[i]}`,
      ),
    );
  }
}
async function read(start, end) {
  for (let attempt = 0; attempt < 40; attempt++) {
    try {
      return await store.report(start, end, version);
    } catch (error) {
      if (
        !String(error).includes("visible") &&
        !String(error).includes("Incomplete")
      )
        throw error;
      if (attempt === 39) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
    }
  }
}
async function publish(name, affected, previous, retry = false) {
  const prepared = store.prepare(++version, inputs.rebuild(affected));
  await store.stage(prepared);
  const unpublished = await store.report(undefined, undefined, version - 1);
  compare(unpublished.rows, previous);
  if (retry) await store.stage(prepared);
  await store.commit(prepared);
  if (retry) await store.commit(prepared);
  const start = performance.now(),
    report = await read();
  compare(report.rows, expected());
  evidence.checks.push({
    name,
    version,
    affectedProfiles: affected.size,
    reportRows: report.rows.length,
    wallMs: Math.round(performance.now() - start),
    statistics: report.statistics,
    passed: true,
  });
  await save();
  console.log(JSON.stringify(evidence.checks.at(-1)));
}
const change = (kind, fact, revision = 1, deleted = false) => ({
  kind,
  fact,
  version: revision,
  deleted,
});
inputs.applyIdentity(
  new Map([
    ["browser:1", "p1"],
    ["customer:1", "p1"],
    ["browser:2", "p2"],
    ["customer:2", "p2"],
  ]),
);
const t1 = touch("t1", 1, { source: "direct", medium: "none" }),
  t2 = touch("t2", 2),
  t3 = touch("t3", 3, { source: "direct", medium: "none" });
const t4 = touch("t4", 4, {
  identityKey: "browser:2",
  sessionTime: null,
  source: "meta",
  medium: "paid_social",
  account: "account:2",
});
const c1 = conversion("c1", 24),
  c2 = conversion("c2", 48, { identityKey: "customer:2" }),
  c3 = conversion("c3", 0, { identityKey: "unknown" });
await publish(
  "initial-and-lost-ack-retries",
  inputs.applyFacts([
    change("touch", t1),
    change("touch", t2),
    change("touch", t3),
    change("touch", t4),
    change("conversion", c1),
    change("conversion", c2),
    change("conversion", c3),
  ]),
  [],
  true,
);
const initial = expected();
assert.equal(inputs.applyFacts([change("touch", structuredClone(t1))]).size, 0);
assert.throws(
  () => inputs.applyFacts([change("touch", { ...t1, source: "other" })]),
  /Conflicting/,
);
compare((await read()).rows, initial);
evidence.checks.push({
  name: "duplicate-and-conflicting-source-version",
  passed: true,
});

let previous = expected();
const refunded = {
  ...c1,
  metrics: c1.metrics.map((value, i) =>
    conversionMetrics[i].includes("revenue") ? value * 0.25 : value,
  ),
};
await publish(
  "refund",
  inputs.applyFacts([change("conversion", refunded, 2)]),
  previous,
);
assert.equal(inputs.applyFacts([change("conversion", c1)]).size, 0);
previous = expected();
await publish(
  "late-earlier-touch",
  inputs.applyFacts([
    change(
      "touch",
      touch("late", 0.5, { source: "meta", medium: "paid_social" }),
    ),
  ]),
  previous,
);
previous = expected();
await publish(
  "touch-correction",
  inputs.applyFacts([
    change(
      "touch",
      { ...t2, sessionTime: epoch - 3600000, ad: "corrected-ad" },
      2,
    ),
  ]),
  previous,
);
previous = expected();
await publish(
  "touch-deletion",
  inputs.applyFacts([
    change(
      "touch",
      { ...t2, sessionTime: epoch - 3600000, ad: "corrected-ad" },
      3,
      true,
    ),
  ]),
  previous,
);
previous = expected();
await publish(
  "atomic-identity-merge",
  inputs.applyIdentity(
    new Map([
      ["browser:2", "p1"],
      ["customer:2", "p1"],
    ]),
  ),
  previous,
);
previous = expected();
await publish(
  "atomic-identity-split",
  inputs.applyIdentity(
    new Map([
      ["browser:2", "p2"],
      ["customer:2", "p2"],
    ]),
  ),
  previous,
);
previous = expected();
await publish(
  "late-identity-link",
  inputs.applyIdentity(new Map([["unknown", "p2"]])),
  previous,
);
previous = expected();
await publish(
  "shared-session-across-profiles",
  inputs.applyFacts([
    change(
      "touch",
      touch("shared1", 1, {
        sessionId: "shared-session",
        identityKey: "browser:1",
      }),
    ),
    change(
      "touch",
      touch("shared2", 1, {
        sessionId: "shared-session",
        identityKey: "browser:2",
      }),
    ),
  ]),
  previous,
);
previous = expected();
await publish(
  "conversion-deletion",
  inputs.applyFacts([change("conversion", c2, 2, true)]),
  previous,
);
previous = expected();
await publish(
  "identity-link-removal",
  inputs.applyIdentity(new Map([["unknown", null]])),
  previous,
);

compare(
  (await read("2026-09-01", "2026-09-02")).rows,
  expected("2026-09-01", "2026-09-02"),
);
compare((await read("2025-01-01", "2025-01-02")).rows, []);
evidence.checks.push({ name: "click-window-and-empty-window", passed: true });

// A missing committed chunk must fail closed even when outside the requested window.
const payload = { groups: [], metrics: [], sessions: [], session_ids: [] };
const missing = { scenario, chunk_id: hash(payload), ...payload };
await store.appendCommit({
  scenario,
  version: ++version,
  transaction_id: `fault:${version}`,
  profiles: ["fault-profile"],
  chunk_ids: [[missing.chunk_id]],
});
let rejected = false;
for (let attempt = 0; attempt < 40; attempt++) {
  try {
    await store.report("2025-01-01", "2025-01-02", version);
  } catch (error) {
    if (String(error).includes("Incomplete")) {
      rejected = true;
      break;
    }
  }
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
assert.ok(
  rejected,
  "Missing committed chunks must not produce a partial report",
);
await store.appendChunks([missing]);
compare((await read()).rows, expected());
evidence.checks.push({
  name: "missing-chunk-fails-closed-and-recovers",
  passed: true,
});

const live = await read();
const ads = [
  {
    date: "2026-09-01",
    source: "meta",
    account: "account:1",
    campaign: "campaign:1",
    adset: "adset:1",
    ad: "ad:1",
    currency: "USD",
    spend: 100,
    impressions: 1000,
    clicks: 30,
  },
  {
    date: "2026-09-01",
    source: "meta",
    account: "other-account",
    campaign: "campaign:1",
    adset: "adset:1",
    ad: "ad:1",
    currency: "USD",
    spend: 200,
    impressions: 2000,
    clicks: 40,
  },
  {
    date: "2026-09-01",
    source: "meta",
    account: "account:1",
    campaign: "campaign:1",
    adset: "adset:1",
    ad: "ad:1",
    currency: "EUR",
    spend: 50,
    impressions: 300,
    clicks: 5,
  },
];
const report = mergeAdPerformance(live.rows, ads);
assert.equal(
  report.reduce((sum, row) => sum + row.spend, 0),
  350,
);
assert.ok(
  report
    .find((row) => row.account === "other-account")
    .metrics.every((value) => value === 0),
);
assert.ok(
  report
    .find((row) => row.currency === "EUR")
    .metrics.every((value) => value === 0),
);
assert.ok(report.some((row) => row.source === "offline" && row.spend === 0));
assert.throws(
  () => mergeAdPerformance(live.rows, [ads[0], ads[0]]),
  /Duplicate/,
);
for (let i = 0; i < metricCount * 6; i++)
  assert.ok(
    Math.abs(
      report.reduce((sum, row) => sum + row.metrics[i], 0) -
        live.rows.reduce((sum, row) => sum + row.values[i], 0),
    ) < 1e-6,
  );
evidence.checks.push({
  name: "ad-grain-full-outer-merge-and-metric-conservation",
  passed: true,
});
// Simulate concurrent writers passing the precheck and committing different transactions.
await store.appendCommit({
  scenario,
  version,
  transaction_id: "conflicting-writer",
  profiles: ["fault-profile"],
  chunk_ids: [[]],
});
let conflictRejected = false;
for (let attempt = 0; attempt < 40; attempt++) {
  try {
    await store.report();
  } catch (error) {
    if (String(error).includes("Conflicting committed")) {
      conflictRejected = true;
      break;
    }
    throw error;
  }
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
assert.ok(
  conflictRejected,
  "Conflicting publication versions must fail closed",
);
evidence.checks.push({
  name: "conflicting-concurrent-version-rejected",
  passed: true,
});
evidence.verified = true;
await writeFile(new URL("report.sql", directory), store.reportSql());
await save();
console.log(
  JSON.stringify({
    verified: true,
    scenario,
    checks: evidence.checks.length,
    liveAdApiTested: false,
  }),
);
