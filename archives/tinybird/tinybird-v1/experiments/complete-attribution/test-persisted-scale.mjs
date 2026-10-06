import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { writeFile } from "node:fs/promises";
import { attributeProfile } from "./engine.mjs";
import { referenceProfile } from "./reference.mjs";
import { touch, conversion } from "./fixtures.mjs";
import { connect } from "./tinybird-store.mjs";
const scenario = process.argv[2] ?? `scale:${crypto.randomUUID()}`;
await writeFile(
  new URL("results/persisted-scale-attempt.json", import.meta.url),
  JSON.stringify({ scenario, verified: false }),
);
const store = await connect(scenario),
  profiles = new Map(),
  expected = new Map();
const started = performance.now();
for (let p = 0; p < 1000; p++) {
  const touches = Array.from({ length: 100 }, (_, i) =>
    touch(`${p}:t:${i}`, i, {
      ad: `ad:${p % 100}:${i % 5}`,
      sessionId: `${p}:s:${i}`,
    }),
  );
  const conversions = Array.from({ length: 150 }, (_, i) =>
    conversion(`${p}:c:${i}`, i, {
      metrics: Array.from({ length: 24 }, (_, m) => (m + 1) * (p + 1)),
    }),
  );
  profiles.set(`profile:${p}`, attributeProfile(touches, conversions));
  // Full quadratic reference at this scale, including distinct conversion times.
  for (const row of referenceProfile(touches, conversions)) {
    if (!expected.has(row.key))
      expected.set(row.key, {
        key: row.key,
        values: Array(144).fill(0),
        sessions: 0,
      });
    const target = expected.get(row.key);
    target.sessions += row.sessions;
    row.values.forEach((v, i) => (target.values[i] += v));
  }
}
const prepareMs = performance.now() - started;
const prepared = store.prepare(1, profiles);
const stageStart = performance.now();
if (!process.argv[2]) {
  await store.stage(prepared);
  await store.commit(prepared);
}
for (let attempt = 0; attempt < 40; attempt++) {
  try {
    await store.report(undefined, undefined, 1);
    break;
  } catch (error) {
    if (!String(error).includes("visible") || attempt === 39) throw error;
  }
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
const publicationMs = performance.now() - stageStart;
const runs = [];
for (let run = 0; run < 3; run++) {
  const before = performance.now();
  const result = await store.report(undefined, undefined, 1);
  assert.equal(result.rows.length, expected.size);
  let maxDifference = 0;
  for (const row of result.rows) {
    const wanted = expected.get(row.key);
    assert.ok(wanted);
    assert.equal(row.sessions, wanted.sessions);
    row.values.forEach((v, i) => {
      const diff = Math.abs(v - wanted.values[i]);
      maxDifference = Math.max(diff, maxDifference);
      assert.ok(diff < 0.001);
    });
  }
  runs.push({
    wallMs: performance.now() - before,
    statistics: result.statistics,
    maxDifference,
  });
}
const evidence = {
  verified: true,
  scenario,
  profiles: profiles.size,
  touches: 100000,
  conversions: 150000,
  distinctConversionTimes: true,
  chunks: prepared.chunks.length,
  contributionRows: [...profiles.values()].reduce((n, r) => n + r.length, 0),
  reportRows: expected.size,
  prepareIncludingReferenceMs: prepareMs,
  publicationMs: process.argv[2] ? null : publicationMs,
  runs,
  reader: "single-commit-scan",
};
await writeFile(
  new URL("results/persisted-scale.json", import.meta.url),
  JSON.stringify(evidence, null, 2),
);
console.log(JSON.stringify(evidence));
