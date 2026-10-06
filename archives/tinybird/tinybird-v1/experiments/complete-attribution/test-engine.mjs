import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import {
  attributeProfile,
  reportDate,
  conversionMetrics,
  metricCount,
} from "./engine.mjs";
import { referenceProfile } from "./reference.mjs";
import { scenario, touch, conversion, epoch } from "./fixtures.mjs";
const directory = new URL("./results/", import.meta.url);
await mkdir(directory, { recursive: true });
export function compare(actual, expected, tolerance = 1e-6) {
  const reference = new Map(expected.map((row) => [row.key, row]));
  assert.equal(actual.length, reference.size);
  for (const row of actual) {
    const wanted = reference.get(row.key);
    assert.ok(wanted, `Unexpected group ${row.key}`);
    assert.equal(row.sessions, wanted.sessions);
    for (let i = 0; i < row.values.length; i++)
      assert.ok(
        Math.abs(row.values[i] - wanted.values[i]) <= tolerance,
        `${row.key} metric ${i}: ${row.values[i]} != ${wanted.values[i]}`,
      );
  }
}
assert.equal(reportDate(Date.parse("2026-09-01T06:59:59Z")), "2026-08-31");
assert.equal(reportDate(Date.parse("2026-09-01T07:00:00Z")), "2026-09-01");
const fixed = [
  { touches: [], conversions: [conversion("offline", 10)] },
  {
    touches: [
      touch("direct", 1, { source: "direct", medium: "none" }),
      touch("paid", 2),
      touch("return", 3, { source: "direct", medium: "none" }),
    ],
    conversions: [conversion("a", 0), conversion("b", 2), conversion("c", 5)],
  },
  {
    touches: [
      touch("future", 20, { sessionTime: epoch }),
      touch("eligible", 10, { sessionTime: epoch + 5 * 3600000 }),
    ],
    conversions: [conversion("c", 12)],
  },
  {
    touches: [
      touch("a", 1, { sessionTime: null }),
      touch("b", 2, { sessionTime: null }),
      touch("c", 3),
    ],
    conversions: [
      conversion("early", 2),
      conversion("late", 48),
      conversion("next", 72, { currency: "EUR" }),
    ],
  },
];
for (const input of fixed)
  compare(
    attributeProfile(input.touches, input.conversions),
    referenceProfile(input.touches, input.conversions),
  );
for (let seed = 0; seed < 1000; seed++) {
  const input = scenario(seed);
  try {
    compare(
      attributeProfile(input.touches, input.conversions),
      referenceProfile(input.touches, input.conversions),
    );
  } catch (error) {
    await writeFile(
      new URL("failure.json", directory),
      JSON.stringify({ seed, input }, null, 2),
    );
    throw error;
  }
}
const result = {
  verified: true,
  randomizedProfiles: 1000,
  fixedFixtures: fixed.length,
  metrics: conversionMetrics,
  modelVariants: 6,
  reportValues: metricCount * 6,
  tolerance: 1e-6,
};
await writeFile(
  new URL("engine.json", directory),
  JSON.stringify(result, null, 2),
);
console.log(JSON.stringify(result));
