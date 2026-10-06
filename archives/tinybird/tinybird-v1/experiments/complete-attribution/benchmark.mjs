import assert from "node:assert/strict";
import { writeFile, mkdir } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import { attributeProfile, metricCount } from "./engine.mjs";
import { referenceProfile } from "./reference.mjs";
import { touch, conversion } from "./fixtures.mjs";
const [name, totalText, lengthText, countText, mode] = process.argv.slice(2);
const total = Number(totalText),
  length = Number(lengthText),
  count = Number(countText),
  profiles = total / length;
assert.ok(Number.isInteger(profiles));
const touches = Array.from({ length }, (_, i) =>
  touch(`touch:${String(i).padStart(8, "0")}`, i, {
    source: i === 0 || i === 7 ? "direct" : i % 2 ? "google" : "meta",
    medium: i === 0 || i === 7 ? "none" : "cpc",
    campaign: `campaign:${i % 4}`,
    ad: `ad:${i % 4}`,
  }),
);
const conversions = Array.from({ length: count }, (_, i) =>
  conversion(
    `conversion:${i}`,
    mode === "distinct" ? i : (((i % 3) + 1) * length) / 3,
  ),
);
// Repeated-time whale reference needs only three representative conversions.
const referenceConversions =
  mode === "distinct" ? conversions : conversions.slice(0, 3);
const factor = mode === "distinct" ? 1 : count / 3;
const reference = referenceProfile(touches, referenceConversions);
const wanted = new Map(
  reference.map((row) => [
    row.key,
    { ...row, values: row.values.map((value) => value * factor) },
  ]),
);
const started = performance.now();
let outputRows = 0,
  maxDifference = 0;
for (let profile = 0; profile < profiles; profile++) {
  const rows = attributeProfile(touches, conversions);
  assert.equal(rows.length, wanted.size);
  outputRows += rows.length;
  for (const row of rows) {
    const target = wanted.get(row.key);
    assert.ok(target);
    assert.equal(row.sessions, target.sessions);
    for (let i = 0; i < row.values.length; i++) {
      const difference = Math.abs(row.values[i] - target.values[i]);
      maxDifference = Math.max(maxDifference, difference);
      assert.ok(difference < 0.001, `Metric ${i} differs by ${difference}`);
    }
  }
}
const result = {
  name,
  touchpoints: total,
  conversions: profiles * count,
  profiles,
  perProfileTouches: length,
  perProfileConversions: count,
  metrics: metricCount,
  variants: 6,
  outputRows,
  elapsedMs: Math.round(performance.now() - started),
  maxRssMiB: process.resourceUsage().maxRSS / 1024,
  maxDifference,
  verified: true,
};
await mkdir(new URL("./results/", import.meta.url), { recursive: true });
await writeFile(
  new URL(`./results/benchmark-${name}.json`, import.meta.url),
  JSON.stringify(result, null, 2),
);
console.log(JSON.stringify(result));
