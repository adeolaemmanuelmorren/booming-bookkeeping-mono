import test from "node:test";
import assert from "node:assert/strict";
import { loadMetaReconciliation } from "../src/checks/meta-reconciliation.js";

test("reconciliation warns on mismatches and reports a bounded sample honestly", async () => {
  const result = await loadMetaReconciliation(async options => {
    assert.equal(options.limit, 200);
    return { rows: [{ date: "2026-10-09", daily_spend: "56982.28", hourly_spend: "56962.53" }], totalRows: "201" };
  }, "business");
  assert.equal(result.status, "warning");
  assert.equal(result.mismatchCount, 201);
  assert.equal(result.truncated, true);
});

test("reconciliation is clear when there are no mismatches and preserves query failures", async () => {
  assert.equal((await loadMetaReconciliation(async () => ({ rows: [], totalRows: "0" }), "business")).status, "ok");
  await assert.rejects(loadMetaReconciliation(async () => { throw new Error("Query unavailable"); }, "business"), /unavailable/);
});
