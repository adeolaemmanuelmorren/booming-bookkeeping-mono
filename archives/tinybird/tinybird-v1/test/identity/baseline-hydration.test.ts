import assert from "node:assert/strict";
import test from "node:test";
import { IdentityStorage, type IdentityRecord } from "../../worker/identity/storage.ts";

function record(stateKey: string, payload: object): IdentityRecord {
  return { tenant_id: "boom", state_kind: "fact", state_key: stateKey, lookup_key: "scope",
    sub_key: "", batch_version: "1", batch_id: "baseline", is_deleted: 0, payload_json: JSON.stringify(payload) };
}

test("authenticated baseline hydration fills a partial version 1 replica read", async () => {
  const first = record("fact:a", { factKey: "a" });
  const second = record("fact:b", { factKey: "b" });
  const client = { query: async () => [{ state_key: first.state_key, batch_version: "1", payload_json: first.payload_json, is_deleted: 0 }] };
  const storage = new IdentityStorage(client as never, { baselineReader: { readRecords: async () => [first, second] } });
  const facts = await storage.reader("boom", 1).facts(["scope"]);
  assert.deepEqual(facts, [{ factKey: "a" }, { factKey: "b" }]);
});

test("visible version 1 content cannot conflict with the authenticated baseline", async () => {
  const baseline = record("fact:a", { factKey: "a" });
  const client = { query: async () => [{ state_key: baseline.state_key, batch_version: "1", payload_json: JSON.stringify({ factKey: "wrong" }), is_deleted: 0 }] };
  const storage = new IdentityStorage(client as never, { baselineReader: { readRecords: async () => [baseline] } });
  await assert.rejects(() => storage.reader("boom", 1).facts(["scope"]), /conflicts/);
});

test("a durable version 2 tombstone wins when Tinybird current-state visibility lags", async () => {
  const baseline = record("fact:a", { factKey: "a", factDeleted: false });
  const tombstone = { ...record("fact:a", { factKey: "a", factDeleted: true }), batch_version: "2", batch_id: "live-2", is_deleted: 1 };
  const client = { query: async () => { throw new Error("lagging Tinybird current read must not be used"); } };
  const storage = new IdentityStorage(client as never, {
    baselineReader: { readRecords: async () => [baseline] },
    overlayReader: { readRecords: async () => [tombstone] },
  });
  const facts = await storage.reader("boom", 2).facts(["scope"]);
  assert.deepEqual(facts, [{ factKey: "a", factDeleted: true }]);
});
