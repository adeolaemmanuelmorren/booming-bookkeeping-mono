import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { handler, validate } from "../src/app.js";

const record = { stream: "contacts", id: "12", payload: { id: "12", contactTags: [] } };
const batch = { account: "activecampaign", workflowId: "fixture", batchId: "fixture:contacts:0", observedAt: new Date().toISOString(), records: [record] };

test("only authenticated fixed operations can call the warehouse", async () => {
  let calls = 0;
  const token = "x".repeat(64);
  const server = createServer(handler({ token, warehouse: async () => { calls++; return { ok: true }; } }));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const url = `http://127.0.0.1:${server.address().port}`;
  try {
    assert.equal((await fetch(url + "/records", { method: "POST", body: JSON.stringify(batch) })).status, 401);
    const headers = { Authorization: `Bearer ${token}` };
    assert.equal((await fetch(url + "/records", { method: "POST", headers, body: JSON.stringify(batch) })).status, 200);
    assert.equal((await fetch(url + "/sql", { method: "POST", headers, body: "{}" })).status, 404);
    assert.equal(calls, 1);
  } finally {
    server.close();
  }
});

test("records require complete tag arrays owned by the same contact", () => {
  assert.equal(validate("/records", batch), true);
  assert.equal(validate("/records", { ...batch, account: "stripe" }), false);
  assert.equal(validate("/records", { ...batch, records: [{ ...record, payload: { id: "12" } }] }), false);
  assert.equal(validate("/records", { ...batch, records: [{ ...record, payload: { id: "12", contactTags: [{ id: "1", tag: "2", contact: "99" }] } }] }), false);
  assert.equal(validate("/complete", { account: "activecampaign", workflowId: "fixture", start: 1, end: 2, catalogObservedAt: new Date().toISOString(), contactCount: 0, tagCount: 0 }), true);
});
