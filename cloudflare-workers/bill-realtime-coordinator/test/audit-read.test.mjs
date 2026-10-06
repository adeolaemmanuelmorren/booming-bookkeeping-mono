import test from "node:test";
import assert from "node:assert/strict";
import { readAuditSource } from "../src/audit-read.js";

test("audit reads keep Stripe accounts separate and only invoke the read gateway", async () => {
  const calls = [];
  const env = { STRIPE_SOURCE: { read: async (...args) => { calls.push(args); return { status: 200, body: { data: [] } }; } } };
  for (const account of ["main", "kajabi"]) await readAuditSource(env, {
    source: "stripe", account, path: "/charges", parameters: { limit: "100", "created[gte]": "1" },
  });
  assert.deepEqual(calls.map(call => call[0]), ["stripe", "stripe_kajabi"]);
});
test("audit cannot call unrelated paths, arbitrary parameters or oversized pages", async () => {
  const input = { source: "stripe", account: "main", path: "/charges" };
  for (const change of [{ path: "/customers" }, { path: "https://other.example/charges" },
    { account: "unknown" }, { parameters: { api_key: "secret" } }, { parameters: { limit: "101" } }]) {
    await assert.rejects(readAuditSource({}, { ...input, ...change }));
  }
});
test("provider error bodies are not returned by the audit endpoint", async () => {
  const result = await readAuditSource({ ACTIVECAMPAIGN_SOURCE: {
    read: async () => ({ status: 401, body: { error: "sensitive error" } }),
  } }, { source: "activecampaign", account: "default", path: "/contacts/1/contactTags" });
  assert.deepEqual(result, { status: 401, body: null });
});

test("registration audits allow only contact-tag sideloading", async () => {
  const input = { source: "activecampaign", account: "default", path: "/contacts" };
  const env = { ACTIVECAMPAIGN_SOURCE: { read: async (path, parameters) => ({ status: 200, body: { path, parameters } }) } };
  const result = await readAuditSource(env, { ...input, parameters: { tagid: "696", include: "contactTags", limit: "100" } });
  assert.equal(result.body.parameters.include, "contactTags");
  await assert.rejects(readAuditSource(env, { ...input, parameters: { include: "notes" } }));
});
