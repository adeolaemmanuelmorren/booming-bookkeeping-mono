import test from "node:test";
import assert from "node:assert/strict";
import { checkSources } from "../src/source-check.js";

function bindings() {
  return {
    STRIPE_SOURCE: { read: async (account, path) => ({ status: 200,
      body: path === "/account" ? { id: account } : { data: [{ email: "private@example.com" }] } }) },
    ACTIVECAMPAIGN_SOURCE: { read: async (path) => ({ status: 200,
      body: { [path.slice(1)]: [{ email: "private@example.com" }] } }) },
  };
}
test("verifies both accounts and ActiveCampaign without returning customer records", async () => {
  const result = await checkSources(bindings());
  assert.equal(result.ok, true);
  assert.ok(!JSON.stringify(result).includes("private@example.com"));
});
test("rejects duplicated Stripe credentials and failed provider access", async () => {
  const env = bindings();
  env.STRIPE_SOURCE.read = async () => ({ status: 200, body: { id: "same", data: [] } });
  assert.equal((await checkSources(env)).ok, false);
  env.ACTIVECAMPAIGN_SOURCE.read = async () => ({ status: 401, body: {} });
  assert.equal((await checkSources(env)).activecampaign.status, 401);
});
test("does not return errors that might contain secrets or provider payloads", async () => {
  const env = bindings();
  env.STRIPE_SOURCE.read = async () => { throw new Error("secret-value"); };
  assert.ok(!JSON.stringify(await checkSources(env)).includes("secret-value"));
});
