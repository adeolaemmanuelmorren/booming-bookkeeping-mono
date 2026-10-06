import { test } from "node:test";
import assert from "node:assert/strict";
import { stripeReadRequest, activeCampaignReadRequest } from "../../cloudflare-workers/source-gateways/requests.ts";

test("Stripe gateway permits only provider reads and keeps credentials out of URLs", () => {
  const request = stripeReadRequest("test-secret", "/charges", { limit: "100", "created[gte]": "42" });
  assert.equal(request.method, "GET");
  assert.equal(new URL(request.url).origin, "https://api.stripe.com");
  assert.equal(new URL(request.url).searchParams.get("created[gte]"), "42");
  assert.equal(request.headers.get("Authorization"), "Bearer test-secret");
  assert.ok(!request.url.includes("test-secret"));
  for (const path of ["//other.example", "/../balance", "/charges/ch_1/capture", "/charges?expand[]=customer", "https://other.example"]) {
    assert.throws(() => stripeReadRequest("test-secret", path));
  }
});

test("ActiveCampaign gateway normalizes the base URL and rejects path escape", () => {
  for (const base of ["https://example.api-us1.com", "https://example.api-us1.com/api/3/"]) {
    const request = activeCampaignReadRequest(base, "test-secret", "/contacts/42/contactTags", { limit: "100" });
    assert.equal(request.method, "GET");
    assert.equal(request.url, "https://example.api-us1.com/api/3/contacts/42/contactTags?limit=100");
    assert.equal(request.headers.get("Api-Token"), "test-secret");
  }
  for (const path of ["/contacts/../users", "/users", "//other.example", "/contacts/42?test=1"]) {
    assert.throws(() => activeCampaignReadRequest("https://example.api-us1.com", "test-secret", path));
  }
  assert.throws(() => activeCampaignReadRequest("http://example.api-us1.com", "test-secret", "/contacts"));
});
