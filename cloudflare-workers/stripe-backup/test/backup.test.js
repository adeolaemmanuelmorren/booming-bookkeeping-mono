import test from "node:test";
import assert from "node:assert/strict";
import { hydrate, list, eventReference } from "../src/stripe.js";
import { windowBounds } from "../src/window.js";


test("recovery windows overlap and never skip forward over unfinished history", () => {
  const start = "2026-11-16T00:00:00Z";
  const now = Date.parse("2026-11-18T03:00:00Z") / 1000;
  const first = windowBounds(start, null, now);
  assert.equal(first.end - first.start, 6 * 3600);
  const next = windowBounds(start, new Date(first.end * 1000).toISOString(), now);
  assert.equal(next.start, first.end - 600);
  assert.throws(() => windowBounds("2026-09-01T00:00:00Z", null, now), /retention/);
});

test("Stripe pagination follows every page and rejects a stuck cursor", async () => {
  const seen = [];
  const source = { read: async (account, path, parameters) => {
    seen.push(parameters.starting_after);
    return { status: 200, body: parameters.starting_after
      ? { data: [{ id: "li_2" }], has_more: false }
      : { data: [{ id: "li_1" }], has_more: true } };
  } };
  assert.equal((await list(source, "stripe", "/invoices/in_1/lines")).length, 2);
  assert.deepEqual(seen, [undefined, "li_1"]);
  source.read = async () => ({ status: 200, body: { data: [], has_more: true } });
  await assert.rejects(list(source, "stripe", "/charges"), /no progress/);
});

test("invoice purchases hydrate complete lines, preserve IDs and strip payment secrets", async () => {
  const objects = {
    "/charges/ch_1": { id: "ch_1", object: "charge", paid: true, status: "succeeded", customer: "cus_1", payment_intent: "pi_1", invoice: "in_1" },
    "/customers/cus_1": { id: "cus_1", object: "customer", email: "fixture@example.test" },
    "/payment_intents/pi_1": { id: "pi_1", latest_charge: "ch_1", client_secret: "never-store" },
    "/invoices/in_1": { id: "in_1", created: 123, customer: "cus_1", subscription: "sub_1", billing_reason: "subscription_cycle" },
    "/invoices/in_1/lines": { data: [{ id: "il_1", price: { id: "price_1", product: "prod_1" } }], has_more: false },
    "/products/prod_1": { id: "prod_1", name: "Mentorship" },
    "/subscriptions/sub_1": { id: "sub_1", items: { data: [], has_more: false } },
  };
  const source = { read: async (account, path) => {
    assert.equal(account, "stripe_kajabi");
    assert.ok(objects[path], path);
    return { status: 200, body: objects[path] };
  } };
  const records = await hydrate(source, "stripe_kajabi", [{ stream: "charges", id: "ch_1" }, { stream: "charges", id: "ch_1" }]);
  assert.equal(records.filter(row => row.stream === "charges").length, 1);
  assert.equal(records.find(row => row.stream === "invoice_line_items").payload.invoice_id, "in_1");
  assert.equal(records.find(row => row.stream === "invoices").payload.billing_reason, "subscription_cycle");
  assert.equal(records.find(row => row.stream === "payment_intents").payload.client_secret, undefined);
  source.read = async () => ({ status: 429 });
  await assert.rejects(hydrate(source, "stripe_kajabi", [{ stream: "charges", id: "ch_1" }]), /429/);
});

test("only current charge references from relevant events are replayed", () => {
  assert.deepEqual(eventReference({ type: "charge.refunded", data: { object: { object: "charge", id: "ch_old" } } }), { stream: "charges", id: "ch_old" });
  assert.equal(eventReference({ type: "invoice.deleted", data: { object: { object: "invoice", id: "in_old" } } }), null);
});
