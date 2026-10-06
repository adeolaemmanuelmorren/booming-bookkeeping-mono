import assert from "node:assert/strict";
import test from "node:test";

import {
  normalizeStripeBatch,
  normalizeStripeEvent,
} from "../../worker/conversions/stripe.mjs";

function charge(overrides = {}) {
  return {
    id: "ch_shared",
    object: "charge",
    created: 1_767_225_600,
    paid: true,
    status: "succeeded",
    captured: true,
    amount: 4_997,
    amount_captured: 4_997,
    amount_refunded: 0,
    refunded: false,
    currency: "usd",
    customer: "cus_123",
    payment_intent: "pi_123",
    invoice: "in_123",
    payment_method: "pm_123",
    balance_transaction: "txn_123",
    billing_details: {
      email: "  Buyer@Example.COM ",
      name: "Ada Buyer",
      phone: "+15555550123",
      address: { country: "US" },
    },
    receipt_email: "receipt@example.com",
    metadata: { products: "Keyboard Rich" },
    refunds: { data: [] },
    livemode: true,
    ...overrides,
  };
}

function stripeEvent(overrides = {}) {
  const object = overrides.object ?? charge();

  return {
    id: overrides.id ?? "evt_shared",
    object: "event",
    type: overrides.type ?? "charge.succeeded",
    created: overrides.created ?? 1_767_225_660,
    data: { object },
  };
}

test("normalizes a successful charge and preserves refund and raw evidence", () => {
  const canonicalCharge = charge({
    amount_refunded: 1_200,
    refunded: false,
    refunds: { data: [{ id: "re_2" }, { id: "re_1" }] },
  });
  const event = stripeEvent({ object: canonicalCharge });
  const related = {
    customer: { id: "cus_123", email: "customer@example.com" },
    paymentIntent: { id: "pi_123", receipt_email: "intent@example.com" },
  };

  const fact = normalizeStripeEvent({
    account: "main",
    observationSequence: 101,
    event,
    canonicalCharge,
    related,
    receivedAt: "2026-01-01T00:02:00.000Z",
  });

  assert.equal(fact.source_fact_id, "stripe:main:charge:ch_shared");
  assert.equal(
    fact.source_version_id,
    "stripe:main:charge:ch_shared:event:evt_shared",
  );
  assert.equal(fact.provider_event_key, "stripe:main:event:evt_shared");
  assert.equal(fact.occurred_at, "2026-01-01T00:00:00.000Z");
  assert.equal(fact.source_version_at, "2026-01-01T00:02:00.000Z");
  assert.equal(fact.provider_event_created_at, "2026-01-01T00:01:00.000Z");
  assert.equal(fact.email, "buyer@example.com");
  assert.equal(fact.amount_minor, 4_997);
  assert.equal(fact.amount_refunded_minor, 1_200);
  assert.equal(fact.net_amount_minor, 3_797);
  assert.equal(fact.has_refund, true);
  assert.equal(fact.is_fully_refunded, false);
  assert.deepEqual(fact.refund_ids, ["re_1", "re_2"]);
  assert.deepEqual(fact.raw_evidence.stripe_event, event);
  assert.deepEqual(fact.raw_evidence.canonical_charge, canonicalCharge);
  assert.deepEqual(fact.raw_evidence.related, related);
});

test("main Stripe keeps the Dataform metadata phone fallback separate from Kajabi", () => {
  const canonicalCharge = charge({ billing_details: {}, metadata: { phone: '4152221234' } });
  const input = { observationSequence: 1, event: stripeEvent(), canonicalCharge, receivedAt: '2026-01-01T00:02:00Z' };
  assert.equal(normalizeStripeEvent({ ...input, account: 'main' }).phone, '4152221234');
  assert.equal(normalizeStripeEvent({ ...input, account: 'kajabi' }).phone, null);
});

test("a refund replaces the same charge fact with integer net amount", () => {
  const succeeded = normalizeStripeEvent({
    account: "main",
    observationSequence: 102,
    event: stripeEvent(),
    canonicalCharge: charge(),
    receivedAt: "2026-01-01T00:01:01.000Z",
  });
  const refundedCharge = charge({
    amount_refunded: 4_997,
    refunded: true,
    refunds: { data: [{ id: "re_full" }] },
  });
  const refunded = normalizeStripeEvent({
    account: "main",
    observationSequence: 103,
    event: stripeEvent({
      id: "evt_refunded",
      type: "charge.refunded",
      created: 1_767_229_200,
      object: refundedCharge,
    }),
    canonicalCharge: refundedCharge,
    receivedAt: "2026-01-01T01:00:01.000Z",
  });

  assert.equal(refunded.source_fact_id, succeeded.source_fact_id);
  assert.notEqual(refunded.source_version_id, succeeded.source_version_id);
  assert.equal(refunded.amount_refunded_minor, 4_997);
  assert.equal(refunded.net_amount_minor, 0);
  assert.equal(refunded.is_fully_refunded, true);
});

test("deduplicates retries without colliding the two Stripe accounts", () => {
  const event = stripeEvent();
  const facts = normalizeStripeBatch([
    {
      account: "main",
      observationSequence: 200,
      event,
      canonicalCharge: event.data.object,
      receivedAt: "2026-01-01T00:03:00.000Z",
    },
    {
      account: "main",
      observationSequence: 200,
      event,
      canonicalCharge: event.data.object,
      receivedAt: "2026-01-01T00:02:00.000Z",
    },
    {
      account: "kajabi",
      observationSequence: 17,
      event,
      canonicalCharge: event.data.object,
      receivedAt: "2026-01-01T00:04:00.000Z",
    },
  ]);

  assert.equal(facts.length, 2);
  assert.deepEqual(
    facts.map((fact) => fact.source_fact_id),
    ["stripe:kajabi:charge:ch_shared", "stripe:main:charge:ch_shared"],
  );
  assert.equal(
    facts.find((fact) => fact.source_account === "main").received_at,
    "2026-01-01T00:02:00.000Z",
  );
});

test("keeps provider amounts as integers without assuming a currency exponent", () => {
  const fact = normalizeStripeEvent({
    account: "kajabi",
    observationSequence: 18,
    event: stripeEvent({ object: charge({ currency: "jpy", amount: 5_000 }) }),
    canonicalCharge: charge({ currency: "jpy", amount: 5_000 }),
    receivedAt: "2026-01-01T00:02:00.000Z",
  });

  assert.equal(fact.currency, "JPY");
  assert.equal(fact.amount_minor, 5_000);
  assert.equal(fact.source_adapter, "stripe_kajabi");
  assert.equal("currency_exponent" in fact, false);
});

test("rejects a charge that is not a successful payment", () => {
  const failed = stripeEvent({
    type: "charge.failed",
    object: charge({ paid: false, status: "failed" }),
  });

  assert.throws(
    () =>
      normalizeStripeEvent({
        account: "main",
        observationSequence: 201,
        event: failed,
        canonicalCharge: failed.data.object,
        receivedAt: "2026-01-01T00:02:00.000Z",
      }),
    /only paid, succeeded Stripe charges/,
  );
});
