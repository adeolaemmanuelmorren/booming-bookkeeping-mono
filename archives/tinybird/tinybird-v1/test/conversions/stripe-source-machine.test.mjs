import assert from "node:assert/strict";
import test from "node:test";

import {
  backfillStripeCharges,
  pollStripeEvents,
  processStripeInbox,
} from "../../worker/conversions/stripe-source-machine.mjs";
import {
  createSliceBudget,
  drainPublicationOutbox,
} from "../../worker/conversions/source-machine.mjs";
import { FakeClock, MemoryStore } from "../support/memory-store.mjs";

function charge(overrides = {}) {
  return {
    id: "ch_1",
    object: "charge",
    created: 1_780_272_000,
    paid: true,
    status: "succeeded",
    amount: 5_000,
    amount_refunded: 0,
    currency: "usd",
    customer: "cus_1",
    payment_intent: "pi_1",
    refunds: { data: [] },
    billing_details: {},
    metadata: {},
    ...overrides,
  };
}

function event({ id, type, object }) {
  return {
    id,
    type,
    created: 1_780_272_060,
    data: { object },
  };
}

test("Stripe event polling resumes a fixed cursor window and stores immutable event IDs", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  const requests = [];
  const pages = [
    {
      items: [event({ id: "evt_charge", type: "charge.updated", object: charge() })],
      hasMore: true,
      nextCursor: "evt_charge",
    },
    {
      items: [
        event({
          id: "evt_refund",
          type: "refund.created",
          object: { id: "re_1", object: "refund", charge: "ch_1" },
        }),
      ],
      hasMore: false,
    },
  ];
  const provider = {
    read: {
      async listEvents(request) {
        requests.push(request);
        return pages.shift();
      },
    },
  };

  await pollStripeEvents({
    account: "main",
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });
  const firstCursor = await store.getCursor("stripe:main:events-cursor");

  assert.equal(firstCursor.activeWindow.startingAfter, "evt_charge");

  await pollStripeEvents({
    account: "main",
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });
  const completed = await store.getCursor("stripe:main:events-cursor");

  assert.equal(completed.activeWindow, null);
  assert.equal(requests[1].startingAfter, "evt_charge");
  assert.deepEqual(
    [...store.inbox.keys()].sort(),
    ["stripe:main:event:evt_charge", "stripe:main:event:evt_refund"],
  );

  const duplicateInserted = await store.putInboxIfAbsent(
    store.inbox.get("stripe:main:event:evt_charge"),
  );
  assert.equal(duplicateInserted, false);
});

test("Stripe charge backfill persists starting_after and ignores failed charges", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  const requests = [];
  const pages = [
    {
      items: [charge(), charge({ id: "ch_failed", paid: false, status: "failed" })],
      hasMore: true,
      nextCursor: "ch_failed",
    },
    {
      items: [charge({ id: "ch_older", created: 1_700_000_000 })],
      hasMore: false,
    },
  ];
  const provider = {
    read: {
      async listCharges(request) {
        requests.push(request);
        return pages.shift();
      },
    },
  };

  await backfillStripeCharges({
    account: "kajabi",
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });
  await backfillStripeCharges({
    account: "kajabi",
    provider,
    store,
    budget: createSliceBudget({ clock }),
    maxPages: 1,
  });

  assert.equal(requests[1].startingAfter, "ch_failed");
  assert.deepEqual(
    [...store.inbox.keys()].sort(),
    [
      "stripe:kajabi:backfill:v1:charge:ch_1",
      "stripe:kajabi:backfill:v1:charge:ch_older",
    ],
  );
  assert.equal(
    (await store.getCursor("stripe:kajabi:charges-backfill:v1")).status,
    "complete",
  );
});

test("Stripe bundle hydration resumes, assigns one sequence, and publishes complete raw records", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  const canonicalCharge = charge({
    amount_refunded: 1_000,
    refunds: { data: [{ id: "re_1" }] },
  });
  await store.putInboxIfAbsent({
    id: "stripe:main:event:evt_refund",
    source: "stripe",
    sourceAccount: "main",
    kind: "stripe_event",
    chargeId: "ch_1",
    receivedAt: "2026-06-01T00:00:00.000Z",
    immutablePayload: {
      event: event({ id: "evt_refund", type: "charge.refunded", object: canonicalCharge }),
    },
  });

  const pages = [
    {
      canonicalCharge,
      relatedRecords: { customers: [{ id: "cus_1", email: "buyer@example.com" }] },
      complete: false,
      nextCursor: { stage: "refunds", startingAfter: null },
    },
    {
      relatedRecords: {
        paymentIntents: [{ id: "pi_1", object: "payment_intent" }],
        refunds: [{ id: "re_1", object: "refund", amount: 1_000 }],
        invoiceLines: [{ id: "il_1", object: "line_item" }],
      },
      observedAt: "2026-06-01T00:00:05.000Z",
      complete: true,
      nextCursor: null,
    },
  ];
  const provider = {
    read: {
      async readChargeBundlePage() {
        return pages.shift();
      },
    },
  };

  await processStripeInbox({
    account: "main",
    provider,
    store,
    budget: createSliceBudget({ clock }),
  });
  assert.equal(store.inbox.get("stripe:main:event:evt_refund").status, "pending");

  await processStripeInbox({
    account: "main",
    provider,
    store,
    budget: createSliceBudget({ clock }),
  });
  const outboxRecord = [...store.outbox.values()][0];
  const fact = outboxRecord.contract.rows[0];

  assert.equal(outboxRecord.contract.observation_sequence, 1);
  assert.equal(fact.net_amount_minor, 4_000);
  assert.equal(fact.email, "buyer@example.com");
  assert.deepEqual(
    fact.raw_evidence.related.rawRecords.invoiceLines,
    [{ id: "il_1", object: "line_item" }],
  );

  const published = [];
  await drainPublicationOutbox({
    source: "stripe",
    sourceAccount: "main",
    store,
    publishSourceReplacement: async (contract) => published.push(contract),
    budget: createSliceBudget({ clock }),
  });

  assert.equal(published.length, 1);
  assert.equal(store.inbox.get("stripe:main:event:evt_refund").status, "processed");
  assert.equal(
    (await store.getPublishedRows("stripe:main:charge:ch_1"))[0].charge_id,
    "ch_1",
  );
});

test("a slice stops opening provider calls near its deadline", async () => {
  const store = new MemoryStore();
  const clock = new FakeClock();
  const deadlines = [];
  const provider = {
    read: {
      async listEvents(request) {
        deadlines.push(request.deadlineAtMs);
        clock.advance(44_500);
        return { items: [], hasMore: true, nextCursor: `cursor-${deadlines.length}` };
      },
    },
  };

  const budget = createSliceBudget({ clock, maxDurationMs: 45_000 });
  const pages = await pollStripeEvents({
    account: "main",
    provider,
    store,
    budget,
    maxPages: 10,
  });

  assert.equal(pages, 1);
  assert.equal(deadlines.length, 1);
  assert.equal(deadlines[0], Date.parse("2026-06-01T00:00:45.000Z"));
});
