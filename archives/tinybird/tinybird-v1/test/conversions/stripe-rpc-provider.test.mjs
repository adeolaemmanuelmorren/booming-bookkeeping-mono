import assert from "node:assert/strict";
import test from "node:test";

import { ProviderReadError } from "../../worker/conversions/provider-rpc.mjs";
import { createStripeRpcProvider } from "../../worker/conversions/stripe-rpc-provider.mjs";

const clock = { now: () => Date.parse("2026-06-01T00:00:00Z") };
const deadlineAtMs = clock.now() + 30_000;

test("Stripe keeps expanded invoice discounts and coupon details", async () => {
  const provider = createStripeRpcProvider({ clock, rpcRead: async () => ({ status: 200, body: {
    id: "ch_discount", invoice: { id: "in_discount", discounts: [{ id: "di_1",
      coupon: { id: "coupon_1", name: "VIP offer" }, promotion_code: { id: "promo_1", code: "VIP" } }],
      total_discount_amounts: [{ discount: "di_1", amount: 1000 }] }, refunds: { data: [] },
  } }) });
  const page = await provider.read.readChargeBundlePage({ account: "main", chargeId: "ch_discount", deadlineAtMs });
  assert.equal(page.relatedRecords.discounts[0].amount, 1000);
  assert.equal(page.relatedRecords.discounts[0].invoice_id, "in_discount");
  assert.equal(page.relatedRecords.coupons[0].name, "VIP offer");
  assert.equal(page.relatedRecords.promotionCodes[0].code, "VIP");
});

test("Stripe list reads map both account names and preserve fixed-window cursors", async () => {
  const calls = [];
  const provider = createStripeRpcProvider({
    clock,
    rpcRead: async (account, path, parameters) => {
      calls.push({ account, path, parameters });
      return {
        status: 200,
        retryAfter: null,
        body: {
          data: [{ id: path === "/events" ? "evt_1" : "ch_1" }],
          has_more: true,
        },
      };
    },
  });

  const events = await provider.read.listEvents({
    account: "main",
    types: ["charge.succeeded", "refund.created"],
    createdGte: 100,
    createdLte: 200,
    startingAfter: "evt_0",
    limit: 100,
    deadlineAtMs,
  });
  const charges = await provider.read.listCharges({
    account: "kajabi",
    createdGte: 10,
    createdLte: 20,
    startingAfter: null,
    limit: 50,
    deadlineAtMs,
  });

  assert.deepEqual(calls, [
    {
      account: "stripe",
      path: "/events",
      parameters: {
        "created[gte]": "100",
        "created[lte]": "200",
        starting_after: "evt_0",
        limit: "100",
        "types[0]": "charge.succeeded",
        "types[1]": "refund.created",
      },
    },
    {
      account: "stripe_kajabi",
      path: "/charges",
      parameters: {
        "created[gte]": "10",
        "created[lte]": "20",
        limit: "50",
      },
    },
  ]);
  assert.equal(events.nextCursor, "evt_1");
  assert.equal(charges.nextCursor, "ch_1");
});

test("Stripe bundle hydration resumes every list and keeps raw catalog evidence", async () => {
  const calls = [];
  let chargeReads = 0;
  const charge = {
    id: "ch_1",
    object: "charge",
    customer: { id: "cus_1", object: "customer" },
    payment_intent: { id: "pi_1", object: "payment_intent" },
    invoice: {
      id: "in_1",
      object: "invoice",
      subscription: {
        id: "sub_1",
        object: "subscription",
        items: { data: [] },
      },
    },
    balance_transaction: "txn_1",
    payment_method: "pm_1",
    refunds: { data: [{ id: "re_embedded", object: "refund" }] },
  };
  const price = {
    id: "price_1",
    object: "price",
    product: { id: "prod_1", object: "product" },
  };
  const provider = createStripeRpcProvider({
    clock,
    rpcRead: async (account, path, parameters) => {
      calls.push({ account, path, parameters });
      assert.equal(account, "stripe");

      if (path === "/charges/ch_1") {
        chargeReads += 1;
        return ok(chargeReads === 1
          ? charge
          : { ...charge, metadata: { snapshot: "final" } });
      }

      if (path === "/refunds" && !parameters.starting_after) {
        return ok({
          data: [{ id: "re_2", object: "refund" }],
          has_more: true,
        });
      }

      if (path === "/refunds") {
        assert.equal(parameters.starting_after, "re_2");
        return ok({ data: [{ id: "re_1", object: "refund" }], has_more: false });
      }

      if (path === "/invoices/in_1/lines") {
        return ok({
          data: [{ id: "il_1", object: "line_item", price }],
          has_more: false,
        });
      }

      if (path === "/checkout/sessions" && parameters.subscription) {
        return ok({ data: [], has_more: false });
      }

      if (path === "/checkout/sessions") {
        assert.equal(parameters.payment_intent, "pi_1");
        return ok({
          data: [
            {
              id: "cs_1",
              object: "checkout.session",
              payment_link: { id: "plink_1", object: "payment_link" },
            },
          ],
          has_more: false,
        });
      }

      if (path === "/checkout/sessions/cs_1/line_items") {
        return ok({
          data: [{ id: "li_cs", object: "item", price }],
          has_more: false,
        });
      }

      if (path === "/payment_links/plink_1/line_items") {
        return ok({
          data: [{ id: "li_pl", object: "item", price }],
          has_more: false,
        });
      }

      throw new Error(`unexpected Stripe test path ${path}`);
    },
  });

  const records = {};
  let page = await provider.read.readChargeBundlePage({
    account: "main",
    chargeId: "ch_1",
    cursor: null,
    deadlineAtMs,
  });
  let canonicalCharge = page.canonicalCharge;

  mergeRecords(records, page.relatedRecords);
  while (!page.complete) {
    const durableCursor = JSON.parse(JSON.stringify(page.nextCursor));
    page = await provider.read.readChargeBundlePage({
      account: "main",
      chargeId: "ch_1",
      cursor: durableCursor,
      deadlineAtMs,
    });
    canonicalCharge = page.canonicalCharge ?? canonicalCharge;
    mergeRecords(records, page.relatedRecords);
  }

  assert.equal(canonicalCharge.id, "ch_1");
  assert.equal(canonicalCharge.metadata.snapshot, "final");
  assert.equal(chargeReads, 2);
  assert.deepEqual([...new Set(records.refunds.map((item) => item.id))].sort(), [
    "re_1",
    "re_2",
    "re_embedded",
  ]);
  assert.equal(records.subscriptions[0].id, "sub_1");
  assert.equal(records.invoiceLines[0].id, "il_1");
  assert.equal(records.checkoutSessions[0].id, "cs_1");
  assert.equal(records.paymentLinks[0].id, "plink_1");
  assert.ok(records.prices.every((item) => item.id === "price_1"));
  assert.ok(records.products.every((item) => item.id === "prod_1"));
  assert.deepEqual(
    [...new Set(
      records.unresolvedReferences.map((item) => item.reference_type),
    )].sort(),
    ["balance_transaction", "payment_method"],
  );
  assert.equal(
    calls.some((call) => call.path.startsWith("/balance_transactions/")),
    false,
  );
  assert.equal(
    calls.find((call) => call.path === "/charges/ch_1").parameters["expand[4]"],
    "payment_intent.payment_method",
  );
});

test("Stripe can retrieve full balance and payment method evidence after allowlist extension", async () => {
  const paths = [];
  const provider = createStripeRpcProvider({
    clock,
    extendedObjectPaths: true,
    rpcRead: async (account, path) => {
      paths.push(path);

      if (path === "/charges/ch_1") {
        return ok({
          id: "ch_1",
          object: "charge",
          balance_transaction: "txn_1",
          payment_method: "pm_1",
          refunds: { data: [] },
        });
      }

      if (path === "/refunds") {
        return ok({ data: [], has_more: false });
      }

      if (path === "/balance_transactions/txn_1") {
        return ok({ id: "txn_1", object: "balance_transaction", fee: 123 });
      }

      if (path === "/payment_methods/pm_1") {
        return ok({ id: "pm_1", object: "payment_method", type: "card" });
      }

      throw new Error(`unexpected Stripe test path ${path}`);
    },
  });

  const records = {};
  let page = await provider.read.readChargeBundlePage({
    account: "main",
    chargeId: "ch_1",
    cursor: null,
    deadlineAtMs,
  });
  mergeRecords(records, page.relatedRecords);

  while (!page.complete) {
    page = await provider.read.readChargeBundlePage({
      account: "main",
      chargeId: "ch_1",
      cursor: page.nextCursor,
      deadlineAtMs,
    });
    mergeRecords(records, page.relatedRecords);
  }

  assert.ok(paths.includes("/balance_transactions/txn_1"));
  assert.ok(paths.includes("/payment_methods/pm_1"));
  assert.equal(records.balanceTransactions[0].fee, 123);
  assert.equal(records.paymentMethods[0].type, "card");
});

test("Stripe exposes retry state and refuses reads after the slice deadline", async () => {
  let calls = 0;
  const provider = createStripeRpcProvider({
    clock,
    rpcRead: async () => {
      calls += 1;
      return {
        status: 429,
        retryAfter: "5",
        body: { error: { message: "kept out of adapter errors" } },
      };
    },
  });

  await assert.rejects(
    provider.read.listCharges({
      account: "main",
      createdGte: 0,
      createdLte: 1,
      startingAfter: null,
      limit: 1,
      deadlineAtMs,
    }),
    (error) => {
      assert.ok(error instanceof ProviderReadError);
      assert.equal(error.status, 429);
      assert.equal(error.retryable, true);
      assert.equal(error.retryAfter, "5");
      assert.equal(error.message.includes("kept out"), false);
      return true;
    },
  );

  await assert.rejects(
    provider.read.listCharges({
      account: "main",
      createdGte: 0,
      createdLte: 1,
      startingAfter: null,
      limit: 1,
      deadlineAtMs: clock.now(),
    }),
    (error) => error.code === "deadline_exceeded",
  );
  assert.equal(calls, 1);
});

test("Stripe stops waiting for an RPC that runs past its call budget", async () => {
  const provider = createStripeRpcProvider({
    clock,
    maxCallMs: 5,
    rpcRead: async () => new Promise(() => {}),
  });

  await assert.rejects(
    provider.read.listCharges({
      account: "main",
      createdGte: 0,
      createdLte: 1,
      startingAfter: null,
      limit: 1,
      deadlineAtMs,
    }),
    (error) => error.code === "deadline_exceeded",
  );
});

function ok(body) {
  return { status: 200, retryAfter: null, body };
}

function mergeRecords(target, additions) {
  for (const [name, values] of Object.entries(additions)) {
    target[name] ??= [];
    target[name].push(...values);
  }
}
