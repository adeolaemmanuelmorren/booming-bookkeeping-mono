import assert from "node:assert/strict";
import test from "node:test";

import { buildActiveCampaignFivetranScope } from "../../worker/fivetran/activecampaign-fivetran.ts";
import { collapseFivetranVersions } from "../../worker/fivetran/raw-collapse.ts";
import { buildStripeFivetranScope } from "../../worker/fivetran/stripe-fivetran.ts";
import type {
  ActiveCampaignRawScope,
  JsonRecord,
  ScopeState,
  StripeRawScope,
} from "../../worker/fivetran/contracts.ts";

const {
  normalizeStripeChargeSnapshot,
} = await import(
  "../../worker/conversions/stripe.mjs"
);
const {
  buildActiveCampaignContactReplacement,
} = await import(
  "../../worker/conversions/activecampaign.mjs"
);
const {
  conversionIdentityFacts,
} = await import(
  "../../worker/conversions/identity.ts"
);

const THROUGH = "2026-09-05T22:31:00.000000Z";

test("Stripe raw facts preserve refunds and namespace both accounts", () => {
  for (const account of ["main", "kajabi"] as const) {
    const built = buildStripeFivetranScope({
      raw: stripeScope(account, {
        amount: 12_500,
        amount_refunded: 2_500,
        billing_detail_email: " ",
        receipt_email: " ",
      }),
      throughInclusive: THROUGH,
      windowId: "window-1",
      observationSequence: 7,
      previous: null,
      normalizeCharge: normalizeStripeChargeSnapshot,
    });
    const fact = built.replacement.rows[0];

    assert.equal(built.scopeId, `stripe:${account}:charge:ch_1`);
    assert.equal(fact.source_account, account);
    assert.equal(fact.amount_minor, 12_500);
    assert.equal(fact.amount_refunded_minor, 2_500);
    assert.equal(fact.net_amount_minor, 10_000);
    assert.equal(fact.email, "customer@example.com");
    assert.equal(fact.provider_event_type, "fivetran_snapshot");
    assert.equal(fact.source_observation_sequence, 7);
    assert.ok(
      String(fact.source_version_id).includes(`stripe:${account}:charge:ch_1`),
    );

    if (account === "main") {
      assert.equal(fact.phone, "555-1000");
    } else {
      assert.equal(fact.phone, null);
    }
  }
});

test("Stripe disqualification publishes an identity tombstone", () => {
  const previous = stripeState({
    source_fact_id: "stripe:main:charge:ch_1",
    charge_id: "ch_1",
    email: "prior@example.com",
    occurred_at: "2026-08-01T00:00:00.000Z",
    is_deleted: false,
  });
  const raw = stripeScope("main", { paid: false, status: "failed" });
  const built = buildStripeFivetranScope({
    raw,
    throughInclusive: THROUGH,
    windowId: "window-2",
    observationSequence: 8,
    previous,
    normalizeCharge: normalizeStripeChargeSnapshot,
  });

  assert.equal(built.replacement.rows.length, 1);
  assert.equal(built.replacement.rows[0].is_deleted, true);
  assert.equal(
    built.replacement.rows[0].tombstone_reason,
    "charge_no_longer_succeeded",
  );
  assert.equal(built.replacement.rows[0].source_observation_sequence, 8);
});

test("ActiveCampaign removal resurrects fallback and tombstones primary", () => {
  const initial = buildActiveCampaignFivetranScope({
    raw: activeCampaignScope(false),
    throughInclusive: THROUGH,
    windowId: "ac-initial",
    observationSequence: 1,
    previous: null,
    normalizeContactReplacement: buildActiveCampaignContactReplacement,
  });

  assert.deepEqual(
    initial.replacement.rows
      .filter((row) => row.is_deleted !== true)
      .map((row) => row.registration_tag_type),
    ["primary"],
  );

  const next = buildActiveCampaignFivetranScope({
    raw: activeCampaignScope(true),
    throughInclusive: "2026-09-05T22:32:00.000000Z",
    windowId: "ac-removal",
    observationSequence: 2,
    previous: {
      rows: initial.replacement.rows,
      compactState: initial.compactState,
      observationSequence: 1,
    },
    normalizeContactReplacement: buildActiveCampaignContactReplacement,
  });
  const live = next.replacement.rows.filter((row) => row.is_deleted !== true);
  const deleted = next.replacement.rows.filter((row) => row.is_deleted === true);

  assert.equal(live.length, 1);
  assert.equal(live[0].registration_tag_type, "fallback");
  assert.equal(live[0].occurred_at, "2026-08-01T17:03:04.000Z");
  assert.equal(deleted.length, 1);
  assert.equal(deleted[0].form_submission_id, "assignment-primary");
  assert.equal(deleted[0].tombstone_reason, "assignment_deleted");
});

test("equal Fivetran versions reject conflicting payloads", () => {
  assert.throws(
    () => collapseFivetranVersions({
      rows: [
        { id: "same", value: "a", _fivetran_synced: THROUGH },
        { id: "same", value: "b", _fivetran_synced: THROUGH },
      ],
    }),
    /Conflicting Fivetran rows/,
  );
});

test("transport observation wins when a delayed load has an older source timestamp", () => {
  const rows = [
    version({ synced: "2026-09-05T22:20:00Z", observed: "2026-09-05T22:21:00Z" }),
    // An identical overlap delivery is collapsed.
    version({ synced: "2026-09-05T22:20:00Z", observed: "2026-09-05T22:22:00Z" }),
    // A mass-delete reconciliation can change state without changing source sync.
    version({
      synced: "2026-09-05T22:20:00Z",
      observed: "2026-09-05T22:23:00Z",
      deleted: true,
    }),
    // A later fixed-time source read is authoritative even when extraction time fell back.
    version({
      synced: "2026-09-05T22:19:00Z",
      observed: "2026-09-05T22:25:00Z",
      email: "late-old@example.com",
    }),
    // This newer extraction arrived before the authoritative fixed-time read above.
    version({
      synced: "2026-09-05T22:24:00Z",
      observed: "2026-09-05T22:24:30Z",
      deleted: false,
      email: "resurrected@example.com",
    }),
  ];
  const collapsed = collapseFivetranVersions({ rows });
  const current = collapsed.currentById.get("contact-1");

  assert.equal(collapsed.versions.length, 4);
  assert.equal(current?._fivetran_deleted, false);
  assert.equal(current?.email, "late-old@example.com");
  assert.equal(current?._fivetran_synced, "2026-09-05T22:19:00Z");
});

test("physical deletion participates in conflicts and same-sync resurrection", () => {
  const original = version({
    synced: "2026-09-05T22:20:00Z",
    observed: "2026-09-05T22:21:00Z",
  });
  const deleted = {
    ...original,
    _v1_observed_at: "2026-09-05T22:22:00Z",
    _v1_deleted: 1,
  };
  const resurrected = {
    ...original,
    _v1_observed_at: "2026-09-05T22:23:00Z",
    _v1_deleted: 0,
    email: "back@example.com",
  };

  const whileDeleted = collapseFivetranVersions({
    rows: [original, deleted, resurrected],
    throughInclusive: "2026-09-05T22:22:30Z",
  });
  assert.equal(whileDeleted.currentById.has("contact-1"), false);

  const current = collapseFivetranVersions({
    rows: [original, deleted, resurrected],
  }).currentById.get("contact-1");
  assert.equal(current?.email, "back@example.com");
  assert.equal(current?._fivetran_synced, original._fivetran_synced);

  assert.throws(
    () => collapseFivetranVersions({
      rows: [
        deleted,
        { ...deleted, _v1_deleted: 0 },
      ],
    }),
    /Conflicting Fivetran rows/,
  );
});

test("Stripe charge physical deletion publishes conversion and identity tombstones", async () => {
  const raw = stripeScope("main") as StripeRawScope;
  const initial = buildStripe(raw, null, 1, "stripe-initial");
  raw.chargeVersions.push(physicalDelete(raw.chargeVersions[0]));

  const deleted = buildStripe(
    raw,
    scopeState(initial),
    2,
    "stripe-charge-deleted",
  );
  assert.equal(deleted.replacement.rows[0].is_deleted, true);
  assert.equal(deleted.replacement.rows[0].tombstone_reason, "charge_missing");
  const identity = await conversionIdentityFacts(deleted.replacement);
  assert.equal(identity[0].factDeleted, true);
  assert.deepEqual(identity[0].evidenceKeys, []);
});

test("Stripe customer and payment intent physical deletes remove related identity evidence", async () => {
  for (const relation of ["customer", "payment_intent"] as const) {
    const raw = stripeScope("main", {
      billing_detail_email: null,
      billing_detail_name: null,
      receipt_email: null,
      metadata: "{}",
    }) as StripeRawScope;
    raw.customerVersions[0].email = relation === "customer"
      ? "customer-only@example.com"
      : null;
    raw.customerVersions[0].name = relation === "customer"
      ? "Customer Only"
      : null;
    raw.paymentIntentVersions[0].receipt_email = relation === "payment_intent"
      ? "intent-only@example.com"
      : null;
    const initial = buildStripe(raw, null, 1, `${relation}-initial`);
    const versions = relation === "customer"
      ? raw.customerVersions
      : raw.paymentIntentVersions;
    versions.push(physicalDelete(versions[0]));

    const changed = buildStripe(
      raw,
      scopeState(initial),
      2,
      `${relation}-deleted`,
    );
    assert.equal(changed.replacement.rows[0].is_deleted, false);
    assert.equal(changed.replacement.rows[0].email, null);
    assert.equal(changed.compactState[relation], null);
    const identity = await conversionIdentityFacts(changed.replacement);
    assert.equal(identity[0].factDeleted, false);
    assert.deepEqual(identity[0].evidenceKeys, []);
  }
});

test("ActiveCampaign physical deletes remove contact, assignment, and tag identity facts", async () => {
  for (const target of ["contact", "assignment", "tag"] as const) {
    const raw = activeCampaignScope(false) as ActiveCampaignRawScope;
    const initial = buildActiveCampaign(raw, null, 1, `${target}-initial`);

    if (target === "contact") {
      raw.contactVersions.push(physicalDelete(raw.contactVersions[0]));
    } else if (target === "assignment") {
      const primary = raw.assignmentVersions.find((row) =>
        row.id === "assignment-primary"
      )!;
      raw.assignmentVersions.push(physicalDelete(primary));
    } else {
      const primary = raw.referencedTagVersions.find((row) =>
        String(row.id) === "11"
      )!;
      raw.referencedTagVersions.push(physicalDelete(primary));
    }

    const changed = buildActiveCampaign(
      raw,
      scopeState(initial),
      2,
      `${target}-deleted`,
    );
    const live = changed.replacement.rows.filter((row) => row.is_deleted !== true);
    const deleted = changed.replacement.rows.filter((row) => row.is_deleted === true);

    if (target === "contact") {
      assert.equal(live.length, 0);
      assert.equal(deleted[0].tombstone_reason, "contact_deleted");
      const identity = await conversionIdentityFacts(changed.replacement);
      assert.ok(identity.length > 0);
      assert.ok(identity.every((fact) => fact.factDeleted));
      continue;
    }
    assert.equal(live[0].registration_tag_type, "fallback");
    assert.equal(deleted[0].form_submission_id, "assignment-primary");
    const identity = await conversionIdentityFacts(changed.replacement);
    const primary = identity.find((fact) =>
      fact.factKey === "activecampaign:assignment-primary"
    );
    assert.equal(primary?.factDeleted, true);
  }
});

function stripeScope(
  account: "main" | "kajabi",
  overrides: JsonRecord = {},
) {
  return {
    account,
    chargeId: "ch_1",
    chargeVersions: [{
      id: "ch_1",
      amount: 12_500,
      amount_captured: 12_500,
      amount_refunded: 0,
      paid: true,
      captured: true,
      refunded: false,
      status: "succeeded",
      currency: "usd",
      created: "2026-08-01T12:00:00.000000Z",
      customer_id: "cus_1",
      payment_intent_id: "pi_1",
      billing_detail_email: "billing@example.com",
      billing_detail_phone: null,
      billing_detail_name: "Billing Person",
      metadata: '{"phone":"555-1000"}',
      livemode: true,
      _fivetran_synced: "2026-09-05T22:29:01.000001Z",
      ...overrides,
    }],
    customerVersions: [{
      id: "cus_1",
      email: "Customer@Example.com",
      phone: null,
      name: "Customer Person",
      metadata: "{}",
      is_deleted: false,
      _fivetran_synced: "2026-09-05T22:29:02.000002Z",
    }],
    paymentIntentVersions: [{
      id: "pi_1",
      receipt_email: "intent@example.com",
      customer_id: "cus_1",
      status: "succeeded",
      metadata: "{}",
      _fivetran_synced: "2026-09-05T22:29:03.000003Z",
    }],
    evidenceRecordIds: ["charge:1", "customer:1", "charge:1"],
  };
}

function stripeState(row: JsonRecord): ScopeState {
  return { rows: [row], compactState: {}, observationSequence: 7 };
}

function scopeState(value: {
  replacement: { rows: JsonRecord[] };
  compactState: JsonRecord;
}): ScopeState {
  return {
    rows: value.replacement.rows,
    compactState: value.compactState,
    observationSequence: 1,
  };
}

function buildStripe(
  raw: StripeRawScope,
  previous: ScopeState | null,
  observationSequence: number,
  windowId: string,
) {
  return buildStripeFivetranScope({
    raw,
    throughInclusive: THROUGH,
    windowId,
    observationSequence,
    previous,
    normalizeCharge: normalizeStripeChargeSnapshot,
  });
}

function buildActiveCampaign(
  raw: ActiveCampaignRawScope,
  previous: ScopeState | null,
  observationSequence: number,
  windowId: string,
) {
  return buildActiveCampaignFivetranScope({
    raw,
    throughInclusive: THROUGH,
    windowId,
    observationSequence,
    previous,
    normalizeContactReplacement: buildActiveCampaignContactReplacement,
  });
}

function physicalDelete(row: JsonRecord): JsonRecord {
  return {
    ...structuredClone(row),
    _fivetran_synced: "2026-09-01T00:00:00.000000Z",
    _v1_observed_at: "2026-09-05T22:30:30.000000Z",
    _v1_observation_kind: "changes_delete",
    _v1_deleted: 1,
  };
}

function activeCampaignScope(primaryDeleted: boolean) {
  const primaryVersions: JsonRecord[] = [{
    id: "assignment-primary",
    contact: 100,
    tags: 11,
    c_date: "2026-08-02T18:04:05.000000Z",
    _fivetran_deleted: false,
    _fivetran_synced: "2026-09-05T22:29:05.000001Z",
  }];

  if (primaryDeleted) {
    primaryVersions.push({
      id: "assignment-primary",
      contact: 100,
      tags: 11,
      c_date: "2026-08-02T18:04:05.000000Z",
      _fivetran_deleted: true,
      _fivetran_synced: "2026-09-05T22:31:05.000001Z",
    });
  }

  return {
    contactId: "100",
    contactVersions: [{
      id: 100,
      email: "Person@Example.com",
      first_name: "Ada",
      last_name: "Person",
      phone: "555-2000",
      _fivetran_deleted: false,
      deleted: 0,
      _fivetran_synced: "2026-09-05T22:29:04.000001Z",
    }],
    assignmentVersions: [
      {
        id: "assignment-fallback",
        contact: 100,
        tags: 10,
        c_date: "2026-08-01T17:03:04.000000Z",
        _fivetran_deleted: false,
        _fivetran_synced: "2026-09-05T22:29:05.000000Z",
      },
      ...primaryVersions,
    ],
    referencedTagVersions: [
      {
        id: "10",
        tags: "[KRC] Registered for Challenge",
        _fivetran_deleted: false,
        _fivetran_synced: "2026-09-05T22:29:06.000000Z",
      },
      {
        id: "11",
        tags: "[KRC] Registered - August 2026",
        _fivetran_deleted: false,
        _fivetran_synced: "2026-09-05T22:29:06.000001Z",
      },
    ],
    evidenceRecordIds: ["contact:100", "assignment:fallback", "assignment:primary"],
  };
}

function version(input: {
  synced: string;
  observed: string;
  deleted?: boolean;
  email?: string;
}): JsonRecord {
  return {
    id: "contact-1",
    email: input.email ?? "person@example.com",
    _fivetran_deleted: input.deleted ?? false,
    _fivetran_synced: input.synced,
    _v1_observed_at: input.observed,
    _v1_observation_kind: input.deleted
      ? "deletion_reconciliation"
      : "incremental",
  };
}
