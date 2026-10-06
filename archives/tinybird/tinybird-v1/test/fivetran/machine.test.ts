import assert from "node:assert/strict";
import test from "node:test";

import type {
  ActiveCampaignRawScope,
  ConversionSourceTable,
  DiscoveryPage,
  FivetranFactReader,
  JsonRecord,
  PipelineId,
  SourceReplacement,
  StripeAccount,
  StripeRawScope,
} from "../../worker/fivetran/contracts.ts";
import { canonicalJson } from "../../worker/fivetran/json.ts";
import { runBootstrapSlice, runIncrementalSlice } from "../../worker/fivetran/machine.ts";
import { MemoryStore } from "./memory-store.ts";

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

const SNAPSHOT = "2026-09-05T22:28:00.000000Z";
const dependencies = (reader: FakeReader, store: MemoryStore, publisher: FakePublisher) => ({
  reader,
  store,
  publisher,
  bootstrapPublisher: publisher,
  builders: {
    stripeNormalizer: normalizeStripeChargeSnapshot,
    activeCampaignNormalizer: buildActiveCampaignContactReplacement,
  },
});

test("bootstrap retries the exact prepared replacement after an ambiguous write", async () => {
  const store = new MemoryStore();
  const reader = new FakeReader();
  const publisher = new FakePublisher();
  reader.bootstrapScopes.set("stripe_main", ["stripe:main:charge:ch_1"]);
  reader.stripeScopes.set("main:ch_1", stripeScope("main", "ch_1", 0));
  publisher.failAfterAccept = true;

  await assert.rejects(
    runBootstrapSlice({
      pipeline: "stripe_main",
      snapshotAt: SNAPSHOT,
      dependencies: dependencies(reader, store, publisher),
    }),
    /ambiguous publication/,
  );
  assert.equal(publisher.calls.length, 1);
  assert.equal((await store.getPipeline("stripe_main")).completedObservationAt, null);

  const completed = await runBootstrapSlice({
    pipeline: "stripe_main",
    snapshotAt: SNAPSHOT,
    dependencies: dependencies(reader, store, publisher),
  });

  assert.equal(completed.phase, "complete");
  assert.equal(completed.completedObservationAt, SNAPSHOT);
  assert.equal(publisher.calls.length, 2);
  assert.equal(
    canonicalJson(publisher.calls[0]),
    canonicalJson(publisher.calls[1]),
  );
  assert.equal(publisher.calls[1][0].observation_sequence, 1);
});

test("incremental work consumes a complete 60-minute transport batch once", async () => {
  const store = new MemoryStore();
  const reader = new FakeReader();
  const publisher = new FakePublisher();
  reader.bootstrapScopes.set("stripe_main", []);

  await runBootstrapSlice({
    pipeline: "stripe_main",
    snapshotAt: SNAPSHOT,
    dependencies: dependencies(reader, store, publisher),
  });

  reader.barrier = "2026-09-05T23:28:00.000000Z";
  reader.changedScopes.set("raw_stripe_charge", ["stripe:main:charge:ch_1"]);
  reader.changedScopes.set("raw_stripe_customer", ["stripe:main:charge:ch_1"]);
  reader.changedScopes.set("raw_stripe_payment_intent", ["stripe:main:charge:ch_2"]);
  reader.stripeScopes.set("main:ch_1", stripeScope("main", "ch_1", 100));
  reader.stripeScopes.set("main:ch_2", stripeScope("main", "ch_2", 200));

  const first = await runIncrementalSlice({
    pipeline: "stripe_main",
    dependencies: dependencies(reader, store, publisher),
  });

  assert.equal(first.phase, "complete");
  assert.equal(first.completedObservationAt, "2026-09-05T23:28:00.000000Z");
  assert.equal(publisher.calls.at(-1)?.length, 2);
  assert.deepEqual(
    publisher.calls.at(-1)?.map((replacement) => replacement.scope_id).sort(),
    ["stripe:main:charge:ch_1", "stripe:main:charge:ch_2"],
  );
  assert.ok(reader.changedReads.every((read) =>
    read.afterExclusive === SNAPSHOT &&
    read.throughInclusive === "2026-09-05T23:28:00.000000Z"
  ));

  const reads = reader.changedReads.length;
  const publications = publisher.calls.length;
  const idle = await runIncrementalSlice({
    pipeline: "stripe_main",
    dependencies: dependencies(reader, store, publisher),
  });

  assert.equal(idle.phase, "idle");
  assert.equal(idle.completedObservationAt, "2026-09-05T23:28:00.000000Z");
  assert.equal(reader.changedReads.length, reads);
  assert.equal(publisher.calls.length, publications);
});

test("failed incremental publication leaves the cursor and prepared batch intact", async () => {
  const store = new MemoryStore();
  const reader = new FakeReader();
  const publisher = new FakePublisher();
  reader.bootstrapScopes.set("stripe_kajabi", []);

  await runBootstrapSlice({
    pipeline: "stripe_kajabi",
    snapshotAt: SNAPSHOT,
    dependencies: dependencies(reader, store, publisher),
  });

  reader.barrier = "2026-09-05T22:29:00.000000Z";
  for (const table of [
    "raw_stripe_kajabi_charge",
    "raw_stripe_kajabi_customer",
    "raw_stripe_kajabi_payment_intent",
  ] as const) {
    reader.changedScopes.set(table, table.endsWith("charge")
      ? ["stripe:kajabi:charge:ch_k"]
      : []);
  }
  reader.stripeScopes.set("kajabi:ch_k", stripeScope("kajabi", "ch_k", 0));
  publisher.failAfterAccept = true;

  await assert.rejects(
    runIncrementalSlice({
      pipeline: "stripe_kajabi",
      dependencies: dependencies(reader, store, publisher),
    }),
    /ambiguous publication/,
  );
  const failedState = await store.getPipeline("stripe_kajabi");
  assert.equal(failedState.completedObservationAt, SNAPSHOT);
  assert.ok(failedState.activeWindow);

  const retried = await runIncrementalSlice({
    pipeline: "stripe_kajabi",
    dependencies: dependencies(reader, store, publisher),
  });
  assert.equal(retried.phase, "complete");
  assert.equal(retried.completedObservationAt, "2026-09-05T22:29:00.000000Z");
  assert.equal(canonicalJson(publisher.calls[0]), canonicalJson(publisher.calls[1]));
});

test("same-version AC reconciliation deletes and a later source update resurrects", async () => {
  const store = new MemoryStore();
  const reader = new FakeReader();
  const publisher = new FakePublisher();
  const scopeId = "activecampaign:contact:100";
  reader.bootstrapScopes.set("activecampaign", [scopeId]);
  reader.activeCampaignScopes.set("100", activeCampaignRaw([]));

  await runBootstrapSlice({
    pipeline: "activecampaign",
    snapshotAt: SNAPSHOT,
    dependencies: dependencies(reader, store, publisher),
  });
  assert.equal(publisher.calls.at(-1)?.[0].rows[0].is_deleted, false);
  assert.equal(publisher.calls.at(-1)?.[0].observation_sequence, 1);

  reader.barrier = "2026-09-05T23:28:00.000000Z";
  reader.changedScopes.set("raw_activecampaign_contact", []);
  reader.changedScopes.set("raw_activecampaign_contact_tag", [scopeId]);
  reader.changedScopes.set("raw_activecampaign_tags", []);
  reader.activeCampaignScopes.set("100", activeCampaignRaw([{
    id: "assignment-primary",
    contact: 100,
    tags: 11,
    c_date: "2026-08-02T18:04:05.000000Z",
    _fivetran_deleted: true,
    _fivetran_synced: "2026-09-05T22:27:00.000000Z",
    _v1_observed_at: "2026-09-05T23:28:00.000000Z",
    _v1_observation_kind: "deletion_reconciliation",
  }]));

  await runIncrementalSlice({
    pipeline: "activecampaign",
    dependencies: dependencies(reader, store, publisher),
  });
  const deleted = publisher.calls.at(-1)?.[0];
  assert.equal(deleted?.observation_sequence, 2);
  assert.equal(deleted?.rows[0].is_deleted, true);

  reader.barrier = "2026-09-06T00:28:00.000000Z";
  reader.activeCampaignScopes.set("100", activeCampaignRaw([
    {
      id: "assignment-primary",
      contact: 100,
      tags: 11,
      c_date: "2026-08-02T18:04:05.000000Z",
      _fivetran_deleted: true,
      _fivetran_synced: "2026-09-05T22:27:00.000000Z",
      _v1_observed_at: "2026-09-05T23:28:00.000000Z",
      _v1_observation_kind: "deletion_reconciliation",
    },
    {
      id: "assignment-primary",
      contact: 100,
      tags: 11,
      c_date: "2026-08-02T18:04:05.000000Z",
      _fivetran_deleted: false,
      _fivetran_synced: "2026-09-06T00:20:00.000000Z",
      _v1_observed_at: "2026-09-06T00:28:00.000000Z",
      _v1_observation_kind: "incremental",
    },
  ]));

  await runIncrementalSlice({
    pipeline: "activecampaign",
    dependencies: dependencies(reader, store, publisher),
  });
  const resurrected = publisher.calls.at(-1)?.[0];
  assert.equal(resurrected?.observation_sequence, 3);
  assert.equal(resurrected?.rows[0].is_deleted, false);
  assert.equal(resurrected?.rows[0].form_submission_id, "assignment-primary");
});

class FakePublisher {
  readonly calls: SourceReplacement[][] = [];
  failAfterAccept = false;

  async publishBootstrapSourceReplacements(input: {
    bootstrapId: string;
    replacements: SourceReplacement[];
  }): Promise<void> {
    await this.record(input.replacements);
  }

  async publishSourceReplacements(replacements: SourceReplacement[]): Promise<void> {
    await this.record(replacements);
  }

  private async record(replacements: SourceReplacement[]): Promise<void> {
    this.calls.push(structuredClone(replacements));

    if (this.failAfterAccept) {
      this.failAfterAccept = false;
      throw new Error("ambiguous publication after remote commit");
    }
  }
}

class FakeReader implements FivetranFactReader {
  barrier = "";
  readonly bootstrapScopes = new Map<PipelineId, string[]>();
  readonly changedScopes = new Map<ConversionSourceTable, string[]>();
  readonly stripeScopes = new Map<string, StripeRawScope>();
  readonly activeCampaignScopes = new Map<string, ActiveCampaignRawScope>();
  readonly changedReads: Array<{
    table: ConversionSourceTable;
    afterExclusive: string;
    throughInclusive: string;
  }> = [];

  async verifiedObservationThrough(
    requiredTables: readonly ConversionSourceTable[],
  ): Promise<string> {
    assert.ok(requiredTables.length > 0);
    if (!this.barrier) throw new Error("missing verified barrier");
    return this.barrier;
  }

  async readChangedScopePage(input: {
    pipeline: PipelineId;
    table: ConversionSourceTable;
    afterExclusive: string;
    throughInclusive: string;
    afterCursor: string;
    limit: number;
  }): Promise<DiscoveryPage> {
    this.changedReads.push({
      table: input.table,
      afterExclusive: input.afterExclusive,
      throughInclusive: input.throughInclusive,
    });
    assert.equal(input.afterCursor, "");
    return {
      scopeIds: this.changedScopes.get(input.table) ?? [],
      nextCursor: "done",
      eof: true,
    };
  }

  async readBootstrapScopePage(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    afterCursor: string;
    limit: number;
  }) {
    assert.equal(input.snapshotAt, SNAPSHOT);
    assert.equal(input.afterCursor, "");
    return {
      scopeIds: this.bootstrapScopes.get(input.pipeline) ?? [],
      nextCursor: "done",
      eof: true,
    };
  }

  async readStripeScopes(input: {
    account: StripeAccount;
    chargeIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<StripeRawScope[]> {
    return input.chargeIds.map((chargeId) => {
      const value = this.stripeScopes.get(`${input.account}:${chargeId}`);
      if (!value) throw new Error("missing Stripe scope");
      return structuredClone(value);
    });
  }

  async readActiveCampaignScopes(input: {
    contactIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<ActiveCampaignRawScope[]> {
    return input.contactIds.map((contactId) => {
      const value = this.activeCampaignScopes.get(contactId);
      if (!value) throw new Error("missing ActiveCampaign scope");
      return structuredClone(value);
    });
  }
}

function stripeScope(
  account: StripeAccount,
  chargeId: string,
  refunded: number,
): StripeRawScope {
  return {
    account,
    chargeId,
    chargeVersions: [{
      id: chargeId,
      amount: 10_000,
      amount_captured: 10_000,
      amount_refunded: refunded,
      paid: true,
      captured: true,
      refunded: refunded === 10_000,
      status: "succeeded",
      currency: "usd",
      created: "2026-08-01T12:00:00.000000Z",
      customer_id: null,
      payment_intent_id: null,
      metadata: "{}",
      livemode: true,
      _fivetran_synced: "2026-09-05T22:28:30.000000Z",
    }],
    customerVersions: [],
    paymentIntentVersions: [],
    evidenceRecordIds: [`charge:${chargeId}:2026-09-05T22:28:30.000000Z`],
  };
}

function activeCampaignRaw(extraAssignments: JsonRecord[]): ActiveCampaignRawScope {
  return {
    contactId: "100",
    contactVersions: [{
      id: 100,
      email: "person@example.com",
      first_name: "Ada",
      last_name: "Person",
      deleted: 0,
      _fivetran_deleted: false,
      _fivetran_synced: "2026-09-05T22:26:00.000000Z",
    }],
    assignmentVersions: [{
      id: "assignment-primary",
      contact: 100,
      tags: 11,
      c_date: "2026-08-02T18:04:05.000000Z",
      _fivetran_deleted: false,
      _fivetran_synced: "2026-09-05T22:27:00.000000Z",
    }, ...extraAssignments],
    referencedTagVersions: [{
      id: 11,
      tags: "[KRC] Registered - August 2026",
      _fivetran_deleted: false,
      _fivetran_synced: "2026-09-05T22:27:30.000000Z",
    }],
    evidenceRecordIds: ["contact:100", "assignment:assignment-primary", "tag:11"],
  };
}
