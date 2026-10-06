import assert from "node:assert/strict";
import test from "node:test";

import {
  runBulkBootstrapSlice,
  runBulkBootstrapToCompletion,
} from "../../worker/fivetran/bulk-bootstrap.ts";
import type {
  ActiveCampaignRawScope,
  BulkBootstrapCheckpoint,
  ConversionSourceTable,
  FivetranFactReader,
  PipelineId,
  PreparedScope,
  SourceReplacement,
  StripeAccount,
  StripeRawScope,
} from "../../worker/fivetran/contracts.ts";
import { canonicalJson } from "../../worker/fivetran/json.ts";

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
const SNAPSHOT_ID = "fivetran-20260905T222800Z";

test("bulk bootstrap retries exact pages and seeds state only after publication", async () => {
  const reader = new BootstrapReader({
    stripe_main: ["stripe:main:charge:ch_1", "stripe:main:charge:ch_2"],
  });
  const publisher = new BootstrapPublisher();
  const state = new BootstrapState();
  const dependencies = {
    reader,
    publisher,
    checkpoints: state,
    seeder: state,
    stripeNormalizer: normalizeStripeChargeSnapshot,
    activeCampaignNormalizer: buildActiveCampaignContactReplacement,
  };
  publisher.failAfterAccept = true;

  await assert.rejects(
    runBulkBootstrapSlice({
      snapshotId: SNAPSHOT_ID,
      snapshotAt: SNAPSHOT,
      pipeline: "stripe_main",
      dependencies,
      pageScopes: 1,
    }),
    /ambiguous bootstrap publication/,
  );
  assert.equal(state.seeds.size, 0);
  assert.equal(state.checkpoints.size, 0);

  const first = await runBulkBootstrapSlice({
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT,
    pipeline: "stripe_main",
    dependencies,
    pageScopes: 1,
  });
  assert.equal(first.complete, false);
  assert.equal(first.publishedScopes, 1);
  assert.equal(state.seeds.size, 1);
  assert.equal(canonicalJson(publisher.calls[0]), canonicalJson(publisher.calls[1]));

  const second = await runBulkBootstrapSlice({
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT,
    pipeline: "stripe_main",
    dependencies,
    pageScopes: 1,
  });
  assert.equal(second.complete, true);
  assert.equal(second.publishedScopes, 2);
  assert.equal(second.identityFactCount, 2);
  assert.equal(state.finalized.get("stripe_main"), SNAPSHOT);
  assert.ok([...state.seeds.values()].every((seed) =>
    seed.replacement.observation_sequence === 1
  ));
});

test("completed bootstrap checkpoints do no more reads or writes", async () => {
  const reader = new BootstrapReader({ stripe_main: [] });
  const publisher = new BootstrapPublisher();
  const state = new BootstrapState();
  const dependencies = {
    reader,
    publisher,
    checkpoints: state,
    seeder: state,
    stripeNormalizer: normalizeStripeChargeSnapshot,
    activeCampaignNormalizer: buildActiveCampaignContactReplacement,
  };

  await runBulkBootstrapToCompletion({
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT,
    dependencies,
    pageScopes: 10,
  });
  const reads = reader.bootstrapReads;
  const calls = publisher.calls.length;

  await runBulkBootstrapSlice({
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT,
    pipeline: "stripe_main",
    dependencies,
  });
  assert.equal(reader.bootstrapReads, reads);
  assert.equal(publisher.calls.length, calls);
});

test("a complete checkpoint resumes finalization without rereading the snapshot", async () => {
  const reader = new BootstrapReader({ stripe_main: [] });
  const state = new BootstrapState();
  state.failFinalizeOnce = true;
  const dependencies = {
    reader,
    publisher: new BootstrapPublisher(),
    checkpoints: state,
    seeder: state,
    stripeNormalizer: normalizeStripeChargeSnapshot,
    activeCampaignNormalizer: buildActiveCampaignContactReplacement,
  };

  await assert.rejects(
    runBulkBootstrapSlice({
      snapshotId: SNAPSHOT_ID,
      snapshotAt: SNAPSHOT,
      pipeline: "stripe_main",
      dependencies,
    }),
    /injected finalization failure/,
  );
  assert.equal(state.checkpoints.get(`${SNAPSHOT_ID}:stripe_main`)?.complete, true);
  const readsAfterFailure = reader.bootstrapReads;

  const resumed = await runBulkBootstrapSlice({
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT,
    pipeline: "stripe_main",
    dependencies,
  });
  assert.equal(resumed.complete, true);
  assert.equal(reader.bootstrapReads, readsAfterFailure);
  assert.equal(state.finalized.get("stripe_main"), SNAPSHOT);
});

class BootstrapPublisher {
  readonly calls: SourceReplacement[][] = [];
  failAfterAccept = false;

  async publishBootstrapSourceReplacements(input: {
    bootstrapId: string;
    replacements: SourceReplacement[];
  }): Promise<void> {
    assert.equal(input.bootstrapId, SNAPSHOT_ID);
    this.calls.push(structuredClone(input.replacements));

    if (this.failAfterAccept) {
      this.failAfterAccept = false;
      throw new Error("ambiguous bootstrap publication");
    }
  }
}

class BootstrapState {
  readonly checkpoints = new Map<string, BulkBootstrapCheckpoint>();
  readonly seeds = new Map<string, PreparedScope>();
  readonly finalized = new Map<PipelineId, string>();
  readonly initialized = new Map<PipelineId, string>();
  failFinalizeOnce = false;

  async initializePipeline(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void> {
    const prior = this.initialized.get(input.pipeline);
    if (prior && prior !== input.snapshotAt) throw new Error("snapshot changed");
    this.initialized.set(input.pipeline, input.snapshotAt);
  }

  async loadBulkBootstrapCheckpoint(input: {
    snapshotId: string;
    pipeline: PipelineId;
  }): Promise<BulkBootstrapCheckpoint | null> {
    const value = this.checkpoints.get(`${input.snapshotId}:${input.pipeline}`);
    return value ? structuredClone(value) : null;
  }

  async saveBulkBootstrapCheckpoint(checkpoint: BulkBootstrapCheckpoint): Promise<void> {
    this.checkpoints.set(
      `${checkpoint.snapshotId}:${checkpoint.pipeline}`,
      structuredClone(checkpoint),
    );
  }

  async seedBulkBootstrapScopes(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    prepared: PreparedScope[];
  }): Promise<void> {
    for (const item of input.prepared) {
      const key = `${input.pipeline}:${item.scopeId}`;
      const prior = this.seeds.get(key);
      if (prior) assert.deepEqual(prior, item);
      this.seeds.set(key, structuredClone(item));
    }
  }

  async finalizeBulkBootstrap(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void> {
    if (this.failFinalizeOnce) {
      this.failFinalizeOnce = false;
      throw new Error("injected finalization failure");
    }
    this.finalized.set(input.pipeline, input.snapshotAt);
  }
}

class BootstrapReader implements FivetranFactReader {
  bootstrapReads = 0;
  private readonly scopes: Partial<Record<PipelineId, string[]>>;

  constructor(scopes: Partial<Record<PipelineId, string[]>>) {
    this.scopes = scopes;
  }

  async verifiedObservationThrough(
    requiredTables: readonly ConversionSourceTable[],
  ): Promise<string> {
    throw new Error("incremental barrier was not expected");
  }

  async readChangedScopePage(): Promise<never> {
    throw new Error("incremental discovery was not expected");
  }

  async readBootstrapScopePage(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    afterCursor: string;
    limit: number;
  }) {
    this.bootstrapReads += 1;
    const all = this.scopes[input.pipeline] ?? [];
    const remaining = all.filter((scopeId) => scopeId > input.afterCursor);
    const values = remaining.slice(0, input.limit);
    return {
      scopeIds: values,
      nextCursor: values.at(-1) ?? input.afterCursor,
      eof: remaining.length <= input.limit,
    };
  }

  async readStripeScopes(input: {
    account: StripeAccount;
    chargeIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<StripeRawScope[]> {
    return input.chargeIds.map((chargeId) => stripeScope(input.account, chargeId));
  }

  async readActiveCampaignScopes(input: {
    contactIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<ActiveCampaignRawScope[]> {
    return [];
  }
}

function stripeScope(account: StripeAccount, chargeId: string): StripeRawScope {
  return {
    account,
    chargeId,
    chargeVersions: [{
      id: chargeId,
      amount: 10_000,
      amount_captured: 10_000,
      amount_refunded: 0,
      paid: true,
      captured: true,
      refunded: false,
      status: "succeeded",
      currency: "usd",
      created: "2026-08-01T12:00:00.000000Z",
      metadata: "{}",
      livemode: true,
      _fivetran_synced: "2026-09-05T22:27:00.000000Z",
    }],
    customerVersions: [],
    paymentIntentVersions: [],
    evidenceRecordIds: [`charge:${chargeId}`],
  };
}
