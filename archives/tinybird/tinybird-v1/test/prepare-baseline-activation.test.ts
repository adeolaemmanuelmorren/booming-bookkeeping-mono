import assert from "node:assert/strict";
import test from "node:test";

import {
  prepareBaselineActivation,
  type BaselineActivationMetadata,
} from "../scripts/prepare-baseline-activation.ts";
import type { BootstrapConfig } from "../worker/bootstrap/executor.ts";
import { hash } from "../worker/bootstrap/hash.ts";
import { canonicalJson, sha256 } from "../worker/storage/json.ts";
import type { CoordinatorStatus } from "../worker/fivetran/runtime-coordinator.ts";
import { bootstrapFixture } from "./helpers/bootstrap-fixture.ts";
import { applyNativeImportFixtureCounts } from "./helpers/import-proof-fixture.ts";

const BASELINE_ID = "boom-browser-20260905-v1";
const SNAPSHOT_ID = "fivetran-20260905T222800Z";
const SNAPSHOT_AT = "2026-09-05T22:28:00.000000Z";
const INPUT_HASH = "1".repeat(64);
const CONVERSION_HASH = "2".repeat(64);
const RECEIPTS_HASH = "3".repeat(64);
const CONTENT_HASH = "4".repeat(64);
const MEMBERSHIP_HASH = "5".repeat(64);

test("emits only the reviewed Worker variables and identity seal hash", async () => {
  const metadata = await fixture();
  const activation = await prepareBaselineActivation(metadata);

  assert.deepEqual(activation, {
    workerVars: {
      BROWSER_BASELINE_ID: BASELINE_ID,
      BROWSER_BASELINE_SEAL: metadata.plan.sourceSeal,
      BROWSER_BASELINE_SEQUENCE: "1",
    },
    expectedIdentityActivation: {
      sealHash: await sha256(canonicalJson(metadata.seal)),
    },
  });
});

test("rejects metadata consistency mismatches", async () => {
  const cases: Array<{
    name: string;
    mutate(value: BaselineActivationMetadata): void;
    message: RegExp;
  }> = [
    {
      name: "conversion manifest",
      mutate: (value) => {
        value.conversionManifests[0].canonical_hash = "9".repeat(64);
      },
      message: /Conversion snapshot manifest differs/,
    },
    {
      name: "identity version",
      mutate: (value) => {
        (value.seal as { identityVersion: number }).identityVersion = 2;
      },
      message: /incompatible activation versions/,
    },
    {
      name: "source seal",
      mutate: (value) => {
        value.seal.sourceSeal = "9".repeat(64);
      },
      message: /does not match its saved plan/,
    },
    {
      name: "session sequence",
      mutate: (value) => {
        value.sessionReceipts = [];
      },
      message: /session sequence does not match/,
    },
    {
      name: "active coordinator",
      mutate: (value) => {
        value.coordinatorStatuses[0].activated = true;
      },
      message: /coordinator is not ready/,
    },
    {
      name: "coordinator identity count",
      mutate: (value) => {
        value.coordinatorStatuses[0].checkpoint!.identityFactCount += 1;
      },
      message: /do not cover the conversion manifest/,
    },
  ];

  for (const item of cases) {
    const metadata = await fixture();
    item.mutate(metadata);
    await assert.rejects(
      prepareBaselineActivation(metadata),
      item.message,
      item.name,
    );
  }
});

test("rejects missing live history or redirected page input even when the combined source seal is recomputed", async () => {
  for (const failure of ["missing-live", "wrong-landing", "wrong-live-cutoff"] as const) {
    const metadata = await fixture();
    if (failure === "missing-live") delete metadata.plan.live;
    if (failure === "wrong-live-cutoff") metadata.plan.live!.cutoff = "2026-09-05 20:24:51.606074";
    if (failure === "wrong-landing") {
      const input = metadata.plan.inputs.find(input => input.partition.table === "raw_jitsu_data_pages")!;
      input.landingTable = "v1_history_boom_domains_pages";
      input.expectedPhysicalRows = 854367;
    }
    metadata.plan.sourceSeal = await hash({ baselineId: metadata.plan.baselineId, inputManifestHash: metadata.plan.inputManifestHash,
      importMode: "one-object-per-content", inputs: metadata.plan.inputs, live: metadata.plan.live ?? null,
      conversionIdentity: metadata.plan.conversionIdentity });
    metadata.seal.sourceSeal = metadata.plan.sourceSeal;
    metadata.sessionReceipts[0].sourceSeal = metadata.plan.sourceSeal;
    await assert.rejects(prepareBaselineActivation(metadata), /Production.*(input|mapping)/);
  }
});

async function fixture(): Promise<BaselineActivationMetadata> {
  const browser = (await bootstrapFixture()).config;
  browser.inputManifestHash = INPUT_HASH;
  for (const input of browser.inputs) {
    input.partition.inputManifestHash = INPUT_HASH;
    input.landingTable = input.landingTable.replace("v1_smoke_raw_", "v1_history_");
  }
  applyNativeImportFixtureCounts(browser);
  const conversionIdentity = {
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
    expectedDistinctFactCount: 6,
    canonicalHash: CONVERSION_HASH,
  };
  const plan = {
    bulk: true,
    baselineId: BASELINE_ID,
    tenantId: "boom",
    sourceSeal: "",
    inputManifestHash: INPUT_HASH,
    algorithmVersion: "v1",
    workspaceId: "00c04079-d0b4-4d8b-8de6-6fa8072b85af",
    workspaceName: "booming_bookkeeping",
    startedAt: SNAPSHOT_AT,
    region: "us-east4",
    inputs: browser.inputs,
    live: browser.live,
    conversionIdentity,
    pageSize: 5_000,
    batchSize: 5_000,
    maxVisitorsPerChunk: 5_000,
    maxSessionRecordsPerChunk: 50_000,
    maxIdentityFactsPerBatch: 50_000,
    maxIdentityFactsPerComponent: 50_000,
    maxIdentifiers: 1_000_000,
  } satisfies BootstrapConfig;
  plan.sourceSeal = await hash({
    baselineId: plan.baselineId,
    inputManifestHash: plan.inputManifestHash,
    importMode: "one-object-per-content",
    inputs: plan.inputs,
    live: plan.live,
    conversionIdentity,
  });

  const sessionReceipt = {
    sourceSeal: plan.sourceSeal,
    sequence: 1,
    firstVisitor: "visitor-a",
    lastVisitor: "visitor-z",
    visitorCount: 2,
    pageHeadCount: 3,
    sessionCount: 2,
    contentHash: CONTENT_HASH,
    verifiedContentHash: CONTENT_HASH,
  };
  const seal = {
    runId: BASELINE_ID,
    tenantId: "boom",
    sourceSeal: plan.sourceSeal,
    inputManifestHash: INPUT_HASH,
    finalSessionSequence: 1,
    identityVersion: 1 as const,
    visitorRevision: "1" as const,
    counts: {
      visitors: 2,
      pageHeads: 3,
      sessions: 2,
      identityFacts: 7,
      identityComponents: 4,
    },
    receiptsHash: RECEIPTS_HASH,
    membership: {
      version: 1 as const,
      buckets: 16_384,
      pageSize: 256,
      pages: {
        source: Array(64).fill(MEMBERSHIP_HASH),
        visitor: Array(64).fill(MEMBERSHIP_HASH),
      },
    },
  };

  return {
    plan,
    seal,
    conversionManifests: [{
      snapshot_at: "2026-09-05 22:28:00.000000",
      expected_distinct_fact_count: "6",
      canonical_hash: CONVERSION_HASH,
    }],
    sessionReceipts: [sessionReceipt],
    coordinatorStatuses: [
      coordinator("stripe_main", 2),
      coordinator("stripe_kajabi", 2),
      coordinator("activecampaign", 2),
    ],
  };
}

function coordinator(
  pipeline: "stripe_main" | "stripe_kajabi" | "activecampaign",
  identityFactCount: number,
): CoordinatorStatus {
  return {
    pipeline,
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
    bootstrapComplete: true,
    completedObservationAt: SNAPSHOT_AT,
    activeWindowId: null,
    activated: false,
    activationId: null,
    running: false,
    leaseUntil: null,
    nextAlarmAt: null,
    pendingScopes: 0,
    preparedScopes: 0,
    storedScopes: 2,
    checkpoint: {
      snapshotId: SNAPSHOT_ID,
      snapshotAt: SNAPSHOT_AT,
      pipeline,
      afterCursor: `${pipeline}:last`,
      complete: true,
      publishedScopes: 2,
      identityFactCount,
    },
    reconciliation: null,
    lastSliceStartedAt: null,
    lastSliceSucceededAt: null,
    lastErrorCode: null,
  };
}
