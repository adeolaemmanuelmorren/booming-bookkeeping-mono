import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { validateProductionBrowserInputs } from "./bootstrap-import-proof.ts";

import type { BootstrapConfig } from "../worker/bootstrap/executor.ts";
import { hash } from "../worker/bootstrap/hash.ts";
import type { SessionReceipt } from "../worker/bootstrap/manifest.ts";
import { validateMembershipSeal } from "../worker/bootstrap/membership.ts";
import { TinybirdBootstrapStorage } from "../worker/bootstrap/tinybird-storage.ts";
import type { BootstrapSeal } from "../worker/bootstrap/configured-baseline.ts";
import { timestampMicros } from "../worker/sessions/session-engine.ts";
import { canonicalJson, sha256 } from "../worker/storage/json.ts";
import {
  Tinybird,
  sqlString,
  uint64Number,
} from "../worker/storage/tinybird.ts";
import type {
  CoordinatorStatus,
} from "../worker/fivetran/runtime-coordinator.ts";
import type { PipelineId } from "../worker/fivetran/contracts.ts";

const TINYBIRD_URL = "https://api.us-east.tinybird.co";
const WORKER_URL = "https://boom-tinybird-facts-v1.bill-3e3.workers.dev";
const TENANT_ID = "boom";
const BASELINE_ID = "boom-browser-20260905-v1";
const SNAPSHOT_ID = "fivetran-20260905T222800Z";
const SNAPSHOT_AT = "2026-09-05T22:28:00.000000Z";
const WORKSPACE_ID = "00c04079-d0b4-4d8b-8de6-6fa8072b85af";
const WORKSPACE_NAME = "booming_bookkeeping";
const PIPELINES = [
  "stripe_main",
  "stripe_kajabi",
  "activecampaign",
] as const satisfies readonly PipelineId[];

export interface ConversionManifestRow {
  snapshot_at: string;
  expected_distinct_fact_count: string | number;
  canonical_hash: string;
}

export interface BaselineActivationMetadata {
  plan: BootstrapConfig;
  seal: BootstrapSeal;
  conversionManifests: ConversionManifestRow[];
  sessionReceipts: SessionReceipt[];
  coordinatorStatuses: CoordinatorStatus[];
}

export interface BaselineActivation {
  workerVars: {
    BROWSER_BASELINE_ID: string;
    BROWSER_BASELINE_SEAL: string;
    BROWSER_BASELINE_SEQUENCE: string;
  };
  expectedIdentityActivation: {
    sealHash: string;
  };
}

/** Validate sealed metadata and return only the values needed for activation. */
export async function prepareBaselineActivation(
  metadata: BaselineActivationMetadata,
): Promise<BaselineActivation> {
  const conversion = await validatePlan(metadata.plan);
  validateConversionManifest(metadata.conversionManifests, conversion);
  validateSeal(metadata.seal, metadata.plan, conversion.expectedDistinctFactCount);
  validateSessionSequence(metadata.sessionReceipts, metadata.seal);
  validateCoordinators(metadata.coordinatorStatuses, conversion);

  return {
    workerVars: {
      BROWSER_BASELINE_ID: BASELINE_ID,
      BROWSER_BASELINE_SEAL: metadata.plan.sourceSeal,
      BROWSER_BASELINE_SEQUENCE: String(metadata.seal.finalSessionSequence),
    },
    expectedIdentityActivation: {
      sealHash: await sha256(canonicalJson(metadata.seal)),
    },
  };
}

async function validatePlan(plan: BootstrapConfig) {
  if (
    plan.baselineId !== BASELINE_ID ||
    plan.tenantId !== TENANT_ID ||
    plan.workspaceId !== WORKSPACE_ID ||
    plan.workspaceName !== WORKSPACE_NAME ||
    plan.region !== "us-east4" ||
    plan.bulk !== true
  ) {
    throw new Error("Bootstrap plan belongs to another activation scope");
  }
  if (!isHash(plan.inputManifestHash) || !isHash(plan.sourceSeal)) {
    throw new Error("Bootstrap plan hashes are incomplete");
  }
  validateProductionBrowserInputs(plan);

  const conversion = plan.conversionIdentity;
  if (!conversion) {
    throw new Error("Bootstrap plan has no combined conversion snapshot");
  }
  if (
    conversion.snapshotId !== SNAPSHOT_ID ||
    timestampMicros(conversion.snapshotAt) !== timestampMicros(SNAPSHOT_AT) ||
    !isCount(conversion.expectedDistinctFactCount) ||
    !isHash(conversion.canonicalHash)
  ) {
    throw new Error("Bootstrap conversion snapshot differs from the approved snapshot");
  }

  const expectedSourceSeal = await hash({
    baselineId: plan.baselineId,
    inputManifestHash: plan.inputManifestHash,
    importMode: "one-object-per-content",
    inputs: plan.inputs,
    live: plan.live ?? null,
    conversionIdentity: conversion,
  });
  if (plan.sourceSeal !== expectedSourceSeal) {
    throw new Error("Bootstrap plan source seal does not cover its combined inputs");
  }
  return conversion;
}

function validateConversionManifest(
  rows: ConversionManifestRow[],
  expected: NonNullable<BootstrapConfig["conversionIdentity"]>,
): void {
  if (rows.length !== 1) {
    throw new Error("Conversion snapshot manifest is missing or conflicting");
  }
  const row = rows[0];
  if (
    timestampMicros(row.snapshot_at) !== timestampMicros(expected.snapshotAt) ||
    uint64Number(row.expected_distinct_fact_count) !==
      expected.expectedDistinctFactCount ||
    row.canonical_hash !== expected.canonicalHash
  ) {
    throw new Error("Conversion snapshot manifest differs from the bootstrap plan");
  }
}

function validateSeal(
  seal: BootstrapSeal,
  plan: BootstrapConfig,
  conversionFactCount: number,
): void {
  if (
    seal.runId !== BASELINE_ID ||
    seal.tenantId !== TENANT_ID ||
    seal.sourceSeal !== plan.sourceSeal ||
    seal.inputManifestHash !== plan.inputManifestHash
  ) {
    throw new Error("Bootstrap seal does not match its saved plan");
  }
  if (
    seal.identityVersion !== 1 ||
    seal.visitorRevision !== "1" ||
    !isCount(seal.finalSessionSequence) ||
    seal.finalSessionSequence < 1
  ) {
    throw new Error("Bootstrap seal has incompatible activation versions");
  }
  if (!isHash(seal.receiptsHash)) {
    throw new Error("Bootstrap seal has no verified receipt hash");
  }
  for (const key of [
    "visitors",
    "pageHeads",
    "sessions",
    "identityFacts",
    "identityComponents",
  ] as const) {
    if (!isCount(seal.counts?.[key])) {
      throw new Error("Bootstrap seal contains an invalid count");
    }
  }
  if (seal.counts.identityFacts < conversionFactCount) {
    throw new Error("Bootstrap seal omits conversion identity facts");
  }
  validateMembershipSeal(seal.membership);
}

function validateSessionSequence(
  receipts: SessionReceipt[],
  seal: BootstrapSeal,
): void {
  if (receipts.length !== seal.finalSessionSequence) {
    throw new Error("Bootstrap session sequence does not match the final seal");
  }

  let visitors = 0;
  let pageHeads = 0;
  let sessions = 0;
  for (let index = 0; index < receipts.length; index++) {
    const receipt = receipts[index];
    if (
      receipt.sequence !== index + 1 ||
      receipt.sourceSeal !== seal.sourceSeal ||
      !isHash(receipt.contentHash) ||
      receipt.verifiedContentHash !== receipt.contentHash
    ) {
      throw new Error("Bootstrap session receipt chain conflicts with the final seal");
    }
    visitors += checkedCount(receipt.visitorCount, "visitorCount");
    pageHeads += checkedCount(receipt.pageHeadCount, "pageHeadCount");
    sessions += checkedCount(receipt.sessionCount, "sessionCount");
  }
  if (
    visitors !== seal.counts.visitors ||
    pageHeads !== seal.counts.pageHeads ||
    sessions !== seal.counts.sessions
  ) {
    throw new Error("Bootstrap session receipt totals differ from the final seal");
  }
}

function validateCoordinators(
  statuses: CoordinatorStatus[],
  conversion: NonNullable<BootstrapConfig["conversionIdentity"]>,
): void {
  if (statuses.length !== PIPELINES.length) {
    throw new Error("All three Fivetran coordinator statuses are required");
  }
  const byPipeline = new Map(statuses.map((status) => [status.pipeline, status]));
  if (byPipeline.size !== PIPELINES.length) {
    throw new Error("Fivetran coordinator statuses contain a duplicate pipeline");
  }

  let identityFacts = 0;
  for (const pipeline of PIPELINES) {
    const status = byPipeline.get(pipeline);
    if (!status || status.pipeline !== pipeline) {
      throw new Error(`Missing ${pipeline} coordinator status`);
    }
    const checkpoint = status.checkpoint;
    if (
      status.snapshotId !== SNAPSHOT_ID ||
      !sameTimestamp(status.snapshotAt, SNAPSHOT_AT) ||
      !status.bootstrapComplete ||
      !sameTimestamp(status.completedObservationAt, SNAPSHOT_AT) ||
      status.activeWindowId !== null ||
      status.activated ||
      status.activationId !== null ||
      status.running ||
      status.leaseUntil !== null ||
      status.nextAlarmAt !== null ||
      status.pendingScopes !== 0 ||
      status.preparedScopes !== 0
    ) {
      throw new Error(`${pipeline} coordinator is not ready for activation`);
    }
    if (
      !checkpoint ||
      checkpoint.pipeline !== pipeline ||
      checkpoint.snapshotId !== SNAPSHOT_ID ||
      !sameTimestamp(checkpoint.snapshotAt, SNAPSHOT_AT) ||
      !checkpoint.complete ||
      !isCount(checkpoint.publishedScopes) ||
      !isCount(checkpoint.identityFactCount) ||
      checkpoint.publishedScopes !== status.storedScopes
    ) {
      throw new Error(`${pipeline} coordinator checkpoint is incomplete`);
    }
    identityFacts += checkpoint.identityFactCount;
  }
  if (identityFacts !== conversion.expectedDistinctFactCount) {
    throw new Error("Coordinator checkpoints do not cover the conversion manifest");
  }
}

async function readProductionMetadata(): Promise<BaselineActivationMetadata> {
  const client = new Tinybird({
    TINYBIRD_URL,
    TINYBIRD_TOKEN: requiredEnvironment("TINYBIRD_TOKEN"),
  });
  const bootstrap = new TinybirdBootstrapStorage(client, {
    baselineId: BASELINE_ID,
    tenantId: TENANT_ID,
    sourceSeal: "0".repeat(64),
  });
  const plan = await bootstrap.getManifest<BootstrapConfig>("plan", "config");
  if (!plan) throw new Error("Saved bootstrap plan is not present");

  const sealed = new TinybirdBootstrapStorage(client, {
    baselineId: BASELINE_ID,
    tenantId: TENANT_ID,
    sourceSeal: plan.sourceSeal,
  });
  const conversion = plan.conversionIdentity;
  if (!conversion) throw new Error("Saved bootstrap plan has no conversion snapshot");
  const adminToken = requiredEnvironment("V1_ADMIN_TOKEN");

  const [seal, conversionManifests, sessionReceipts, ...coordinatorStatuses] =
    await Promise.all([
      sealed.getManifest<BootstrapSeal>("seal", "complete"),
      readConversionManifests(client, conversion.snapshotId),
      sealed.checkpointPayloads<SessionReceipt>("sessions", "all"),
      ...PIPELINES.map((pipeline) => readCoordinatorStatus(adminToken, pipeline)),
    ]);
  if (!seal) throw new Error("Saved bootstrap seal is not present");

  return {
    plan,
    seal,
    conversionManifests,
    sessionReceipts,
    coordinatorStatuses,
  };
}

async function readConversionManifests(
  client: Tinybird,
  snapshotId: string,
): Promise<ConversionManifestRow[]> {
  return client.query<ConversionManifestRow>(`
    SELECT DISTINCT
      toString(snapshot_at) AS snapshot_at,
      expected_distinct_fact_count,
      canonical_hash
    FROM v1_bootstrap_conversion_identity_manifests
    WHERE tenant_id = ${sqlString(TENANT_ID)}
      AND snapshot_id = ${sqlString(snapshotId)}
    LIMIT 2
  `);
}

async function readCoordinatorStatus(
  token: string,
  pipeline: PipelineId,
): Promise<CoordinatorStatus> {
  const response = await fetch(
    `${WORKER_URL}/admin/fivetran/${pipeline}/status`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: "{}",
      redirect: "error",
      signal: AbortSignal.timeout(20_000),
    },
  );
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`Coordinator status failed with HTTP ${response.status}`);
  }
  const result: unknown = await response.json();
  if (!result || typeof result !== "object" || Array.isArray(result)) {
    throw new Error("Coordinator returned an invalid status");
  }
  return result as CoordinatorStatus;
}

function sameTimestamp(value: string | null, expected: string): boolean {
  return typeof value === "string" &&
    timestampMicros(value) === timestampMicros(expected);
}

function checkedCount(value: unknown, fieldName: string): number {
  if (!isCount(value)) throw new Error(`${fieldName} is not a safe count`);
  return value;
}

function isCount(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function isHash(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
}

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}

async function main(): Promise<void> {
  const activation = await prepareBaselineActivation(
    await readProductionMetadata(),
  );
  process.stdout.write(`${JSON.stringify(activation, null, 2)}\n`);
}

const invokedPath = process.argv[1]
  ? pathToFileURL(resolve(process.argv[1])).href
  : null;
if (invokedPath === import.meta.url) {
  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : "Unknown error";
    process.stderr.write(`${JSON.stringify({ ready: false, message })}\n`);
    process.exitCode = 1;
  });
}
