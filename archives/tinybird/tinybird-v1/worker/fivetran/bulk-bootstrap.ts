import {
  type ActiveCampaignReplacementNormalizer,
  type BootstrapIdentityManifest,
  type BootstrapIdentitySnapshotSealer,
  type BootstrapSourceReplacementPublisher,
  type BulkBootstrapCheckpoint,
  type BulkBootstrapCheckpointStore,
  type BulkBootstrapStateSeeder,
  type FivetranFactReader,
  type PipelineId,
  type PreparedScope,
  type StripeAccount,
  type StripeChargeNormalizer,
} from "./contracts.ts";
import { buildActiveCampaignFivetranScope } from "./activecampaign-fivetran.ts";
import { buildStripeFivetranScope } from "./stripe-fivetran.ts";
import { parseTimestamp } from "./timestamp.ts";

const DEFAULT_PAGE_SCOPES = 5_000;

export interface BulkBootstrapDependencies {
  reader: FivetranFactReader;
  publisher: BootstrapSourceReplacementPublisher;
  checkpoints: BulkBootstrapCheckpointStore;
  seeder: BulkBootstrapStateSeeder;
  stripeNormalizer: StripeChargeNormalizer;
  activeCampaignNormalizer: ActiveCampaignReplacementNormalizer;
}

export interface BulkBootstrapSliceResult extends BulkBootstrapCheckpoint {
  processedScopes: number;
  processedIdentityFacts: number;
}

/**
 * Publish one keyset page from the fixed-time snapshot. Publication, durable
 * state seeding, and checkpoint advancement are deliberately ordered so any
 * ambiguous failure retries the exact same replacement IDs and sequences.
 */
export async function runBulkBootstrapSlice(input: {
  snapshotId: string;
  snapshotAt: string;
  pipeline: PipelineId;
  dependencies: BulkBootstrapDependencies;
  pageScopes?: number;
}): Promise<BulkBootstrapSliceResult> {
  const snapshotId = requiredText(input.snapshotId, "snapshotId");
  const snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;
  const pageScopes = positiveInteger(input.pageScopes ?? DEFAULT_PAGE_SCOPES, "pageScopes");
  const { dependencies } = input;

  await dependencies.seeder.initializePipeline({
    pipeline: input.pipeline,
    snapshotAt,
  });

  const prior = await dependencies.checkpoints.loadBulkBootstrapCheckpoint({
    snapshotId,
    pipeline: input.pipeline,
  });
  const checkpoint = prior ?? initialCheckpoint({
    snapshotId,
    snapshotAt,
    pipeline: input.pipeline,
  });
  assertCheckpoint(checkpoint, { snapshotId, snapshotAt, pipeline: input.pipeline });

  if (checkpoint.complete) {
    await dependencies.seeder.finalizeBulkBootstrap({
      pipeline: input.pipeline,
      snapshotAt,
    });
    return { ...checkpoint, processedScopes: 0, processedIdentityFacts: 0 };
  }

  const page = await dependencies.reader.readBootstrapScopePage({
    pipeline: input.pipeline,
    snapshotAt,
    afterCursor: checkpoint.afterCursor,
    limit: pageScopes,
  });
  const prepared = await prepareBootstrapPage({
    snapshotId,
    snapshotAt,
    pipeline: input.pipeline,
    scopeIds: page.scopeIds,
    dependencies,
  });

  if (prepared.length) {
    await dependencies.publisher.publishBootstrapSourceReplacements({
      bootstrapId: snapshotId,
      replacements: prepared.map((item) => item.replacement),
    });
    await dependencies.seeder.seedBulkBootstrapScopes({
      pipeline: input.pipeline,
      snapshotAt,
      prepared,
    });
  }

  const identityFacts = prepared.reduce(
    (total, item) => total + item.replacement.rows.length,
    0,
  );
  const next: BulkBootstrapCheckpoint = {
    ...checkpoint,
    afterCursor: page.nextCursor,
    complete: page.eof,
    publishedScopes: checkpoint.publishedScopes + prepared.length,
    identityFactCount: checkpoint.identityFactCount + identityFacts,
  };

  await dependencies.checkpoints.saveBulkBootstrapCheckpoint(next);
  if (page.eof) {
    await dependencies.seeder.finalizeBulkBootstrap({
      pipeline: input.pipeline,
      snapshotAt,
    });
  }
  return {
    ...next,
    processedScopes: prepared.length,
    processedIdentityFacts: identityFacts,
  };
}

/** Run all three source namespaces. The caller seals identity after all return. */
export async function runBulkBootstrapToCompletion(input: {
  snapshotId: string;
  snapshotAt: string;
  dependencies: BulkBootstrapDependencies;
  pageScopes?: number;
  onProgress?: (result: BulkBootstrapSliceResult) => void | Promise<void>;
}): Promise<BulkBootstrapCheckpoint[]> {
  const completed: BulkBootstrapCheckpoint[] = [];

  for (const pipeline of [
    "stripe_main",
    "stripe_kajabi",
    "activecampaign",
  ] as const) {
    while (true) {
      const result = await runBulkBootstrapSlice({
        ...input,
        pipeline,
      });
      await input.onProgress?.(result);

      if (result.complete) {
        completed.push(result);
        break;
      }
    }
  }

  return completed;
}

/** Publish all three baselines, then seal the identity input exactly once. */
export async function runBulkBootstrapAndSeal(input: {
  snapshotId: string;
  snapshotAt: string;
  sealedAt: string;
  dependencies: BulkBootstrapDependencies;
  identitySealer: BootstrapIdentitySnapshotSealer;
  pageScopes?: number;
  onProgress?: (result: BulkBootstrapSliceResult) => void | Promise<void>;
}): Promise<{
  checkpoints: BulkBootstrapCheckpoint[];
  identity: BootstrapIdentityManifest;
}> {
  const checkpoints = await runBulkBootstrapToCompletion(input);
  const expectedDistinctFactCount = checkpoints.reduce(
    (total, checkpoint) => total + checkpoint.identityFactCount,
    0,
  );
  const identity = await input.identitySealer.sealBootstrapIdentitySnapshot({
    snapshotId: input.snapshotId,
    snapshotAt: input.snapshotAt,
    expectedDistinctFactCount,
    sealedAt: input.sealedAt,
  });
  return { checkpoints, identity };
}

async function prepareBootstrapPage(input: {
  snapshotId: string;
  snapshotAt: string;
  pipeline: PipelineId;
  scopeIds: string[];
  dependencies: BulkBootstrapDependencies;
}): Promise<PreparedScope[]> {
  if (!input.scopeIds.length) return [];

  if (input.pipeline === "activecampaign") {
    const contactIds = input.scopeIds.map((scopeId) =>
      removePrefix(scopeId, "activecampaign:contact:"),
    );
    const rawScopes = await input.dependencies.reader.readActiveCampaignScopes({
      contactIds,
      throughInclusive: input.snapshotAt,
      snapshotOnly: true,
    });
    const rawById = uniqueBy(rawScopes, (raw) => raw.contactId);

    return contactIds.map((contactId) => {
      const raw = rawById.get(contactId);
      if (!raw) throw new Error(`Reader omitted ActiveCampaign contact ${contactId}`);

      return buildActiveCampaignFivetranScope({
        raw,
        throughInclusive: input.snapshotAt,
        windowId: `bootstrap:${input.snapshotId}`,
        observationSequence: 1,
        previous: null,
        normalizeContactReplacement: input.dependencies.activeCampaignNormalizer,
        snapshotOnly: true,
      });
    });
  }

  const account: StripeAccount = input.pipeline === "stripe_main"
    ? "main"
    : "kajabi";
  const prefix = `stripe:${account}:charge:`;
  const chargeIds = input.scopeIds.map((scopeId) => removePrefix(scopeId, prefix));
  const rawScopes = await input.dependencies.reader.readStripeScopes({
    account,
    chargeIds,
    throughInclusive: input.snapshotAt,
    snapshotOnly: true,
  });
  const rawById = uniqueBy(rawScopes, (raw) => raw.chargeId);

  return chargeIds.map((chargeId) => {
    const raw = rawById.get(chargeId);
    if (!raw) throw new Error(`Reader omitted Stripe charge ${chargeId}`);

    return buildStripeFivetranScope({
      raw,
      throughInclusive: input.snapshotAt,
      windowId: `bootstrap:${input.snapshotId}`,
      observationSequence: 1,
      previous: null,
      normalizeCharge: input.dependencies.stripeNormalizer,
      snapshotOnly: true,
    });
  });
}

function initialCheckpoint(input: {
  snapshotId: string;
  snapshotAt: string;
  pipeline: PipelineId;
}): BulkBootstrapCheckpoint {
  return {
    ...input,
    afterCursor: "",
    complete: false,
    publishedScopes: 0,
    identityFactCount: 0,
  };
}

function assertCheckpoint(
  checkpoint: BulkBootstrapCheckpoint,
  expected: { snapshotId: string; snapshotAt: string; pipeline: PipelineId },
): void {
  if (
    checkpoint.snapshotId !== expected.snapshotId ||
    checkpoint.snapshotAt !== expected.snapshotAt ||
    checkpoint.pipeline !== expected.pipeline
  ) {
    throw new Error("Bulk bootstrap checkpoint belongs to another snapshot");
  }
}

function uniqueBy<Value>(
  values: readonly Value[],
  getId: (value: Value) => string,
): Map<string, Value> {
  const result = new Map<string, Value>();

  for (const value of values) {
    const id = getId(value);
    if (result.has(id)) throw new Error(`Reader returned duplicate scope ${id}`);
    result.set(id, value);
  }

  return result;
}

function removePrefix(value: string, prefix: string): string {
  if (!value.startsWith(prefix) || value.length === prefix.length) {
    throw new Error(`${value} does not start with ${prefix}`);
  }

  return value.slice(prefix.length);
}

function requiredText(value: string, fieldName: string): string {
  if (!value.trim()) throw new TypeError(`${fieldName} is required`);
  return value.trim();
}

function positiveInteger(value: number, fieldName: string): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > 20_000) {
    throw new TypeError(`${fieldName} must be an integer from 1 to 20000`);
  }

  return value;
}
