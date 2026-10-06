import {
  PIPELINE_TABLES,
  type BootstrapSourceReplacementPublisher,
  type ChangeWindow,
  type FactBuilders,
  type FactCoordinatorStore,
  type FivetranFactReader,
  type PipelineId,
  type PreparedScope,
  type SourceReplacementPublisher,
  type StripeAccount,
} from "./contracts.ts";
import { buildActiveCampaignFivetranScope } from "./activecampaign-fivetran.ts";
import { buildStripeFivetranScope } from "./stripe-fivetran.ts";
import { compareTimestamps, parseTimestamp } from "./timestamp.ts";

const DEFAULT_DISCOVERY_PAGE = 5_000;
const DEFAULT_PUBLICATION_BATCH = 100;

export interface MachineDependencies {
  reader: FivetranFactReader;
  store: FactCoordinatorStore;
  publisher: SourceReplacementPublisher;
  bootstrapPublisher: BootstrapSourceReplacementPublisher;
  builders: FactBuilders;
  baselineScopeFacts?: (scopeIds: string[]) => Promise<Map<string, import("../identity/engine.ts").PendingIdentityFact[]>>;
}

export interface SliceLimits {
  discoveryPageSize?: number;
  publicationBatchSize?: number;
  maxDiscoveryPages?: number;
  maxPublicationBatches?: number;
  deadlineAtMs?: number;
  now?: () => number;
}

export interface SliceResult {
  pipeline: PipelineId;
  phase: "idle" | "discovering" | "publishing" | "complete";
  windowId: string | null;
  discoveredPages: number;
  publishedScopes: number;
  completedObservationAt: string | null;
}

export async function runBootstrapSlice(input: {
  pipeline: PipelineId;
  snapshotAt: string;
  dependencies: MachineDependencies;
  limits?: SliceLimits;
}): Promise<SliceResult> {
  const { pipeline, dependencies } = input;
  const snapshotAt = parseTimestamp(input.snapshotAt, "snapshotAt").iso;
  const limits = normalizedLimits(input.limits);

  await dependencies.store.initializePipeline({ pipeline, snapshotAt });
  const pipelineState = await dependencies.store.getPipeline(pipeline);

  if (pipelineState.bootstrapComplete) {
    return result(pipeline, "complete", null, 0, 0, pipelineState.completedObservationAt);
  }

  const window = pipelineState.activeWindow ??
    await dependencies.store.beginBootstrap({ pipeline, snapshotAt });
  let discoveredPages = 0;
  let publishedScopes = 0;

  while (!deadlineReached(limits)) {
    const published = await publishOneBatch({
      window,
      snapshotOnly: true,
      dependencies,
      batchSize: limits.publicationBatchSize,
      bootstrapId: window.id,
    });
    publishedScopes += published;

    if (published > 0) {
      if (publishedScopes >= limits.publicationBatchSize * limits.maxPublicationBatches) {
        return result(
          pipeline,
          "publishing",
          window.id,
          discoveredPages,
          publishedScopes,
          pipelineState.completedObservationAt,
        );
      }
      continue;
    }

    const discovery = await dependencies.store.getDiscovery({
      windowId: window.id,
      table: "bootstrap_scopes",
    });

    if (discovery.eof) {
      await dependencies.store.finishWindow(window);
      return result(
        pipeline,
        "complete",
        window.id,
        discoveredPages,
        publishedScopes,
        snapshotAt,
      );
    }

    if (discoveredPages >= limits.maxDiscoveryPages) {
      return result(
        pipeline,
        "discovering",
        window.id,
        discoveredPages,
        publishedScopes,
        pipelineState.completedObservationAt,
      );
    }

    const page = await dependencies.reader.readBootstrapScopePage({
      pipeline,
      snapshotAt,
      afterCursor: discovery.cursor,
      limit: limits.discoveryPageSize,
    });
    await dependencies.store.recordDiscoveryPage({
      window,
      table: "bootstrap_scopes",
      expectedCursor: discovery.cursor,
      page,
    });
    discoveredPages += 1;
  }

  return result(
    pipeline,
    "publishing",
    window.id,
    discoveredPages,
    publishedScopes,
    pipelineState.completedObservationAt,
  );
}

export async function runIncrementalSlice(input: {
  pipeline: PipelineId;
  dependencies: MachineDependencies;
  limits?: SliceLimits;
}): Promise<SliceResult> {
  const { pipeline, dependencies } = input;
  const limits = normalizedLimits(input.limits);
  const pipelineState = await dependencies.store.getPipeline(pipeline);

  if (!pipelineState.bootstrapComplete || !pipelineState.completedObservationAt) {
    throw new Error(`${pipeline} bootstrap must finish before incremental work`);
  }

  let window = pipelineState.activeWindow;

  if (!window) {
    const barrier = await dependencies.reader.verifiedObservationThrough(
      PIPELINE_TABLES[pipeline],
    );

    if (compareTimestamps(barrier, pipelineState.completedObservationAt) <= 0) {
      return result(
        pipeline,
        "idle",
        null,
        0,
        0,
        pipelineState.completedObservationAt,
      );
    }

    window = await dependencies.store.beginIncrementalWindow({
      pipeline,
      afterExclusive: pipelineState.completedObservationAt,
      throughInclusive: barrier,
    });
  }

  let discoveredPages = 0;

  for (const table of PIPELINE_TABLES[pipeline]) {
    while (!deadlineReached(limits)) {
      const discovery = await dependencies.store.getDiscovery({
        windowId: window.id,
        table,
      });
      if (discovery.eof) break;

      if (discoveredPages >= limits.maxDiscoveryPages) {
        return result(
          pipeline,
          "discovering",
          window.id,
          discoveredPages,
          0,
          pipelineState.completedObservationAt,
        );
      }

      const page = await dependencies.reader.readChangedScopePage({
        pipeline,
        table,
        afterExclusive: window.afterExclusive,
        throughInclusive: window.throughInclusive,
        afterCursor: discovery.cursor,
        limit: limits.discoveryPageSize,
      });
      await dependencies.store.recordDiscoveryPage({
        window,
        table,
        expectedCursor: discovery.cursor,
        page,
      });
      discoveredPages += 1;
    }
  }

  if (!(await discoveryComplete(dependencies.store, window))) {
    return result(
      pipeline,
      "discovering",
      window.id,
      discoveredPages,
      0,
      pipelineState.completedObservationAt,
    );
  }

  let publishedScopes = 0;

  while (!deadlineReached(limits)) {
    const published = await publishOneBatch({
      window,
      snapshotOnly: false,
      dependencies,
      batchSize: limits.publicationBatchSize,
      bootstrapId: null,
    });
    publishedScopes += published;

    if (published === 0) {
      await dependencies.store.finishWindow(window);
      return result(
        pipeline,
        "complete",
        window.id,
        discoveredPages,
        publishedScopes,
        window.throughInclusive,
      );
    }

    if (publishedScopes >= limits.publicationBatchSize * limits.maxPublicationBatches) {
      break;
    }
  }

  return result(
    pipeline,
    "publishing",
    window.id,
    discoveredPages,
    publishedScopes,
    pipelineState.completedObservationAt,
  );
}

async function publishOneBatch(input: {
  window: ChangeWindow;
  snapshotOnly: boolean;
  dependencies: MachineDependencies;
  batchSize: number;
  bootstrapId: string | null;
}): Promise<number> {
  const { window, dependencies } = input;
  let prepared = await dependencies.store.listPreparedScopes(
    window.id,
    input.batchSize,
  );

  if (!prepared.length) {
    const scopeIds = await dependencies.store.listUnpreparedScopes(
      window.id,
      input.batchSize,
    );

    const preparedScopes = await prepareScopes({
      window,
      scopeIds,
      snapshotOnly: input.snapshotOnly,
      dependencies,
    });

    for (const next of preparedScopes) {
      await dependencies.store.savePreparedScope(next);
    }

    prepared = await dependencies.store.listPreparedScopes(
      window.id,
      input.batchSize,
    );
  }

  if (!prepared.length) return 0;

  const replacements = prepared.map((item) => item.replacement);

  if (input.bootstrapId) {
    await dependencies.bootstrapPublisher.publishBootstrapSourceReplacements({
      bootstrapId: input.bootstrapId,
      replacements,
    });
  } else {
    await dependencies.publisher.publishSourceReplacements(replacements);
  }
  await dependencies.store.markPublished(prepared);
  return prepared.length;
}

async function prepareScopes(input: {
  window: ChangeWindow;
  scopeIds: string[];
  snapshotOnly: boolean;
  dependencies: MachineDependencies;
}): Promise<PreparedScope[]> {
  const { window, scopeIds, dependencies } = input;
  const baselineFacts = dependencies.baselineScopeFacts
    ? await dependencies.baselineScopeFacts(scopeIds)
    : new Map();
  const contexts = await Promise.all(scopeIds.map(async (scopeId) => ({
    scopeId,
    observationSequence: await dependencies.store.reserveScope({
      windowId: window.id,
      scopeId,
    }),
    current: await currentScopeState(dependencies, window.pipeline, scopeId, baselineFacts),
  })));

  if (window.pipeline === "activecampaign") {
    const contactIds = contexts.map(({ scopeId }) =>
      removePrefix(scopeId, "activecampaign:contact:"),
    );
    const rawScopes = await dependencies.reader.readActiveCampaignScopes({
      contactIds,
      throughInclusive: window.throughInclusive,
      snapshotOnly: input.snapshotOnly,
    });
    const rawById = uniqueBy(rawScopes, (raw) => raw.contactId, "contact");

    return Promise.all(contexts.map(async (context) => {
      const contactId = removePrefix(context.scopeId, "activecampaign:contact:");
      const raw = rawById.get(contactId);
      if (!raw) throw new Error(`Reader omitted ActiveCampaign contact ${contactId}`);

      const prepared = buildActiveCampaignFivetranScope({
        raw,
        throughInclusive: window.throughInclusive,
        windowId: window.id,
        observationSequence: context.observationSequence,
        previous: context.current,
        normalizeContactReplacement: dependencies.builders.activeCampaignNormalizer,
        snapshotOnly: input.snapshotOnly,
      });
      return attachIdentityProjection(prepared, raw, context.current?.compactState, window.throughInclusive, dependencies, "activecampaign");
    }));
  }

  const account: StripeAccount = window.pipeline === "stripe_main"
    ? "main"
    : "kajabi";
  const chargeIds = contexts.map(({ scopeId }) =>
    removePrefix(scopeId, `stripe:${account}:charge:`),
  );
  const rawScopes = await dependencies.reader.readStripeScopes({
    account,
    chargeIds,
    throughInclusive: window.throughInclusive,
    snapshotOnly: input.snapshotOnly,
  });
  const rawById = uniqueBy(rawScopes, (raw) => raw.chargeId, "charge");

  return Promise.all(contexts.map(async (context) => {
    const chargeId = removePrefix(context.scopeId, `stripe:${account}:charge:`);
    const raw = rawById.get(chargeId);
    if (!raw) throw new Error(`Reader omitted Stripe charge ${chargeId}`);

    const prepared = buildStripeFivetranScope({
      raw,
      throughInclusive: window.throughInclusive,
      windowId: window.id,
      observationSequence: context.observationSequence,
      previous: context.current,
      normalizeCharge: dependencies.builders.stripeNormalizer,
      snapshotOnly: input.snapshotOnly,
    });
    return attachIdentityProjection(prepared, raw, context.current?.compactState, window.throughInclusive, dependencies, "stripe");
  }));
}

async function currentScopeState(
  dependencies: MachineDependencies,
  pipeline: PipelineId,
  scopeId: string,
  baseline: Map<string, import("../identity/engine.ts").PendingIdentityFact[]>,
) {
  const stored = await dependencies.store.getScopeState({ pipeline, scopeId });
  if (stored) return stored;
  const facts = baseline.get(scopeId);
  if (!facts) {
    if (dependencies.baselineScopeFacts) throw new Error(`Identity baseline omitted scope proof ${scopeId}`);
    return null;
  }
  return { rows: [], compactState: { identityFacts: facts }, observationSequence: 0 };
}

async function attachIdentityProjection(
  prepared: PreparedScope,
  raw: import("./contracts.ts").StripeRawScope | import("./contracts.ts").ActiveCampaignRawScope,
  previousState: Record<string, unknown> | undefined,
  observedAt: string,
  dependencies: MachineDependencies,
  kind: "stripe" | "activecampaign",
): Promise<PreparedScope> {
  const replace = dependencies.builders.replaceIdentityScope;
  const projector = kind === "stripe"
    ? dependencies.builders.stripeIdentityProjector
    : dependencies.builders.activeCampaignIdentityProjector;
  if (!replace || !projector) return prepared;
  const current = await projector(raw as never, observedAt);
  const previous = Array.isArray(previousState?.identityFacts)
    ? previousState.identityFacts as import("../identity/engine.ts").PendingIdentityFact[]
    : [];
  const facts = await replace(current, previous, observedAt);
  return {
    ...prepared,
    replacement: { ...prepared.replacement, rows: [], source_evidence: { identityFacts: facts } },
    compactState: { identityFacts: current },
  };
}

function uniqueBy<Item>(
  values: readonly Item[],
  key: (value: Item) => string,
  kind: string,
): Map<string, Item> {
  const result = new Map<string, Item>();

  for (const value of values) {
    const id = key(value);
    if (result.has(id)) throw new Error(`Reader returned duplicate ${kind} ${id}`);
    result.set(id, value);
  }

  return result;
}

async function discoveryComplete(
  store: FactCoordinatorStore,
  window: ChangeWindow,
): Promise<boolean> {
  for (const table of PIPELINE_TABLES[window.pipeline]) {
    const state = await store.getDiscovery({ windowId: window.id, table });
    if (!state.eof) return false;
  }

  return true;
}

function removePrefix(value: string, prefix: string): string {
  if (!value.startsWith(prefix) || value.length === prefix.length) {
    throw new Error(`${value} is not a ${prefix} scope`);
  }

  return value.slice(prefix.length);
}

function normalizedLimits(input: SliceLimits | undefined): Required<SliceLimits> {
  const limits = {
    discoveryPageSize: input?.discoveryPageSize ?? DEFAULT_DISCOVERY_PAGE,
    publicationBatchSize: input?.publicationBatchSize ?? DEFAULT_PUBLICATION_BATCH,
    maxDiscoveryPages: input?.maxDiscoveryPages ?? 3,
    maxPublicationBatches: input?.maxPublicationBatches ?? 1,
    deadlineAtMs: input?.deadlineAtMs ?? Number.POSITIVE_INFINITY,
    now: input?.now ?? Date.now,
  };

  for (const [name, value] of Object.entries(limits)) {
    if (name === "deadlineAtMs" || name === "now") continue;
    if (!Number.isSafeInteger(value) || Number(value) < 1) {
      throw new TypeError(`${name} must be a positive integer`);
    }
  }

  return limits as Required<SliceLimits>;
}

function deadlineReached(limits: Required<SliceLimits>): boolean {
  return limits.now() >= limits.deadlineAtMs;
}

function result(
  pipeline: PipelineId,
  phase: SliceResult["phase"],
  windowId: string | null,
  discoveredPages: number,
  publishedScopes: number,
  completedObservationAt: string | null,
): SliceResult {
  return {
    pipeline,
    phase,
    windowId,
    discoveredPages,
    publishedScopes,
    completedObservationAt,
  };
}
