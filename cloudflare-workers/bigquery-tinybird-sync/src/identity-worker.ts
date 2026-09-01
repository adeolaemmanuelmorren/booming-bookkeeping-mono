import {
  affectedIdentifierKeys,
  changedIdentityFacts,
  runIdentityEngine,
  type IdentityEngineResult,
} from "./identity-engine";
import {
  appendIdentityBatchOutputRows,
  appendIdentityJournalRows,
  appendJourneyIdentityQueueRow,
  identityBatchOutputRows,
  readCurrentIdentityEvidence,
  readCurrentIdentityFacts,
  readCurrentIdentityMappings,
  readCurrentIdentityProfiles,
  readIdentityCompactionManifest,
  readPendingIdentityFacts,
  type IdentityCompactionPosition,
  type IdentityCompactionManifest,
  type TinybirdApiConfig,
} from "./tinybird-api";
import type { Fetcher } from "./bigquery";

export interface IdentityWorkerBatchInput {
  tenantId: string;
  batchVersion: number;
  batchId: string;
  batchLimit: number;
  cursor: IdentityCompactionPosition;
}

export interface IdentityWorkerBatchResult {
  engine: IdentityEngineResult;
  manifest: IdentityCompactionManifest;
  journeyProfileIds: string[];
  journeyConversionIds: string[];
}

export async function processIdentityWorkerBatch(
  input: IdentityWorkerBatchInput,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<IdentityWorkerBatchResult> {
  const engine = await computeIdentityWorkerBatch(input, config, fetcher);
  const manifest = await commitIdentityWorkerBatch(input, engine, config, fetcher);
  return {
    engine,
    manifest,
    journeyProfileIds: affectedJourneyProfileIds(engine),
    journeyConversionIds: affectedJourneyConversionIds(engine),
  };
}

export function affectedJourneyProfileIds(engine: IdentityEngineResult): string[] {
  return uniqueStrings(engine.rows.flatMap((row) => [
    ...row.prior_profile_ids,
    ...row.dirty_profile_ids,
  ]));
}

export function affectedJourneyIdentifierKeys(engine: IdentityEngineResult): string[] {
  return uniqueStrings(engine.rows.flatMap((row) => [
    row.identifier_key,
    ...row.member_identifier_keys,
  ]));
}

export function orphanedJourneyIdentifierKeys(engine: IdentityEngineResult): string[] {
  const liveKeys = new Set(engine.rows
    .filter((row) => !row.is_deleted)
    .flatMap((row) => [row.identifier_key, ...row.member_identifier_keys]));
  return uniqueStrings(engine.rows
    .filter((row) => row.is_deleted === 1)
    .map((row) => row.identifier_key))
    .filter((key) => !liveKeys.has(key));
}

export function affectedJourneyConversionIds(engine: IdentityEngineResult): string[] {
  return uniqueStrings(engine.changedFacts.flatMap((fact) => (
    conversionIdsForIdentityFact(fact.factPayload)
  )));
}

function conversionIdsForIdentityFact(factPayload: string): string[] {
  let payload: unknown;
  try {
    payload = JSON.parse(factPayload) as unknown;
  } catch {
    return [];
  }
  if (!isRecord(payload)) return [];

  const sourceSystem = payload.source_system;
  const sourceRecordId = payload.source_record_id;
  if (typeof sourceSystem !== "string" || typeof sourceRecordId !== "string") {
    return [];
  }
  if (!sourceRecordId) return [];

  switch (sourceSystem) {
    case "activecampaign":
      return [`server_form:${sourceRecordId}`];
    case "segment_form":
      return [
        `client_form:${sourceRecordId}`,
        `client_payment:historical_form_${sourceRecordId}`,
      ];
    case "segment_order_completed":
      return [`client_payment:segment_order_${sourceRecordId}`];
    case "stripe":
    case "stripe_kajabi":
      return [`server_payment:${sourceSystem}:${sourceRecordId}`];
    default:
      return [];
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export async function computeIdentityWorkerBatch(
  input: IdentityWorkerBatchInput,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<IdentityEngineResult> {
  const pendingFacts = await readPendingIdentityFacts(
    input.tenantId,
    input.cursor,
    input.batchLimit,
    config,
    fetcher,
  );
  const checkpoint = batchCheckpoint(pendingFacts, input.cursor);

  const pendingFactKeys = pendingFacts.map((fact) => fact.factKey);
  const pendingCurrentFacts = await readCurrentIdentityFacts(
    input.tenantId,
    pendingFactKeys,
    config,
    fetcher,
  );
  const changedFacts = changedIdentityFacts(pendingFacts, pendingCurrentFacts);
  const initialIdentifiers = affectedIdentifierKeys(changedFacts, pendingCurrentFacts);

  const initialMappings = await readCurrentIdentityMappings(
    input.tenantId,
    initialIdentifiers,
    config,
    fetcher,
  );
  const touchedProfileIds = initialMappings.map((mapping) => mapping.profileId);
  const currentProfiles = await readCurrentIdentityProfiles(
    input.tenantId,
    touchedProfileIds,
    config,
    fetcher,
  );

  const affectedIdentifiers = uniqueStrings([
    ...initialIdentifiers,
    ...currentProfiles.flatMap((profile) => profile.memberIdentifierKeys),
  ]);
  const currentMappings = await readCurrentIdentityMappings(
    input.tenantId,
    affectedIdentifiers,
    config,
    fetcher,
  );
  const evidence = await readCurrentIdentityEvidence(
    input.tenantId,
    affectedIdentifiers,
    config,
    fetcher,
  );
  const retainedFactKeys = uniqueStrings([
    ...changedFacts.map((fact) => fact.factKey),
    ...evidence.map((pair) => pair.factKey),
  ]);
  const currentFacts = await readCurrentIdentityFacts(
    input.tenantId,
    retainedFactKeys,
    config,
    fetcher,
  );

  const engine = await runIdentityEngine({
    tenantId: input.tenantId,
    batchVersion: input.batchVersion,
    batchId: input.batchId,
    committedAt: workerTimestamp(),
    pendingFacts,
    currentFacts,
    currentMappings,
    currentProfiles,
    checkpointIngestedAt: checkpoint.ingestedAt,
    checkpointEventId: checkpoint.eventId,
  });
  engine.diagnostics = {
    pendingFactKeys: new Set(pendingFactKeys).size,
    pendingCurrentFacts: pendingCurrentFacts.length,
    initialIdentifiers: initialIdentifiers.length,
    initialMappings: initialMappings.length,
    touchedProfiles: currentProfiles.length,
    affectedIdentifiers: affectedIdentifiers.length,
    currentMappings: currentMappings.length,
    evidencePairs: evidence.length,
    retainedFactKeys: retainedFactKeys.length,
    currentFacts: currentFacts.length,
  };
  return engine;
}

export async function commitIdentityWorkerBatch(
  input: IdentityWorkerBatchInput,
  engine: IdentityEngineResult,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<IdentityCompactionManifest> {
  await appendIdentityJournalRows(engine.rows, config, fetcher);
  await appendIdentityBatchOutputRows(
    identityBatchOutputRows(engine.rows),
    config,
    fetcher,
  );
  await appendJourneyIdentityQueueRow({
    tenantId: input.tenantId,
    batchVersion: input.batchVersion,
    batchId: input.batchId,
    committedAt: engine.manifest.committed_at,
    profileIds: affectedJourneyProfileIds(engine),
    orphanIdentifierKeys: orphanedJourneyIdentifierKeys(engine),
    conversionIds: affectedJourneyConversionIds(engine),
  }, config, fetcher);
  await appendIdentityJournalRows([engine.manifest], config, fetcher);

  return waitForIdentityManifest(input, engine, config, fetcher);
}

async function waitForIdentityManifest(
  input: IdentityWorkerBatchInput,
  engine: IdentityEngineResult,
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<IdentityCompactionManifest> {
  const pollIntervalMs = 1_000;
  const pollAttempts = 31;
  let finalError: unknown;

  for (let attempt = 0; attempt < pollAttempts; attempt += 1) {
    if (attempt > 0) await delay(pollIntervalMs);

    try {
      const manifest = await readIdentityCompactionManifest(
        input.tenantId,
        input.batchVersion,
        input.batchId,
        config,
        fetcher,
      );
      assertManifestMatchesEngine(manifest, engine);
      return manifest;
    } catch (error) {
      finalError = error;
    }
  }

  throw finalError;
}

function batchCheckpoint(
  pendingFacts: { ingestedAt: string; eventId: string }[],
  cursor: IdentityCompactionPosition,
): { ingestedAt: string; eventId: string } {
  const final = pendingFacts.at(-1);
  if (!final) {
    return {
      ingestedAt: cursor.checkpointIngestedAt,
      eventId: cursor.checkpointEventId,
    };
  }
  return { ingestedAt: final.ingestedAt, eventId: final.eventId };
}

function assertManifestMatchesEngine(
  manifest: IdentityCompactionManifest,
  engine: IdentityEngineResult,
): void {
  if (!manifest.isValid) throw new Error("Identity Worker manifest is invalid.");
  if (manifest.inputHash !== engine.inputHash) {
    throw new Error("Identity Worker manifest input hash does not match the engine.");
  }
  if (manifest.actualOutputRowCount !== engine.rows.length) {
    throw new Error("Identity Worker manifest row count does not match the engine.");
  }
  if (manifest.actualOutputHash !== engine.outputHash) {
    throw new Error("Identity Worker manifest output hash does not match the engine.");
  }
}

function workerTimestamp(): string {
  return new Date().toISOString();
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values)].sort();
}
