import type { Fetcher } from "./bigquery";
import type {
  CurrentIdentityFact,
  CurrentIdentityMapping,
  CurrentIdentityProfile,
  IdentityJournalRow,
  PendingIdentityFact,
} from "./identity-engine";

const ACTIVE_INGESTION_STATUSES = ["waiting", "working"] as const;
const INGESTION_JOB_KINDS = ["gcs_sync", "import"] as const;

export interface TinybirdApiConfig {
  apiUrl: string;
  adminToken: string;
  fetchTimeoutMs: number;
}

export class TinybirdRequestError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly retryAfterMs: number | null,
  ) {
    super(message);
    this.name = "TinybirdRequestError";
  }
}

export const IDENTITY_ACTIVATION_NOT_VISIBLE_PREFIX =
  "Identity activation is not visible yet for batch ";

export class IdentityActivationNotVisibleError extends Error {
  constructor(batchId: string) {
    super(`${IDENTITY_ACTIVATION_NOT_VISIBLE_PREFIX}${batchId}.`);
    this.name = "IdentityActivationNotVisibleError";
  }
}

export interface PendingJourneyBatch {
  tenantId: string;
  batchVersion: number;
  batchId: string;
  profileIds: string[];
  orphanIdentifierKeys: string[];
  conversionIds: string[];
}

export interface TinybirdCopyJob {
  id: string;
  status: "waiting" | "working" | "done" | "error" | "cancelled" | "cancelling";
  error?: string;
}

export interface IdentityCompactionPosition {
  activeBatchVersion: number;
  checkpointIngestedAt: string;
  checkpointEventId: string;
}

export interface IdentityCompactionCursor extends IdentityCompactionPosition {
  activeBatchId: string;
}

export interface IdentityCompactionManifest {
  tenantId: string;
  batchVersion: number;
  batchId: string;
  inputEventCount: number;
  inputHash: string;
  checkpointIngestedAt: string;
  checkpointEventId: string;
  expectedOutputRowCount: number;
  expectedOutputHash: string;
  actualOutputRowCount: number;
  actualOutputHash: string;
  isValid: boolean;
}

export interface IdentityActivationInput {
  tenantId: string;
  manifest: IdentityCompactionManifest;
}

interface CopyRunResponse {
  job?: {
    job_id?: unknown;
  };
}

interface JobsResponse {
  jobs?: unknown;
}

interface RowsResponse {
  data?: unknown;
}

type PipeParameters = Readonly<Record<string, string | string[]>>;

export interface IdentityEvidencePair {
  identifierKey: string;
  factKey: string;
}

export interface IdentityBatchOutputRow {
  tenant_id: string;
  batch_version: number;
  batch_id: string;
  state_kind: string;
  lookup_key: string;
  sub_key: string;
  row_hash: string;
}

export async function submitCopyJob(
  pipeName: string,
  config: TinybirdApiConfig,
  parameters: Readonly<Record<string, string>> = {},
  fetcher: Fetcher = fetch,
): Promise<string> {
  validateResourceName(pipeName, "Copy pipe");
  const query = copyParametersQuery(parameters);
  const response = await tinybirdRequest(
    `/v0/pipes/${encodeURIComponent(pipeName)}/copy${query}`,
    { method: "POST" },
    config,
    fetcher,
  );
  const body = await readJson<CopyRunResponse>(response, `${pipeName} Copy submission`);
  const jobId = body.job?.job_id;

  if (typeof jobId === "string" && jobId) return jobId;
  throw new Error(`${pipeName} Copy submission did not return a job ID.`);
}

function copyParametersQuery(parameters: Readonly<Record<string, string>>): string {
  const entries = Object.entries(parameters)
    .sort(([left], [right]) => left.localeCompare(right));
  if (entries.length === 0) return "";

  const query = new URLSearchParams();
  for (const [name, value] of entries) {
    validateCopyParameter(name, value);
    query.set(name, value);
  }

  return `?${query.toString()}`;
}

function validateCopyParameter(name: string, value: string): void {
  if (!/^[a-z][a-z0-9_]{0,99}$/.test(name)) {
    throw new Error("Tinybird Copy parameter name is invalid.");
  }
  if (typeof value !== "string" || value.length > 500) {
    throw new Error(`Tinybird Copy parameter ${name} is invalid.`);
  }
}

export async function getCopyJob(
  jobId: string,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<TinybirdCopyJob> {
  validateJobId(jobId);
  const response = await tinybirdRequest(
    `/v0/jobs/${encodeURIComponent(jobId)}`,
    { method: "GET" },
    config,
    fetcher,
  );
  const body = await readJson<Record<string, unknown>>(response, `Tinybird job ${jobId}`);
  const status = body.status;

  if (!isJobStatus(status)) {
    throw new Error(`Tinybird job ${jobId} returned an unknown status.`);
  }

  return {
    id: jobId,
    status,
    error: jobError(body),
  };
}

export async function countActiveIngestionJobs(
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<number> {
  const requests = ACTIVE_INGESTION_STATUSES.flatMap((status) => (
    INGESTION_JOB_KINDS.map((kind) => listJobs(status, kind, config, fetcher))
  ));
  const results = await Promise.all(requests);

  return results.reduce((total, jobs) => total + jobs.length, 0);
}

export async function readIdentityCompactionCursor(
  tenantId: string,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<IdentityCompactionCursor> {
  validateTenantId(tenantId);
  const rows = await readPipeRows(
    "current_identity_compaction_cursor",
    { p_tenant_id: tenantId },
    config,
    fetcher,
  );
  const row = firstRow(rows, "identity compaction cursor");

  return {
    activeBatchVersion: nonNegativeInteger(
      row.active_batch_version,
      "active_batch_version",
    ),
    activeBatchId: requiredString(row.active_batch_id, "active_batch_id"),
    checkpointIngestedAt: requiredString(
      row.checkpoint_ingested_at,
      "checkpoint_ingested_at",
    ),
    checkpointEventId: fixedHash(
      row.checkpoint_event_id,
      "checkpoint_event_id",
    ),
  };
}

export async function readIdentityCompactionManifest(
  tenantId: string,
  batchVersion: number,
  batchId: string,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<IdentityCompactionManifest> {
  validateTenantId(tenantId);
  validateBatchVersion(batchVersion);
  validateBatchId(batchId);
  const rows = await readPipeRows(
    "identity_compaction_manifest",
    {
      p_tenant_id: tenantId,
      p_batch_version: String(batchVersion),
      p_batch_id: batchId,
    },
    config,
    fetcher,
  );
  const row = firstRow(rows, "identity compaction manifest");

  const manifest: IdentityCompactionManifest = {
    tenantId: requiredString(row.tenant_id, "tenant_id"),
    batchVersion: nonNegativeInteger(row.batch_version, "batch_version"),
    batchId: requiredString(row.batch_id, "batch_id"),
    inputEventCount: nonNegativeInteger(row.input_event_count, "input_event_count"),
    inputHash: fixedHash(row.input_hash, "input_hash"),
    checkpointIngestedAt: requiredString(
      row.checkpoint_ingested_at,
      "checkpoint_ingested_at",
    ),
    checkpointEventId: fixedHash(row.checkpoint_event_id, "checkpoint_event_id"),
    expectedOutputRowCount: nonNegativeInteger(
      row.expected_output_row_count,
      "expected_output_row_count",
    ),
    expectedOutputHash: fixedHash(
      row.expected_output_hash,
      "expected_output_hash",
    ),
    actualOutputRowCount: nonNegativeInteger(
      row.actual_output_row_count,
      "actual_output_row_count",
    ),
    actualOutputHash: fixedHash(row.actual_output_hash, "actual_output_hash"),
    isValid: booleanValue(row.is_valid, "is_valid"),
  };
  if (
    manifest.tenantId !== tenantId
    || manifest.batchVersion !== batchVersion
    || manifest.batchId !== batchId
  ) {
    throw new Error("Tinybird identity compaction manifest returned the wrong batch.");
  }

  return manifest;
}

export async function appendIdentityActivation(
  input: IdentityActivationInput,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  validateTenantId(input.tenantId);
  validateManifestForActivation(input);
  const event = identityActivationEvent(input);

  await tinybirdRequest(
    "/v0/events?name=identity_state_delta_versions&wait=true",
    {
      method: "POST",
      headers: { "Content-Type": "application/x-ndjson; charset=utf-8" },
      body: `${JSON.stringify(event)}\n`,
    },
    config,
    fetcher,
  );
  await waitForIdentityActivation(input, config, fetcher);
}

async function waitForIdentityActivation(
  input: IdentityActivationInput,
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<void> {
  const retryDelaysMs = [0, 1_000, 2_000, 4_000, 8_000, 16_000];

  for (const delayMs of retryDelaysMs) {
    if (delayMs > 0) await delay(delayMs);

    const cursor = await readIdentityCompactionCursor(
      input.tenantId,
      config,
      fetcher,
    );
    if (
      cursor.activeBatchVersion === input.manifest.batchVersion
      && cursor.activeBatchId === input.manifest.batchId
    ) {
      return;
    }
    if (cursor.activeBatchVersion <= input.manifest.batchVersion) continue;

    throw new Error("A newer identity batch became active during activation.");
  }

  throw new IdentityActivationNotVisibleError(input.manifest.batchId);
}

export async function readPendingIdentityFacts(
  tenantId: string,
  cursor: IdentityCompactionPosition,
  batchLimit: number,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<PendingIdentityFact[]> {
  validateTenantId(tenantId);
  if (!Number.isSafeInteger(batchLimit) || batchLimit < 1 || batchLimit > 5_000) {
    throw new Error("Identity batch limit is invalid.");
  }

  const rows = await readPipeRows(
    "identity_worker_pending_facts",
    {
      p_tenant_id: tenantId,
      p_cursor_ingested_at: cursor.checkpointIngestedAt,
      p_cursor_event_id: cursor.checkpointEventId,
      p_batch_limit: String(batchLimit),
    },
    config,
    fetcher,
  );

  return rows.map(parsePendingIdentityFact);
}

export async function readSourceIdentityFacts(
  producerId: string,
  sourceIngestedFrom: string,
  afterFactKey: string,
  batchLimit: number,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
  sourceIngestedTo?: string,
): Promise<PendingIdentityFact[]> {
  if (!producerId.startsWith("source_identity:")) {
    throw new Error("Identity source producer is invalid.");
  }
  if (!Number.isSafeInteger(batchLimit) || batchLimit < 1 || batchLimit > 1_000) {
    throw new Error("Identity source batch limit is invalid.");
  }

  const parameters: Record<string, string> = {
    p_identity_producer: producerId,
    p_source_ingested_from: sourceIngestedFrom,
    p_after_fact_key: afterFactKey,
    p_batch_limit: String(batchLimit),
  };
  if (sourceIngestedTo) parameters.p_source_ingested_to = sourceIngestedTo;

  const rows = await readPipeRows(
    "identity_worker_source_facts",
    parameters,
    config,
    fetcher,
  );

  return rows.map(parsePendingIdentityFact);
}

export async function readCurrentIdentityFacts(
  tenantId: string,
  factKeys: string[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<CurrentIdentityFact[]> {
  validateTenantId(tenantId);
  const keys = uniqueStrings(factKeys);
  if (keys.length === 0) return [];

  const rows = await readLiteralKeyChunks(
    "identity_worker_fact_heads",
    keys,
    (chunk) => literalFactParameters(tenantId, chunk),
    config,
    fetcher,
  );
  return rows.map(parseCurrentIdentityFact);
}

export async function readCurrentIdentityMappings(
  tenantId: string,
  identifierKeys: string[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<CurrentIdentityMapping[]> {
  validateTenantId(tenantId);
  const keys = uniqueStrings(identifierKeys);
  if (keys.length === 0) return [];

  const rows = await readLiteralKeyChunks(
    "identity_worker_mapping_heads",
    keys,
    (chunk) => literalStateParameters(
      tenantId,
      "identifier",
      "mapping",
      chunk,
    ),
    config,
    fetcher,
  );
  return rows.map(parseCurrentIdentityMapping);
}

export async function readCurrentIdentityProfiles(
  tenantId: string,
  profileIds: string[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<CurrentIdentityProfile[]> {
  validateTenantId(tenantId);
  const ids = uniqueStrings(profileIds);
  if (ids.length === 0) return [];

  const rows = await readLiteralKeyChunks(
    "identity_worker_profile_heads",
    ids,
    (chunk) => literalStateParameters(
      tenantId,
      "profile",
      "profile",
      chunk,
    ),
    config,
    fetcher,
  );
  return rows.map(parseCurrentIdentityProfile);
}

export async function readCurrentIdentityEvidence(
  tenantId: string,
  identifierKeys: string[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<IdentityEvidencePair[]> {
  validateTenantId(tenantId);
  const keys = uniqueStrings(identifierKeys);
  if (keys.length === 0) return [];

  const rows = await readLiteralKeyChunks(
    "identity_worker_evidence_heads",
    keys,
    (chunk) => literalLookupParameters(tenantId, "identifier", chunk),
    config,
    fetcher,
  );
  return rows.map((row) => ({
    identifierKey: requiredString(row.identifier_key, "identifier_key"),
    factKey: requiredString(row.fact_key, "fact_key"),
  }));
}

export async function appendIdentityJournalRows(
  rows: IdentityJournalRow[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("identity_state_delta_versions", rows, config, fetcher);
}

export async function appendIdentityBatchOutputRows(
  rows: IdentityBatchOutputRow[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("identity_batch_output_rows", rows, config, fetcher);
}

export async function appendIdentityPendingRows(
  rows: object[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("identity_events_cursor", rows, config, fetcher);
}

export async function appendIdentityPendingFacts(
  facts: PendingIdentityFact[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  const rows = facts.map(identityPendingFactEvent);
  await appendEventRows("identity_events_cursor", rows, config, fetcher);
}

export async function readProfileJourneyRows(
  identifierKeys: string[],
  conversionIds: string[],
  identifierProfileIds: string[] | undefined,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<Record<string, unknown>[]> {
  const ids = uniqueStrings(identifierKeys);
  const conversions = uniqueStrings(conversionIds);
  const profileByIdentifierKey = journeyProfileByIdentifierKey(
    identifierKeys,
    identifierProfileIds,
  );
  if (ids.length === 0 && conversions.length === 0) return [];
  if (ids.length > 500) throw new Error("Journey identifier lookup exceeds 500 keys.");
  if (conversions.length > 500) {
    throw new Error("Journey conversion lookup exceeds 500 IDs.");
  }

  const journeyParameters = journeyIdentifierParameters(ids, profileByIdentifierKey);
  const identifierRows = profileByIdentifierKey
    ? await readPipeRowsFromBody(
        "reporting_profile_journey_window_build",
        journeyParameters,
        config,
        fetcher,
      )
    : await readLiteralKeyChunks(
        "reporting_profile_journey_window_build",
        ids,
        (chunk) => journeyIdentifierParameters(chunk, null),
        config,
        fetcher,
      );
  const foundConversionIds = new Set(identifierRows.map((row) => (
    requiredString(row.conversion_id, "conversion_id")
  )));
  const remainingConversionIds = conversions.filter((conversionId) => (
    !foundConversionIds.has(conversionId)
  ));
  const conversionRows = await readLiteralKeyChunks(
    "reporting_profile_journey_window_build",
    remainingConversionIds,
    (chunk) => ({ p_conversion_ids: arrayParameter(chunk) }),
    config,
    fetcher,
  );

  return [...identifierRows, ...conversionRows];
}

function journeyProfileByIdentifierKey(
  identifierKeys: string[],
  identifierProfileIds: string[] | undefined,
): Map<string, string> | null {
  if (identifierProfileIds === undefined) return null;
  if (identifierKeys.length !== identifierProfileIds.length) {
    throw new Error("Journey identifier keys and profile IDs must have equal lengths.");
  }
  return new Map(identifierKeys.map((key, index) => [key, identifierProfileIds[index]]));
}

function journeyIdentifierParameters(
  identifierKeys: string[],
  profileByIdentifierKey: Map<string, string> | null,
): PipeParameters {
  if (!profileByIdentifierKey) {
    return literalValueParameters("identifier", identifierKeys);
  }

  const profileIds = identifierKeys.map((key) => {
    const profileId = profileByIdentifierKey.get(key);
    if (!profileId) throw new Error(`Journey identifier ${key} has no profile ID.`);
    return profileId;
  });
  return {
    p_identifier_mappings_json: JSON.stringify(identifierKeys.map((identifierKey, index) => ({
      identifier_key: identifierKey,
      profile_id: profileIds[index],
    }))),
  };
}

export async function readJourneyBackfillProfilePage(
  tenantId: string,
  afterProfileId: string,
  batchLimit: number,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<string[]> {
  validateTenantId(tenantId);
  if (!Number.isSafeInteger(batchLimit) || batchLimit < 1 || batchLimit > 500) {
    throw new Error("Journey backfill page limit is invalid.");
  }

  const rows = await readPipeRows(
    "reporting_journey_profile_page",
    {
      p_tenant_id: tenantId,
      p_after_profile_id: afterProfileId,
      p_batch_limit: String(batchLimit),
    },
    config,
    fetcher,
  );
  return uniqueStrings(rows.map((row) => requiredString(
    row.profile_id,
    "profile_id",
  )));
}

export async function appendJourneyVersionRows(
  rows: Record<string, unknown>[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("reporting_journey_versions", rows, config, fetcher);
}

export async function appendJourneyCommitRows(
  rows: Record<string, unknown>[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("reporting_journey_commits", rows, config, fetcher);
}

export async function appendJourneyIdentityQueueRow(
  row: {
    tenantId: string;
    batchVersion: number;
    batchId: string;
    committedAt: string;
    profileIds: string[];
    orphanIdentifierKeys: string[];
    conversionIds: string[];
  },
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("reporting_journey_identity_queue", [{
    tenant_id: row.tenantId,
    batch_version: row.batchVersion,
    batch_id: row.batchId,
    committed_at: row.committedAt,
    profile_ids: uniqueStrings(row.profileIds),
    identifier_keys: uniqueStrings(row.orphanIdentifierKeys),
    conversion_ids: uniqueStrings(row.conversionIds),
  }], config, fetcher);
}

export async function readPendingJourneyBatch(
  tenantId: string,
  afterBatchVersion: number,
  afterBatchId: string,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<PendingJourneyBatch | null> {
  validateTenantId(tenantId);
  const rows = await readPipeRows(
    "reporting_journey_pending_batch",
    {
      p_tenant_id: tenantId,
      p_after_batch_version: String(afterBatchVersion),
      p_after_batch_id: afterBatchId,
    },
    config,
    fetcher,
  );
  const row = rows[0];
  if (!row) return null;

  return {
    tenantId: requiredString(row.tenant_id, "tenant_id"),
    batchVersion: nonNegativeInteger(row.batch_version, "batch_version"),
    batchId: requiredString(row.batch_id, "batch_id"),
    profileIds: stringArray(row.profile_ids, "profile_ids"),
    orphanIdentifierKeys: stringArray(row.identifier_keys, "identifier_keys"),
    conversionIds: stringArray(row.conversion_ids, "conversion_ids"),
  };
}

export interface ChangedVisitor {
  visitorKind: "anonymous" | "user" | "page_view";
  visitorValue: string;
  lastIngestedAt: string;
}

export interface ChangedConversionEntity {
  entityKind: "client_form" | "client_order";
  entityId: string;
  lastIngestedAt: string;
}

export interface TouchpointFactHead {
  identityAnchorKey: string;
  touchpointId: string;
}

export interface ConversionFactHead {
  identityAnchorKey: string;
  conversionId: string;
}

export async function readChangedVisitors(
  ingestedFrom: string,
  ingestedTo: string,
  limit: number,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<ChangedVisitor[]> {
  const rows = await readPipeRows(
    "reporting_cdc_changed_visitors",
    {
      p_ingested_from: ingestedFrom,
      p_ingested_to: ingestedTo,
      p_limit: String(limit),
    },
    config,
    fetcher,
  );
  return rows.map((row) => ({
    visitorKind: visitorKind(row.visitor_kind),
    visitorValue: requiredString(row.visitor_value, "visitor_value"),
    lastIngestedAt: requiredString(row.last_ingested_at, "last_ingested_at"),
  }));
}

export async function readChangedConversionEntities(
  ingestedFrom: string,
  ingestedTo: string,
  limit: number,
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<ChangedConversionEntity[]> {
  const rows = await readPipeRows(
    "reporting_cdc_changed_conversions",
    {
      p_ingested_from: ingestedFrom,
      p_ingested_to: ingestedTo,
      p_limit: String(limit),
    },
    config,
    fetcher,
  );
  return rows.map((row) => ({
    entityKind: conversionEntityKind(row.entity_kind),
    entityId: requiredString(row.entity_id, "entity_id"),
    lastIngestedAt: requiredString(row.last_ingested_at, "last_ingested_at"),
  }));
}

export async function readTouchpointFactCdcBuild(
  visitors: {
    anonymousIds: string[];
    userIds: string[];
    pageViewIds: string[];
  },
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<Record<string, unknown>[]> {
  const parameters: PipeParameters = {
    ...cdcLiteralParameters("p_anonymous_ids", "p_anonymous_id", visitors.anonymousIds),
    ...cdcLiteralParameters("p_user_ids", "p_user_id", visitors.userIds),
    ...cdcLiteralParameters("p_page_view_ids", "p_page_view_id", visitors.pageViewIds),
  };
  if (Object.keys(parameters).length === 0) return [];

  return readPipeRows(
    "reporting_touchpoint_facts_cdc_build",
    parameters,
    config,
    fetcher,
  );
}

export async function readTouchpointFactHeads(
  anchorKeys: string[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<TouchpointFactHead[]> {
  const keys = uniqueStrings(anchorKeys);
  if (keys.length === 0) return [];

  const rows = await readLiteralKeyChunks(
    "reporting_touchpoint_fact_heads",
    keys,
    (chunk) => cdcLiteralParameters("p_anchor_keys", "p_anchor_key", chunk),
    config,
    fetcher,
  );
  return rows.map((row) => ({
    identityAnchorKey: requiredString(row.identity_anchor_key, "identity_anchor_key"),
    touchpointId: requiredString(row.touchpoint_id, "touchpoint_id"),
  }));
}

export async function readConversionFactCdcBuild(
  entities: {
    formSubmissionIds: string[];
    orderEventIds: string[];
  },
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<Record<string, unknown>[]> {
  const parameters: PipeParameters = {
    ...cdcLiteralParameters(
      "p_form_submission_ids",
      "p_form_submission_id",
      entities.formSubmissionIds,
    ),
    ...cdcLiteralParameters("p_order_event_ids", "p_order_event_id", entities.orderEventIds),
  };
  if (Object.keys(parameters).length === 0) return [];

  return readPipeRows(
    "reporting_conversion_facts_cdc_build",
    parameters,
    config,
    fetcher,
  );
}

export async function readConversionFactHeads(
  conversionIds: string[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<ConversionFactHead[]> {
  const ids = uniqueStrings(conversionIds);
  if (ids.length === 0) return [];

  const rows = await readLiteralKeyChunks(
    "reporting_conversion_fact_heads",
    ids,
    (chunk) => cdcLiteralParameters("p_conversion_ids", "p_conversion_id", chunk),
    config,
    fetcher,
  );
  return rows.map((row) => ({
    identityAnchorKey: stringValue(
      row.identity_anchor_key ?? "",
      "identity_anchor_key",
    ),
    conversionId: requiredString(row.conversion_id, "conversion_id"),
  }));
}

export async function appendTouchpointFactDeltas(
  rows: Record<string, unknown>[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("mart_touchpoints_all_fact_deltas", rows, config, fetcher);
}

export async function appendConversionFactDeltas(
  rows: Record<string, unknown>[],
  config: TinybirdApiConfig,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await appendEventRows("reporting_conversion_fact_deltas", rows, config, fetcher);
}

// Mirrors literalValueParameters: comma-joined array values, with a singleton
// parameter fallback for a value that itself contains a comma.
function cdcLiteralParameters(
  plural: string,
  singular: string,
  values: string[],
): PipeParameters {
  if (values.length === 0) return {};
  if (values.length === 1 && values[0].includes(",")) {
    return { [singular]: values[0] };
  }
  if (values.some((value) => value.includes(","))) {
    throw new Error(`${plural} values containing commas need singleton lookups.`);
  }
  return { [plural]: values.join(",") };
}

function visitorKind(value: unknown): ChangedVisitor["visitorKind"] {
  if (value === "anonymous" || value === "user" || value === "page_view") return value;
  throw new Error("Tinybird visitor_kind value is invalid.");
}

function conversionEntityKind(value: unknown): ChangedConversionEntity["entityKind"] {
  if (value === "client_form" || value === "client_order") return value;
  throw new Error("Tinybird entity_kind value is invalid.");
}

function identityPendingFactEvent(fact: PendingIdentityFact): object {
  const payload = parseFactPayload(fact.factPayload);

  return {
    tenant_id: "boom",
    event_id: fact.eventId,
    producer_id: fact.producerId,
    event_kind: "identity_observation",
    observed_at: fact.observedAt,
    ingested_at: fact.ingestedAt,
    anonymous_id: payload.anonymous_id,
    user_id: payload.user_id,
    email: payload.email,
    phone: payload.phone,
    first_name: payload.first_name,
    last_name: payload.last_name,
    fact_kind: fact.factKind,
    fact_key: fact.factKey,
    source_fact_version: fact.sourceFactVersion,
    fact_deleted: Number(fact.factDeleted),
    fact_payload_hash: fact.factPayloadHash,
    fact_payload: fact.factPayload,
    evidence_keys: fact.evidenceKeys,
  };
}

function parseFactPayload(value: string): Record<string, string> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value) as unknown;
  } catch {
    throw new Error("Identity fact payload is invalid JSON.");
  }
  if (!isRecord(parsed)) throw new Error("Identity fact payload is not an object.");

  const fields = [
    "anonymous_id",
    "user_id",
    "email",
    "phone",
    "first_name",
    "last_name",
  ];
  return Object.fromEntries(fields.map((field) => [
    field,
    typeof parsed[field] === "string" ? parsed[field] : "",
  ]));
}

export function identityBatchOutputRows(
  rows: IdentityJournalRow[],
): IdentityBatchOutputRow[] {
  return rows.map((row) => ({
    tenant_id: row.tenant_id,
    batch_version: row.batch_version,
    batch_id: row.batch_id,
    state_kind: row.state_kind,
    lookup_key: row.lookup_key,
    sub_key: row.sub_key,
    row_hash: row.row_hash,
  }));
}

function validateManifestForActivation(input: IdentityActivationInput): void {
  const manifest = input.manifest;
  if (!manifest.isValid) throw new Error("Identity compaction manifest is invalid.");
  if (manifest.tenantId !== input.tenantId) {
    throw new Error("Identity compaction manifest tenant does not match activation.");
  }
  if (manifest.expectedOutputRowCount !== manifest.actualOutputRowCount) {
    throw new Error("Identity compaction row count does not match its manifest.");
  }
  if (manifest.expectedOutputHash !== manifest.actualOutputHash) {
    throw new Error("Identity compaction hash does not match its manifest.");
  }
}

function identityActivationEvent(input: IdentityActivationInput): Record<string, unknown> {
  const manifest = input.manifest;

  return {
    tenant_id: input.tenantId,
    state_kind: "activation_audit",
    state_key: `activation_audit:${String(manifest.batchVersion).padStart(20, "0")}`,
    lookup_key: manifest.batchId,
    sub_key: "",
    batch_version: manifest.batchVersion,
    batch_id: manifest.batchId,
    committed_at: new Date().toISOString(),
    is_deleted: 0,
    row_hash: manifest.actualOutputHash,
    identifier_type: "",
    identifier_value: "",
    identifier_key: "",
    profile_id: "",
    profile_key: "",
    winner_identifier_key: "",
    member_identifier_keys: [],
    anonymous_ids: [],
    user_ids: [],
    emails: [],
    phones: [],
    historical_profile_ids: [],
    first_name: "",
    last_name: "",
    first_seen_at: "1970-01-01T00:00:00.000000Z",
    last_seen_at: "1970-01-01T00:00:00.000000Z",
    fact_kind: "",
    fact_key: "",
    source_fact_version: 0,
    fact_deleted: 0,
    fact_observed_at: "1970-01-01T00:00:00.000000Z",
    fact_payload_hash: "0".repeat(64),
    fact_payload: "",
    evidence_keys: [],
    producer_id: "identity_compactor",
    checkpoint_sequence: 0,
    checkpoint_ingested_at: manifest.checkpointIngestedAt,
    checkpoint_event_id: manifest.checkpointEventId,
    prior_profile_ids: [],
    dirty_profile_ids: [],
    input_event_count: manifest.inputEventCount,
    input_hash: manifest.inputHash,
    output_row_count: manifest.actualOutputRowCount,
    output_hash: manifest.actualOutputHash,
  };
}

async function listJobs(
  status: string,
  kind: string,
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<unknown[]> {
  const query = new URLSearchParams({ status, kind });
  const response = await tinybirdRequest(
    `/v0/jobs?${query.toString()}`,
    { method: "GET" },
    config,
    fetcher,
  );
  const body = await readJson<JobsResponse>(response, `Tinybird ${kind} job list`);

  if (Array.isArray(body.jobs)) return body.jobs;
  throw new Error(`Tinybird ${kind} job list did not return a jobs array.`);
}

async function readPipeRows(
  pipeName: string,
  parameters: PipeParameters,
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<Record<string, unknown>[]> {
  validateResourceName(pipeName, "Pipe");
  const query = validatedPipeParameters(parameters);
  const response = await tinybirdRequest(
    `/v0/pipes/${encodeURIComponent(pipeName)}.json?${query.toString()}`,
    { method: "GET" },
    config,
    fetcher,
  );
  const body = await readJson<RowsResponse>(response, `${pipeName} query`);

  if (!Array.isArray(body.data)) {
    throw new Error(`Tinybird ${pipeName} query did not return a data array.`);
  }

  return body.data.filter(isRecord);
}

async function readPipeRowsFromBody(
  pipeName: string,
  parameters: PipeParameters,
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<Record<string, unknown>[]> {
  validateResourceName(pipeName, "Pipe");
  const bodyParameters = validatedPipeParameters(parameters);
  const response = await tinybirdRequest(
    `/v0/pipes/${encodeURIComponent(pipeName)}.json`,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: bodyParameters.toString(),
    },
    config,
    fetcher,
  );
  const body = await readJson<RowsResponse>(response, `${pipeName} query`);

  if (!Array.isArray(body.data)) {
    throw new Error(`Tinybird ${pipeName} query did not return a data array.`);
  }

  return body.data.filter(isRecord);
}

function validatedPipeParameters(parameters: PipeParameters): URLSearchParams {
  const query = new URLSearchParams();
  for (const [name, value] of Object.entries(parameters).sort()) {
    const values = Array.isArray(value) ? value : [value];
    for (const item of values) {
      validatePipeParameter(name, item);
      query.append(name, item);
    }
  }
  return query;
}

async function readLiteralKeyChunks(
  pipeName: string,
  keys: string[],
  parameters: (chunk: string[]) => PipeParameters,
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<Record<string, unknown>[]> {
  const result: Record<string, unknown>[] = [];
  const keyChunks = literalKeyChunks(keys, parameters);

  for (const requestGroup of chunks(keyChunks, 6)) {
    const responses = await Promise.all(requestGroup.map((chunk) => readPipeRows(
        pipeName,
        parameters(chunk),
        config,
        fetcher,
      )));
    for (const rows of responses) result.push(...rows);
  }
  return result;
}

function literalKeyChunks(
  keys: string[],
  parameters: (chunk: string[]) => PipeParameters,
): string[][] {
  const result: string[][] = [];
  let current: string[] = [];

  for (const key of keys) {
    if (key.includes(",")) {
      if (current.length > 0) result.push(current);
      result.push([key]);
      current = [];
      continue;
    }
    const candidate = [...current, key];
    const queryLength = pipeParametersQuery(parameters(candidate)).length;
    if (candidate.length <= 500 && queryLength <= 12_000) {
      current = candidate;
      continue;
    }

    if (current.length === 0) throw new Error("Identity literal key is too large.");
    result.push(current);
    current = [key];
  }

  if (current.length > 0) result.push(current);
  return result;
}

async function appendEventRows(
  dataSourceName: string,
  rows: object[],
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<void> {
  validateResourceName(dataSourceName, "Data Source");
  const rowChunks = chunks(rows, 500);

  for (const requestGroup of chunks(rowChunks, 6)) {
    await Promise.all(requestGroup.map((chunk) => appendEventChunk(
      dataSourceName,
      chunk,
      config,
      fetcher,
    )));
  }
}

async function appendEventChunk(
  dataSourceName: string,
  rows: object[],
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<void> {
  const body = `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`;
  await tinybirdRequest(
    `/v0/events?name=${encodeURIComponent(dataSourceName)}&wait=true`,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-ndjson; charset=utf-8" },
      body,
    },
    config,
    fetcher,
  );
}

async function tinybirdRequest(
  path: string,
  init: RequestInit,
  config: TinybirdApiConfig,
  fetcher: Fetcher,
): Promise<Response> {
  const baseUrl = config.apiUrl.replace(/\/+$/g, "");
  const response = await fetcher(`${baseUrl}${path}`, {
    ...init,
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${config.adminToken}`,
      ...init.headers,
    },
    signal: AbortSignal.timeout(config.fetchTimeoutMs),
  });

  if (response.ok) return response;

  const details = await response.text().catch(() => "");
  const suffix = details.trim().slice(0, 2_000) || response.statusText;
  throw new TinybirdRequestError(
    `Tinybird request failed (${response.status}): ${suffix}`,
    response.status,
    parseRetryAfterMs(response.headers.get("Retry-After")),
  );
}

function parseRetryAfterMs(value: string | null): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds * 1_000);

  const retryAt = Date.parse(value);
  if (Number.isNaN(retryAt)) return null;
  return Math.max(0, retryAt - Date.now());
}

async function readJson<T>(response: Response, label: string): Promise<T> {
  try {
    return await response.json() as T;
  } catch {
    throw new Error(`${label} returned invalid JSON.`);
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function firstRow(
  rows: Record<string, unknown>[],
  label: string,
): Record<string, unknown> {
  const row = rows[0];
  if (row) return row;
  throw new Error(`Tinybird ${label} query returned no rows.`);
}

function parsePendingIdentityFact(row: Record<string, unknown>): PendingIdentityFact {
  return {
    eventId: fixedHash(row.event_id, "event_id"),
    producerId: requiredString(row.producer_id, "producer_id"),
    observedAt: requiredString(row.observed_at, "observed_at"),
    ingestedAt: requiredString(row.ingested_at, "ingested_at"),
    factKind: requiredString(row.fact_kind, "fact_kind"),
    factKey: requiredString(row.fact_key, "fact_key"),
    sourceFactVersion: nonNegativeInteger(row.source_fact_version, "source_fact_version"),
    factDeleted: booleanValue(row.fact_deleted, "fact_deleted"),
    factPayloadHash: fixedHash(row.fact_payload_hash, "fact_payload_hash"),
    factPayload: stringValue(row.fact_payload, "fact_payload"),
    evidenceKeys: stringArray(row.evidence_keys, "evidence_keys"),
  };
}

function parseCurrentIdentityFact(row: Record<string, unknown>): CurrentIdentityFact {
  return {
    producerId: requiredString(row.producer_id, "producer_id"),
    factKind: requiredString(row.fact_kind, "fact_kind"),
    factKey: requiredString(row.fact_key, "fact_key"),
    sourceFactVersion: nonNegativeInteger(row.source_fact_version, "source_fact_version"),
    factDeleted: booleanValue(row.fact_deleted, "fact_deleted"),
    factObservedAt: requiredString(row.fact_observed_at, "fact_observed_at"),
    factPayloadHash: fixedHash(row.fact_payload_hash, "fact_payload_hash"),
    evidenceKeys: stringArray(row.evidence_keys, "evidence_keys"),
    firstName: stringValue(row.first_name, "first_name"),
    lastName: stringValue(row.last_name, "last_name"),
    isDeleted: booleanValue(row.is_deleted, "is_deleted"),
  };
}

function parseCurrentIdentityMapping(row: Record<string, unknown>): CurrentIdentityMapping {
  return {
    identifierType: requiredString(row.identifier_type, "identifier_type"),
    identifierValue: requiredString(row.identifier_value, "identifier_value"),
    identifierKey: requiredString(row.identifier_key, "identifier_key"),
    profileId: requiredString(row.profile_id, "profile_id"),
    firstSeenAt: requiredString(row.first_seen_at, "first_seen_at"),
    lastSeenAt: requiredString(row.last_seen_at, "last_seen_at"),
  };
}

function parseCurrentIdentityProfile(row: Record<string, unknown>): CurrentIdentityProfile {
  return {
    profileId: requiredString(row.profile_id, "profile_id"),
    profileKey: requiredString(row.profile_key, "profile_key"),
    winnerIdentifierKey: requiredString(row.winner_identifier_key, "winner_identifier_key"),
    memberIdentifierKeys: stringArray(row.member_identifier_keys, "member_identifier_keys"),
    historicalProfileIds: stringArray(row.historical_profile_ids, "historical_profile_ids"),
    firstName: stringValue(row.first_name, "first_name"),
    lastName: stringValue(row.last_name, "last_name"),
    firstSeenAt: requiredString(row.first_seen_at, "first_seen_at"),
    lastSeenAt: requiredString(row.last_seen_at, "last_seen_at"),
  };
}

function booleanValue(value: unknown, name: string): boolean {
  if (value === true || value === 1 || value === "1") return true;
  if (value === false || value === 0 || value === "0") return false;
  throw new Error(`Tinybird ${name} value is invalid.`);
}

function requiredString(value: unknown, name: string): string {
  if (typeof value === "string" && value.length > 0) return value;
  throw new Error(`Tinybird ${name} value is invalid.`);
}

function stringValue(value: unknown, name: string): string {
  if (typeof value === "string") return value;
  throw new Error(`Tinybird ${name} value is invalid.`);
}

function stringArray(value: unknown, name: string): string[] {
  if (Array.isArray(value) && value.every((item) => typeof item === "string")) {
    return value;
  }
  throw new Error(`Tinybird ${name} value is invalid.`);
}

function fixedHash(value: unknown, name: string): string {
  if (typeof value === "string" && /^[a-fA-F0-9]{64}$/.test(value)) {
    return value.toLowerCase();
  }

  throw new Error(`Tinybird ${name} value is invalid.`);
}

function nonNegativeInteger(value: unknown, name: string): number {
  const parsed = typeof value === "string" ? Number(value) : value;
  if (typeof parsed === "number" && Number.isSafeInteger(parsed) && parsed >= 0) {
    return parsed;
  }

  throw new Error(`Tinybird ${name} value is invalid.`);
}

function validateResourceName(value: string, label: string): void {
  if (/^[a-z0-9_]{1,200}$/.test(value)) return;
  throw new Error(`${label} name is invalid.`);
}

function validatePipeParameter(name: string, value: string): void {
  if (!/^[a-z][a-z0-9_]{0,99}$/.test(name)) {
    throw new Error("Tinybird Pipe parameter name is invalid.");
  }
  if (typeof value !== "string" || value.length > 100_000) {
    throw new Error(`Tinybird Pipe parameter ${name} is invalid.`);
  }
}

function validateTenantId(value: string): void {
  if (/^[a-zA-Z0-9_-]{1,100}$/.test(value)) return;
  throw new Error("Identity tenant ID is invalid.");
}

function validateBatchVersion(value: number): void {
  if (Number.isSafeInteger(value) && value > 0) return;
  throw new Error("Identity batch version is invalid.");
}

function validateBatchId(value: string): void {
  if (/^[a-zA-Z0-9_-]{1,200}$/.test(value)) return;
  throw new Error("Identity batch ID is invalid.");
}

function validateJobId(value: string): void {
  if (/^[a-zA-Z0-9_-]{1,200}$/.test(value)) return;
  throw new Error("Tinybird job ID is invalid.");
}

function isJobStatus(value: unknown): value is TinybirdCopyJob["status"] {
  return [
    "waiting",
    "working",
    "done",
    "error",
    "cancelled",
    "cancelling",
  ].includes(String(value));
}

function jobError(body: Record<string, unknown>): string | undefined {
  if (typeof body.error === "string" && body.error) return body.error;
  if (Array.isArray(body.errors) && body.errors.length > 0) {
    return JSON.stringify(body.errors).slice(0, 2_000);
  }

  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values)].sort();
}

function arrayParameter(values: string[]): string {
  if (values.some((value) => value.includes(","))) {
    throw new Error("Comma-bearing identity keys require a singleton lookup.");
  }
  return values.join(",");
}

function literalFactParameters(tenantId: string, factKeys: string[]): PipeParameters {
  const stateKeys = factKeys.map((key) => `fact:${key}`);
  return {
    p_tenant_id: tenantId,
    ...literalValueParameters("fact", factKeys),
    ...literalValueParameters("state", stateKeys),
  };
}

function literalStateParameters(
  tenantId: string,
  lookupName: "identifier" | "profile",
  stateKind: "mapping" | "profile",
  lookupKeys: string[],
): PipeParameters {
  const stateKeys = lookupKeys.map((key) => identityStateKey(stateKind, key));
  return {
    p_tenant_id: tenantId,
    ...literalValueParameters(lookupName, lookupKeys),
    ...literalValueParameters("state", stateKeys),
  };
}

function literalLookupParameters(
  tenantId: string,
  lookupName: "identifier",
  lookupKeys: string[],
): PipeParameters {
  return {
    p_tenant_id: tenantId,
    ...literalValueParameters(lookupName, lookupKeys),
  };
}

function literalValueParameters(
  name: "fact" | "identifier" | "lookup" | "profile" | "state",
  values: string[],
): PipeParameters {
  const singular = name === "profile" ? "p_profile_id" : `p_${name}_key`;
  const plural = name === "profile" ? "p_profile_ids" : `p_${name}_keys`;
  if (values.length === 1 && values[0].includes(",")) {
    return { [singular]: values[0] };
  }
  return { [plural]: arrayParameter(values) };
}

function pipeParametersQuery(parameters: PipeParameters): string {
  const query = new URLSearchParams();
  for (const [name, value] of Object.entries(parameters).sort()) {
    const values = Array.isArray(value) ? value : [value];
    for (const item of values) query.append(name, item);
  }
  return query.toString();
}

function identityStateKey(kind: string, value: string): string {
  return `${kind}:${new TextEncoder().encode(value).length}:${value}`;
}

function chunks<T>(values: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
}
