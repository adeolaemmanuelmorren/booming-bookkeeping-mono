import {
  buildExportPlan,
  createGoogleAccessToken,
  runBigQueryExport,
  type BigQueryConfig,
  type ExportConfig,
  type Fetcher,
  type Sleep,
} from "./bigquery";
import {
  TABLE_MANIFEST,
  type SourceTable,
} from "./table-manifest.generated";
import {
  type TinybirdGateResult,
  type TinybirdSyncGate,
} from "./tinybird-gate";
import type {
  PublicationCoordinator,
} from "./publication-coordinator";
import type { JourneyCoordinator } from "./journey-coordinator";
import {
  MAX_TABLES_PER_RUN,
  SHARD_COUNT,
} from "./cadence";

export {
  MAX_TABLES_PER_RUN,
  MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE,
  RAW_GENERATION_COVERAGE_MINUTES,
  RAW_TRIGGER_INTERVAL_MINUTES,
  SHARD_COUNT,
} from "./cadence";

export interface WorkerEnv {
  BIGQUERY_PROJECT_ID: string;
  BIGQUERY_LOCATION: string;
  GCS_BUCKET: string;
  GCS_PREFIX: string;
  TINYBIRD_API_URL: string;
  TINYBIRD_FETCH_TIMEOUT_MS: string;
  TINYBIRD_GATE_TIMEOUT_MS: string;
  COPY_POLL_INTERVAL_MS: string;
  COPY_JOB_TIMEOUT_MS: string;
  IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM: string;
  EXPORT_OVERLAP_MINUTES: string;
  BIGQUERY_POLL_INTERVAL_MS: string;
  BIGQUERY_JOB_TIMEOUT_MS: string;
  GCP_SERVICE_ACCOUNT_JSON: string;
  TINYBIRD_ADMIN_TOKEN: string;
  SYNC_ADMIN_TOKEN: string;
  TINYBIRD_SYNC_GATE: DurableObjectNamespace<TinybirdSyncGate>;
  PUBLICATION_COORDINATOR: DurableObjectNamespace<PublicationCoordinator>;
  JOURNEY_COORDINATOR: DurableObjectNamespace<JourneyCoordinator>;
}

export interface SyncResult {
  resourceName: string;
  uri: string;
  jobId: string;
  status: "synced" | "deduplicated" | "in_flight" | "failed";
  gateOutcome?: TinybirdGateResult["outcome"];
  tinybirdStatus?: number;
  error?: string;
}

export interface SyncSummary {
  ok: boolean;
  scheduledAt: string;
  slot: number;
  tableCount: number;
  succeeded: number;
  inFlight: number;
  failed: number;
  results: SyncResult[];
}

export interface SyncDependencies {
  fetcher?: Fetcher;
  sleep?: Sleep;
  exportOverlapMinutes?: number;
  getAccessToken?: (
    serviceAccountJson: string,
    fetcher: Fetcher,
    now: Date,
  ) => Promise<string>;
}

interface RuntimeConfig {
  export: ExportConfig;
  bigQuery: BigQueryConfig;
  serviceAccountJson: string;
}

export function slotForTime(runAt: Date): number {
  const minute = Math.floor(runAt.valueOf() / 60_000);
  return ((minute % SHARD_COUNT) + SHARD_COUNT) % SHARD_COUNT;
}

export function tablesForSlot(slot: number): readonly SourceTable[] {
  assertSlot(slot);

  const tables = TABLE_MANIFEST.filter((_, index) => (
    index % SHARD_COUNT === slot
  ));
  if (tables.length > MAX_TABLES_PER_RUN) {
    throw new Error(
      `Slot ${slot} has ${tables.length} tables; the maximum is ${MAX_TABLES_PER_RUN}.`,
    );
  }

  return tables;
}

export function shardSizes(): number[] {
  return Array.from(
    { length: SHARD_COUNT },
    (_, slot) => tablesForSlot(slot).length,
  );
}

export async function runSync(
  env: WorkerEnv,
  requestedRunAt: Date,
  requestedSlot?: number,
  dependencies: SyncDependencies = {},
): Promise<SyncSummary> {
  const config = runtimeConfig(env);
  if (dependencies.exportOverlapMinutes !== undefined) {
    config.export.overlapMinutes = positiveInteger(
      String(dependencies.exportOverlapMinutes),
      "exportOverlapMinutes",
    );
  }
  const runAt = truncateToMinute(requestedRunAt);
  const slot = requestedSlot ?? slotForTime(runAt);
  const tables = tablesForSlot(slot);
  const fetcher = dependencies.fetcher ?? fetch;
  const sleep = dependencies.sleep;
  const getAccessToken = dependencies.getAccessToken ?? createGoogleAccessToken;
  const accessToken = await getAccessToken(
    config.serviceAccountJson,
    fetcher,
    new Date(),
  );
  const tinybirdGate = env.TINYBIRD_SYNC_GATE.getByName(
    "bill-tinybird-workspace",
  );
  const results = await Promise.all(tables.map((table) => runTable(
    table,
    runAt,
    config,
    accessToken,
    fetcher,
    sleep,
    tinybirdGate,
  )));
  const failed = results.filter((result) => result.status === "failed").length;
  const inFlight = results.filter((result) => result.status === "in_flight").length;

  return {
    ok: failed === 0 && inFlight === 0,
    scheduledAt: runAt.toISOString(),
    slot,
    tableCount: tables.length,
    succeeded: results.length - failed - inFlight,
    inFlight,
    failed,
    results,
  };
}

async function runTable(
  table: SourceTable,
  runAt: Date,
  config: RuntimeConfig,
  accessToken: string,
  fetcher: Fetcher,
  sleep?: Sleep,
  tinybirdGate?: DurableObjectStub<TinybirdSyncGate>,
): Promise<SyncResult> {
  const plan = buildExportPlan(table, runAt, config.export);

  try {
    await runBigQueryExport(
      plan,
      config.bigQuery,
      accessToken,
      fetcher,
      sleep,
    );
    if (!tinybirdGate) throw new Error("Tinybird sync gate is unavailable.");
    const gateResult = await tinybirdGate.trigger({
      requestKey: plan.jobId,
      resourceName: table.resourceName,
    });

    const status = syncStatus(gateResult.outcome);
    return {
      resourceName: table.resourceName,
      uri: plan.uri,
      jobId: plan.jobId,
      status,
      gateOutcome: gateResult.outcome,
      tinybirdStatus: gateResult.tinybirdStatus,
    };
  } catch (error) {
    return {
      resourceName: table.resourceName,
      uri: plan.uri,
      jobId: plan.jobId,
      status: "failed",
      error: errorMessage(error),
    };
  }
}

function runtimeConfig(env: WorkerEnv): RuntimeConfig {
  requireValue(env.BIGQUERY_PROJECT_ID, "BIGQUERY_PROJECT_ID");
  requireValue(env.BIGQUERY_LOCATION, "BIGQUERY_LOCATION");
  requireValue(env.GCS_BUCKET, "GCS_BUCKET");
  requireValue(env.TINYBIRD_API_URL, "TINYBIRD_API_URL");
  requireValue(env.GCP_SERVICE_ACCOUNT_JSON, "GCP_SERVICE_ACCOUNT_JSON");
  requireValue(env.TINYBIRD_ADMIN_TOKEN, "TINYBIRD_ADMIN_TOKEN");
  validateBucket(env.GCS_BUCKET);
  validatePrefix(env.GCS_PREFIX);

  return {
    export: {
      bucket: env.GCS_BUCKET,
      prefix: env.GCS_PREFIX,
      overlapMinutes: positiveInteger(
        env.EXPORT_OVERLAP_MINUTES,
        "EXPORT_OVERLAP_MINUTES",
      ),
    },
    bigQuery: {
      projectId: env.BIGQUERY_PROJECT_ID,
      location: env.BIGQUERY_LOCATION,
      pollIntervalMs: positiveInteger(
        env.BIGQUERY_POLL_INTERVAL_MS,
        "BIGQUERY_POLL_INTERVAL_MS",
      ),
      jobTimeoutMs: positiveInteger(
        env.BIGQUERY_JOB_TIMEOUT_MS,
        "BIGQUERY_JOB_TIMEOUT_MS",
      ),
    },
    serviceAccountJson: env.GCP_SERVICE_ACCOUNT_JSON,
  };
}

function syncStatus(
  outcome: TinybirdGateResult["outcome"],
): SyncResult["status"] {
  if (outcome === "synced") return "synced";
  if (outcome === "already_synced") return "deduplicated";
  return "in_flight";
}

function truncateToMinute(value: Date): Date {
  if (Number.isNaN(value.valueOf())) throw new Error("Run time is invalid.");
  return new Date(Math.floor(value.valueOf() / 60_000) * 60_000);
}

function assertSlot(slot: number): void {
  if (Number.isInteger(slot) && slot >= 0 && slot < SHARD_COUNT) return;
  throw new Error(`Slot must be an integer from 0 through ${SHARD_COUNT - 1}.`);
}

function positiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (Number.isInteger(parsed) && parsed > 0) return parsed;
  throw new Error(`${name} must be a positive integer.`);
}

function requireValue(value: string, name: string): void {
  if (typeof value === "string" && value.trim()) return;
  throw new Error(`${name} is required.`);
}

function validateBucket(bucket: string): void {
  if (/^[a-z0-9][a-z0-9._-]*[a-z0-9]$/.test(bucket)) return;
  throw new Error("GCS_BUCKET is invalid.");
}

function validatePrefix(prefix: string): void {
  const invalid = !prefix
    || prefix.includes("..")
    || prefix.includes("//")
    || !/^[a-zA-Z0-9._/-]+$/.test(prefix);

  if (invalid) {
    throw new Error("GCS_PREFIX is invalid.");
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
