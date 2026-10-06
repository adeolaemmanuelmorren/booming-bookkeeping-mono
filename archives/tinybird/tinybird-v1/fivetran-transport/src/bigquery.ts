import type { SourceTable } from "./table-manifest.generated";

const BIGQUERY_API_URL = "https://bigquery.googleapis.com/bigquery/v2";
const GOOGLE_CLOUD_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
const GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";
const JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";
const UNIX_EPOCH = "TIMESTAMP '1970-01-01 00:00:00+00'";
const PARTITION_TIME_EXPORT_PATH = "partition_time_v1";
const PARTITION_TIME_TARGET_COLUMN = "source_partition_time";
const MINUTE = 60_000;

export type Fetcher = (
  input: Request | string | URL,
  init?: RequestInit,
) => Promise<Response>;

export type Sleep = (milliseconds: number) => Promise<void>;

export interface ExportConfig {
  bucket: string;
  prefix: string;
  overlapMinutes: number;
  changeHistory?: ChangeHistoryGuard;
}

export interface ChangeHistoryGuard {
  enabledAt: Date;
  sourceCreatedAt: Date;
  retentionHours: number;
  checkedAt: Date;
}

export interface BigQueryConfig {
  projectId: string;
  location: string;
  pollIntervalMs: number;
  jobTimeoutMs: number;
}

export interface ExportPlan {
  resourceName: string;
  jobId: string;
  query: string;
  uri: string;
}

export interface BridgePlanConfig
  extends Omit<ExportConfig, "overlapMinutes" | "changeHistory"> {
  bridgeStart: Date;
  sourceMetadata: ChangeHistoryGuard;
}

export function nextCatchupTarget(completed: Date, now: Date): Date | null {
  const gapMinutes = Math.floor((now.valueOf() - completed.valueOf()) / MINUTE);

  if (gapMinutes <= 0) return null;

  const windowMinutes = Math.min(60, gapMinutes);
  return new Date(completed.valueOf() + windowMinutes * MINUTE);
}

interface ServiceAccountCredentials {
  client_email: string;
  private_key: string;
  private_key_id?: string;
  token_uri?: string;
}

export interface BigQueryJob {
  jobReference?: { jobId?: string; projectId?: string; location?: string };
  configuration?: { query?: { query?: string } };
  status?: {
    state?: string;
    errorResult?: BigQueryError;
    errors?: BigQueryError[];
  };
  statistics?: {
    query?: {
      exportDataStatistics?: { fileCount?: string; rowCount?: string };
    };
  };
}

interface BigQueryError {
  message?: string;
  reason?: string;
}

export function buildExportPlan(
  table: SourceTable,
  runAt: Date,
  config: ExportConfig,
  observedAt: Date = runAt,
): ExportPlan {
  const windowStart = new Date(
    runAt.valueOf() - config.overlapMinutes * MINUTE,
  );
  assertChangeWindow(table, windowStart, runAt, config.changeHistory);
  const uri = buildGcsUri(table, runAt, config);
  const query = [
    buildMetadataAssertion(table, config.changeHistory),
    buildExportQuery(table, windowStart, runAt, uri, observedAt),
  ].join("\n");

  return {
    resourceName: table.resourceName,
    jobId: buildJobId(table.resourceName, runAt),
    query,
    uri,
  };
}

export function buildBridgeExportPlan(
  table: SourceTable,
  bridgeEnd: Date,
  config: BridgePlanConfig,
): ExportPlan {
  assertTableWithPrimaryId(table);
  assertOrderedWindow(config.bridgeStart, bridgeEnd);

  const uri = buildBridgeGcsUri(table, bridgeEnd, config);
  return {
    resourceName: table.resourceName,
    jobId: buildBridgeJobId(table.resourceName, bridgeEnd),
    query: [
      buildMetadataAssertion(table, config.sourceMetadata),
      buildBridgeExportQuery(table, config.bridgeStart, bridgeEnd, uri),
    ].join("\n"),
    uri,
  };
}

function buildMetadataAssertion(
  table: SourceTable,
  metadata?: ChangeHistoryGuard,
): string {
  if (!metadata) throw new Error(`${table.resourceName} has no source metadata pin.`);
  const source = table.source;
  return [
    "ASSERT (",
    "  SELECT COUNT(*) = 1",
    `  FROM \`${source.project}.${source.dataset}.INFORMATION_SCHEMA.TABLES\``,
    `  WHERE table_name = '${escapeSqlString(source.table)}'`,
    `    AND UNIX_MILLIS(creation_time) = ${metadata.sourceCreatedAt.valueOf()}`,
    "    AND is_change_history_enabled = 'YES'",
    `) AS '${escapeSqlString(table.resourceName)} metadata mismatch';`,
  ].join("\n");
}

export function buildBackfillExportPlan(
  table: SourceTable,
  snapshotAt: Date,
  config: ExportConfig,
): ExportPlan {
  if (table.exportKind !== "typed_union") {
    throw new Error(`${table.resourceName} is not a derived typed union.`);
  }

  const uri = buildBackfillGcsUri(table, snapshotAt, config);

  return {
    resourceName: table.resourceName,
    jobId: buildBackfillJobId(table.resourceName, snapshotAt),
    query: buildTypedUnionQuery(table, uri),
    uri,
  };
}

export function buildSnapshotExportPlan(
  table: SourceTable,
  snapshotAt: Date,
  config: ExportConfig,
): ExportPlan {
  if (table.exportKind !== "table") {
    throw new Error(
      `${table.resourceName} cannot use a table snapshot export.`,
    );
  }

  const stamp = formatTimestamp(snapshotAt);
  const uri = `gs://${config.bucket}/${config.prefix}/${table.resourceName}/snapshot-${stamp.replace(/[^0-9]/g, "")}-*.parquet`;
  const query = [
    "EXPORT DATA OPTIONS(",
    `  uri='${escapeSqlString(uri)}',`,
    "  format='PARQUET',",
    "  overwrite=true",
    ") AS",
    "SELECT",
    buildSelectList(table, null, stamp),
    `FROM ${quoteTable(table.source)} FOR SYSTEM_TIME AS OF TIMESTAMP '${stamp}'`,
  ].join("\n");

  return {
    resourceName: table.resourceName,
    jobId: `tinybird_snapshot_${stamp.replace(/[^0-9]/g, "")}_${table.resourceName}`,
    query,
    uri,
  };
}

export function buildExportQuery(
  table: SourceTable,
  windowStart: Date,
  windowEnd: Date,
  uri: string,
  observedAt: Date = windowEnd,
): string {
  assertTableWithPrimaryId(table);
  assertOrderedWindow(windowStart, windowEnd);

  const source = quoteTable(table.source);
  const start = formatTimestamp(windowStart);
  const end = formatTimestamp(windowEnd);
  const cutoff = `TIMESTAMP_SUB(TIMESTAMP '${end}', INTERVAL 1 MICROSECOND)`;
  const observed = formatTimestamp(observedAt);
  const currentProjection = buildAliasedSelectList(table, "cutoff_row", observed, false);
  const deletedProjection = buildAliasedSelectList(table, "fallback", observed, true);

  return [
    "CREATE TEMP TABLE changed AS",
    "SELECT",
    "  * EXCEPT(",
    "    `_CHANGE_TYPE`,",
    "    `_CHANGE_TIMESTAMP`,",
    "    `_CHANGE_IS_FOR_UPDATE`",
    "  ),",
    "  `_CHANGE_TYPE` AS `v1_change_type`,",
    "  `_CHANGE_TIMESTAMP` AS `v1_commit_at`,",
    "  `_CHANGE_IS_FOR_UPDATE` AS `v1_is_for_update`",
    `FROM CHANGES(TABLE ${source}, TIMESTAMP '${start}', TIMESTAMP '${end}');`,
    "CREATE TEMP TABLE changed_ids AS",
    "SELECT DISTINCT `id` FROM changed;",
    "CREATE TEMP TABLE current_state AS",
    "SELECT cutoff_row.*",
    `FROM ${source} AS cutoff_row FOR SYSTEM_TIME AS OF ${cutoff}`,
    "INNER JOIN changed_ids USING (`id`);",
    "EXPORT DATA OPTIONS(",
    `  uri='${escapeSqlString(uri)}',`,
    "  format='PARQUET',",
    "  overwrite=false",
    ") AS",
    "WITH latest_change AS (",
    "  SELECT * EXCEPT(",
    "    `v1_change_type`,",
    "    `v1_commit_at`,",
    "    `v1_is_for_update`",
    "  )",
    "  FROM changed",
    "  WHERE `v1_change_type` = 'DELETE'",
    "    AND NOT COALESCE(`v1_is_for_update`, FALSE)",
    "  QUALIFY ROW_NUMBER() OVER (",
    "    PARTITION BY `id`",
    "    ORDER BY `v1_commit_at` DESC,",
    `      TO_JSON_STRING(STRUCT(${table.columns.map(quoteIdentifier).join(", ")})) DESC`,
    "  ) = 1",
    ")",
    "SELECT",
    currentProjection,
    "FROM current_state AS cutoff_row",
    "UNION ALL",
    "SELECT",
    deletedProjection,
    "FROM latest_change AS fallback",
    "LEFT JOIN current_state AS cutoff_row USING (`id`)",
    "WHERE cutoff_row.`id` IS NULL",
    "ORDER BY `id`",
  ].join("\n");
}

function buildTypedUnionExportQuery(
  table: SourceTable,
  runAt: Date,
  uri: string,
  overlapMinutes: number,
): string {
  const startAt = new Date(runAt.valueOf() - overlapMinutes * 60_000);

  return buildTypedUnionQuery(table, uri, startAt, runAt);
}

function buildTypedUnionQuery(
  table: SourceTable,
  uri: string,
  startAt?: Date,
  runAt?: Date,
): string {
  if (table.unionSources.length === 0) {
    throw new Error(`${table.resourceName} has no union source tables.`);
  }

  const sourceQueries = table.unionSources.map((source) =>
    buildTypedUnionSourceQuery(table, source, startAt, runAt),
  );

  return [
    "EXPORT DATA OPTIONS(",
    `  uri='${escapeSqlString(uri)}',`,
    "  format='PARQUET',",
    "  overwrite=true",
    ") AS",
    sourceQueries.join("\nUNION ALL\n"),
  ].join("\n");
}

function buildTypedUnionSourceQuery(
  table: SourceTable,
  unionSource: SourceTable["unionSources"][number],
  startAt?: Date,
  runAt?: Date,
): string {
  const watermark = quoteIdentifier(unionSource.watermarkColumn);
  const projections = table.columns.map((targetColumn) => {
    if (targetColumn === "record_type") {
      return literalProjection(unionSource.recordType, targetColumn);
    }

    if (targetColumn === "payment_source") {
      return literalProjection(unionSource.paymentSource, targetColumn);
    }

    const sourceColumn = unionSource.columnMappings[targetColumn];
    if (sourceColumn) {
      return `  ${quoteIdentifier(sourceColumn)} AS ${quoteIdentifier(targetColumn)}`;
    }

    const bigQueryType = table.columnTypes[targetColumn];
    if (!bigQueryType) {
      throw new Error(`${table.resourceName} has no type for ${targetColumn}.`);
    }

    return `  CAST(NULL AS ${bigQueryType}) AS ${quoteIdentifier(targetColumn)}`;
  });

  const query = [
    "SELECT",
    projections.join(",\n"),
    `FROM ${quoteTable(unionSource.source)}`,
  ];

  if (!startAt || !runAt) return query.join("\n");

  query.push(
    `WHERE COALESCE(${watermark}, ${UNIX_EPOCH}) >= TIMESTAMP '${formatTimestamp(startAt)}'`,
    `  AND COALESCE(${watermark}, ${UNIX_EPOCH}) < TIMESTAMP '${formatTimestamp(runAt)}'`,
  );

  return query.join("\n");
}

function literalProjection(value: string, targetColumn: string): string {
  return `  '${escapeSqlString(value)}' AS ${quoteIdentifier(targetColumn)}`;
}

export async function createGoogleAccessToken(
  serviceAccountJson: string,
  fetcher: Fetcher = fetch,
  now = new Date(),
): Promise<string> {
  const credentials = parseServiceAccount(serviceAccountJson);
  const tokenUrl = credentials.token_uri || GOOGLE_TOKEN_URL;
  if (tokenUrl !== GOOGLE_TOKEN_URL) {
    throw new Error(
      "GCP service account token_uri must use Google's OAuth endpoint.",
    );
  }

  const assertion = await signServiceAccountJwt(credentials, now);
  const response = await fetcher(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      assertion,
      grant_type: JWT_GRANT_TYPE,
    }),
  });

  if (!response.ok) {
    const details = await responseSummary(response);
    throw new Error(
      `Google OAuth token exchange failed (${response.status}): ${details}`,
    );
  }

  const payload = (await response.json()) as { access_token?: unknown };
  if (typeof payload.access_token !== "string" || !payload.access_token) {
    throw new Error("Google OAuth token exchange returned no access token.");
  }

  return payload.access_token;
}

export async function runBigQueryExport(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher = fetch,
  sleep: Sleep = wait,
): Promise<BigQueryJob> {
  await insertBigQueryJob(plan, config, accessToken, fetcher);
  return pollBigQueryJob(plan, config, accessToken, fetcher, sleep);
}

export async function submitBigQueryExport(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher = fetch,
): Promise<void> {
  await insertBigQueryJob(plan, config, accessToken, fetcher);
}

export async function verifyFailedExportRecovery(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher = fetch,
): Promise<void> {
  const params = new URLSearchParams({ location: config.location });
  const jobUrl = `${BIGQUERY_API_URL}/projects/${encodeURIComponent(config.projectId)}/jobs/${encodeURIComponent(plan.jobId)}?${params}`;
  const jobResponse = await fetcher(jobUrl, {
    headers: googleHeaders(accessToken),
    signal: AbortSignal.timeout(Math.min(config.jobTimeoutMs, 30_000)),
  });
  if (!jobResponse.ok) {
    throw new Error(`${plan.resourceName} failed job lookup returned ${jobResponse.status}`);
  }
  const job = (await jobResponse.json()) as BigQueryJob;
  assertJobMatchesPlan(plan, config, job);
  if (job.status?.state !== "DONE" || !job.status.errorResult) {
    throw new Error(`${plan.resourceName} BigQuery job is not a terminal failure`);
  }

  const destination = parseGcsDestination(plan.uri);
  const objectParams = new URLSearchParams({
    prefix: destination.prefix,
    maxResults: "1",
  });
  const objectUrl = `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(destination.bucket)}/o?${objectParams}`;
  const objectResponse = await fetcher(objectUrl, {
    headers: googleHeaders(accessToken),
    signal: AbortSignal.timeout(Math.min(config.jobTimeoutMs, 30_000)),
  });
  if (!objectResponse.ok) {
    throw new Error(`${plan.resourceName} GCS destination lookup returned ${objectResponse.status}`);
  }
  const objects = (await objectResponse.json()) as {
    items?: unknown[];
    nextPageToken?: unknown;
  };
  if (
    (objects.items?.length ?? 0) !== 0 ||
    (typeof objects.nextPageToken === "string" && objects.nextPageToken !== "")
  ) {
    throw new Error(`${plan.resourceName} GCS destination is not empty`);
  }
}

function parseGcsDestination(uri: string): { bucket: string; prefix: string } {
  const match = /^gs:\/\/([^/]+)\/(.+)\/part-\*\.parquet$/.exec(uri);
  if (!match) throw new Error("Export plan has an invalid immutable GCS URI");
  return { bucket: match[1], prefix: `${match[2]}/` };
}

export async function verifyBigQuerySourceMetadata(
  tables: readonly SourceTable[],
  metadata: (table: SourceTable) => ChangeHistoryGuard,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher = fetch,
): Promise<void> {
  const query = tables
    .map((table) => buildMetadataAssertion(table, metadata(table)))
    .join("\n");
  const url = `${BIGQUERY_API_URL}/projects/${encodeURIComponent(config.projectId)}/queries`;
  const response = await fetcher(url, {
    method: "POST",
    headers: googleHeaders(accessToken),
    body: JSON.stringify({
      query,
      useLegacySql: false,
      location: config.location,
      timeoutMs: Math.min(config.jobTimeoutMs, 30_000),
    }),
    signal: AbortSignal.timeout(Math.min(config.jobTimeoutMs, 30_000)),
  });
  if (!response.ok) {
    throw new Error(`BigQuery source metadata verification failed (${response.status})`);
  }
  const result = (await response.json()) as {
    jobComplete?: unknown;
    errors?: unknown[];
  };
  if (result.jobComplete !== true || (result.errors?.length ?? 0) > 0) {
    throw new Error("BigQuery source metadata verification was incomplete");
  }
}

export async function checkBigQueryExport(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher = fetch,
): Promise<
  | { status: "missing" }
  | { status: "pending" }
  | { status: "done"; job: BigQueryJob }
> {
  const query = new URLSearchParams({ location: config.location });
  const url = [
    `${BIGQUERY_API_URL}/projects/${encodeURIComponent(config.projectId)}`,
    `jobs/${encodeURIComponent(plan.jobId)}?${query.toString()}`,
  ].join("/");
  const response = await fetcher(url, {
    headers: googleHeaders(accessToken),
    signal: AbortSignal.timeout(Math.min(config.jobTimeoutMs, 30_000)),
  });
  if (response.status === 404) return { status: "missing" };
  if (!response.ok)
    throw new Error(
      `${plan.resourceName} BigQuery job lookup failed (${response.status})`,
    );
  const job = (await response.json()) as BigQueryJob;
  assertJobMatchesPlan(plan, config, job);
  if (job.status?.state !== "DONE") return { status: "pending" };
  assertBigQueryJobSucceeded(plan, job);
  const exportJob = await resolveExportJob(
    plan,
    job,
    config,
    accessToken,
    fetcher,
  );
  return { status: "done", job: exportJob };
}

async function resolveExportJob(
  plan: ExportPlan,
  parent: BigQueryJob,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher,
): Promise<BigQueryJob> {
  if (parent.statistics?.query?.exportDataStatistics) return parent;

  const query = new URLSearchParams({
    location: config.location,
    parentJobId: plan.jobId,
    projection: "full",
  });
  const url = `${BIGQUERY_API_URL}/projects/${encodeURIComponent(config.projectId)}/jobs?${query}`;
  const response = await fetcher(url, {
    headers: googleHeaders(accessToken),
    signal: AbortSignal.timeout(Math.min(config.jobTimeoutMs, 30_000)),
  });
  if (!response.ok) {
    throw new Error(
      `${plan.resourceName} BigQuery child job lookup failed (${response.status})`,
    );
  }

  const payload = (await response.json()) as { jobs?: BigQueryJob[] };
  const exportJobs = (payload.jobs ?? []).filter((job) =>
    job.configuration?.query?.query?.trimStart().startsWith("EXPORT DATA"),
  );
  if (exportJobs.length !== 1) {
    throw new Error(`${plan.resourceName} expected exactly one EXPORT DATA child job`);
  }

  const child = exportJobs[0];
  if (child.status?.state !== "DONE") {
    throw new Error(`${plan.resourceName} EXPORT DATA child job is not complete`);
  }
  assertBigQueryJobSucceeded(plan, child);
  if (!child.statistics?.query?.exportDataStatistics) {
    throw new Error(`${plan.resourceName} EXPORT DATA child statistics missing`);
  }
  return child;
}

function buildGcsUri(
  table: SourceTable,
  runAt: Date,
  config: ExportConfig,
): string {
  const runDate = runAt.toISOString().slice(0, 10);
  const runTime = runAt.toISOString().slice(11, 19).replaceAll(":", "");
  const prefix = config.prefix.replace(/^\/+|\/+$/g, "");

  return [
    `gs://${config.bucket}`,
    prefix,
    table.resourceName,
    `commit-${runDate.replaceAll("-", "")}-${runTime}`,
    "part-*.parquet",
  ]
    .filter(Boolean)
    .join("/");
}

function buildBackfillGcsUri(
  table: SourceTable,
  snapshotAt: Date,
  config: ExportConfig,
): string {
  const snapshot = snapshotAt
    .toISOString()
    .slice(0, 19)
    .replaceAll(/[-:T]/g, "");
  const prefix = config.prefix.replace(/^\/+|\/+$/g, "");

  return [
    `gs://${config.bucket}`,
    prefix,
    table.resourceName,
    `backfill-${snapshot}-*.parquet`,
  ].join("/");
}

function buildBridgeGcsUri(
  table: SourceTable,
  bridgeEnd: Date,
  config: Pick<ExportConfig, "bucket" | "prefix">,
): string {
  const timestamp = compactTimestamp(bridgeEnd);
  const prefix = config.prefix.replace(/^\/+|\/+$/g, "");
  return `gs://${config.bucket}/${prefix}/${table.resourceName}/bridge-${timestamp}/part-*.parquet`;
}

function buildJobId(resourceName: string, runAt: Date): string {
  const timestamp = runAt.toISOString().slice(0, 19).replaceAll(/[-:T]/g, "");
  const resource = resourceName.replaceAll(/[^a-zA-Z0-9_-]/g, "_");

  return `tinybird_incremental_${timestamp}_${resource}`;
}

function buildBridgeJobId(resourceName: string, bridgeEnd: Date): string {
  return `tinybird_bridge_${compactTimestamp(bridgeEnd)}_${safeJobPart(resourceName)}`;
}

function compactTimestamp(value: Date): string {
  return value.toISOString().slice(0, 19).replaceAll(/[-:T]/g, "");
}

function safeJobPart(value: string): string {
  return value.replaceAll(/[^a-zA-Z0-9_-]/g, "_");
}

function assertTableWithPrimaryId(table: SourceTable): void {
  if (table.exportKind !== "table") {
    throw new Error(`${table.resourceName} is not a physical source table.`);
  }
  if (!table.columns.includes("id")) {
    throw new Error(`${table.resourceName} has no supported primary id.`);
  }
}

function assertOrderedWindow(start: Date, end: Date): void {
  if (!Number.isFinite(start.valueOf()) || !Number.isFinite(end.valueOf())) {
    throw new Error("Change window timestamps must be valid.");
  }
  if (start >= end) {
    throw new Error("Change window start must precede its end.");
  }
}

function assertChangeWindow(
  table: SourceTable,
  start: Date,
  end: Date,
  guard?: ChangeHistoryGuard,
): void {
  assertOrderedWindow(start, end);
  if (end.valueOf() - start.valueOf() > 24 * 60 * MINUTE) {
    throw new Error("CHANGES windows cannot exceed 24 hours.");
  }
  if (!guard) {
    throw new Error(`${table.resourceName} has no pinned change-history metadata.`);
  }
  if (guard.retentionHours <= 0) {
    throw new Error("Change-history retention must be positive.");
  }
  const earliest = Math.max(
    guard.enabledAt.valueOf(),
    guard.sourceCreatedAt.valueOf(),
    guard.checkedAt.valueOf() - guard.retentionHours * 60 * MINUTE,
  );
  if (start.valueOf() < earliest) {
    throw new Error(`${table.resourceName} change window is outside retained history.`);
  }
}

function buildBridgeExportQuery(
  table: SourceTable,
  bridgeStart: Date,
  bridgeEnd: Date,
  uri: string,
): string {
  const source = quoteTable(table.source);
  const start = formatTimestamp(bridgeStart);
  const end = formatTimestamp(bridgeEnd);
  const cutoff = `TIMESTAMP_SUB(TIMESTAMP '${end}', INTERVAL 1 MICROSECOND)`;
  const endProjection = buildAliasedSelectList(table, "at_end", end, false, "bridge");
  const deletedProjection = buildAliasedSelectList(table, "at_start", end, true, "bridge");
  const comparableColumns = table.columns.map(quoteIdentifier).join(", ");

  return [
    `CREATE TEMP TABLE at_start AS SELECT * FROM ${source} FOR SYSTEM_TIME AS OF TIMESTAMP '${start}';`,
    `CREATE TEMP TABLE at_end AS SELECT * FROM ${source} FOR SYSTEM_TIME AS OF ${cutoff};`,
    "EXPORT DATA OPTIONS(",
    `  uri='${escapeSqlString(uri)}',`,
    "  format='PARQUET',",
    "  overwrite=false",
    ") AS",
    "SELECT",
    endProjection,
    "FROM at_end",
    "LEFT JOIN at_start USING (`id`)",
    "WHERE at_start.`id` IS NULL",
    `   OR TO_JSON_STRING(STRUCT(${table.columns.map((column) => `at_end.${quoteIdentifier(column)}`).join(", ")}))`,
    `      != TO_JSON_STRING(STRUCT(${table.columns.map((column) => `at_start.${quoteIdentifier(column)}`).join(", ")}))`,
    "UNION ALL",
    "SELECT",
    deletedProjection,
    "FROM at_start",
    "LEFT JOIN at_end USING (`id`)",
    "WHERE at_end.`id` IS NULL",
    "ORDER BY `id`;",
    `-- Compared source columns: ${comparableColumns}`,
  ].join("\n");
}

function buildAliasedSelectList(
  table: SourceTable,
  alias: string,
  observedAt: string,
  deleted: boolean,
  kind = "commit_window",
): string {
  const jsonColumns = new Set(table.jsonColumns);
  const geographyColumns = new Set(table.geographyColumns);
  const projections = table.columns.map((column) => {
    const identifier = quoteIdentifier(column);
    const value = `${alias}.${identifier}`;
    if (jsonColumns.has(column)) {
      return `  TO_JSON_STRING(${value}) AS ${identifier}`;
    }
    if (geographyColumns.has(column)) {
      return `  ST_ASWKT(${value}) AS ${identifier}`;
    }
    return `  ${value} AS ${identifier}`;
  });
  projections.push(`  TIMESTAMP '${observedAt}' AS \`_v1_observed_at\``);
  projections.push(`  ${deleted ? "TRUE" : "FALSE"} AS \`_v1_deleted\``);
  projections.push(`  '${kind}' AS \`_v1_observation_kind\``);
  return projections.join(",\n");
}

function buildBackfillJobId(resourceName: string, snapshotAt: Date): string {
  const timestamp = snapshotAt
    .toISOString()
    .slice(0, 19)
    .replaceAll(/[-:T]/g, "");
  const resource = resourceName.replaceAll(/[^a-zA-Z0-9_-]/g, "_");

  return `tinybird_backfill_${timestamp}_${resource}`;
}

interface DeletionReconciliation {
  observedAt: string;
  predicate: string;
}

function activeCampaignDeletionReconciliation(
  table: SourceTable,
  runAt: Date,
): DeletionReconciliation | null {
  if (!table.resourceName.startsWith("raw_activecampaign_")) return null;
  if (
    !table.columns.includes("id") ||
    !table.columns.includes("_fivetran_deleted")
  ) {
    return null;
  }

  const tenMinuteWindow = Math.floor(runAt.valueOf() / 600_000);
  const bucket = ((tenMinuteWindow % 6) + 6) % 6;
  return {
    observedAt: formatTimestamp(runAt),
    predicate: [
      "COALESCE(`_fivetran_deleted`, FALSE)",
      `MOD(MOD(FARM_FINGERPRINT(CAST(\`id\` AS STRING)), 6) + 6, 6) = ${bucket}`,
    ].join(" AND "),
  };
}

function buildSelectList(
  table: SourceTable,
  deletionReconciliation: DeletionReconciliation | null,
  observedAt: string,
): string {
  const jsonColumns = new Set(table.jsonColumns);
  const geographyColumns = new Set(table.geographyColumns);
  const projections = table.columns.map((column) => {
    const identifier = quoteIdentifier(column);

    if (jsonColumns.has(column)) {
      return `  TO_JSON_STRING(${identifier}) AS ${identifier}`;
    }

    if (geographyColumns.has(column)) {
      return `  ST_ASWKT(${identifier}) AS ${identifier}`;
    }

    return `  ${identifier}`;
  });

  if (table.sourcePartitioning?.pseudoColumn) {
    const pseudoColumn = quoteIdentifier(table.sourcePartitioning.pseudoColumn);
    const targetColumn = quoteIdentifier(PARTITION_TIME_TARGET_COLUMN);
    projections.push(`  ${pseudoColumn} AS ${targetColumn}`);
  }

  projections.push(`  TIMESTAMP '${observedAt}' AS \`_v1_observed_at\``);
  if (deletionReconciliation) {
    projections.push(
      [
        "  IF(",
        `    ${deletionReconciliation.predicate},`,
        "    'deletion_reconciliation',",
        "    'incremental'",
        "  ) AS `_v1_observation_kind`",
      ].join("\n"),
    );
  } else {
    projections.push("  'incremental' AS `_v1_observation_kind`");
  }

  return projections.join(",\n");
}

function greatestTimestamp(table: SourceTable): string {
  if (table.watermarkColumns.length === 0) {
    throw new Error(`${table.resourceName} has no temporal version column.`);
  }

  const expressions = table.watermarkColumns.map(({ name, bigqueryType }) => {
    const identifier = quoteIdentifier(name);
    const timestamp = timestampExpression(identifier, bigqueryType);

    return `COALESCE(${timestamp}, ${UNIX_EPOCH})`;
  });

  if (expressions.length === 1) return expressions[0];
  return `GREATEST(${expressions.join(", ")})`;
}

function partitionPredicate(
  table: SourceTable,
  startAt: Date,
  runAt: Date,
): string {
  const partitioning = table.sourcePartitioning;
  if (!partitioning) return "";

  const column = partitioning.field || partitioning.pseudoColumn;
  if (!column) {
    throw new Error(`${table.resourceName} has no partition column.`);
  }

  const start = truncateUtc(startAt, partitioning.type);
  const end = ceilUtc(runAt, partitioning.type);
  const identifier = quoteIdentifier(column);
  const lowerBound = temporalLiteral(start, partitioning.bigqueryType);
  const upperBound = temporalLiteral(end, partitioning.bigqueryType);

  return `${identifier} >= ${lowerBound}\n  AND ${identifier} < ${upperBound}`;
}

function truncateUtc(value: Date, granularity: string): Date {
  const date = new Date(value);
  date.setUTCSeconds(0, 0);

  if (granularity === "HOUR") {
    date.setUTCMinutes(0);
    return date;
  }

  date.setUTCHours(0, 0, 0, 0);
  if (granularity === "DAY") return date;

  date.setUTCDate(1);
  if (granularity === "MONTH") return date;

  date.setUTCMonth(0);
  if (granularity === "YEAR") return date;

  throw new Error(`Unsupported partition granularity: ${granularity}`);
}

function ceilUtc(value: Date, granularity: string): Date {
  const truncated = truncateUtc(value, granularity);
  if (truncated.valueOf() === value.valueOf()) return truncated;

  const end = new Date(truncated);
  if (granularity === "HOUR") end.setUTCHours(end.getUTCHours() + 1);
  if (granularity === "DAY") end.setUTCDate(end.getUTCDate() + 1);
  if (granularity === "MONTH") end.setUTCMonth(end.getUTCMonth() + 1);
  if (granularity === "YEAR") end.setUTCFullYear(end.getUTCFullYear() + 1);

  return end;
}

function temporalLiteral(value: Date, bigqueryType: string): string {
  if (bigqueryType === "TIMESTAMP") {
    return `TIMESTAMP '${formatTimestamp(value)}'`;
  }

  if (bigqueryType === "DATETIME") {
    return `DATETIME '${value.toISOString().slice(0, 19).replace("T", " ")}'`;
  }

  if (bigqueryType === "DATE") {
    return `DATE '${value.toISOString().slice(0, 10)}'`;
  }

  throw new Error(`Unsupported partition column type: ${bigqueryType}`);
}

function timestampExpression(identifier: string, bigqueryType: string): string {
  if (bigqueryType === "TIMESTAMP") return identifier;
  if (bigqueryType === "DATETIME") return `TIMESTAMP(${identifier}, 'UTC')`;
  if (bigqueryType === "DATE") return `TIMESTAMP(${identifier})`;

  throw new Error(`Unsupported watermark type: ${bigqueryType}`);
}

async function insertBigQueryJob(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher,
): Promise<void> {
  const projectId = encodeURIComponent(config.projectId);
  const url = `${BIGQUERY_API_URL}/projects/${projectId}/jobs`;
  const response = await fetcher(url, {
    method: "POST",
    headers: googleHeaders(accessToken),
    body: JSON.stringify({
      jobReference: {
        jobId: plan.jobId,
        location: config.location,
        projectId: config.projectId,
      },
      configuration: {
        query: {
          query: plan.query,
          useLegacySql: false,
        },
      },
    }),
    signal: AbortSignal.timeout(Math.min(config.jobTimeoutMs, 30_000)),
  });

  if (response.ok) {
    await response.body?.cancel();
    return;
  }

  if (response.status === 409) {
    await response.body?.cancel();
    await assertExistingJobMatches(plan, config, accessToken, fetcher);
    return;
  }

  await response.body?.cancel().catch(() => undefined);
  throw new Error(
    `${plan.resourceName} BigQuery job creation failed (${response.status})`,
  );
}

async function assertExistingJobMatches(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher,
): Promise<void> {
  const params = new URLSearchParams({ location: config.location });
  const url = `${BIGQUERY_API_URL}/projects/${encodeURIComponent(config.projectId)}/jobs/${encodeURIComponent(plan.jobId)}?${params}`;
  const response = await fetcher(url, {
    headers: googleHeaders(accessToken),
    signal: AbortSignal.timeout(Math.min(config.jobTimeoutMs, 30_000)),
  });
  if (!response.ok) {
    throw new Error(`${plan.resourceName} could not verify conflicting BigQuery job`);
  }
  const job = (await response.json()) as BigQueryJob;
  assertJobMatchesPlan(plan, config, job);
}

function assertJobMatchesPlan(
  plan: ExportPlan,
  config: BigQueryConfig,
  job: BigQueryJob,
): void {
  const reference = job.jobReference;
  if (
    reference?.jobId === plan.jobId &&
    reference.projectId === config.projectId &&
    reference.location === config.location &&
    job.configuration?.query?.query === plan.query
  ) {
    return;
  }
  throw new Error(`${plan.resourceName} BigQuery job does not match saved plan`);
}

async function pollBigQueryJob(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher,
  sleep: Sleep,
): Promise<BigQueryJob> {
  const maximumPolls =
    Math.ceil(config.jobTimeoutMs / config.pollIntervalMs) + 1;
  const query = new URLSearchParams({ location: config.location });
  const url = [
    `${BIGQUERY_API_URL}/projects/${encodeURIComponent(config.projectId)}`,
    `jobs/${encodeURIComponent(plan.jobId)}?${query.toString()}`,
  ].join("/");

  for (let poll = 0; poll < maximumPolls; poll += 1) {
    const response = await fetcher(url, {
      headers: googleHeaders(accessToken),
    });

    if (!response.ok) {
      throw new Error(
        `${plan.resourceName} BigQuery job lookup failed (${response.status}): ${await responseSummary(response)}`,
      );
    }

    const job = (await response.json()) as BigQueryJob;
    if (job.status?.state === "DONE") {
      assertBigQueryJobSucceeded(plan, job);
      return job;
    }

    if (poll === maximumPolls - 1) break;
    await sleep(config.pollIntervalMs);
  }

  throw new Error(
    `${plan.resourceName} BigQuery job did not finish within ${config.jobTimeoutMs} ms.`,
  );
}

function assertBigQueryJobSucceeded(plan: ExportPlan, job: BigQueryJob): void {
  const error = job.status?.errorResult;
  if (!error) return;

  const details = job.status?.errors?.map(formatBigQueryError).join("; ");
  const message = details || formatBigQueryError(error);
  throw new Error(`${plan.resourceName} BigQuery export failed: ${message}`);
}

function formatBigQueryError(error: BigQueryError): string {
  const reason = error.reason ? `${error.reason}: ` : "";
  return `${reason}${error.message || "unknown BigQuery error"}`;
}

function googleHeaders(accessToken: string): HeadersInit {
  return {
    Authorization: `Bearer ${accessToken}`,
    "Content-Type": "application/json",
  };
}

function parseServiceAccount(value: string): ServiceAccountCredentials {
  let credentials: unknown;

  try {
    credentials = JSON.parse(value);
  } catch {
    throw new Error("GCP_SERVICE_ACCOUNT_JSON is not valid JSON.");
  }

  if (!isRecord(credentials)) {
    throw new Error("GCP_SERVICE_ACCOUNT_JSON must be a JSON object.");
  }

  const clientEmail = credentials.client_email;
  const privateKey = credentials.private_key;
  if (typeof clientEmail !== "string" || !clientEmail) {
    throw new Error("GCP_SERVICE_ACCOUNT_JSON has no client_email.");
  }

  if (typeof privateKey !== "string" || !privateKey) {
    throw new Error("GCP_SERVICE_ACCOUNT_JSON has no private_key.");
  }

  return {
    client_email: clientEmail,
    private_key: privateKey,
    private_key_id: optionalString(credentials.private_key_id),
    token_uri: optionalString(credentials.token_uri),
  };
}

async function signServiceAccountJwt(
  credentials: ServiceAccountCredentials,
  now: Date,
): Promise<string> {
  const issuedAt = Math.floor(now.valueOf() / 1_000) - 5;
  const header = {
    alg: "RS256",
    typ: "JWT",
    ...(credentials.private_key_id ? { kid: credentials.private_key_id } : {}),
  };
  const claims = {
    aud: GOOGLE_TOKEN_URL,
    exp: issuedAt + 3_600,
    iat: issuedAt,
    iss: credentials.client_email,
    scope: GOOGLE_CLOUD_SCOPE,
  };
  const unsigned = [header, claims]
    .map((value) => base64Url(new TextEncoder().encode(JSON.stringify(value))))
    .join(".");
  const privateKey = await importPrivateKey(credentials.private_key);
  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    privateKey,
    new TextEncoder().encode(unsigned),
  );

  return `${unsigned}.${base64Url(new Uint8Array(signature))}`;
}

async function importPrivateKey(pem: string): Promise<CryptoKey> {
  const encoded = pem
    .replace("-----BEGIN PRIVATE KEY-----", "")
    .replace("-----END PRIVATE KEY-----", "")
    .replaceAll(/\s/g, "");

  if (!encoded) {
    throw new Error("GCP service account private key is empty.");
  }

  let keyBytes: Uint8Array;

  try {
    keyBytes = decodeBase64(encoded);
  } catch {
    throw new Error("GCP service account private key is not valid PKCS#8 PEM.");
  }

  try {
    return await crypto.subtle.importKey(
      "pkcs8",
      keyBytes,
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
      false,
      ["sign"],
    );
  } catch {
    throw new Error("GCP service account private key is not valid PKCS#8 PEM.");
  }
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";

  for (const byte of bytes) binary += String.fromCharCode(byte);

  return btoa(binary)
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/g, "");
}

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);

  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }

  return bytes;
}

function quoteTable(source: SourceTable["source"]): string {
  const path = [source.project, source.dataset, source.table]
    .map((part) => part.replaceAll("`", ""))
    .join(".");

  return `\`${path}\``;
}

function quoteIdentifier(value: string): string {
  return `\`${value.replaceAll("`", "")}\``;
}

function normalizeObjectSegment(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function formatTimestamp(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "+00");
}

function escapeSqlString(value: string): string {
  return value.replaceAll("'", "''");
}

async function responseSummary(response: Response): Promise<string> {
  const text = (await response.text()).trim();
  if (!text) return response.statusText || "empty response";
  return text.slice(0, 2_000);
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
