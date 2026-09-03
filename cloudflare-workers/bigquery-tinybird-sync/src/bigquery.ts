import type { SourceTable } from "./table-manifest.generated";

const BIGQUERY_API_URL = "https://bigquery.googleapis.com/bigquery/v2";
const GOOGLE_CLOUD_SCOPE = "https://www.googleapis.com/auth/cloud-platform";
const GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";
const JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";
const UNIX_EPOCH = "TIMESTAMP '1970-01-01 00:00:00+00'";
const PARTITION_TIME_EXPORT_PATH = "partition_time_v1";
const PARTITION_TIME_TARGET_COLUMN = "source_partition_time";

export type Fetcher = (
  input: Request | string | URL,
  init?: RequestInit,
) => Promise<Response>;

export type Sleep = (milliseconds: number) => Promise<void>;

export interface ExportConfig {
  bucket: string;
  prefix: string;
  overlapMinutes: number;
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

interface ServiceAccountCredentials {
  client_email: string;
  private_key: string;
  private_key_id?: string;
  token_uri?: string;
}

interface BigQueryJob {
  status?: {
    state?: string;
    errorResult?: BigQueryError;
    errors?: BigQueryError[];
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
): ExportPlan {
  const uri = buildGcsUri(table, runAt, config);
  const query = buildExportQuery(table, runAt, uri, config.overlapMinutes);

  return {
    resourceName: table.resourceName,
    jobId: buildJobId(table.resourceName, runAt),
    query,
    uri,
  };
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

export function buildExportQuery(
  table: SourceTable,
  runAt: Date,
  uri: string,
  overlapMinutes: number,
): string {
  if (table.exportKind === "typed_union") {
    return buildTypedUnionExportQuery(
      table,
      runAt,
      uri,
      overlapMinutes,
    );
  }

  const startAt = new Date(runAt.valueOf() - overlapMinutes * 60_000);
  const version = greatestTimestamp(table);
  const partition = partitionPredicate(table, startAt, runAt);
  const source = quoteTable(table.source);
  const deletionReconciliation = activeCampaignDeletionReconciliation(table, runAt);
  const selectList = buildSelectList(table, deletionReconciliation);
  const versionPredicates = [
    `${version} >= TIMESTAMP '${formatTimestamp(startAt)}'`,
    `${version} < TIMESTAMP '${formatTimestamp(runAt)}'`,
  ];
  const incrementalPredicates = partition
    ? [partition, ...versionPredicates]
    : versionPredicates;
  const where = deletionReconciliation
    ? `(${incrementalPredicates.join("\n  AND ")})\n  OR (${deletionReconciliation.predicate})`
    : incrementalPredicates.join("\n  AND ");

  return [
    "EXPORT DATA OPTIONS(",
    `  uri='${escapeSqlString(uri)}',`,
    "  format='PARQUET',",
    "  overwrite=true",
    ") AS",
    "SELECT",
    selectList,
    `FROM ${source}`,
    `WHERE ${where}`,
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

  const sourceQueries = table.unionSources.map((source) => (
    buildTypedUnionSourceQuery(table, source, startAt, runAt)
  ));

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
    throw new Error("GCP service account token_uri must use Google's OAuth endpoint.");
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

  const payload = await response.json() as { access_token?: unknown };
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
): Promise<void> {
  await insertBigQueryJob(plan, config, accessToken, fetcher);
  await pollBigQueryJob(plan, config, accessToken, fetcher, sleep);
}

function buildGcsUri(
  table: SourceTable,
  runAt: Date,
  config: ExportConfig,
): string {
  const dataset = normalizeObjectSegment(table.source.dataset);
  const tableName = normalizeObjectSegment(table.source.table);
  const runDate = runAt.toISOString().slice(0, 10);
  const runTime = runAt.toISOString().slice(11, 19).replaceAll(":", "");
  const prefix = config.prefix.replace(/^\/+|\/+$/g, "");

  return [
    `gs://${config.bucket}`,
    prefix,
    dataset,
    tableName,
    table.sourcePartitioning?.pseudoColumn ? PARTITION_TIME_EXPORT_PATH : "",
    "incremental",
    `run_date=${runDate}`,
    `run_time=${runTime}`,
    "part-*.parquet",
  ].filter(Boolean).join("/");
}

function buildBackfillGcsUri(
  table: SourceTable,
  snapshotAt: Date,
  config: ExportConfig,
): string {
  const dataset = normalizeObjectSegment(table.source.dataset);
  const tableName = normalizeObjectSegment(table.source.table);
  const snapshot = snapshotAt.toISOString()
    .slice(0, 19)
    .replaceAll(/[-:T]/g, "");
  const prefix = config.prefix.replace(/^\/+|\/+$/g, "");

  return [
    `gs://${config.bucket}`,
    prefix,
    dataset,
    tableName,
    "backfill",
    `snapshot_at=${snapshot}`,
    "part-*.parquet",
  ].join("/");
}

function buildJobId(resourceName: string, runAt: Date): string {
  const timestamp = runAt.toISOString()
    .slice(0, 19)
    .replaceAll(/[-:T]/g, "");
  const resource = resourceName.replaceAll(/[^a-zA-Z0-9_-]/g, "_");

  return `tinybird_incremental_${timestamp}_${resource}`;
}

function buildBackfillJobId(resourceName: string, snapshotAt: Date): string {
  const timestamp = snapshotAt.toISOString()
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
  if (table.resourceName !== "raw_activecampaign_contact_tag") return null;

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
): string {
  const jsonColumns = new Set(table.jsonColumns);
  const geographyColumns = new Set(table.geographyColumns);
  const projections = table.columns.map((column) => {
    const identifier = quoteIdentifier(column);

    if (column === "_fivetran_synced" && deletionReconciliation) {
      return [
        "  IF(",
        "    COALESCE(`_fivetran_deleted`, FALSE),",
        `    TIMESTAMP '${deletionReconciliation.observedAt}',`,
        "    `_fivetran_synced`",
        "  ) AS `_fivetran_synced`",
      ].join("\n");
    }

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
  });

  if (response.ok || response.status === 409) {
    await response.body?.cancel();
    return;
  }

  throw new Error(
    `${plan.resourceName} BigQuery job creation failed (${response.status}): ${await responseSummary(response)}`,
  );
}

async function pollBigQueryJob(
  plan: ExportPlan,
  config: BigQueryConfig,
  accessToken: string,
  fetcher: Fetcher,
  sleep: Sleep,
): Promise<void> {
  const maximumPolls = Math.ceil(config.jobTimeoutMs / config.pollIntervalMs) + 1;
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

    const job = await response.json() as BigQueryJob;
    if (job.status?.state === "DONE") {
      assertBigQueryJobSucceeded(plan, job);
      return;
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
