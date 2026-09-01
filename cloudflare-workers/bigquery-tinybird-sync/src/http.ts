import {
  MAX_TABLES_PER_RUN,
  MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE,
  RAW_GENERATION_COVERAGE_MINUTES,
  RAW_TRIGGER_INTERVAL_MINUTES,
  SHARD_COUNT,
  shardSizes,
  slotForTime,
  type WorkerEnv,
} from "./sync";
import { TABLE_MANIFEST } from "./table-manifest.generated";
import type { RecoveryAction } from "./publication-coordinator";

const SERVICE_NAME = "bigquery-tinybird-sync";

export async function handleRequest(
  request: Request,
  env: WorkerEnv,
): Promise<Response> {
  const url = new URL(request.url);

  if (url.pathname === "/health") {
    if (request.method !== "GET") return methodNotAllowed("GET");
    const authError = authorize(request, env);
    if (authError) return authError;

    return jsonResponse(health(env));
  }

  if (url.pathname === "/run") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handleManualRun(request, env);
  }

  if (url.pathname === "/publication/status") {
    if (request.method !== "GET") return methodNotAllowed("GET");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handlePublicationStatus(env);
  }

  if (url.pathname === "/publication/pause") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handlePublicationPause(request, env);
  }

  if (url.pathname === "/publication/compact-backlog") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handleBacklogCompaction(env);
  }

  if (url.pathname === "/publication/recover-raw-lease") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handleRawLeaseRecovery(env);
  }

  if (url.pathname === "/publication/bootstrap") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handleBootstrap(request, env);
  }

  if (url.pathname === "/publication/journey-backfill") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handleJourneyBackfill(env);
  }

  if (url.pathname === "/publication/recover") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handleRecovery(request, env);
  }

  if (url.pathname === "/journey/status") {
    if (request.method !== "GET") return methodNotAllowed("GET");
    const authError = authorize(request, env);
    if (authError) return authError;

    return jsonResponse(await journeyCoordinator(env).status());
  }

  if (url.pathname === "/journey/recover") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return jsonResponse(await journeyCoordinator(env).recoverFailed());
  }

  if (url.pathname === "/journey/repair") {
    if (request.method !== "POST") return methodNotAllowed("POST");
    const authError = authorize(request, env);
    if (authError) return authError;

    return handleJourneyRepair(request, env);
  }

  return jsonResponse({ error: "Not found" }, 404);
}

async function handleManualRun(
  request: Request,
  env: WorkerEnv,
): Promise<Response> {
  let input: { runAt: Date; slot?: number };

  try {
    input = await parseManualRun(request);
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }

  try {
    const result = await coordinator(env).enqueueRawRun({
      scheduledAt: input.runAt.toISOString(),
      slot: input.slot,
    });
    logQueuedRun(result);
    return jsonResponse(result, 202);
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 502);
  }
}

async function handlePublicationStatus(env: WorkerEnv): Promise<Response> {
  try {
    return jsonResponse(await coordinator(env).status());
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 502);
  }
}

async function handlePublicationPause(
  request: Request,
  env: WorkerEnv,
): Promise<Response> {
  try {
    const input = await parseJsonObject(request);
    if (typeof input.paused !== "boolean") {
      throw new Error("paused must be true or false.");
    }
    if (input.paused && input.afterPublication === true) {
      return jsonResponse(await coordinator(env).pauseAfterPublication());
    }
    return jsonResponse(await coordinator(env).setOperatorPaused(input.paused));
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }
}

async function handleBacklogCompaction(env: WorkerEnv): Promise<Response> {
  try {
    return jsonResponse(await coordinator(env).compactRawBacklog());
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }
}

async function handleRawLeaseRecovery(env: WorkerEnv): Promise<Response> {
  try {
    return jsonResponse(await coordinator(env).recoverExpiredRawRunLease());
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }
}

async function handleBootstrap(request: Request, env: WorkerEnv): Promise<Response> {
  try {
    const mode = await parseBootstrapMode(request);
    return jsonResponse(await coordinator(env).configureBootstrap(mode));
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }
}

async function handleJourneyBackfill(env: WorkerEnv): Promise<Response> {
  try {
    return jsonResponse(await coordinator(env).startJourneyBackfill(), 202);
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }
}

async function handleJourneyRepair(request: Request, env: WorkerEnv): Promise<Response> {
  try {
    const input = await parseJsonObject(request);
    const repairId = input.repairId;
    const profileIds = input.profileIds;
    if (typeof repairId !== "string") throw new Error("repairId must be a string.");
    if (!Array.isArray(profileIds)) throw new Error("profileIds must be an array.");

    return jsonResponse(await journeyCoordinator(env).enqueueRepair({
      repairId,
      profileIds: profileIds.map((profileId) => {
        if (typeof profileId !== "string") {
          throw new Error("profileIds must contain only strings.");
        }
        return profileId;
      }),
    }), 202);
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }
}

async function handleRecovery(request: Request, env: WorkerEnv): Promise<Response> {
  try {
    const input = await parseJsonObject(request);
    const action = parseRecoveryAction(input.action);
    return jsonResponse(await coordinator(env).recoverFailed(action));
  } catch (error) {
    return jsonResponse({ error: errorMessage(error) }, 400);
  }
}

async function parseManualRun(request: Request): Promise<{
  runAt: Date;
  slot?: number;
}> {
  const text = await request.text();
  if (!text.trim()) return { runAt: new Date() };

  let input: unknown;

  try {
    input = JSON.parse(text);
  } catch {
    throw new Error("Request body must be valid JSON.");
  }

  if (!isRecord(input)) throw new Error("Request body must be a JSON object.");

  const runAt = input.scheduledAt === undefined
    ? new Date()
    : parseRunAt(input.scheduledAt);
  const slot = input.slot === undefined ? undefined : parseSlot(input.slot);

  return { runAt, slot };
}

function health(env: WorkerEnv) {
  const now = new Date();

  return {
    ok: true,
    service: SERVICE_NAME,
    tableCount: TABLE_MANIFEST.length,
    shardCount: SHARD_COUNT,
    shardSizes: shardSizes(),
    maximumTablesPerRun: MAX_TABLES_PER_RUN,
    cadence: {
      plannedTriggerIntervalMinutes: RAW_TRIGGER_INTERVAL_MINUTES,
      rawSlotsPerGeneration: SHARD_COUNT,
      healthyPathRawCoverageMinutes: RAW_GENERATION_COVERAGE_MINUTES,
      maximumTinybirdIngestionCallsPerRollingMinute:
        MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE,
      endToEndPublicationIntervalGuaranteed: false,
    },
    currentSlot: slotForTime(now),
    configured: {
      gcpServiceAccount: Boolean(env.GCP_SERVICE_ACCOUNT_JSON),
      tinybirdAdminToken: Boolean(env.TINYBIRD_ADMIN_TOKEN),
      project: env.BIGQUERY_PROJECT_ID,
      location: env.BIGQUERY_LOCATION,
      bucket: env.GCS_BUCKET,
      prefix: env.GCS_PREFIX,
    },
  };
}

async function parseBootstrapMode(
  request: Request,
): Promise<"run" | "acknowledge_existing"> {
  const input = await parseJsonObject(request);
  if (input.mode === "run" || input.mode === "acknowledge_existing") {
    return input.mode;
  }

  throw new Error("mode must be run or acknowledge_existing.");
}

async function parseJsonObject(request: Request): Promise<Record<string, unknown>> {
  const text = await request.text();
  if (!text.trim()) throw new Error("Request body must be a JSON object.");

  let input: unknown;
  try {
    input = JSON.parse(text);
  } catch {
    throw new Error("Request body must be valid JSON.");
  }

  if (isRecord(input)) return input;
  throw new Error("Request body must be a JSON object.");
}

function parseRecoveryAction(value: unknown): RecoveryAction {
  if (
    value === "retry_known_job_or_raw"
    || value === "retry_copy_after_confirming_no_job"
    || value === "retry_terminal_copy"
  ) {
    return value;
  }

  throw new Error(
    "action must be retry_known_job_or_raw, retry_copy_after_confirming_no_job, or retry_terminal_copy.",
  );
}

function authorize(request: Request, env: WorkerEnv): Response | null {
  if (!env.SYNC_ADMIN_TOKEN) {
    return jsonResponse({ error: "SYNC_ADMIN_TOKEN is not configured" }, 503);
  }

  const authorization = request.headers.get("Authorization");
  const expected = `Bearer ${env.SYNC_ADMIN_TOKEN}`;
  if (authorization && constantTimeEqual(authorization, expected)) return null;

  return jsonResponse(
    { error: "Unauthorized" },
    401,
    { "WWW-Authenticate": "Bearer" },
  );
}

function constantTimeEqual(left: string, right: string): boolean {
  const length = Math.max(left.length, right.length);
  let difference = left.length ^ right.length;

  for (let index = 0; index < length; index += 1) {
    difference |= (left.charCodeAt(index) || 0) ^ (right.charCodeAt(index) || 0);
  }

  return difference === 0;
}

function parseRunAt(value: unknown): Date {
  if (typeof value !== "string") {
    throw new Error("scheduledAt must be an ISO timestamp string.");
  }

  const runAt = new Date(value);
  if (Number.isNaN(runAt.valueOf())) {
    throw new Error("scheduledAt must be an ISO timestamp string.");
  }

  return runAt;
}

function parseSlot(value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new Error(`slot must be an integer from 0 through ${SHARD_COUNT - 1}.`);
  }

  if (value < 0 || value >= SHARD_COUNT) {
    throw new Error(`slot must be an integer from 0 through ${SHARD_COUNT - 1}.`);
  }

  return value;
}

function methodNotAllowed(method: string): Response {
  return jsonResponse(
    { error: "Method not allowed" },
    405,
    { Allow: method },
  );
}

function jsonResponse(
  body: object,
  status = 200,
  headers: HeadersInit = {},
): Response {
  return Response.json(body, {
    status,
    headers: {
      "Cache-Control": "no-store",
      ...headers,
    },
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function coordinator(env: WorkerEnv) {
  return env.PUBLICATION_COORDINATOR.getByName(
    "bill-publication-coordinator",
  );
}

function journeyCoordinator(env: WorkerEnv) {
  return env.JOURNEY_COORDINATOR.getByName("boom");
}

function logQueuedRun(result: object): void {
  console.log(JSON.stringify({
    service: SERVICE_NAME,
    event: "manual_run_queued",
    ...result,
  }));
}
