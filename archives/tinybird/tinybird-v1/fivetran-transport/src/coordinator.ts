import { DurableObject } from "cloudflare:workers";
import {
  buildExportPlan,
  buildBridgeExportPlan,
  checkBigQueryExport,
  createGoogleAccessToken,
  nextCatchupTarget,
  submitBigQueryExport,
  verifyBigQuerySourceMetadata,
  verifyFailedExportRecovery,
  type BigQueryConfig,
  type ExportPlan,
} from "./bigquery";
import { SOURCE_TABLES, landingName } from "./sources";

const MINUTE = 60_000;
type Phase =
  | "idle"
  | "bq_pending"
  | "exported"
  | "import_unknown"
  | "import_pending"
  | "attention";
export interface Env {
  BIGQUERY_PROJECT_ID: string;
  BIGQUERY_LOCATION: string;
  GCS_BUCKET: string;
  GCS_PREFIX: string;
  GCP_SERVICE_ACCOUNT_JSON: string;
  COMPLETE_THROUGH: string;
  TINYBIRD_API_URL: string;
  TINYBIRD_ADMIN_TOKEN: string;
  EXPORT_OVERLAP_MINUTES?: string;
  BIGQUERY_POLL_INTERVAL_MS?: string;
  BIGQUERY_JOB_TIMEOUT_MS?: string;
  INGESTION_ENABLED?: string;
  TEST_MODE?: string;
  CHANGE_HISTORY_ENABLED_AT: string;
  SOURCE_BRIDGE_THROUGH: string;
  SOURCE_CREATED_AT_JSON: string;
  CHANGE_HISTORY_RETENTION_HOURS?: string;
}
interface State {
  [key: string]: string | number | null;
  completed_through: string;
  target_end: string | null;
  table_index: number;
  phase: Phase;
  plan_json: string | null;
  file_count: number | null;
  row_count: number | null;
  import_job_id: string | null;
  import_attempted_at_ms: number | null;
  completion_seen_at_ms: number | null;
}

export interface FailedExportRecoveryRequest {
  expectedJobId: string;
  expectedSource: string;
  expectedWindowStart: string;
  expectedWindowEnd: string;
}

export class SourceExportCoordinator extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => this.migrate());
  }
  async start() {
    this.state();
    if ((await this.ctx.storage.getAlarm()) === null)
      await this.ctx.storage.setAlarm(Date.now());
    return this.status();
  }
  async status() {
    const s = this.state();
    return {
      completedThrough: s.completed_through,
      targetEnd: s.target_end,
      source: s.target_end ? SOURCE_TABLES[s.table_index]?.resourceName : null,
      phase: s.phase,
      barrierComplete: new Date(s.completed_through) >= currentMinute(),
    };
  }
  async runTestStep() {
    if (this.env.TEST_MODE !== "true") {
      throw new Error("Test stepping is disabled");
    }
    await this.step();
    return this.status();
  }
  async recoverFailedExport(request: FailedExportRecoveryRequest) {
    if (this.env.INGESTION_ENABLED !== "false") {
      throw new Error("Disable ingestion before recovering a failed export");
    }
    const state = this.state();
    const table = SOURCE_TABLES[state.table_index];
    const plan = state.plan_json
      ? (JSON.parse(state.plan_json) as ExportPlan)
      : null;
    if (
      state.phase !== "bq_pending" ||
      !table ||
      !plan ||
      plan.jobId !== request.expectedJobId ||
      table.resourceName !== request.expectedSource ||
      state.completed_through !== request.expectedWindowStart ||
      state.target_end !== request.expectedWindowEnd
    ) {
      throw new Error("Persisted failed export does not match recovery request");
    }
    if (
      state.file_count !== null ||
      state.row_count !== null ||
      state.import_job_id !== null ||
      state.import_attempted_at_ms !== null ||
      state.completion_seen_at_ms !== null
    ) {
      throw new Error("Failed export recovery is no longer before import");
    }
    const receipt = this.ctx.storage.sql
      .exec<{ count: number }>(
        "SELECT count(*) count FROM verified_receipts WHERE source_name=?",
        table.resourceName,
      )
      .one();
    if (receipt.count !== 0) {
      throw new Error("Failed export already has a verified receipt");
    }
    const expectedState = recoveryStateFingerprint(state);

    const token = await createGoogleAccessToken(
      this.env.GCP_SERVICE_ACCOUNT_JSON,
    );
    await verifyFailedExportRecovery(plan, this.bq(), token);
    const currentState = this.state();
    if (recoveryStateFingerprint(currentState) !== expectedState) {
      throw new Error("Failed export state changed during provider verification");
    }
    const currentReceipt = this.ctx.storage.sql
      .exec<{ count: number }>(
        "SELECT count(*) count FROM verified_receipts WHERE source_name=?",
        table.resourceName,
      )
      .one();
    if (currentReceipt.count !== 0) {
      throw new Error("Failed export receipt changed during provider verification");
    }
    const retryPlan = { ...plan, jobId: `${plan.jobId}_retry1` };
    this.update({ phase: "idle", plan_json: JSON.stringify(retryPlan) });
    return this.status();
  }
  async alarm() {
    if (
      this.env.INGESTION_ENABLED !== "true" &&
      this.env.TEST_MODE !== "true"
    ) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    if (this.state().phase === "attention") {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    try {
      await this.ctx.storage.setAlarm(Date.now() + (await this.step()));
    } catch {
      await this.ctx.storage.setAlarm(Date.now() + 30_000);
      throw new Error("Source export step failed");
    }
  }

  private async step(): Promise<number> {
    let s = this.state();
    if (s.phase === "attention") return nextMinute() - Date.now();
    if (!s.target_end) {
      const target = nextTarget(this.env, s.completed_through);
      if (!target) return nextMinute() - Date.now();
      this.update({
        target_end: target.toISOString(),
        table_index: 0,
        phase: "idle",
      });
      s = this.state();
    }
    const table = SOURCE_TABLES[s.table_index];
    if (!table || !s.target_end)
      throw new Error("Invalid durable source state");
    const bridge = bridgeWindow(this.env, s.completed_through, s.target_end);
    const plan = s.plan_json
      ? (JSON.parse(s.plan_json) as ExportPlan)
      : bridge
        ? buildBridgeExportPlan(table, bridge.end, {
            bucket: required(this.env.GCS_BUCKET, "GCS_BUCKET"),
            prefix: required(this.env.GCS_PREFIX, "GCS_PREFIX"),
            bridgeStart: bridge.start,
            sourceMetadata: changeHistoryGuard(this.env, table.resourceName),
          })
        : buildExportPlan(table, new Date(s.target_end), {
            bucket: required(this.env.GCS_BUCKET, "GCS_BUCKET"),
            prefix: required(this.env.GCS_PREFIX, "GCS_PREFIX"),
            overlapMinutes: windowMinutes(s.completed_through, s.target_end),
            changeHistory: changeHistoryGuard(this.env, table.resourceName),
          });
    if (s.phase === "idle") {
      this.update({ phase: "bq_pending", plan_json: JSON.stringify(plan) });
      const token = await createGoogleAccessToken(
        this.env.GCP_SERVICE_ACCOUNT_JSON,
      );
      await submitBigQueryExport(plan, this.bq(), token);
      return 1_000;
    }
    if (s.phase === "bq_pending") {
      const token = await createGoogleAccessToken(
        this.env.GCP_SERVICE_ACCOUNT_JSON,
      );
      const result = await checkBigQueryExport(plan, this.bq(), token);
      if (result.status === "missing") {
        await submitBigQueryExport(plan, this.bq(), token);
        return 1_000;
      }
      if (result.status === "pending") return this.bq().pollIntervalMs;
      const stats = exportStats(result.job);
      this.update({
        phase: "exported",
        file_count: stats.files,
        row_count: stats.rows,
      });
      return 1;
    }
    const expected = { files: number(s.file_count), rows: number(s.row_count) };
    if (
      await receipt(
        landingName(table.resourceName),
        plan.uri,
        expected,
        this.env,
      )
    ) {
      await this.finish(s, table.resourceName, plan, expected);
      return 1;
    }
    if (s.phase === "import_pending") {
      const status = await jobStatus(text(s.import_job_id), this.env);
      if (status === "failed") {
        this.update({ phase: "attention" });
        return 1;
      }
      if (status !== "done") return 5_000;
      if (s.completion_seen_at_ms === null) {
        this.update({ completion_seen_at_ms: Date.now() });
        return 5_000;
      }
      if (Date.now() - s.completion_seen_at_ms >= 5 * MINUTE) {
        this.update({ phase: "attention" });
        return 1;
      }
      return 5_000;
    }
    if (s.phase === "import_unknown") {
      const discovered = await discoverImportJob(
        landingName(table.resourceName),
        number(s.import_attempted_at_ms),
        this.env,
      );
      if (discovered.status === "multiple") {
        this.update({ phase: "attention" });
        return 1;
      }
      if (discovered.status === "missing") {
        if (Date.now() - number(s.import_attempted_at_ms) >= 10 * MINUTE) {
          this.update({ phase: "attention" });
          return 1;
        }
        return 5_000;
      }
      this.update({ phase: "import_pending", import_job_id: discovered.id });
      return 1;
    }
    const wait = this.reserve();
    if (wait) return wait;
    this.update({
      phase: "import_unknown",
      import_attempted_at_ms: Date.now(),
    });
    const id = await submitImport(landingName(table.resourceName), this.env);
    if (id) this.update({ phase: "import_pending", import_job_id: id });
    return 1_000;
  }
  private async finish(
    s: State,
    source: string,
    plan: ExportPlan,
    expected: { files: number; rows: number },
  ) {
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO verified_receipts(source_name,job_id,gcs_uri,file_count,row_count,observation_json) VALUES(?,?,?,?,?,?)",
      source,
      plan.jobId,
      plan.uri,
      expected.files,
      expected.rows,
      JSON.stringify(observationMetadata(source, s.target_end)),
    );
    const clear = {
      phase: "idle" as Phase,
      plan_json: null,
      file_count: null,
      row_count: null,
      import_job_id: null,
      import_attempted_at_ms: null,
      completion_seen_at_ms: null,
    };
    if (s.table_index + 1 < SOURCE_TABLES.length) {
      this.update({ ...clear, table_index: s.table_index + 1 });
      return;
    }
    await this.publishBarrier(s);
    this.update({
      ...clear,
      completed_through: text(s.target_end),
      target_end: null,
      table_index: 0,
    });
    this.ctx.storage.sql.exec("DELETE FROM verified_receipts");
  }
  private async publishBarrier(s: State) {
    const token = await createGoogleAccessToken(
      this.env.GCP_SERVICE_ACCOUNT_JSON,
    );
    await verifyBigQuerySourceMetadata(
      SOURCE_TABLES,
      (table) => changeHistoryGuard(this.env, table.resourceName),
      this.bq(),
      token,
    );
    const rows = this.ctx.storage.sql
      .exec<{
        source_name: string;
        job_id: string;
        gcs_uri: string;
        file_count: number;
        row_count: number;
        observation_json: string;
      }>("SELECT * FROM verified_receipts ORDER BY source_name")
      .toArray();
    if (rows.length !== SOURCE_TABLES.length)
      throw new Error("Source barrier is incomplete");
    const end = text(s.target_end);
    const record = {
      tenant_id: "boom",
      receipt_key: `boom:${end}`,
      window_started_at: s.completed_through,
      complete_through_exclusive: end,
      source_count: rows.length,
      manifest_json: JSON.stringify(rows),
      verified_at: new Date().toISOString(),
    };
    const r = await fetch(
      `${base(this.env)}/v0/events?name=v1_fivetran_source_receipts`,
      {
        method: "POST",
        headers: { ...headers(this.env), "Content-Type": "application/json" },
        body: JSON.stringify(record),
        signal: AbortSignal.timeout(30_000),
      },
    );
    if (!r.ok) throw new Error(`Barrier publication failed (${r.status})`);
    const b = (await r.json()) as {
      successful_rows?: unknown;
      quarantined_rows?: unknown;
    };
    if (b.successful_rows !== 1 || b.quarantined_rows !== 0)
      throw new Error("Barrier publication was incomplete");
  }
  private migrate() {
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS source_state(singleton INTEGER PRIMARY KEY CHECK(singleton=1),completed_through TEXT NOT NULL,target_end TEXT,table_index INTEGER NOT NULL,phase TEXT NOT NULL,plan_json TEXT,file_count INTEGER,row_count INTEGER,import_job_id TEXT,import_attempted_at_ms INTEGER); CREATE TABLE IF NOT EXISTS import_attempts(attempted_at_ms INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS verified_receipts(source_name TEXT PRIMARY KEY,job_id TEXT NOT NULL,gcs_uri TEXT NOT NULL,file_count INTEGER NOT NULL,row_count INTEGER NOT NULL,observation_json TEXT NOT NULL)",
    );
    try {
      this.ctx.storage.sql.exec(
        "ALTER TABLE source_state ADD COLUMN completion_seen_at_ms INTEGER",
      );
    } catch {
      // The column already exists.
    }
  }
  private state(): State {
    const found = this.ctx.storage.sql
      .exec<State>("SELECT * FROM source_state WHERE singleton=1")
      .toArray()[0];
    if (found) return found;
    const start = minute(this.env.COMPLETE_THROUGH).toISOString();
    this.ctx.storage.sql.exec(
      "INSERT INTO source_state(singleton,completed_through,target_end,table_index,phase,plan_json,file_count,row_count,import_job_id,import_attempted_at_ms,completion_seen_at_ms) VALUES(1,?,NULL,0,'idle',NULL,NULL,NULL,NULL,NULL,NULL)",
      start,
    );
    return this.ctx.storage.sql
      .exec<State>("SELECT * FROM source_state WHERE singleton=1")
      .one();
  }
  private update(values: Record<string, string | number | null>) {
    const entries = Object.entries(values);
    this.ctx.storage.sql.exec(
      `UPDATE source_state SET ${entries.map(([k]) => `${k}=?`).join(",")} WHERE singleton=1`,
      ...entries.map(([, v]) => v),
    );
  }
  private reserve() {
    const now = Date.now();
    this.ctx.storage.sql.exec(
      "DELETE FROM import_attempts WHERE attempted_at_ms<=?",
      now - MINUTE,
    );
    const rows = this.ctx.storage.sql
      .exec<{
        attempted_at_ms: number;
      }>("SELECT attempted_at_ms FROM import_attempts ORDER BY attempted_at_ms")
      .toArray();
    if (rows.length >= 5)
      return Math.max(1, rows[0].attempted_at_ms + MINUTE + 1 - now);
    this.ctx.storage.sql.exec("INSERT INTO import_attempts VALUES(?)", now);
    return 0;
  }
  private bq(): BigQueryConfig {
    return {
      projectId: required(this.env.BIGQUERY_PROJECT_ID, "BIGQUERY_PROJECT_ID"),
      location: required(this.env.BIGQUERY_LOCATION, "BIGQUERY_LOCATION"),
      pollIntervalMs: integer(
        this.env.BIGQUERY_POLL_INTERVAL_MS ?? "5000",
        100,
        10_000,
        "BIGQUERY_POLL_INTERVAL_MS",
      ),
      jobTimeoutMs: integer(
        this.env.BIGQUERY_JOB_TIMEOUT_MS ?? "480000",
        1_000,
        600_000,
        "BIGQUERY_JOB_TIMEOUT_MS",
      ),
    };
  }
}

function recoveryStateFingerprint(state: State): string {
  return JSON.stringify({
    completedThrough: state.completed_through,
    targetEnd: state.target_end,
    tableIndex: state.table_index,
    phase: state.phase,
    plan: state.plan_json,
    fileCount: state.file_count,
    rowCount: state.row_count,
    importJobId: state.import_job_id,
    importAttemptedAt: state.import_attempted_at_ms,
    completionSeenAt: state.completion_seen_at_ms,
  });
}

async function submitImport(resource: string, env: Env) {
  const r = await fetch(
    `${base(env)}/v0/datasources/${encodeURIComponent(resource)}/scheduling/runs`,
    {
      method: "POST",
      headers: headers(env),
      signal: AbortSignal.timeout(30_000),
    },
  );
  if (!r.ok) throw new Error(`Tinybird import request failed (${r.status})`);
  const b = (await r.json().catch(() => null)) as {
    job_id?: unknown;
    job?: { job_id?: unknown };
  } | null;
  const id = b?.job_id ?? b?.job?.job_id;
  // An accepted sync can omit its job ID. Recover from the job list or receipt.
  return typeof id === "string" && id.length > 0 ? id : null;
}
async function jobStatus(id: string, env: Env) {
  const r = await fetch(`${base(env)}/v0/jobs/${encodeURIComponent(id)}`, {
    headers: headers(env),
    signal: AbortSignal.timeout(30_000),
  });
  if (!r.ok) throw new Error(`Tinybird job lookup failed (${r.status})`);
  const b = (await r.json()) as { status?: unknown };
  if (b.status === "done") return "done";
  if (b.status === "error" || b.status === "cancelled") return "failed";
  return "working";
}
async function discoverImportJob(
  resource: string,
  attemptedAtMs: number,
  env: Env,
): Promise<
  | { status: "missing" }
  | { status: "multiple" }
  | { status: "found"; id: string }
> {
  const url = new URL(`${base(env)}/v0/jobs`);
  url.searchParams.set("kind", "gcs_sync");
  url.searchParams.set(
    "created_after",
    new Date(attemptedAtMs - 5_000).toISOString(),
  );
  const response = await fetch(url, {
    headers: headers(env),
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    throw new Error(`Tinybird import discovery failed (${response.status})`);
  }
  const payload = (await response.json()) as {
    jobs?: Array<{
      id?: unknown;
      datasource?: { name?: unknown };
    }>;
  };
  const matches = (payload.jobs ?? []).filter(
    (job) => job.datasource?.name === resource && typeof job.id === "string",
  );
  if (matches.length === 0) return { status: "missing" };
  if (matches.length > 1) return { status: "multiple" };
  return { status: "found", id: text(matches[0].id) };
}
async function receipt(
  resource: string,
  uri: string,
  expected: { files: number; rows: number },
  env: Env,
) {
  const marker = uri.replace("gs://", "").replace("*.parquet", "");
  const q = `WITH arrayElement(Options.Values,indexOf(Options.Names,'source')) AS s SELECT countDistinct(arrayElement(splitByChar('?',s),1)) files,sum(rows) rows,sum(rows_quarantine) quarantine FROM tinybird.datasources_ops_log WHERE datasource_name='${resource}' AND event_type='append' AND result='ok' AND position(s,'${marker}')>0 FORMAT JSON`;
  const u = new URL(`${base(env)}/v0/sql`);
  u.searchParams.set("q", q);
  const r = await fetch(u, {
    headers: headers(env),
    signal: AbortSignal.timeout(30_000),
  });
  if (!r.ok) throw new Error(`Tinybird verification failed (${r.status})`);
  const b = (await r.json()) as {
    data?: Array<{ files?: unknown; rows?: unknown; quarantine?: unknown }>;
  };
  const x = b.data?.[0];
  return (
    Number(x?.files) === expected.files &&
    Number(x?.rows) === expected.rows &&
    Number(x?.quarantine) === 0
  );
}
function exportStats(job: {
  statistics?: {
    query?: {
      exportDataStatistics?: { fileCount?: string; rowCount?: string };
    };
  };
}) {
  const s = job.statistics?.query?.exportDataStatistics;
  const files = Number(s?.fileCount),
    rows = Number(s?.rowCount);
  if (
    Number.isInteger(files) &&
    files >= 0 &&
    Number.isInteger(rows) &&
    rows >= 0
  )
    return { files, rows };
  throw new Error("BigQuery export statistics missing");
}
export function transportContract() {
  return SOURCE_TABLES.map((t) => ({
    source: t.resourceName,
    landing: landingName(t.resourceName),
    watermark: t.watermarkColumns.map((c) => c.name),
    preservesFivetranDelete: t.columns.includes("_fivetran_deleted"),
    preservesFivetranSynced: t.columns.includes("_fivetran_synced"),
  }));
}
function observationMetadata(source: string, targetEnd: string | null) {
  return {
    kind: "commit_window",
    source,
    observedAt: targetEnd,
  };
}

function nextTarget(env: Env, completedThrough: string): Date | null {
  const completed = timestamp(completedThrough, "completedThrough");
  const enabled = timestamp(
    required(env.CHANGE_HISTORY_ENABLED_AT, "CHANGE_HISTORY_ENABLED_AT"),
    "CHANGE_HISTORY_ENABLED_AT",
  );

  if (completed < enabled) {
    return validateBridge(env, completed).end;
  }
  return nextCatchupTarget(completed, currentMinute());
}

function bridgeWindow(
  env: Env,
  completedThrough: string,
  targetEnd: string,
): { start: Date; end: Date } | null {
  const completed = timestamp(completedThrough, "completedThrough");
  const enabled = timestamp(
    required(env.CHANGE_HISTORY_ENABLED_AT, "CHANGE_HISTORY_ENABLED_AT"),
    "CHANGE_HISTORY_ENABLED_AT",
  );
  if (completed >= enabled) return null;

  const bridge = validateBridge(env, completed);
  if (timestamp(targetEnd, "targetEnd").valueOf() !== bridge.end.valueOf()) {
    throw new Error("Persisted pre-history target is not the pinned bridge end");
  }
  return bridge;
}

function validateBridge(env: Env, completed: Date) {
  const frozenStart = timestamp(
    required(env.COMPLETE_THROUGH, "COMPLETE_THROUGH"),
    "COMPLETE_THROUGH",
  );
  if (completed.valueOf() !== frozenStart.valueOf()) {
    throw new Error("Unexpected cursor before change-history enablement");
  }

  const enabled = timestamp(
    required(env.CHANGE_HISTORY_ENABLED_AT, "CHANGE_HISTORY_ENABLED_AT"),
    "CHANGE_HISTORY_ENABLED_AT",
  );
  const end = timestamp(
    required(env.SOURCE_BRIDGE_THROUGH, "SOURCE_BRIDGE_THROUGH"),
    "SOURCE_BRIDGE_THROUGH",
  );
  if (end < enabled || frozenStart >= end) {
    throw new Error("SOURCE_BRIDGE_THROUGH must follow the frozen start and enablement");
  }

  const retentionHours = integer(
    env.CHANGE_HISTORY_RETENTION_HOURS ?? "168",
    1,
    168,
    "CHANGE_HISTORY_RETENTION_HOURS",
  );
  if (Date.now() - frozenStart.valueOf() > retentionHours * 60 * MINUTE) {
    throw new Error("Bridge start is outside BigQuery time-travel retention");
  }

  const creationTimes = parseSourceCreationTimes(env.SOURCE_CREATED_AT_JSON);
  for (const table of SOURCE_TABLES) {
    const createdAt = creationTimes[table.resourceName];
    if (!createdAt) {
      throw new Error(`SOURCE_CREATED_AT_JSON is missing ${table.resourceName}`);
    }
    if (timestamp(createdAt, `${table.resourceName} creation time`) > frozenStart) {
      throw new Error(`${table.resourceName} was created after the frozen snapshot`);
    }
  }
  return { start: frozenStart, end };
}

function changeHistoryGuard(env: Env, source: string) {
  const enabledAt = timestamp(
    required(env.CHANGE_HISTORY_ENABLED_AT, "CHANGE_HISTORY_ENABLED_AT"),
    "CHANGE_HISTORY_ENABLED_AT",
  );
  const created = parseSourceCreationTimes(env.SOURCE_CREATED_AT_JSON);
  const sourceCreatedAt = created[source];
  if (!sourceCreatedAt) {
    throw new Error(`SOURCE_CREATED_AT_JSON is missing ${source}`);
  }
  return {
    enabledAt,
    sourceCreatedAt: timestamp(sourceCreatedAt, `${source} creation time`),
    retentionHours: integer(
      env.CHANGE_HISTORY_RETENTION_HOURS ?? "168",
      1,
      168,
      "CHANGE_HISTORY_RETENTION_HOURS",
    ),
    checkedAt: new Date(),
  };
}

function parseSourceCreationTimes(value: string): Record<string, string> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(required(value, "SOURCE_CREATED_AT_JSON"));
  } catch {
    throw new Error("SOURCE_CREATED_AT_JSON must be valid JSON");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("SOURCE_CREATED_AT_JSON must be an object");
  }
  return parsed as Record<string, string>;
}

function timestamp(value: string, name: string): Date {
  const result = new Date(value);
  if (Number.isNaN(result.valueOf())) throw new Error(`${name} is invalid`);
  return result;
}
function currentMinute() {
  return new Date(Math.floor(Date.now() / MINUTE) * MINUTE);
}
function nextMinute() {
  return currentMinute().valueOf() + MINUTE;
}
function windowMinutes(a: string, b: string) {
  return Math.max(
    1,
    Math.round((new Date(b).valueOf() - new Date(a).valueOf()) / MINUTE),
  );
}
function base(e: Env) {
  return required(e.TINYBIRD_API_URL, "TINYBIRD_API_URL").replace(/\/+$/, "");
}
function headers(e: Env) {
  return {
    Authorization: `Bearer ${required(e.TINYBIRD_ADMIN_TOKEN, "TINYBIRD_ADMIN_TOKEN")}`,
  };
}
function minute(v: string) {
  const d = new Date(v);
  if (Number.isNaN(d.valueOf()))
    throw new Error("COMPLETE_THROUGH must be an ISO timestamp");
  return new Date(Math.floor(d.valueOf() / MINUTE) * MINUTE);
}
function required(v: string, n: string) {
  if (v?.trim()) return v;
  throw new Error(`${n} is required`);
}
function text(v: unknown) {
  if (typeof v === "string" && v) return v;
  throw new Error("Required identifier missing");
}
function number(v: number | null) {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  throw new Error("Required numeric state missing");
}
function integer(v: string, a: number, b: number, n: string) {
  const x = Number(v);
  if (Number.isInteger(x) && x >= a && x <= b) return x;
  throw new Error(`${n} must be ${a}-${b}`);
}
