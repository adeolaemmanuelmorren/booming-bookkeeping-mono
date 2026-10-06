import { DurableObject } from "cloudflare:workers";

import { buildActiveCampaignContactReplacement } from "../conversions/activecampaign.mjs";
import { normalizeStripeChargeSnapshot } from "../conversions/stripe.mjs";
import type {
  BulkBootstrapCheckpoint,
  JsonRecord,
  PipelineId,
  PreparedScope,
  SourceReplacement,
} from "./contracts.ts";
import { canonicalJson } from "./json.ts";
import { runIncrementalSlice } from "./machine.ts";
import {
  SqliteFactCoordinatorStore,
  type TransactionalSqlStorage,
} from "./sqlite-store.ts";
import {
  TinybirdFivetranFactReader,
  type TinybirdQueryClient,
} from "./tinybird-reader.ts";
import { parseTimestamp } from "./timestamp.ts";
import { IdentityOnlyReplacementPublisher, type IdentityFactReceiver } from "../identity/fivetran-publisher.ts";
import { projectActiveCampaignIdentity, projectStripeIdentity, replaceIdentityScope } from "../identity/fivetran-facts.ts";
import type { PendingIdentityFact } from "../identity/engine.ts";

const SLICE_BUDGET_MS = 55_000;
const LEASE_MS = 70_000;
const NEXT_ALARM_MS = 60_000;
const MAX_SEED_RPC_BYTES = 950_000;
const MAX_SEED_SCOPES = 100;
const TINYBIRD_TIMEOUT_MS = 20_000;

export interface SourcePublisherBinding {
  publishSourceReplacements(replacements: SourceReplacement[]): Promise<void>;
}

export interface FivetranFactCoordinatorEnv {
  TENANT_ID: string;
  TINYBIRD_URL: string;
  TINYBIRD_TOKEN: string;
  FIVETRAN_SNAPSHOT_AT: string;
  IDENTITY_INGESTION_ENABLED?: string;
  IDENTITY: { getByName(name: string): IdentityFactReceiver & { status(): Promise<{ publishedVersion: number; baseline: {
    identityVersion: number; baselineId: string; sealHash: string; snapshotAt: string;
  } | null }> } };
  IDENTITY_BASELINE_ID: string;
  IDENTITY_BASELINE_SEAL: string;
  IDENTITY_BASELINE: {
    readScopes(input: { tenantId: string; baselineId: string; scopeIds: string[] }): Promise<Record<string, PendingIdentityFact[]>>;
  };
}

export interface PipelineIdentity {
  pipeline: PipelineId;
  snapshotId: string;
  snapshotAt: string;
}

export interface CoordinatorStatus {
  pipeline: PipelineId | null;
  snapshotId: string | null;
  snapshotAt: string | null;
  bootstrapComplete: boolean;
  completedObservationAt: string | null;
  activeWindowId: string | null;
  activated: boolean;
  activationId: string | null;
  running: boolean;
  leaseUntil: string | null;
  nextAlarmAt: string | null;
  pendingScopes: number;
  preparedScopes: number;
  storedScopes: number;
  checkpoint: BulkBootstrapCheckpoint | null;
  lastSliceStartedAt: string | null;
  lastSliceSucceededAt: string | null;
  lastErrorCode: string | null;
}

interface RuntimeRow extends Record<string, unknown> {
  pipeline: PipelineId | null;
  snapshot_id: string | null;
  snapshot_at: string | null;
  activated: number;
  activation_id: string | null;
  lease_id: string | null;
  lease_until_ms: number;
  last_slice_started_at: string | null;
  last_slice_succeeded_at: string | null;
  last_error_code: string | null;
  identity_only: number;
}

/** One instance is routed by the stable name `fivetran:<pipeline>`. */
export class FivetranFactCoordinator extends DurableObject<
  FivetranFactCoordinatorEnv
> {
  private readonly store: SqliteFactCoordinatorStore;
  private readonly reader: TinybirdFivetranFactReader;

  constructor(
    ctx: DurableObjectState,
    env: FivetranFactCoordinatorEnv,
  ) {
    super(ctx, env);
    this.store = new SqliteFactCoordinatorStore(
      ctx.storage as unknown as TransactionalSqlStorage,
    );
    this.reader = new TinybirdFivetranFactReader(
      new WorkerTinybirdQueryClient(env),
      requiredText(env.TENANT_ID, "TENANT_ID"),
      this.configuredSnapshotAt(),
    );

    ctx.blockConcurrencyWhile(async () => {
      this.store.initializeSchema();
      this.initializeRuntimeSchema();
      await this.restoreActivatedAlarm();
    });
  }

  async initializePipeline(input: PipelineIdentity): Promise<CoordinatorStatus> {
    const identity = normalizeIdentity(input);
    if (identity.snapshotAt !== this.configuredSnapshotAt()) {
      throw new Error("Fivetran fact snapshot does not match configuration");
    }
    this.assertInactive();
    this.bindIdentity(identity);
    await this.store.initializePipeline(identity);
    return this.status(identity.pipeline);
  }

  /** Initializes only the raw receipt cursor. Scope facts are loaded lazily from the sealed identity baseline. */
  async initializeIdentityPipeline(input: PipelineIdentity): Promise<CoordinatorStatus> {
    const identity = normalizeIdentity(input);
    if (identity.snapshotAt !== this.configuredSnapshotAt()) throw new Error("Identity snapshot does not match configuration");
    this.assertInactive();
    this.bindIdentity(identity);
    this.ctx.storage.sql.exec("UPDATE fivetran_fact_runtime SET identity_only = 1 WHERE singleton = 1");
    await this.store.initializePipeline(identity);
    await this.store.saveBulkBootstrapCheckpoint({
      ...identity,
      afterCursor: "",
      complete: true,
      publishedScopes: 0,
      identityFactCount: 0,
    });
    await this.store.finalizeBulkBootstrap(identity);
    return this.status(identity.pipeline);
  }

  async loadBulkBootstrapCheckpoint(input: {
    pipeline: PipelineId;
    snapshotId: string;
  }): Promise<BulkBootstrapCheckpoint | null> {
    this.assertBound(input.pipeline, input.snapshotId);
    return this.store.loadBulkBootstrapCheckpoint(input);
  }

  async saveBulkBootstrapCheckpoint(
    checkpoint: BulkBootstrapCheckpoint,
  ): Promise<void> {
    const normalized = normalizeCheckpoint(checkpoint);
    this.assertInactive();
    this.assertBound(normalized.pipeline, normalized.snapshotId);
    this.assertSnapshotAt(normalized.snapshotAt, this.runtime());
    await this.store.saveBulkBootstrapCheckpoint(normalized);
  }

  async seedBulkBootstrapScopes(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    prepared: PreparedScope[];
  }): Promise<{ accepted: number }> {
    this.assertInactive();
    if (!Array.isArray(input.prepared)) {
      throw new TypeError("prepared must be an array");
    }
    if (input.prepared.length > MAX_SEED_SCOPES) {
      throw new Error("Fivetran fact seed request exceeds 100 scopes");
    }
    this.assertRpcSize(input);
    const runtime = this.runtime();
    this.assertBound(input.pipeline, runtime.snapshot_id);
    this.assertSnapshotAt(input.snapshotAt, runtime);
    await this.store.seedBulkBootstrapScopes(input);
    return { accepted: input.prepared.length };
  }

  async finalizeBulkBootstrap(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<CoordinatorStatus> {
    this.assertInactive();
    const runtime = this.runtime();
    this.assertBound(input.pipeline, runtime.snapshot_id);
    this.assertSnapshotAt(input.snapshotAt, runtime);
    const checkpoint = await this.store.loadBulkBootstrapCheckpoint({
      pipeline: input.pipeline,
      snapshotId: requiredText(runtime.snapshot_id, "snapshotId"),
    });
    if (!checkpoint?.complete) {
      throw new Error("Fivetran fact bootstrap checkpoint is incomplete");
    }
    const storedScopes = this.storedScopeCount();
    if (storedScopes !== checkpoint.publishedScopes) {
      throw new Error("Fivetran fact bootstrap seed count does not match checkpoint");
    }
    await this.store.finalizeBulkBootstrap(input);
    return this.status(input.pipeline);
  }

  /** Activation only schedules work. The alarm remains the sole executor. */
  async start(input: {
    pipeline: PipelineId;
    snapshotId: string;
    activationId: string;
  }): Promise<CoordinatorStatus> {
    if (this.env.IDENTITY_INGESTION_ENABLED !== "true") {
      throw new Error("Fivetran fact ingestion is disabled");
    }
    if (this.runtime().identity_only !== 1) throw new Error("Legacy Fivetran coordinator cannot run identity-only ingestion");
    const identity = this.env.IDENTITY.getByName(this.env.TENANT_ID);
    const identityStatus = await identity.status();
    if (!identityStatus.baseline || identityStatus.baseline.identityVersion !== 1 || identityStatus.publishedVersion < 1) {
      throw new Error("Identity baseline version 1 has not been activated");
    }
    if (identityStatus.baseline.baselineId !== this.env.IDENTITY_BASELINE_ID) {
      throw new Error("Identity baseline configuration changed");
    }
    if (identityStatus.baseline.sealHash !== this.env.IDENTITY_BASELINE_SEAL ||
      parseTimestamp(identityStatus.baseline.snapshotAt, "identity snapshotAt").iso !== this.configuredSnapshotAt()) {
      throw new Error("Identity baseline seal does not match the raw snapshot cursor");
    }
    this.assertBound(input.pipeline, input.snapshotId);
    const activationId = requiredText(input.activationId, "activationId");
    const pipeline = await this.store.getPipeline(input.pipeline);
    const checkpoint = await this.store.loadBulkBootstrapCheckpoint({
      pipeline: input.pipeline,
      snapshotId: input.snapshotId,
    });
    if (!pipeline.bootstrapComplete || !checkpoint?.complete) {
      throw new Error("Fivetran fact bootstrap is incomplete");
    }

    this.ctx.storage.transactionSync(() => {
      const runtime = this.runtime();
      if (runtime.activated === 1 && runtime.activation_id !== activationId) {
        throw new Error("Fivetran fact activation cannot change");
      }
      this.ctx.storage.sql.exec(
        `UPDATE fivetran_fact_runtime
         SET activated = 1, activation_id = ?
         WHERE singleton = 1`,
        activationId,
      );
    });
    await this.scheduleAlarm(Date.now());
    return this.status(input.pipeline);
  }

  async status(expectedPipeline?: PipelineId): Promise<CoordinatorStatus> {
    const runtime = this.runtime();
    if (expectedPipeline && runtime.pipeline && expectedPipeline !== runtime.pipeline) {
      throw new Error("Fivetran fact coordinator is misrouted");
    }
    const pipeline = runtime.pipeline
      ? await this.store.getPipeline(runtime.pipeline)
      : null;
    const checkpoint = runtime.pipeline && runtime.snapshot_id
      ? await this.store.loadBulkBootstrapCheckpoint({
        pipeline: runtime.pipeline,
        snapshotId: runtime.snapshot_id,
      })
      : null;
    const counts = this.workCounts();
    const alarm = await this.ctx.storage.getAlarm();

    return {
      pipeline: runtime.pipeline,
      snapshotId: runtime.snapshot_id,
      snapshotAt: runtime.snapshot_at,
      bootstrapComplete: pipeline?.bootstrapComplete ?? false,
      completedObservationAt: pipeline?.completedObservationAt ?? null,
      activeWindowId: pipeline?.activeWindow?.id ?? null,
      activated: runtime.activated === 1,
      activationId: runtime.activation_id,
      running: Boolean(runtime.lease_id && runtime.lease_until_ms > Date.now()),
      leaseUntil: millisecondsToIso(runtime.lease_until_ms),
      nextAlarmAt: millisecondsToIso(alarm),
      pendingScopes: counts.pending,
      preparedScopes: counts.prepared,
      storedScopes: counts.stored,
      checkpoint,
      lastSliceStartedAt: runtime.last_slice_started_at,
      lastSliceSucceededAt: runtime.last_slice_succeeded_at,
      lastErrorCode: runtime.last_error_code,
    };
  }

  async alarm(): Promise<void> {
    const runtime = this.runtime();
    if (this.env.IDENTITY_INGESTION_ENABLED !== "true" || runtime.activated !== 1 || runtime.identity_only !== 1) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    if (!runtime.pipeline) throw new Error("Fivetran fact pipeline is not bound");

    const leaseId = crypto.randomUUID();
    if (!this.claimLease(leaseId)) {
      await this.scheduleAlarm(Math.max(Date.now() + 1_000, runtime.lease_until_ms));
      return;
    }
    try {
      await this.scheduleAlarm(Date.now() + LEASE_MS);
      await runIncrementalSlice({
        pipeline: runtime.pipeline,
        dependencies: {
          reader: this.reader,
          store: this.store,
          publisher: new IdentityOnlyReplacementPublisher(
            this.env.IDENTITY.getByName(this.env.TENANT_ID),
          ),
          bootstrapPublisher: disabledBootstrapPublisher,
          builders: {
            stripeNormalizer: normalizeStripeChargeSnapshot,
            activeCampaignNormalizer: buildActiveCampaignContactReplacement,
            stripeIdentityProjector: projectStripeIdentity,
            activeCampaignIdentityProjector: projectActiveCampaignIdentity,
            replaceIdentityScope,
          },
          baselineScopeFacts: async (scopeIds) => {
            const rows = await this.env.IDENTITY_BASELINE.readScopes({
              tenantId: this.env.TENANT_ID,
              baselineId: this.env.IDENTITY_BASELINE_ID,
              scopeIds,
            });
            const result = new Map<string, PendingIdentityFact[]>();
            for (const scopeId of scopeIds) {
              if (!Object.hasOwn(rows, scopeId) || !Array.isArray(rows[scopeId])) {
                throw new Error(`Identity baseline omitted scope proof ${scopeId}`);
              }
              result.set(scopeId, rows[scopeId]);
            }
            return result;
          },
        },
        limits: { deadlineAtMs: Date.now() + SLICE_BUDGET_MS },
      });
      this.recordSuccess(leaseId);
    } catch {
      this.recordFailure(leaseId, "incremental_slice_failed");
    } finally {
      this.releaseLease(leaseId);
    }

    if (this.runtime().activated === 1 && this.env.IDENTITY_INGESTION_ENABLED === "true") {
      await this.scheduleAlarm(Date.now() + NEXT_ALARM_MS);
    }
  }

  private initializeRuntimeSchema(): void {
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS fivetran_fact_runtime (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        pipeline TEXT,
        snapshot_id TEXT,
        snapshot_at TEXT,
        activated INTEGER NOT NULL DEFAULT 0,
        activation_id TEXT,
        lease_id TEXT,
        lease_until_ms INTEGER NOT NULL DEFAULT 0,
        last_slice_started_at TEXT,
        last_slice_succeeded_at TEXT,
        last_error_code TEXT
        ,identity_only INTEGER NOT NULL DEFAULT 0
      );
      INSERT OR IGNORE INTO fivetran_fact_runtime (singleton) VALUES (1);
    `);
    const columns = this.ctx.storage.sql.exec<{ name: string }>("PRAGMA table_info(fivetran_fact_runtime)").toArray();
    if (!columns.some(column => column.name === "identity_only")) {
      this.ctx.storage.sql.exec("ALTER TABLE fivetran_fact_runtime ADD COLUMN identity_only INTEGER NOT NULL DEFAULT 0");
    }
  }

  private async restoreActivatedAlarm(): Promise<void> {
    const runtime = this.runtime();
    if (runtime.activated !== 1 || runtime.identity_only !== 1 || this.env.IDENTITY_INGESTION_ENABLED !== "true") return;
    if (await this.ctx.storage.getAlarm() !== null) return;
    await this.ctx.storage.setAlarm(Date.now() + NEXT_ALARM_MS);
  }

  private configuredSnapshotAt(): string {
    return parseTimestamp(
      this.env.FIVETRAN_SNAPSHOT_AT,
      "FIVETRAN_SNAPSHOT_AT",
    ).iso;
  }

  private bindIdentity(input: PipelineIdentity): void {
    this.ctx.storage.transactionSync(() => {
      const current = this.runtime();
      if (current.pipeline) {
        this.assertBound(input.pipeline, input.snapshotId);
        this.assertSnapshotAt(input.snapshotAt, current);
        return;
      }
      this.ctx.storage.sql.exec(
        `UPDATE fivetran_fact_runtime
         SET pipeline = ?, snapshot_id = ?, snapshot_at = ?
         WHERE singleton = 1`,
        input.pipeline,
        input.snapshotId,
        input.snapshotAt,
      );
    });
  }

  private assertBound(pipeline: PipelineId, snapshotId: string | null): void {
    const runtime = this.runtime();
    if (
      runtime.pipeline !== pipeline ||
      runtime.snapshot_id !== requiredText(snapshotId, "snapshotId")
    ) {
      throw new Error("Fivetran fact coordinator is misrouted");
    }
  }

  private assertSnapshotAt(value: string, runtime: RuntimeRow): void {
    if (
      !runtime.snapshot_at ||
      parseTimestamp(value, "snapshotAt").iso !== runtime.snapshot_at
    ) {
      throw new Error("Fivetran fact snapshot changed");
    }
  }

  private assertInactive(): void {
    if (this.runtime().activated === 1) {
      throw new Error("Fivetran fact bootstrap is closed after activation");
    }
  }

  private assertRpcSize(value: unknown): void {
    const bytes = new TextEncoder().encode(canonicalJson(value)).byteLength;
    if (bytes > MAX_SEED_RPC_BYTES) {
      throw new Error("Fivetran fact seed request exceeds 950000 bytes");
    }
  }

  private claimLease(leaseId: string): boolean {
    return this.ctx.storage.transactionSync(() => {
      const runtime = this.runtime();
      if (runtime.lease_id && runtime.lease_until_ms > Date.now()) return false;
      this.ctx.storage.sql.exec(
        `UPDATE fivetran_fact_runtime
         SET lease_id = ?, lease_until_ms = ?, last_slice_started_at = ?
         WHERE singleton = 1`,
        leaseId,
        Date.now() + LEASE_MS,
        new Date().toISOString(),
      );
      return true;
    });
  }

  private recordSuccess(leaseId: string): void {
    this.ctx.storage.sql.exec(
      `UPDATE fivetran_fact_runtime
       SET last_slice_succeeded_at = ?, last_error_code = NULL
       WHERE singleton = 1 AND lease_id = ?`,
      new Date().toISOString(),
      leaseId,
    );
  }

  private recordFailure(leaseId: string, code: string): void {
    this.ctx.storage.sql.exec(
      `UPDATE fivetran_fact_runtime
       SET last_error_code = ?
       WHERE singleton = 1 AND lease_id = ?`,
      code,
      leaseId,
    );
  }

  private releaseLease(leaseId: string): void {
    this.ctx.storage.sql.exec(
      `UPDATE fivetran_fact_runtime
       SET lease_id = NULL, lease_until_ms = 0
       WHERE singleton = 1 AND lease_id = ?`,
      leaseId,
    );
  }

  private async scheduleAlarm(timestamp: number): Promise<void> {
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > timestamp) {
      await this.ctx.storage.setAlarm(timestamp);
    }
  }

  private runtime(): RuntimeRow {
    return this.ctx.storage.sql.exec(
      "SELECT * FROM fivetran_fact_runtime WHERE singleton = 1",
    ).one() as unknown as RuntimeRow;
  }

  private workCounts(): { pending: number; prepared: number; stored: number } {
    const dirty = this.ctx.storage.sql.exec<{ status: string; count: number }>(
      `SELECT status, count(*) AS count
       FROM fivetran_fact_dirty_scopes
       GROUP BY status`,
    ).toArray();
    const stored = this.storedScopeCount();
    const counts = new Map(dirty.map((row) => [row.status, row.count]));
    return {
      pending: (counts.get("pending") ?? 0) + (counts.get("reserved") ?? 0),
      prepared: counts.get("prepared") ?? 0,
      stored,
    };
  }

  private storedScopeCount(): number {
    return this.ctx.storage.sql.exec<{ count: number }>(
      "SELECT count(*) AS count FROM fivetran_fact_scope_state",
    ).one().count;
  }
}

class WorkerTinybirdQueryClient implements TinybirdQueryClient {
  private readonly env: Pick<
    FivetranFactCoordinatorEnv,
    "TINYBIRD_URL" | "TINYBIRD_TOKEN"
  >;

  constructor(env: FivetranFactCoordinatorEnv) {
    this.env = env;
  }

  async query<Row extends JsonRecord>(sql: string): Promise<Row[]> {
    if (this.env.TINYBIRD_URL !== "https://api.us-east.tinybird.co") {
      throw new Error("Unexpected Tinybird region");
    }
    const response = await fetch(new URL("/v0/sql", this.env.TINYBIRD_URL), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.env.TINYBIRD_TOKEN}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({ q: `${sql}\nFORMAT JSON` }),
      redirect: "manual",
      signal: AbortSignal.timeout(TINYBIRD_TIMEOUT_MS),
    });
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error(`Tinybird query failed with HTTP ${response.status}`);
    }
    const payload = await response.json() as { data?: Row[]; error?: unknown };
    if (!Array.isArray(payload.data) || payload.error) {
      throw new Error("Tinybird query returned an invalid response");
    }
    return payload.data;
  }
}

const disabledBootstrapPublisher = {
  async publishBootstrapSourceReplacements(): Promise<void> {
    throw new Error("Runtime coordinator cannot publish bootstrap replacements");
  },
};

function normalizeIdentity(input: PipelineIdentity): PipelineIdentity {
  if (!isPipeline(input.pipeline)) throw new Error("Invalid Fivetran fact pipeline");
  return {
    pipeline: input.pipeline,
    snapshotId: requiredText(input.snapshotId, "snapshotId"),
    snapshotAt: parseTimestamp(input.snapshotAt, "snapshotAt").iso,
  };
}

function normalizeCheckpoint(
  input: BulkBootstrapCheckpoint,
): BulkBootstrapCheckpoint {
  if (!isPipeline(input.pipeline)) throw new Error("Invalid Fivetran fact pipeline");
  if (typeof input.afterCursor !== "string") {
    throw new TypeError("afterCursor must be a string");
  }
  if (typeof input.complete !== "boolean") {
    throw new TypeError("complete must be a boolean");
  }
  assertNonNegativeInteger(input.publishedScopes, "publishedScopes");
  assertNonNegativeInteger(input.identityFactCount, "identityFactCount");

  return {
    snapshotId: requiredText(input.snapshotId, "snapshotId"),
    snapshotAt: parseTimestamp(input.snapshotAt, "snapshotAt").iso,
    pipeline: input.pipeline,
    afterCursor: input.afterCursor,
    complete: input.complete,
    publishedScopes: input.publishedScopes,
    identityFactCount: input.identityFactCount,
  };
}

function isPipeline(value: unknown): value is PipelineId {
  return value === "stripe_main" || value === "stripe_kajabi" ||
    value === "activecampaign";
}

function requiredText(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError(`${fieldName} is required`);
  }
  return value.trim();
}

function assertNonNegativeInteger(value: unknown, fieldName: string): void {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new TypeError(`${fieldName} must be a non-negative integer`);
  }
}

function millisecondsToIso(value: number | null): string | null {
  if (value === null || value <= 0) return null;
  return new Date(value).toISOString();
}
