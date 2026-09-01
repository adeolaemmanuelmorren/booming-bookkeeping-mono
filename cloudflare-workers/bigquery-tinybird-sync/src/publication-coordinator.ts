import { DurableObject } from "cloudflare:workers";
import {
  RETIRED_IDENTITY_COPIES,
  copiesForSection,
  identityEnqueueBatchesForKind,
  type IdentityPhase,
  type PublicationKind,
  type PublicationSection,
} from "./copy-plan";
import { SHARD_COUNT, runSync, slotForTime, type WorkerEnv } from "./sync";
import type { CurrentIdentityFact, PendingIdentityFact } from "./identity-engine";
import {
  appendIdentityPendingFacts,
  appendIdentityActivation,
  countActiveIngestionJobs,
  getCopyJob,
  readCurrentIdentityFacts,
  readIdentityCompactionCursor,
  readIdentityCompactionManifest,
  readSourceIdentityFacts,
  submitCopyJob,
  TinybirdRequestError,
  type IdentityCompactionManifest,
  type TinybirdApiConfig,
} from "./tinybird-api";
import { processIdentityWorkerBatch } from "./identity-worker";
import { recoveredIdentityBatchId } from "./identity-batch-recovery";

const RAW_SLOT_COUNT = SHARD_COUNT;
const RAW_RUN_MAX_ATTEMPTS = 3;
const RAW_RUN_LEASE_MS = 5 * 60_000;
const REQUIRED_IDLE_INGESTION_POLLS = 2;
const MIN_ALARM_DELAY_MS = 100;
const COORDINATOR_BACKPRESSURE_MS = 60_000;
const HISTORY_RETENTION_MS = 30 * 24 * 60 * 60_000;
const IDENTITY_EPOCH = "1970-01-01 00:00:00";
const IDENTITY_TENANT_ID = "boom";
// Keep the graph working set comfortably below the Durable Object memory limit.
// The coordinator keeps producing batches until the pending stream is empty.
const IDENTITY_BATCH_LIMIT = 1_000;
const IDENTITY_ENGINE_REVISION = 1;
const IDENTITY_SOURCE_PAGE_LIMIT = 1_000;
const ACTIVECAMPAIGN_WINDOW_MINUTES = 24 * 60;
const TINYBIRD_DATETIME_PATTERN = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
const RETIRED_IDENTITY_COPY_SET = new Set<string>(RETIRED_IDENTITY_COPIES);

type CoordinatorPhase =
  | "collecting_raw"
  | "settling_raw"
  | "awaiting_bootstrap"
  | "copying"
  | "failed";

type BootstrapStatus = "not_requested" | "requested" | "complete";

interface CoordinatorState {
  [key: string]: string | number | null;
  phase: CoordinatorPhase;
  bootstrap_status: BootstrapStatus;
  active_raw_generation: string | null;
  publication_kind: PublicationKind | null;
  publication_section: "pre_identity" | "identity" | "post_identity" | null;
  publication_index: number;
  identity_phase: IdentityPhase;
  identity_iteration: number;
  idle_ingestion_polls: number;
  active_pipe: string | null;
  active_job_id: string | null;
  active_job_started_at_ms: number | null;
  active_copy_parameters_json: string;
  identity_enqueue_index: number;
  identity_enqueue_after_fact_key: string;
  identity_enqueue_ingested_at: string | null;
  identity_enqueue_window_from: string | null;
  identity_overlap_cutoff: string | null;
  identity_batch_version: number | null;
  identity_batch_id: string | null;
  identity_cursor_ingested_at: string | null;
  identity_cursor_event_id: string | null;
  identity_manifest_json: string;
  journey_profile_ids_json: string;
  journey_profile_index: number;
  journey_conversion_ids_json: string;
  journey_conversion_index: number;
  journey_backfill_status: "not_requested" | "running" | "complete";
  journey_backfill_after_profile_id: string;
  journey_backfill_profiles_completed: number;
  journey_backfill_batches_completed: number;
  last_error: string | null;
  updated_at_ms: number;
}

interface RawRunRow {
  [key: string]: string | number | null;
  request_key: string;
  scheduled_at: string;
  slot: number;
  generation_id: string | null;
  status: "queued" | "running" | "completed" | "failed" | "compacted";
  attempt_count: number;
  overlap_minutes: number | null;
}

export interface CompactRawBacklogResult {
  queuedBefore: number;
  queuedAfter: number;
  compactedRuns: number;
  protectedActiveGenerationRuns: number;
  retainedCatchUpRuns: number;
  earliestCoveredAt: string | null;
  latestCoveredAt: string | null;
}

export interface RawLeaseRecoveryResult {
  recoveredRuns: number;
  runningRuns: number;
  oldestRunningUpdatedAt: string | null;
}

interface CountRow {
  [key: string]: number;
  value: number;
}

interface PreparedIdentityBatch {
  version: number;
  id: string;
  cursorIngestedAt: string;
  cursorEventId: string;
}

interface LatestGenerationRow {
  [key: string]: string | number | null;
  generation_id: string;
  kind: PublicationKind;
  status: string;
  completed_at_ms: number | null;
}

export interface EnqueueRawRunInput {
  scheduledAt: string;
  slot?: number;
}

export interface EnqueueRawRunResult {
  accepted: boolean;
  requestKey: string;
  status: RawRunRow["status"];
  slot: number;
  scheduledAt: string;
}

export interface CoordinatorStatus {
  operatorPaused: boolean;
  operatorPauseAfterPublication: boolean;
  phase: CoordinatorPhase;
  bootstrapStatus: BootstrapStatus;
  activeRawGeneration: string | null;
  completedRawSlots: number[];
  queuedRawRuns: number;
  publicationKind: PublicationKind | null;
  publicationSection: CoordinatorState["publication_section"];
  publicationIndex: number;
  identityPhase: IdentityPhase;
  identityEnqueue: {
    completedBatches: number;
    totalBatches: number;
    currentProducerId: string | null;
    sourceIngestedFrom: string | null;
  } | null;
  identityBatch: {
    version: number;
    id: string;
    cursorIngestedAt: string;
    cursorEventId: string;
  } | null;
  journeyBatch: {
    completedProfiles: number;
    totalProfiles: number;
    completedConversions: number;
    totalConversions: number;
  } | null;
  journeyBackfill: {
    status: CoordinatorState["journey_backfill_status"];
    afterProfileId: string;
    completedProfiles: number;
    completedBatches: number;
  };
  activeCopyPipe: string | null;
  activeCopyJobId: string | null;
  activeCopyParameters: Record<string, string>;
  lastError: string | null;
  latestPublication: {
    generationId: string;
    kind: PublicationKind;
    status: string;
    completedAt: string | null;
  } | null;
}

export type RecoveryAction =
  | "retry_known_job_or_raw"
  | "retry_copy_after_confirming_no_job"
  | "retry_terminal_copy";

export class PublicationCoordinator extends DurableObject<WorkerEnv> {
  constructor(ctx: DurableObjectState, env: WorkerEnv) {
    super(ctx, env);

    ctx.blockConcurrencyWhile(async () => {
      await this.migrate();
    });
  }

  async enqueueRawRun(input: EnqueueRawRunInput): Promise<EnqueueRawRunResult> {
    const scheduledAt = parseScheduledAt(input.scheduledAt);
    const slot = input.slot === undefined
      ? slotForTime(scheduledAt)
      : parseSlot(input.slot);
    const requestKey = rawRequestKey(scheduledAt, slot);
    const now = Date.now();
    const existing = this.rawRun(requestKey);

    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO raw_runs (
          request_key,
          scheduled_at,
          slot,
          generation_id,
          status,
          attempt_count,
          last_error,
          created_at_ms,
          updated_at_ms
        ) VALUES (?, ?, ?, NULL, 'queued', 0, NULL, ?, ?)
      `,
      requestKey,
      scheduledAt.toISOString(),
      slot,
      now,
      now,
    );

    const row = this.rawRun(requestKey);
    if (!row) throw new Error("Raw run could not be queued.");

    const state = this.state();
    if (state.phase === "collecting_raw") await this.ensureAlarm();

    return {
      accepted: existing === null,
      requestKey,
      status: row.status,
      slot,
      scheduledAt: scheduledAt.toISOString(),
    };
  }

  async tick(): Promise<CoordinatorStatus> {
    await this.alarm();
    return this.status();
  }

  async configureBootstrap(mode: "run" | "acknowledge_existing"): Promise<CoordinatorStatus> {
    const state = this.state();

    if (mode === "run") {
      if (state.bootstrap_status === "complete") {
        throw new Error("Bootstrap is already marked complete.");
      }
      if (state.publication_kind === "bootstrap") {
        throw new Error("Bootstrap publication is already running.");
      }

      this.updateState({ bootstrap_status: "requested" });
      if (state.phase === "awaiting_bootstrap") {
        this.startPublication("bootstrap");
        await this.ensureAlarm();
      }

      return this.status();
    }

    if (mode !== "acknowledge_existing") {
      throw new Error("Bootstrap mode is invalid.");
    }
    if (state.publication_kind === "bootstrap" && state.phase === "copying") {
      throw new Error("Cannot acknowledge bootstrap while its publication is running.");
    }

    this.updateState({ bootstrap_status: "complete" });
    if (state.phase === "awaiting_bootstrap") {
      this.startPublication("recurring");
      await this.ensureAlarm();
    }

    return this.status();
  }

  async recoverFailed(action: RecoveryAction): Promise<CoordinatorStatus> {
    const state = this.state();
    if (state.phase !== "failed") {
      throw new Error("The coordinator is not in failed state.");
    }

    if (state.active_job_id) {
      if (action === "retry_terminal_copy") {
        return this.retryTerminalCopy(state);
      }

      if (action !== "retry_known_job_or_raw") {
        throw new Error(
          "A known Copy job must be polled or verified terminal before resubmission.",
        );
      }

      this.updateState({
        phase: "copying",
        active_job_started_at_ms: Date.now(),
        last_error: null,
      });
      this.updatePublicationStatus("running");
      await this.ensureAlarm();
      return this.status();
    }

    if (action === "retry_terminal_copy") {
      throw new Error("Terminal Copy recovery requires a stored job ID.");
    }

    if (!state.publication_kind) {
      if (action !== "retry_known_job_or_raw") {
        throw new Error("Raw or import recovery does not submit a Copy job.");
      }

      this.ctx.storage.sql.exec(
        `
          UPDATE raw_runs
          SET status = 'queued', attempt_count = 0, last_error = NULL, updated_at_ms = ?
          WHERE status = 'failed'
        `,
        Date.now(),
      );
      const completedSlots = this.completedRawSlots(state.active_raw_generation);
      this.updateState({
        phase: completedSlots.length === RAW_SLOT_COUNT
          ? "settling_raw"
          : "collecting_raw",
        last_error: null,
      });
      await this.ensureAlarm();
      return this.status();
    }

    const retriesWithoutCopySubmission = state.publication_section === "identity";
    if (retriesWithoutCopySubmission) {
      if (action !== "retry_known_job_or_raw") {
        throw new Error("Identity validation or activation recovery is idempotent.");
      }

      let replacementBatchId: string | null = null;
      if (
        state.identity_phase === "compute"
        && state.last_error === "Identity Worker manifest is invalid."
        && state.identity_batch_id !== null
      ) {
        replacementBatchId = recoveredIdentityBatchId(
          state.identity_batch_id,
          Date.now(),
        );
      }

      this.updateState({
        phase: "copying",
        last_error: null,
        ...(replacementBatchId === null
          ? {}
          : {
              identity_batch_id: replacementBatchId,
              identity_manifest_json: "{}",
            }),
      });
      if (replacementBatchId !== null) {
        logEvent("invalid_identity_batch_restarted", {
          generationId: state.active_raw_generation,
          batchVersion: state.identity_batch_version,
          priorBatchId: state.identity_batch_id,
          replacementBatchId,
        });
      }
      this.updatePublicationStatus("running");
      await this.ensureAlarm();
      return this.status();
    }

    if (action !== "retry_copy_after_confirming_no_job") {
      throw new Error(
        "Copy submission may be ambiguous. Confirm in Tinybird that no job was created before retrying.",
      );
    }

    this.updateState({ phase: "copying", last_error: null });
    this.updatePublicationStatus("running");
    await this.ensureAlarm();
    return this.status();
  }

  private async retryTerminalCopy(
    state: CoordinatorState,
  ): Promise<CoordinatorStatus> {
    const jobId = state.active_job_id;
    const pipeName = state.active_pipe;
    const generationId = state.active_raw_generation;
    if (!jobId || !pipeName || !generationId) {
      throw new Error("Terminal Copy recovery state is incomplete.");
    }

    const job = await getCopyJob(jobId, this.tinybirdConfig());
    if (job.status === "waiting" || job.status === "working" || job.status === "cancelling") {
      throw new Error(
        `${pipeName} Copy job ${jobId} is still ${job.status}; resume polling it instead.`,
      );
    }
    if (job.status === "done") {
      throw new Error(
        `${pipeName} Copy job ${jobId} completed; resume polling it instead.`,
      );
    }

    const now = Date.now();
    this.ctx.storage.sql.exec(
      `
        UPDATE publication_jobs
        SET status = ?, completed_at_ms = ?, error = ?
        WHERE generation_id = ? AND job_id = ?
      `,
      job.status,
      now,
      job.error?.slice(0, 2_000) ?? `Tinybird job ended with ${job.status}`,
      generationId,
      jobId,
    );
    this.updateState({
      phase: "copying",
      active_pipe: null,
      active_job_id: null,
      active_job_started_at_ms: null,
      active_copy_parameters_json: "{}",
      last_error: null,
    });
    this.updatePublicationStatus("running");
    logEvent("terminal_copy_retry_approved", {
      generationId,
      pipeName,
      jobId,
      terminalStatus: job.status,
    });
    await this.ensureAlarm();
    return this.status();
  }

  async status(): Promise<CoordinatorStatus> {
    const state = this.state();
    const operatorPaused = await this.operatorPaused();
    const operatorPauseAfterPublication = await this.operatorPauseAfterPublication();
    const latest = this.latestPublication();
    const enqueueBatches = state.publication_kind
      ? identityEnqueueBatchesForKind(state.publication_kind)
      : [];
    const enqueueBatch = enqueueBatches[state.identity_enqueue_index];
    const isEnqueueing = state.publication_section === "identity"
      && state.identity_phase === "enqueue";

    return {
      operatorPaused,
      operatorPauseAfterPublication,
      phase: state.phase,
      bootstrapStatus: state.bootstrap_status,
      activeRawGeneration: state.active_raw_generation,
      completedRawSlots: this.completedRawSlots(state.active_raw_generation),
      queuedRawRuns: this.countRawRuns("queued"),
      publicationKind: state.publication_kind,
      publicationSection: state.publication_section,
      publicationIndex: state.publication_index,
      identityPhase: state.identity_phase,
      identityEnqueue: isEnqueueing
        ? {
            completedBatches: state.identity_enqueue_index,
            totalBatches: enqueueBatches.length,
            currentProducerId: enqueueBatch?.producerId ?? null,
            sourceIngestedFrom: enqueueBatch?.usesGenerationOverlapCutoff
              ? state.identity_enqueue_window_from ?? state.identity_overlap_cutoff
              : null,
          }
        : null,
      identityBatch: state.identity_batch_version !== null
        && state.identity_batch_id
        && state.identity_cursor_ingested_at
        && state.identity_cursor_event_id
        ? {
            version: state.identity_batch_version,
            id: state.identity_batch_id,
            cursorIngestedAt: state.identity_cursor_ingested_at,
            cursorEventId: state.identity_cursor_event_id,
          }
        : null,
      journeyBatch: state.identity_phase === "journey"
        ? {
            completedProfiles: state.journey_profile_index,
            totalProfiles: parseStringArray(
              state.journey_profile_ids_json,
              "journey_profile_ids_json",
            ).length,
            completedConversions: state.journey_conversion_index,
            totalConversions: parseStringArray(
              state.journey_conversion_ids_json,
              "journey_conversion_ids_json",
            ).length,
          }
        : null,
      journeyBackfill: {
        status: state.journey_backfill_status,
        afterProfileId: state.journey_backfill_after_profile_id,
        completedProfiles: state.journey_backfill_profiles_completed,
        completedBatches: state.journey_backfill_batches_completed,
      },
      activeCopyPipe: state.active_pipe,
      activeCopyJobId: state.active_job_id,
      activeCopyParameters: parseCopyParameters(state.active_copy_parameters_json),
      lastError: state.last_error,
      latestPublication: latest
        ? {
            generationId: latest.generation_id,
            kind: latest.kind,
            status: latest.status,
            completedAt: latest.completed_at_ms === null
              ? null
              : new Date(latest.completed_at_ms).toISOString(),
          }
        : null,
    };
  }

  async setOperatorPaused(paused: boolean): Promise<CoordinatorStatus> {
    await this.ctx.storage.delete("operator_pause_after_publication");
    if (paused) {
      await this.ctx.storage.put("operator_paused", true);
      await this.ctx.storage.deleteAlarm();
      return this.status();
    }

    await this.ctx.storage.delete("operator_paused");
    await this.ensureAlarm();
    return this.status();
  }

  async startJourneyBackfill(): Promise<CoordinatorStatus> {
    throw new Error("Journey backfill is retired. The immutable seed is complete.");
  }

  async compactRawBacklog(): Promise<CompactRawBacklogResult> {
    if (!await this.operatorPaused()) {
      throw new Error("Pause the coordinator before compacting its raw backlog.");
    }

    const state = this.state();
    if (state.phase !== "collecting_raw" || state.publication_kind) {
      throw new Error("Raw backlog compaction requires the collecting_raw phase.");
    }

    const queued = this.ctx.storage.sql.exec<RawRunRow>(
      `
        SELECT
          request_key,
          scheduled_at,
          slot,
          generation_id,
          status,
          attempt_count,
          overlap_minutes
        FROM raw_runs
        WHERE status = 'queued'
        ORDER BY scheduled_at, slot
      `,
    ).toArray();
    const queuedBefore = queued.length;
    if (queuedBefore <= RAW_SLOT_COUNT) {
      return {
        queuedBefore,
        queuedAfter: queuedBefore,
        compactedRuns: 0,
        protectedActiveGenerationRuns: 0,
        retainedCatchUpRuns: queuedBefore,
        earliestCoveredAt: queued[0]?.scheduled_at ?? null,
        latestCoveredAt: queued.at(-1)?.scheduled_at ?? null,
      };
    }

    const completedSlots = new Set(this.completedRawSlots(state.active_raw_generation));
    const missingActiveSlots = state.active_raw_generation
      ? new Set(Array.from({ length: RAW_SLOT_COUNT }, (_, slot) => slot)
        .filter((slot) => !completedSlots.has(slot)))
      : new Set<number>();
    const protectedKeys = new Set<string>();
    const protectedSlots = new Set<number>();
    for (const row of queued) {
      if (!missingActiveSlots.has(row.slot)) continue;
      if (protectedSlots.has(row.slot)) continue;
      protectedKeys.add(row.request_key);
      protectedSlots.add(row.slot);
    }

    const candidates = queued.filter((row) => !protectedKeys.has(row.request_key));
    const retainedBySlot = new Map<number, RawRunRow>();
    for (const row of candidates) retainedBySlot.set(row.slot, row);

    const retainedKeys = new Set(
      [...protectedKeys, ...[...retainedBySlot.values()].map((row) => row.request_key)],
    );
    const compacted = queued.filter((row) => !retainedKeys.has(row.request_key));
    const defaultOverlap = positiveInteger(
      this.env.EXPORT_OVERLAP_MINUTES,
      "EXPORT_OVERLAP_MINUTES",
    );
    const now = Date.now();

    this.ctx.storage.transactionSync(() => {
      for (const [slot, retained] of retainedBySlot) {
        const earliest = candidates.find((row) => row.slot === slot);
        if (!earliest) continue;
        const elapsedMinutes = Math.ceil(
          (new Date(retained.scheduled_at).valueOf() - new Date(earliest.scheduled_at).valueOf())
          / 60_000,
        );
        this.ctx.storage.sql.exec(
          `
            UPDATE raw_runs
            SET overlap_minutes = ?, updated_at_ms = ?
            WHERE request_key = ? AND status = 'queued'
          `,
          defaultOverlap + elapsedMinutes,
          now,
          retained.request_key,
        );
      }

      for (const row of compacted) {
        this.ctx.storage.sql.exec(
          `
            UPDATE raw_runs
            SET
              status = 'compacted',
              last_error = 'Covered by a retained catch-up run.',
              updated_at_ms = ?
            WHERE request_key = ? AND status = 'queued'
          `,
          now,
          row.request_key,
        );
      }
    });

    return {
      queuedBefore,
      queuedAfter: this.countRawRuns("queued"),
      compactedRuns: compacted.length,
      protectedActiveGenerationRuns: protectedKeys.size,
      retainedCatchUpRuns: retainedBySlot.size,
      earliestCoveredAt: candidates[0]?.scheduled_at ?? null,
      latestCoveredAt: candidates.at(-1)?.scheduled_at ?? null,
    };
  }

  async recoverExpiredRawRunLease(): Promise<RawLeaseRecoveryResult> {
    if (!await this.operatorPaused()) {
      throw new Error("Pause the coordinator before recovering a raw-run lease.");
    }

    const now = Date.now();
    const running = this.ctx.storage.sql.exec<{ updated_at_ms: number }>(
      `
        SELECT updated_at_ms
        FROM raw_runs
        WHERE status = 'running'
        ORDER BY updated_at_ms
      `,
    ).toArray();
    const recoveredRuns = running.filter(
      (run) => run.updated_at_ms <= now - RAW_RUN_LEASE_MS,
    ).length;

    this.requeueExpiredRawRuns(now);
    await this.ctx.storage.deleteAlarm();

    return {
      recoveredRuns,
      runningRuns: running.length,
      oldestRunningUpdatedAt: running[0]
        ? new Date(running[0].updated_at_ms).toISOString()
        : null,
    };
  }

  async pauseAfterPublication(): Promise<CoordinatorStatus> {
    const state = this.state();
    if (!state.publication_kind) return this.setOperatorPaused(true);

    await this.ctx.storage.put("operator_pause_after_publication", true);
    await this.ctx.storage.delete("operator_paused");
    await this.ensureAlarm();
    return this.status();
  }

  async alarm(): Promise<void> {
    try {
      if (await this.operatorPaused()) return;
      const state = this.state();

      switch (state.phase) {
        case "collecting_raw":
          await this.processNextRawRun(state);
          return;
        case "settling_raw":
          await this.waitForRawImports(state);
          return;
        case "copying":
          await this.advancePublication(state);
          return;
        case "awaiting_bootstrap":
        case "failed":
          return;
      }
    } catch (error) {
      await this.handleAlarmError(error);
    }
  }

  // Rate limits, upstream 5xx responses, and timeouts are backpressure, not
  // failure: every phase resumes from durable checkpoints, so the safe move
  // is to wait and retry. Terminal `failed` is reserved for correctness
  // errors that need a human.
  private async handleAlarmError(error: unknown): Promise<void> {
    const message = errorMessage(error);
    if (!isTransientUpstreamError(error)) {
      this.failCoordinator(message);
      return;
    }

    const delayMs = error instanceof TinybirdRequestError
      ? error.retryAfterMs ?? COORDINATOR_BACKPRESSURE_MS
      : COORDINATOR_BACKPRESSURE_MS;
    this.updateState({ last_error: message.slice(0, 2_000) });
    logEvent("coordinator_backpressure", {
      message: message.slice(0, 500),
      delayMs,
    });
    await this.ensureAlarm(delayMs);
  }

  private async migrate(): Promise<void> {
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS publication_schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS coordinator_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        phase TEXT NOT NULL,
        bootstrap_status TEXT NOT NULL,
        active_raw_generation TEXT,
        publication_kind TEXT,
        publication_section TEXT,
        publication_index INTEGER NOT NULL,
        identity_phase TEXT NOT NULL,
        identity_iteration INTEGER NOT NULL,
        idle_ingestion_polls INTEGER NOT NULL,
        active_pipe TEXT,
        active_job_id TEXT,
        active_job_started_at_ms INTEGER,
        last_error TEXT,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS raw_runs (
        request_key TEXT PRIMARY KEY,
        scheduled_at TEXT NOT NULL,
        slot INTEGER NOT NULL,
        generation_id TEXT,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL,
        overlap_minutes INTEGER,
        last_error TEXT,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS raw_runs_queue
        ON raw_runs(status, scheduled_at, slot);

      CREATE TABLE IF NOT EXISTS publication_generations (
        generation_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at_ms INTEGER NOT NULL,
        completed_at_ms INTEGER,
        last_error TEXT
      );

      CREATE TABLE IF NOT EXISTS publication_jobs (
        generation_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        pipe_name TEXT NOT NULL,
        job_id TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at_ms INTEGER NOT NULL,
        completed_at_ms INTEGER,
        error TEXT,
        PRIMARY KEY (generation_id, sequence)
      );
    `);

    this.addColumnIfMissing(
      "raw_runs",
      "overlap_minutes",
      "ALTER TABLE raw_runs ADD COLUMN overlap_minutes INTEGER",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "identity_enqueue_window_from",
      "ALTER TABLE coordinator_state ADD COLUMN identity_enqueue_window_from TEXT",
    );

    const now = Date.now();
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO coordinator_state (
          id,
          phase,
          bootstrap_status,
          active_raw_generation,
          publication_kind,
          publication_section,
          publication_index,
          identity_phase,
          identity_iteration,
          idle_ingestion_polls,
          active_pipe,
          active_job_id,
          active_job_started_at_ms,
          last_error,
          updated_at_ms
        ) VALUES (
          1,
          'collecting_raw',
          'not_requested',
          NULL,
          NULL,
          NULL,
          0,
          'enqueue',
          0,
          0,
          NULL,
          NULL,
          NULL,
          NULL,
          ?
        )
      `,
      now,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (1, ?)
      `,
      now,
    );
    this.migrateIdentityEnqueueBatches(now);
    this.migrateActivatedDeltaFlow(now);
    this.migrateStagedIdentityCompaction(now);
    this.migrateComponentBuildPhase(now);
    this.migrateFactScopePhase(now);
    applyBootstrapIdentityCutoffMigration(
      this.ctx.storage,
      this.env.IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM,
      now,
    );
    this.migrateWorkerIdentityEngine(now);
    const shouldResumeIdentity = applyWorkerIdentityCursorResetMigration(
      this.ctx.storage,
      now,
    );
    const shouldFinishIdentityOnlyPublication = applyIdentityOnlyPublicationMigration(
      this.ctx.storage,
      now,
    );
    const shouldResumeRawRun = applyRawRunRecoveryMigration(
      this.ctx.storage,
      now,
    );
    const shouldResumeWorkerEnqueue = applyWorkerIdentityEnqueueMigration(
      this.ctx.storage,
      now,
    );
    const shouldResumeVersionedIdentityBatch = applyVersionedIdentityBatchMigration(
      this.ctx.storage,
      now,
    );
    this.migrateIncrementalJourneys(now);
    this.migrateIncrementalJourneyBackfill(now);
    this.migrateJourneySeedOverlay(now);
    if (
      shouldResumeIdentity
      || shouldFinishIdentityOnlyPublication
      || shouldResumeRawRun
      || shouldResumeWorkerEnqueue
      || shouldResumeVersionedIdentityBatch
    ) {
      await this.ensureAlarm();
    }
  }

  private migrateIncrementalJourneys(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 18) return;

    this.addColumnIfMissing(
      "coordinator_state",
      "journey_profile_ids_json",
      "ALTER TABLE coordinator_state ADD COLUMN journey_profile_ids_json TEXT NOT NULL DEFAULT '[]'",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "journey_profile_index",
      "ALTER TABLE coordinator_state ADD COLUMN journey_profile_index INTEGER NOT NULL DEFAULT 0",
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (18, ?)
      `,
      now,
    );
  }

  private migrateIncrementalJourneyBackfill(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 19) return;

    this.addColumnIfMissing(
      "coordinator_state",
      "journey_conversion_ids_json",
      "ALTER TABLE coordinator_state ADD COLUMN journey_conversion_ids_json TEXT NOT NULL DEFAULT '[]'",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "journey_conversion_index",
      "ALTER TABLE coordinator_state ADD COLUMN journey_conversion_index INTEGER NOT NULL DEFAULT 0",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "journey_backfill_status",
      "ALTER TABLE coordinator_state ADD COLUMN journey_backfill_status TEXT NOT NULL DEFAULT 'not_requested'",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "journey_backfill_after_profile_id",
      "ALTER TABLE coordinator_state ADD COLUMN journey_backfill_after_profile_id TEXT NOT NULL DEFAULT ''",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "journey_backfill_profiles_completed",
      "ALTER TABLE coordinator_state ADD COLUMN journey_backfill_profiles_completed INTEGER NOT NULL DEFAULT 0",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "journey_backfill_batches_completed",
      "ALTER TABLE coordinator_state ADD COLUMN journey_backfill_batches_completed INTEGER NOT NULL DEFAULT 0",
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (19, ?)
      `,
      now,
    );
  }

  private migrateJourneySeedOverlay(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 20) return;

    this.ctx.storage.sql.exec(
      `
        UPDATE coordinator_state
        SET journey_backfill_status = 'complete'
        WHERE id = 1
      `,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (20, ?)
      `,
      now,
    );
  }

  private migrateWorkerIdentityEngine(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 8) return;

    this.ctx.storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          phase = 'copying',
          identity_phase = 'compute',
          active_pipe = NULL,
          active_job_id = NULL,
          active_job_started_at_ms = NULL,
          active_copy_parameters_json = '{}',
          identity_manifest_json = '{}',
          last_error = NULL
        WHERE publication_section = 'identity'
          AND identity_phase != 'enqueue'
      `,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (8, ?)
      `,
      now,
    );
  }

  private migrateIdentityEnqueueBatches(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 2) return;

    this.addColumnIfMissing(
      "coordinator_state",
      "identity_enqueue_index",
      "ALTER TABLE coordinator_state ADD COLUMN identity_enqueue_index INTEGER NOT NULL DEFAULT 0",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "identity_overlap_cutoff",
      "ALTER TABLE coordinator_state ADD COLUMN identity_overlap_cutoff TEXT",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "active_copy_parameters_json",
      "ALTER TABLE coordinator_state ADD COLUMN active_copy_parameters_json TEXT NOT NULL DEFAULT '{}'",
    );
    this.addColumnIfMissing(
      "publication_jobs",
      "parameters_json",
      "ALTER TABLE publication_jobs ADD COLUMN parameters_json TEXT NOT NULL DEFAULT '{}'",
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (2, ?)
      `,
      now,
    );
  }

  private migrateActivatedDeltaFlow(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 3) return;

    this.addColumnIfMissing(
      "coordinator_state",
      "identity_batch_version",
      "ALTER TABLE coordinator_state ADD COLUMN identity_batch_version INTEGER",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "identity_batch_id",
      "ALTER TABLE coordinator_state ADD COLUMN identity_batch_id TEXT",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "identity_cursor_ingested_at",
      "ALTER TABLE coordinator_state ADD COLUMN identity_cursor_ingested_at TEXT",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "identity_cursor_event_id",
      "ALTER TABLE coordinator_state ADD COLUMN identity_cursor_event_id TEXT",
    );
    this.addColumnIfMissing(
      "coordinator_state",
      "identity_manifest_json",
      "ALTER TABLE coordinator_state ADD COLUMN identity_manifest_json TEXT NOT NULL DEFAULT '{}'",
    );

    this.ctx.storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          phase = CASE WHEN phase = 'rebuild_required' THEN 'copying' ELSE phase END,
          identity_phase = 'enqueue',
          identity_iteration = 0,
          identity_enqueue_index = 0,
          identity_batch_version = NULL,
          identity_batch_id = NULL,
          identity_cursor_ingested_at = NULL,
          identity_cursor_event_id = NULL,
          identity_manifest_json = '{}',
          last_error = NULL
        WHERE active_job_id IS NULL
          AND (
            phase = 'rebuild_required'
            OR identity_phase NOT IN (
              'enqueue',
              'prepare',
              'changes',
              'profiles',
              'touched_profiles',
              'scope',
              'fact_keys',
              'facts_0',
              'facts_1',
              'facts_2',
              'facts_3',
              'facts_4',
              'facts_5',
              'facts_6',
              'facts_7',
              'components',
              'compact',
              'validate',
              'activate'
            )
          )
      `,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (3, ?)
      `,
      now,
    );
  }

  private migrateStagedIdentityCompaction(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 4) return;

    this.ctx.storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          identity_phase = 'prepare',
          identity_batch_version = NULL,
          identity_batch_id = NULL,
          identity_cursor_ingested_at = NULL,
          identity_cursor_event_id = NULL,
          identity_manifest_json = '{}',
          last_error = NULL
        WHERE active_job_id IS NULL
          AND publication_section = 'identity'
          AND identity_phase IN ('compact', 'validate', 'activate')
      `,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (4, ?)
      `,
      now,
    );
  }

  private migrateComponentBuildPhase(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 5) return;

    this.ctx.storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          identity_phase = 'components',
          identity_manifest_json = '{}',
          last_error = NULL
        WHERE active_job_id IS NULL
          AND publication_section = 'identity'
          AND identity_phase IN ('compact', 'validate', 'activate')
      `,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (5, ?)
      `,
      now,
    );
  }

  private migrateFactScopePhase(now: number): void {
    const version = this.ctx.storage.sql.exec<{ version: number }>(
      "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
    ).one().version;
    if (version >= 6) return;

    this.ctx.storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          identity_phase = 'facts',
          identity_manifest_json = '{}',
          last_error = NULL
        WHERE active_job_id IS NULL
          AND publication_section = 'identity'
          AND identity_phase IN ('components', 'compact', 'validate', 'activate')
      `,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
        VALUES (6, ?)
      `,
      now,
    );
  }

  private addColumnIfMissing(
    tableName: "coordinator_state" | "publication_jobs" | "raw_runs",
    columnName: string,
    alterStatement: string,
  ): void {
    const columns = this.ctx.storage.sql.exec<{ name: string }>(
      `PRAGMA table_info(${tableName})`,
    ).toArray();
    if (columns.some((column) => column.name === columnName)) return;
    this.ctx.storage.sql.exec(alterStatement);
  }

  private async processNextRawRun(state: CoordinatorState): Promise<void> {
    this.requeueExpiredRawRuns(Date.now());
    let generationId = state.active_raw_generation;

    if (this.completedRawSlots(generationId).length === RAW_SLOT_COUNT) {
      this.updateState({
        phase: "settling_raw",
        idle_ingestion_polls: 0,
      });
      await this.ensureAlarm(this.copyPollIntervalMs());
      return;
    }

    let run = this.nextEligibleRawRun(generationId);

    if (!run) {
      return;
    }

    if (!generationId) {
      generationId = rawGenerationId(run);
      this.updateState({ active_raw_generation: generationId });
      run = this.nextEligibleRawRun(generationId) ?? run;
    }

    const attempt = run.attempt_count + 1;
    this.ctx.storage.sql.exec(
      `
        UPDATE raw_runs
        SET
          generation_id = ?,
          status = 'running',
          attempt_count = ?,
          last_error = NULL,
          updated_at_ms = ?
        WHERE request_key = ? AND status = 'queued'
      `,
      generationId,
      attempt,
      Date.now(),
      run.request_key,
    );

    let summary;
    try {
      summary = await runSync(
        this.env,
        new Date(run.scheduled_at),
        run.slot,
        { exportOverlapMinutes: run.overlap_minutes ?? undefined },
      );
    } catch (error) {
      await this.handleRawRunFailure(run.request_key, attempt, errorMessage(error));
      return;
    }

    if (!summary.ok) {
      const details = summary.results
        .filter((result) => result.status === "failed" || result.status === "in_flight")
        .map((result) => `${result.resourceName}: ${result.error ?? result.status}`)
        .join("; ");
      await this.handleRawRunFailure(
        run.request_key,
        attempt,
        details || "Raw sync did not complete.",
      );
      return;
    }

    this.ctx.storage.sql.exec(
      `
        UPDATE raw_runs
        SET status = 'completed', last_error = NULL, updated_at_ms = ?
        WHERE request_key = ?
      `,
      Date.now(),
      run.request_key,
    );
    logEvent("raw_slot_completed", {
      generationId,
      requestKey: run.request_key,
      slot: run.slot,
      scheduledAt: run.scheduled_at,
    });

    const completedSlots = this.completedRawSlots(generationId);
    if (completedSlots.length === RAW_SLOT_COUNT) {
      this.updateState({
        phase: "settling_raw",
        idle_ingestion_polls: 0,
      });
      await this.ensureAlarm(this.copyPollIntervalMs());
      return;
    }

    if (this.nextEligibleRawRun(generationId)) {
      await this.ensureAlarm();
    }
  }

  private requeueExpiredRawRuns(now: number): void {
    this.ctx.storage.sql.exec(
      `
        UPDATE raw_runs
        SET
          status = 'queued',
          last_error = 'Recovered an expired raw-run lease.',
          updated_at_ms = ?
        WHERE status = 'running' AND updated_at_ms <= ?
      `,
      now,
      now - RAW_RUN_LEASE_MS,
    );
  }

  private async handleRawRunFailure(
    requestKey: string,
    attempt: number,
    message: string,
  ): Promise<void> {
    const terminal = attempt >= RAW_RUN_MAX_ATTEMPTS;
    this.ctx.storage.sql.exec(
      `
        UPDATE raw_runs
        SET status = ?, last_error = ?, updated_at_ms = ?
        WHERE request_key = ?
      `,
      terminal ? "failed" : "queued",
      message.slice(0, 2_000),
      Date.now(),
      requestKey,
    );

    if (terminal) {
      this.failCoordinator(
        `Raw run ${requestKey} failed ${RAW_RUN_MAX_ATTEMPTS} times: ${message}`,
      );
      return;
    }

    await this.ensureAlarm(this.copyPollIntervalMs());
  }

  private async waitForRawImports(state: CoordinatorState): Promise<void> {
    const activeJobs = await countActiveIngestionJobs(this.tinybirdConfig());
    if (activeJobs > 0) {
      this.updateState({ idle_ingestion_polls: 0 });
      await this.ensureAlarm(this.copyPollIntervalMs());
      return;
    }

    const idlePolls = state.idle_ingestion_polls + 1;
    if (idlePolls < REQUIRED_IDLE_INGESTION_POLLS) {
      this.updateState({ idle_ingestion_polls: idlePolls });
      await this.ensureAlarm(this.copyPollIntervalMs());
      return;
    }

    if (state.bootstrap_status === "not_requested") {
      this.updateState({ phase: "awaiting_bootstrap", idle_ingestion_polls: idlePolls });
      return;
    }

    const kind = state.bootstrap_status === "requested" ? "bootstrap" : "recurring";
    this.startPublication(kind);
    await this.ensureAlarm();
  }

  private startPublication(kind: PublicationKind): void {
    const state = this.state();
    const generationId = state.active_raw_generation;
    if (!generationId) throw new Error("A complete raw generation is required before publication.");
    if (state.publication_kind) throw new Error("A publication is already active.");

    const now = Date.now();
    const identitySourceIngestedFrom = this.publicationIdentitySourceIngestedFrom(
      kind,
      generationId,
    );
    this.ctx.storage.sql.exec(
      `
        INSERT INTO publication_generations (
          generation_id,
          kind,
          status,
          started_at_ms,
          completed_at_ms,
          last_error
        ) VALUES (?, ?, 'running', ?, NULL, NULL)
      `,
      generationId,
      kind,
      now,
    );
    this.updateState({
      phase: "copying",
      publication_kind: kind,
      publication_section: "pre_identity",
      publication_index: 0,
      identity_phase: "enqueue",
      identity_iteration: 0,
      identity_enqueue_index: 0,
      identity_enqueue_after_fact_key: "",
      identity_enqueue_ingested_at: formatTinybirdDateTime64(new Date(now)),
      identity_enqueue_window_from: null,
      identity_overlap_cutoff: identitySourceIngestedFrom,
      identity_batch_version: null,
      identity_batch_id: null,
      identity_cursor_ingested_at: null,
      identity_cursor_event_id: null,
      identity_manifest_json: "{}",
      journey_profile_ids_json: "[]",
      journey_profile_index: 0,
      journey_conversion_ids_json: "[]",
      journey_conversion_index: 0,
      active_pipe: null,
      active_job_id: null,
      active_job_started_at_ms: null,
      active_copy_parameters_json: "{}",
      last_error: null,
    });
    logEvent("publication_started", { generationId, kind });
  }

  private async advancePublication(state: CoordinatorState): Promise<void> {
    if (state.active_job_id) {
      await this.pollActiveCopy(state);
      return;
    }

    if (!state.publication_kind || !state.publication_section) {
      throw new Error("Publication state is incomplete.");
    }

    if (state.publication_section === "identity") {
      await this.advanceIdentity(state);
      return;
    }

    const copies = copiesForSection(state.publication_kind, state.publication_section);
    const copy = copies[state.publication_index];
    if (copy) {
      await this.startCopy(copy.pipeName, copy.parameters);
      return;
    }

    if (state.publication_section === "pre_identity") {
      this.updateState({
        publication_section: "identity",
        publication_index: 0,
        identity_phase: "enqueue",
        identity_enqueue_index: 0,
      });
      await this.ensureAlarm();
      return;
    }

    await this.completePublication(state);
  }

  private async advanceIdentity(state: CoordinatorState): Promise<void> {
    switch (state.identity_phase) {
      case "enqueue":
        await this.advanceIdentityEnqueue(state);
        return;
      case "compute":
        await this.advanceIdentityCompute(state);
        return;
      case "validate":
        await this.validateIdentityCompaction(state);
        return;
      case "activate":
        await this.activateIdentityCompaction(state);
        return;
      case "journey":
        await this.advanceIdentityJourney(state);
        return;
    }
  }

  private async advanceIdentityEnqueue(state: CoordinatorState): Promise<void> {
    const kind = state.publication_kind;
    if (!kind) throw new Error("Identity enqueue has no active publication.");

    const batches = identityEnqueueBatchesForKind(kind);
    const batch = batches[state.identity_enqueue_index];
    if (!batch) {
      this.updateState({
        identity_phase: "compute",
        identity_enqueue_index: 0,
        identity_enqueue_after_fact_key: "",
      });
      await this.ensureAlarm();
      return;
    }

    const sourceIngestedFrom = batch.usesGenerationOverlapCutoff
      ? this.identitySourceIngestedFrom(state)
      : IDENTITY_EPOCH;
    const sourceWindow = this.identitySourceWindow(
      state,
      batch.producerId,
      sourceIngestedFrom,
    );
    const enqueueIngestedAt = state.identity_enqueue_ingested_at;
    if (!enqueueIngestedAt) {
      this.updateState({
        identity_enqueue_ingested_at: formatTinybirdDateTime64(new Date()),
      });
      await this.ensureAlarm();
      return;
    }

    const sourceFacts = await readSourceIdentityFacts(
      batch.producerId,
      sourceWindow.from,
      state.identity_enqueue_after_fact_key,
      IDENTITY_SOURCE_PAGE_LIMIT,
      this.tinybirdConfig(),
      undefined,
      sourceWindow.to,
    );
    const currentFacts = await readCurrentIdentityFacts(
      IDENTITY_TENANT_ID,
      sourceFacts.map((fact) => fact.factKey),
      this.tinybirdConfig(),
    );
    const currentByKey = new Map(currentFacts.map((fact) => [
      `${fact.factKind}\u0000${fact.factKey}`,
      fact,
    ]));
    const pendingFacts = sourceFacts
      .filter((fact) => shouldEnqueueSourceFact(
        fact,
        currentByKey.get(`${fact.factKind}\u0000${fact.factKey}`),
      ))
      .map((fact) => ({ ...fact, ingestedAt: enqueueIngestedAt }));
    await appendIdentityPendingFacts(pendingFacts, this.tinybirdConfig());

    const lastFactKey = sourceFacts.at(-1)?.factKey ?? "";
    const sourcePageComplete = sourceFacts.length < IDENTITY_SOURCE_PAGE_LIMIT;
    const hasNextWindow = sourcePageComplete && sourceWindow.hasNext;
    const producerComplete = sourcePageComplete && !hasNextWindow;
    this.updateState({
      identity_enqueue_index: producerComplete
        ? state.identity_enqueue_index + 1
        : state.identity_enqueue_index,
      identity_enqueue_after_fact_key: sourcePageComplete ? "" : lastFactKey,
      identity_enqueue_window_from: hasNextWindow
        ? sourceWindow.to
        : producerComplete
          ? null
          : state.identity_enqueue_window_from,
    });
    logEvent("identity_source_page_enqueued", {
      generationId: state.active_raw_generation,
      producerId: batch.producerId,
      sourceRows: sourceFacts.length,
      appendedRows: pendingFacts.length,
      producerComplete,
    });
    await this.ensureAlarm();
  }

  private async advanceIdentityCompute(state: CoordinatorState): Promise<void> {
    if (!this.hasPreparedIdentityBatch(state)) {
      await this.prepareIdentityBatch(state);
      return;
    }

    const batch = this.preparedIdentityBatch(state);
    if (await this.skipAlreadyActivatedIdentityBatch(state, batch)) return;

    const result = await processIdentityWorkerBatch({
      tenantId: IDENTITY_TENANT_ID,
      batchVersion: batch.version,
      batchId: batch.id,
      batchLimit: IDENTITY_BATCH_LIMIT,
      cursor: {
        activeBatchVersion: batch.version - 1,
        checkpointIngestedAt: batch.cursorIngestedAt,
        checkpointEventId: batch.cursorEventId,
      },
    }, this.tinybirdConfig());

    this.updateState({
      identity_phase: "validate",
      identity_manifest_json: JSON.stringify(result.manifest),
      journey_profile_ids_json: "[]",
      journey_profile_index: 0,
      journey_conversion_ids_json: "[]",
      journey_conversion_index: 0,
    });
    logEvent("identity_batch_computed", {
      generationId: state.active_raw_generation,
      batchId: batch.id,
      batchVersion: batch.version,
      inputEventCount: result.manifest.inputEventCount,
      outputRowCount: result.manifest.actualOutputRowCount,
    });
    await this.ensureAlarm();
  }

  private async skipAlreadyActivatedIdentityBatch(
    state: CoordinatorState,
    batch: PreparedIdentityBatch,
  ): Promise<boolean> {
    const cursor = await readIdentityCompactionCursor(
      IDENTITY_TENANT_ID,
      this.tinybirdConfig(),
    );
    if (cursor.activeBatchVersion < batch.version) return false;

    if (
      cursor.activeBatchVersion === batch.version
      && cursor.activeBatchId !== batch.id
    ) {
      throw new Error(
        `Identity batch version ${batch.version} is active with a different batch ID.`,
      );
    }

    logEvent("identity_batch_already_activated", {
      generationId: state.active_raw_generation,
      batchId: batch.id,
      batchVersion: batch.version,
      activeBatchVersion: cursor.activeBatchVersion,
    });
    this.continueIdentityCompaction(state);
    await this.ensureAlarm();
    return true;
  }

  private async prepareIdentityBatch(state: CoordinatorState): Promise<void> {
    const generationId = state.active_raw_generation;
    if (!generationId) throw new Error("Identity compaction has no active generation.");

    const cursor = await readIdentityCompactionCursor(
      IDENTITY_TENANT_ID,
      this.tinybirdConfig(),
    );
    const batchVersion = Math.max(
      cursor.activeBatchVersion + 1,
      this.generationBatchVersion(generationId),
    );
    const batchId = identityBatchId(generationId, batchVersion);

    this.updateState({
      identity_batch_version: batchVersion,
      identity_batch_id: batchId,
      identity_cursor_ingested_at: cursor.checkpointIngestedAt,
      identity_cursor_event_id: cursor.checkpointEventId,
      identity_manifest_json: "{}",
      journey_profile_ids_json: "[]",
      journey_profile_index: 0,
      journey_conversion_ids_json: "[]",
      journey_conversion_index: 0,
    });
    logEvent("identity_batch_prepared", {
      generationId,
      batchId,
      batchVersion,
      activeBatchVersion: cursor.activeBatchVersion,
    });
    await this.ensureAlarm();
  }

  private async validateIdentityCompaction(state: CoordinatorState): Promise<void> {
    const batch = this.preparedIdentityBatch(state);
    const manifest = await readIdentityCompactionManifest(
      IDENTITY_TENANT_ID,
      batch.version,
      batch.id,
      this.tinybirdConfig(),
    );

    this.assertIdentityManifest(batch, manifest);
    this.updateState({ identity_manifest_json: JSON.stringify(manifest) });
    logEvent("identity_batch_validated", {
      generationId: state.active_raw_generation,
      batchId: batch.id,
      batchVersion: batch.version,
      inputEventCount: manifest.inputEventCount,
      outputRowCount: manifest.actualOutputRowCount,
    });

    if (manifest.inputEventCount === 0) {
      this.finishIdentitySection();
      await this.ensureAlarm();
      return;
    }

    this.updateState({ identity_phase: "activate" });
    await this.ensureAlarm();
  }

  private async activateIdentityCompaction(state: CoordinatorState): Promise<void> {
    const batch = this.preparedIdentityBatch(state);
    const manifest = parseIdentityManifest(state.identity_manifest_json);
    this.assertIdentityManifest(batch, manifest);

    await appendIdentityActivation(
      { tenantId: IDENTITY_TENANT_ID, manifest },
      this.tinybirdConfig(),
    );
    logEvent("identity_batch_activated", {
      generationId: state.active_raw_generation,
      batchId: batch.id,
      batchVersion: batch.version,
      inputEventCount: manifest.inputEventCount,
    });
    // Wake the journey coordinator so the just-activated batch's journey
    // repair starts now instead of at its next poll. A journey stall must
    // never block publication, so failures here are logged and dropped.
    try {
      await this.env.JOURNEY_COORDINATOR.getByName(IDENTITY_TENANT_ID).tick();
    } catch (error) {
      logEvent("journey_tick_failed", { message: errorMessage(error) });
    }
    this.continueIdentityCompaction(state);
    await this.ensureAlarm();
  }

  // The journey handoff happens inside the identity worker's batch commit
  // (a queue append gated on activation). This phase only remains so a
  // Durable Object stranded in it by an older deploy can drain out.
  private async advanceIdentityJourney(state: CoordinatorState): Promise<void> {
    this.continueIdentityCompaction(state);
    await this.ensureAlarm();
  }

  private continueIdentityCompaction(state: CoordinatorState): void {
    this.updateState({
      identity_phase: "compute",
      identity_iteration: state.identity_iteration + 1,
      identity_batch_version: null,
      identity_batch_id: null,
      identity_cursor_ingested_at: null,
      identity_cursor_event_id: null,
      identity_manifest_json: "{}",
      journey_profile_ids_json: "[]",
      journey_profile_index: 0,
      journey_conversion_ids_json: "[]",
      journey_conversion_index: 0,
    });
  }

  private finishIdentitySection(): void {
    this.updateState({
      publication_section: "post_identity",
      publication_index: 0,
      identity_phase: "enqueue",
      identity_enqueue_index: 0,
      identity_enqueue_after_fact_key: "",
      identity_enqueue_ingested_at: null,
      identity_enqueue_window_from: null,
      journey_profile_ids_json: "[]",
      journey_profile_index: 0,
      journey_conversion_ids_json: "[]",
      journey_conversion_index: 0,
    });
  }

  private assertIdentityManifest(
    batch: PreparedIdentityBatch,
    manifest: IdentityCompactionManifest,
  ): void {
    if (manifest.tenantId !== IDENTITY_TENANT_ID) {
      throw new Error("Identity compaction manifest returned the wrong tenant.");
    }
    if (manifest.batchVersion !== batch.version || manifest.batchId !== batch.id) {
      throw new Error("Identity compaction manifest returned the wrong batch.");
    }
    if (!manifest.isValid) {
      throw new Error(
        `Identity compaction manifest failed validation for batch ${batch.id}.`,
      );
    }
    if (manifest.inputEventCount === 0 && manifest.actualOutputRowCount !== 0) {
      throw new Error(
        `Empty identity batch ${batch.id} unexpectedly produced output rows.`,
      );
    }
  }

  private hasPreparedIdentityBatch(state: CoordinatorState): state is CoordinatorState & {
    identity_batch_version: number;
    identity_batch_id: string;
    identity_cursor_ingested_at: string;
    identity_cursor_event_id: string;
  } {
    return state.identity_batch_version !== null
      && state.identity_batch_id !== null
      && state.identity_cursor_ingested_at !== null
      && state.identity_cursor_event_id !== null;
  }

  private preparedIdentityBatch(state: CoordinatorState): PreparedIdentityBatch {
    if (!this.hasPreparedIdentityBatch(state)) {
      throw new Error("Identity batch metadata is incomplete.");
    }

    return {
      version: state.identity_batch_version,
      id: state.identity_batch_id,
      cursorIngestedAt: state.identity_cursor_ingested_at,
      cursorEventId: state.identity_cursor_event_id,
    };
  }

  private async startCopy(
    pipeName: string,
    parameters: Readonly<Record<string, string>> = {},
  ): Promise<void> {
    if (RETIRED_IDENTITY_COPY_SET.has(pipeName)) {
      throw new Error(`Retired identity Copy ${pipeName} cannot run automatically.`);
    }

    const state = this.state();
    const generationId = state.active_raw_generation;
    if (!generationId) throw new Error("Copy job has no active generation.");

    const normalizedParameters = normalizeCopyParameters(parameters);
    const jobId = await submitCopyJob(
      pipeName,
      this.tinybirdConfig(),
      normalizedParameters,
    );
    const startedAt = Date.now();
    const sequence = this.nextCopySequence(generationId);
    this.ctx.storage.sql.exec(
      `
        INSERT INTO publication_jobs (
          generation_id,
          sequence,
          pipe_name,
          job_id,
          status,
          started_at_ms,
          completed_at_ms,
          error,
          parameters_json
        ) VALUES (?, ?, ?, ?, 'working', ?, NULL, NULL, ?)
      `,
      generationId,
      sequence,
      pipeName,
      jobId,
      startedAt,
      JSON.stringify(normalizedParameters),
    );
    this.updateState({
      active_pipe: pipeName,
      active_job_id: jobId,
      active_job_started_at_ms: startedAt,
      active_copy_parameters_json: JSON.stringify(normalizedParameters),
    });
    logEvent("copy_submitted", {
      generationId,
      pipeName,
      jobId,
      sequence,
      parameterNames: Object.keys(normalizedParameters),
    });
    await this.ensureAlarm(this.copyPollIntervalMs());
  }

  private async pollActiveCopy(state: CoordinatorState): Promise<void> {
    const jobId = state.active_job_id;
    const pipeName = state.active_pipe;
    const startedAt = state.active_job_started_at_ms;
    if (!jobId || !pipeName || startedAt === null) {
      throw new Error("Active Copy job state is incomplete.");
    }

    const job = await getCopyJob(jobId, this.tinybirdConfig());
    if (job.status === "waiting" || job.status === "working" || job.status === "cancelling") {
      if (Date.now() - startedAt >= this.copyJobTimeoutMs()) {
        throw new Error(`${pipeName} Copy job ${jobId} exceeded its polling timeout.`);
      }

      await this.ensureAlarm(this.copyPollIntervalMs());
      return;
    }

    if (job.status !== "done") {
      throw new Error(
        `${pipeName} Copy job ${jobId} ended with ${job.status}: ${job.error ?? "no details"}`,
      );
    }

    this.ctx.storage.sql.exec(
      `
        UPDATE publication_jobs
        SET status = 'done', completed_at_ms = ?, error = NULL
        WHERE generation_id = ? AND job_id = ?
      `,
      Date.now(),
      state.active_raw_generation,
      jobId,
    );
    this.advanceAfterCopy(state);
    logEvent("copy_completed", {
      generationId: state.active_raw_generation,
      pipeName,
      jobId,
    });
    await this.ensureAlarm();
  }

  private advanceAfterCopy(state: CoordinatorState): void {
    const resetActiveJob = {
      active_pipe: null,
      active_job_id: null,
      active_job_started_at_ms: null,
      active_copy_parameters_json: "{}",
    };

    if (state.publication_section !== "identity") {
      this.updateState({
        ...resetActiveJob,
        publication_index: state.publication_index + 1,
      });
      return;
    }

    throw new Error("The Worker identity path cannot own a Tinybird Copy job.");
  }

  private async completePublication(state: CoordinatorState): Promise<void> {
    const generationId = state.active_raw_generation;
    const kind = state.publication_kind;
    if (!generationId || !kind) throw new Error("Completed publication state is incomplete.");

    const now = Date.now();
    this.ctx.storage.sql.exec(
      `
        UPDATE publication_generations
        SET status = 'complete', completed_at_ms = ?, last_error = NULL
        WHERE generation_id = ?
      `,
      now,
      generationId,
    );
    this.updateState({
      phase: "collecting_raw",
      bootstrap_status: kind === "bootstrap" ? "complete" : state.bootstrap_status,
      active_raw_generation: null,
      publication_kind: null,
      publication_section: null,
      publication_index: 0,
      identity_phase: "enqueue",
      identity_iteration: 0,
      identity_enqueue_index: 0,
      identity_enqueue_after_fact_key: "",
      identity_enqueue_ingested_at: null,
      identity_enqueue_window_from: null,
      identity_overlap_cutoff: null,
      identity_batch_version: null,
      identity_batch_id: null,
      identity_cursor_ingested_at: null,
      identity_cursor_event_id: null,
      identity_manifest_json: "{}",
      journey_profile_ids_json: "[]",
      journey_profile_index: 0,
      journey_conversion_ids_json: "[]",
      journey_conversion_index: 0,
      idle_ingestion_polls: 0,
      active_pipe: null,
      active_job_id: null,
      active_job_started_at_ms: null,
      active_copy_parameters_json: "{}",
      last_error: null,
    });
    this.deleteOldHistory(now);
    logEvent("publication_completed", { generationId, kind });

    if (await this.operatorPauseAfterPublication()) {
      await this.ctx.storage.delete("operator_pause_after_publication");
      await this.ctx.storage.put("operator_paused", true);
      await this.ctx.storage.deleteAlarm();
      return;
    }

    if (this.countRawRuns("queued") > 0) await this.ensureAlarm();
  }

  private failCoordinator(message: string): void {
    const state = this.state();
    this.updateState({ phase: "failed", last_error: message.slice(0, 2_000) });
    if (state.active_job_id && state.active_raw_generation) {
      this.ctx.storage.sql.exec(
        `
          UPDATE publication_jobs
          SET status = 'unknown', error = ?
          WHERE generation_id = ? AND job_id = ?
        `,
        message.slice(0, 2_000),
        state.active_raw_generation,
        state.active_job_id,
      );
    }
    this.updatePublicationStatus("failed", message);
    logEvent("coordinator_failed", {
      generationId: state.active_raw_generation,
      message: message.slice(0, 2_000),
    });
  }

  private updatePublicationStatus(status: string, error?: string): void {
    const generationId = this.state().active_raw_generation;
    if (!generationId) return;

    this.ctx.storage.sql.exec(
      `
        UPDATE publication_generations
        SET status = ?, last_error = ?
        WHERE generation_id = ?
      `,
      status,
      error?.slice(0, 2_000) ?? null,
      generationId,
    );
  }

  private state(): CoordinatorState {
    return this.ctx.storage.sql.exec<CoordinatorState>(
      "SELECT * FROM coordinator_state WHERE id = 1",
    ).one();
  }

  private updateState(values: Partial<CoordinatorState>): void {
    const entries = Object.entries(values);
    if (entries.length === 0) return;

    const assignments = entries.map(([name]) => `${name} = ?`);
    assignments.push("updated_at_ms = ?");
    this.ctx.storage.sql.exec(
      `UPDATE coordinator_state SET ${assignments.join(", ")} WHERE id = 1`,
      ...entries.map(([, value]) => value),
      Date.now(),
    );
  }

  private rawRun(requestKey: string): RawRunRow | null {
    return this.ctx.storage.sql.exec<RawRunRow>(
      `
        SELECT request_key, scheduled_at, slot, generation_id, status, attempt_count, overlap_minutes
        FROM raw_runs
        WHERE request_key = ?
      `,
      requestKey,
    ).toArray()[0] ?? null;
  }

  private nextEligibleRawRun(generationId: string | null): RawRunRow | null {
    const completed = new Set(this.completedRawSlots(generationId));
    const rows = this.ctx.storage.sql.exec<RawRunRow>(
      `
        SELECT request_key, scheduled_at, slot, generation_id, status, attempt_count, overlap_minutes
        FROM raw_runs
        WHERE status = 'queued' AND (generation_id IS NULL OR generation_id = ?)
        ORDER BY scheduled_at, slot
      `,
      generationId,
    ).toArray();

    return rows.find((row) => !completed.has(row.slot)) ?? null;
  }

  private completedRawSlots(generationId: string | null): number[] {
    if (!generationId) return [];

    return this.ctx.storage.sql.exec<{ slot: number }>(
      `
        SELECT DISTINCT slot
        FROM raw_runs
        WHERE
          status = 'completed'
          AND (
            generation_id = ?
            OR scheduled_at = (
              SELECT scheduled_at
              FROM raw_runs
              WHERE generation_id = ?
              ORDER BY scheduled_at
              LIMIT 1
            )
          )
        ORDER BY slot
      `,
      generationId,
      generationId,
    ).toArray().map((row) => row.slot);
  }

  private countRawRuns(status: RawRunRow["status"]): number {
    return this.ctx.storage.sql.exec<CountRow>(
      "SELECT COUNT(*) AS value FROM raw_runs WHERE status = ?",
      status,
    ).one().value;
  }

  private latestPublication(): LatestGenerationRow | null {
    return this.ctx.storage.sql.exec<LatestGenerationRow>(
      `
        SELECT generation_id, kind, status, completed_at_ms
        FROM publication_generations
        ORDER BY started_at_ms DESC
        LIMIT 1
      `,
    ).toArray()[0] ?? null;
  }

  private nextCopySequence(generationId: string): number {
    return this.ctx.storage.sql.exec<CountRow>(
      `
        SELECT COALESCE(MAX(sequence), 0) + 1 AS value
        FROM publication_jobs
        WHERE generation_id = ?
      `,
      generationId,
    ).one().value;
  }

  private identitySourceIngestedFrom(state: CoordinatorState): string {
    if (state.identity_overlap_cutoff) return state.identity_overlap_cutoff;

    const generationId = state.active_raw_generation;
    if (!generationId) throw new Error("Identity enqueue has no active raw generation.");
    const kind = state.publication_kind;
    if (!kind) throw new Error("Identity enqueue has no active publication.");

    const cutoff = this.publicationIdentitySourceIngestedFrom(kind, generationId);
    this.updateState({ identity_overlap_cutoff: cutoff });
    return cutoff;
  }

  private identitySourceWindow(
    state: CoordinatorState,
    producerId: string,
    sourceIngestedFrom: string,
  ): { from: string; to?: string; hasNext: boolean } {
    if (producerId !== "source_identity:activecampaign") {
      return { from: sourceIngestedFrom, hasNext: false };
    }

    const generationId = state.active_raw_generation;
    if (!generationId) throw new Error("Identity enqueue has no active raw generation.");
    const from = state.identity_enqueue_window_from ?? sourceIngestedFrom;
    const fromDate = tinybirdDateTime(from, "identity_enqueue_window_from");
    const generationEnd = this.generationEndedAt(generationId);
    const proposedEnd = new Date(
      fromDate.valueOf() + ACTIVECAMPAIGN_WINDOW_MINUTES * 60_000,
    );
    const toDate = proposedEnd < generationEnd ? proposedEnd : generationEnd;

    return {
      from,
      to: formatTinybirdDateTime(toDate),
      hasNext: toDate < generationEnd,
    };
  }

  private publicationIdentitySourceIngestedFrom(
    kind: PublicationKind,
    generationId: string,
  ): string {
    if (kind === "bootstrap") {
      return validateTinybirdDateTime(
        this.env.IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM,
        "IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM",
      );
    }

    return this.generationOverlapCutoff(generationId);
  }

  private generationOverlapCutoff(generationId: string): string {
    const defaultOverlap = positiveInteger(
      this.env.EXPORT_OVERLAP_MINUTES,
      "EXPORT_OVERLAP_MINUTES",
    );
    const runs = this.ctx.storage.sql.exec<{
      scheduled_at: string;
      overlap_minutes: number | null;
    }>(
      `
        SELECT scheduled_at, overlap_minutes
        FROM raw_runs
        WHERE generation_id = ? AND status = 'completed'
      `,
      generationId,
    ).toArray();
    if (runs.length === 0) {
      throw new Error(`Raw generation ${generationId} has no completed scheduled runs.`);
    }

    const cutoffMs = Math.min(...runs.map((run) => (
      new Date(run.scheduled_at).valueOf()
      - (run.overlap_minutes ?? defaultOverlap) * 60_000
    )));
    return formatTinybirdDateTime(new Date(cutoffMs));
  }

  private generationBatchVersion(generationId: string): number {
    return this.generationStartedAt(generationId).valueOf();
  }

  private generationStartedAt(generationId: string): Date {
    const row = this.ctx.storage.sql.exec<{ scheduled_at: string | null }>(
      `
        SELECT MIN(scheduled_at) AS scheduled_at
        FROM raw_runs
        WHERE generation_id = ? AND status = 'completed'
      `,
      generationId,
    ).one();
    if (!row.scheduled_at) {
      throw new Error(`Raw generation ${generationId} has no completed scheduled runs.`);
    }

    const earliestRun = new Date(row.scheduled_at);
    if (Number.isNaN(earliestRun.valueOf())) {
      throw new Error(`Raw generation ${generationId} has an invalid scheduled timestamp.`);
    }
    return earliestRun;
  }

  private generationEndedAt(generationId: string): Date {
    const row = this.ctx.storage.sql.exec<{ scheduled_at: string | null }>(
      `
        SELECT MAX(scheduled_at) AS scheduled_at
        FROM raw_runs
        WHERE generation_id = ? AND status = 'completed'
      `,
      generationId,
    ).one();
    if (!row.scheduled_at) {
      throw new Error(`Raw generation ${generationId} has no completed scheduled runs.`);
    }

    const latestRun = new Date(row.scheduled_at);
    if (Number.isNaN(latestRun.valueOf())) {
      throw new Error(`Raw generation ${generationId} has an invalid scheduled timestamp.`);
    }
    return latestRun;
  }

  private deleteOldHistory(now: number): void {
    const cutoff = now - HISTORY_RETENTION_MS;
    this.ctx.storage.sql.exec(
      "DELETE FROM raw_runs WHERE status = 'completed' AND updated_at_ms <= ?",
      cutoff,
    );
    this.ctx.storage.sql.exec(
      `
        DELETE FROM publication_jobs
        WHERE generation_id IN (
          SELECT generation_id
          FROM publication_generations
          WHERE completed_at_ms IS NOT NULL AND completed_at_ms <= ?
        )
      `,
      cutoff,
    );
    this.ctx.storage.sql.exec(
      `
        DELETE FROM publication_generations
        WHERE completed_at_ms IS NOT NULL AND completed_at_ms <= ?
      `,
      cutoff,
    );
  }

  private tinybirdConfig(): TinybirdApiConfig {
    requireValue(this.env.TINYBIRD_API_URL, "TINYBIRD_API_URL");
    requireValue(this.env.TINYBIRD_ADMIN_TOKEN, "TINYBIRD_ADMIN_TOKEN");

    return {
      apiUrl: this.env.TINYBIRD_API_URL,
      adminToken: this.env.TINYBIRD_ADMIN_TOKEN,
      fetchTimeoutMs: positiveInteger(
        this.env.TINYBIRD_FETCH_TIMEOUT_MS,
        "TINYBIRD_FETCH_TIMEOUT_MS",
      ),
    };
  }

  private copyPollIntervalMs(): number {
    return positiveInteger(this.env.COPY_POLL_INTERVAL_MS, "COPY_POLL_INTERVAL_MS");
  }

  private copyJobTimeoutMs(): number {
    return positiveInteger(this.env.COPY_JOB_TIMEOUT_MS, "COPY_JOB_TIMEOUT_MS");
  }

  private async ensureAlarm(delayMs = MIN_ALARM_DELAY_MS): Promise<void> {
    if (await this.operatorPaused()) return;
    const now = Date.now();
    const runAt = now + Math.max(delayMs, MIN_ALARM_DELAY_MS);
    const current = await this.ctx.storage.getAlarm();
    if (current !== null && current > now && current <= runAt) return;
    await this.ctx.storage.setAlarm(runAt);
  }

  private async operatorPaused(): Promise<boolean> {
    return await this.ctx.storage.get<boolean>("operator_paused") ?? false;
  }

  private async operatorPauseAfterPublication(): Promise<boolean> {
    return await this.ctx.storage.get<boolean>("operator_pause_after_publication") ?? false;
  }
}

export function applyWorkerIdentityCursorResetMigration(
  storage: DurableObjectStorage,
  now: number,
): boolean {
  const version = storage.sql.exec<{ version: number }>(
    "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
  ).one().version;
  if (version >= 9) return false;

  const state = storage.sql.exec<{
    publication_section: string | null;
    identity_phase: string;
    active_job_id: string | null;
  }>(
    `
      SELECT publication_section, identity_phase, active_job_id
      FROM coordinator_state
      WHERE id = 1
    `,
  ).one();
  const shouldResume = state.publication_section === "identity"
    && state.identity_phase === "compute"
    && state.active_job_id === null;

  if (shouldResume) {
    storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          phase = 'copying',
          identity_batch_version = NULL,
          identity_batch_id = NULL,
          identity_cursor_ingested_at = NULL,
          identity_cursor_event_id = NULL,
          identity_manifest_json = '{}',
          last_error = NULL
        WHERE id = 1
      `,
    );
  }

  storage.sql.exec(
    `
      INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
      VALUES (9, ?)
    `,
    now,
  );
  return shouldResume;
}

export function applyIdentityOnlyPublicationMigration(
  storage: DurableObjectStorage,
  now: number,
): boolean {
  const version = storage.sql.exec<{ version: number }>(
    "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
  ).one().version;
  if (version >= 10) return false;

  const state = storage.sql.exec<{
    phase: string;
    publication_section: string | null;
  }>(
    "SELECT phase, publication_section FROM coordinator_state WHERE id = 1",
  ).one();
  const shouldFinish = state.phase === "failed"
    && state.publication_section === "post_identity";

  if (shouldFinish) {
    storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          phase = 'copying',
          publication_index = 0,
          active_pipe = NULL,
          active_job_id = NULL,
          active_job_started_at_ms = NULL,
          active_copy_parameters_json = '{}',
          last_error = NULL
        WHERE id = 1
      `,
    );
  }

  storage.sql.exec(
    `
      INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
      VALUES (10, ?)
    `,
    now,
  );
  return shouldFinish;
}

export function applyRawRunRecoveryMigration(
  storage: DurableObjectStorage,
  now: number,
): boolean {
  const version = storage.sql.exec<{ version: number }>(
    "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
  ).one().version;
  if (version >= 11) return false;

  const running = storage.sql.exec<{ value: number }>(
    "SELECT COUNT(*) AS value FROM raw_runs WHERE status = 'running'",
  ).one().value;
  if (running > 0) {
    storage.sql.exec(
      `
        UPDATE raw_runs
        SET
          status = 'queued',
          last_error = 'Recovered during the raw-run lease migration.',
          updated_at_ms = ?
        WHERE status = 'running'
      `,
      now,
    );
  }

  storage.sql.exec(
    `
      INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
      VALUES (11, ?)
    `,
    now,
  );
  return running > 0;
}

export function applyWorkerIdentityEnqueueMigration(
  storage: DurableObjectStorage,
  now: number,
): boolean {
  const version = storage.sql.exec<{ version: number }>(
    "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
  ).one().version;
  if (version >= 12) return false;

  addTextColumnIfMissing(
    storage,
    "identity_enqueue_after_fact_key",
    "TEXT NOT NULL DEFAULT ''",
  );
  addTextColumnIfMissing(
    storage,
    "identity_enqueue_ingested_at",
    "TEXT",
  );

  const state = storage.sql.exec<{
    phase: string;
    publication_kind: string | null;
    publication_section: string | null;
    identity_phase: string;
  }>(
    `
      SELECT phase, publication_kind, publication_section, identity_phase
      FROM coordinator_state
      WHERE id = 1
    `,
  ).one();
  const hasActivePublication = state.publication_kind !== null;
  const isLegacyIdentityCopyPath = state.publication_section === "pre_identity"
    || (
      state.publication_section === "identity"
      && state.identity_phase === "enqueue"
    );
  const shouldResume = hasActivePublication && isLegacyIdentityCopyPath;

  if (shouldResume) {
    storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          phase = 'copying',
          publication_section = 'identity',
          publication_index = 0,
          identity_phase = 'enqueue',
          identity_enqueue_index = 0,
          identity_enqueue_after_fact_key = '',
          identity_enqueue_ingested_at = ?,
          active_pipe = NULL,
          active_job_id = NULL,
          active_job_started_at_ms = NULL,
          active_copy_parameters_json = '{}',
          last_error = NULL
        WHERE id = 1
      `,
      formatTinybirdDateTime64(new Date(now)),
    );
    storage.sql.exec(
      `
        UPDATE publication_generations
        SET status = 'running', last_error = NULL
        WHERE generation_id = (
          SELECT active_raw_generation FROM coordinator_state WHERE id = 1
        )
      `,
    );
  }

  storage.sql.exec(
    `
      INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
      VALUES (12, ?)
    `,
    now,
  );
  return shouldResume;
}

export function applyVersionedIdentityBatchMigration(
  storage: DurableObjectStorage,
  now: number,
): boolean {
  const version = storage.sql.exec<{ version: number }>(
    "SELECT COALESCE(MAX(version), 0) AS version FROM publication_schema_migrations",
  ).one().version;
  if (version >= 17) return false;

  const state = storage.sql.exec<{
    publication_section: string | null;
    identity_phase: string;
    identity_batch_id: string | null;
    active_job_id: string | null;
  }>(
    `
      SELECT publication_section, identity_phase, identity_batch_id, active_job_id
      FROM coordinator_state
      WHERE id = 1
    `,
  ).one();
  const shouldResume = state.publication_section === "identity"
    && state.identity_phase === "compute"
    && state.identity_batch_id !== null
    && state.active_job_id === null;

  if (shouldResume) {
    storage.sql.exec(
      `
        UPDATE coordinator_state
        SET
          phase = 'copying',
          identity_batch_version = NULL,
          identity_batch_id = NULL,
          identity_cursor_ingested_at = NULL,
          identity_cursor_event_id = NULL,
          identity_manifest_json = '{}',
          last_error = NULL
        WHERE id = 1
      `,
    );
  }

  storage.sql.exec(
    `
      INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
      VALUES (17, ?)
    `,
    now,
  );
  return shouldResume;
}

function identityBatchId(generationId: string, batchVersion: number): string {
  return [
    generationId,
    "identity",
    batchVersion,
    `engine${IDENTITY_ENGINE_REVISION}`,
    `limit${IDENTITY_BATCH_LIMIT}`,
  ].join("_");
}

function addTextColumnIfMissing(
  storage: DurableObjectStorage,
  columnName: string,
  definition: string,
): void {
  const columns = storage.sql.exec<{ name: string }>(
    "PRAGMA table_info(coordinator_state)",
  ).toArray();
  if (columns.some((column) => column.name === columnName)) return;
  storage.sql.exec(`ALTER TABLE coordinator_state ADD COLUMN ${columnName} ${definition}`);
}

export function shouldEnqueueSourceFact(
  incoming: PendingIdentityFact,
  stable: CurrentIdentityFact | undefined,
): boolean {
  if (!stable) return true;
  if (incoming.sourceFactVersion < stable.sourceFactVersion) return false;

  return incoming.factDeleted !== stable.factDeleted
    || incoming.factPayloadHash !== stable.factPayloadHash;
}

function parseScheduledAt(value: string): Date {
  if (typeof value !== "string") {
    throw new Error("scheduledAt must be an ISO timestamp string.");
  }

  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) {
    throw new Error("scheduledAt must be an ISO timestamp string.");
  }

  return new Date(Math.floor(date.valueOf() / 60_000) * 60_000);
}

function parseSlot(value: number): number {
  if (Number.isInteger(value) && value >= 0 && value < RAW_SLOT_COUNT) return value;
  throw new Error(`slot must be an integer from 0 through ${RAW_SLOT_COUNT - 1}.`);
}

function rawRequestKey(scheduledAt: Date, slot: number): string {
  const timestamp = scheduledAt.toISOString().replace(/[-:.TZ]/g, "");
  return `raw_${timestamp}_slot_${slot}`;
}

function rawGenerationId(run: RawRunRow): string {
  return `generation_${run.request_key}`;
}

function formatTinybirdDateTime(value: Date): string {
  return value.toISOString().slice(0, 19).replace("T", " ");
}

function formatTinybirdDateTime64(value: Date): string {
  return `${value.toISOString().slice(0, -1).replace("T", " ")}000`;
}

export function validateTinybirdDateTime(value: string, name: string): string {
  const message = `${name} must use Tinybird DateTime format YYYY-MM-DD HH:MM:SS.`;
  if (typeof value !== "string" || !TINYBIRD_DATETIME_PATTERN.test(value)) {
    throw new Error(message);
  }

  const parsed = new Date(`${value.replace(" ", "T")}Z`);
  if (Number.isNaN(parsed.valueOf()) || formatTinybirdDateTime(parsed) !== value) {
    throw new Error(message);
  }

  return value;
}

function tinybirdDateTime(value: string, name: string): Date {
  const normalized = validateTinybirdDateTime(value, name);
  return new Date(`${normalized.replace(" ", "T")}Z`);
}

export function bootstrapIdentityCutoffMigrationValue(
  publicationKind: PublicationKind | null,
  publicationSection: PublicationSection | null,
  configuredValue: string,
): string | null {
  if (publicationKind !== "bootstrap" || publicationSection !== "pre_identity") {
    return null;
  }

  return validateTinybirdDateTime(
    configuredValue,
    "IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM",
  );
}

export function applyBootstrapIdentityCutoffMigration(
  storage: DurableObjectStorage,
  configuredValue: string,
  now: number,
): void {
  const applied = storage.sql.exec<{ value: number }>(
    "SELECT COUNT(*) AS value FROM publication_schema_migrations WHERE version = 7",
  ).one().value;
  if (applied > 0) return;

  const state = storage.sql.exec<{
    publication_kind: PublicationKind | null;
    publication_section: PublicationSection | null;
  }>(
    "SELECT publication_kind, publication_section FROM coordinator_state WHERE id = 1",
  ).one();
  const cutoff = bootstrapIdentityCutoffMigrationValue(
    state.publication_kind,
    state.publication_section,
    configuredValue,
  );
  if (cutoff !== null) {
    storage.sql.exec(
      `
        UPDATE coordinator_state
        SET identity_overlap_cutoff = ?, updated_at_ms = ?
        WHERE id = 1
      `,
      cutoff,
      now,
    );
  }

  storage.sql.exec(
    `
      INSERT OR IGNORE INTO publication_schema_migrations (version, applied_at_ms)
      VALUES (7, ?)
    `,
    now,
  );
}

function normalizeCopyParameters(
  parameters: Readonly<Record<string, string>>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(parameters)
      .sort(([left], [right]) => left.localeCompare(right)),
  );
}

function parseCopyParameters(value: string): Record<string, string> {
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!isStringRecord(parsed)) return {};
    return normalizeCopyParameters(parsed);
  } catch {
    return {};
  }
}

function isStringRecord(value: unknown): value is Record<string, string> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  return Object.values(value).every((item) => typeof item === "string");
}

function parseStringArray(value: string, name: string): string[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value) as unknown;
  } catch {
    throw new Error(`Stored ${name} is invalid JSON.`);
  }

  if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== "string")) {
    throw new Error(`Stored ${name} must be an array of strings.`);
  }
  return [...new Set(parsed)].sort();
}

function parseIdentityManifest(value: string): IdentityCompactionManifest {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value) as unknown;
  } catch {
    throw new Error("Stored identity compaction manifest is invalid JSON.");
  }

  if (!isIdentityManifest(parsed)) {
    throw new Error("Stored identity compaction manifest is incomplete.");
  }
  return parsed;
}

function isIdentityManifest(value: unknown): value is IdentityCompactionManifest {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const manifest = value as Record<string, unknown>;

  return typeof manifest.tenantId === "string"
    && typeof manifest.batchVersion === "number"
    && typeof manifest.batchId === "string"
    && typeof manifest.inputEventCount === "number"
    && typeof manifest.inputHash === "string"
    && typeof manifest.checkpointIngestedAt === "string"
    && typeof manifest.checkpointEventId === "string"
    && typeof manifest.expectedOutputRowCount === "number"
    && typeof manifest.expectedOutputHash === "string"
    && typeof manifest.actualOutputRowCount === "number"
    && typeof manifest.actualOutputHash === "string"
    && typeof manifest.isValid === "boolean";
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

function isTransientUpstreamError(error: unknown): boolean {
  if (error instanceof TinybirdRequestError) {
    return error.status === 429 || error.status >= 500;
  }
  return error instanceof DOMException && error.name === "TimeoutError";
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function logEvent(event: string, details: Record<string, unknown>): void {
  console.log(JSON.stringify({
    service: "bigquery-tinybird-sync",
    event,
    ...details,
  }));
}
