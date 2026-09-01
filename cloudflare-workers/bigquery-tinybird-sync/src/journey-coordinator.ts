import { DurableObject } from "cloudflare:workers";
import type { WorkerEnv } from "./sync";
import {
  readCurrentIdentityMappings,
  readCurrentIdentityProfiles,
  readPendingJourneyBatch,
  TinybirdRequestError,
  type PendingJourneyBatch,
  type TinybirdApiConfig,
} from "./tinybird-api";
import {
  JOURNEY_PROFILE_BATCH_LIMIT,
  processJourneyProfileBatch,
  takeProfilePage,
} from "./journey-worker";

const TENANT_ID = "boom";
const IDLE_POLL_MS = 600_000;
const BUILD_CALL_INTERVAL_MS = 1_000;
const DEFAULT_BACKPRESSURE_MS = 60_000;
const PROFILE_PAGE_LOOKAHEAD = 200;

const JOURNEY_STATE_INSERT_SQL = `
  INSERT OR IGNORE INTO journey_state (
    id,
    phase,
    cursor_batch_version,
    cursor_batch_id,
    active_batch_json,
    active_journey_version_base,
    identifier_index,
    conversion_index,
    last_error,
    next_attempt_at_ms,
    updated_at_ms
  ) VALUES (1, 'idle', 0, '', '', 0, 0, 0, NULL, 0, ?)
`;

type JourneyPhase = "idle" | "running" | "failed";

interface JourneyState {
  [key: string]: string | number | null;
  phase: JourneyPhase;
  cursor_batch_version: number;
  cursor_batch_id: string;
  active_batch_json: string;
  active_journey_version_base: number;
  conversion_index: number;
  orphan_index: number;
  profile_index: number;
  page_number: number;
  last_error: string | null;
  next_attempt_at_ms: number;
  updated_at_ms: number;
}

export interface JourneyCoordinatorStatus {
  phase: JourneyPhase;
  cursorBatchVersion: number;
  cursorBatchId: string;
  activeIdentityBatchId: string | null;
  completedConversions: number;
  totalConversions: number;
  completedOrphanKeys: number;
  totalOrphanKeys: number;
  completedProfiles: number;
  totalProfiles: number;
  nextAttemptAt: string | null;
  lastError: string | null;
}

export class JourneyCoordinator extends DurableObject<WorkerEnv> {
  constructor(ctx: DurableObjectState, env: WorkerEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => this.migrate());
  }

  async tick(): Promise<JourneyCoordinatorStatus> {
    const state = this.state();
    if (state.phase !== "failed") await this.ensureAlarm();
    return this.status();
  }

  async recoverFailed(): Promise<JourneyCoordinatorStatus> {
    const state = this.state();
    if (state.phase === "failed") {
      this.updateState({ phase: "running", next_attempt_at_ms: 0 });
      await this.ensureAlarm();
    }
    return this.status();
  }

  async status(): Promise<JourneyCoordinatorStatus> {
    const state = this.state();
    const batch = parseActiveBatch(state.active_batch_json);

    return {
      phase: state.phase,
      cursorBatchVersion: state.cursor_batch_version,
      cursorBatchId: state.cursor_batch_id,
      activeIdentityBatchId: batch?.batchId ?? null,
      completedConversions: state.conversion_index,
      totalConversions: batch?.conversionIds.length ?? 0,
      completedOrphanKeys: state.orphan_index,
      totalOrphanKeys: batch?.orphanIdentifierKeys.length ?? 0,
      completedProfiles: state.profile_index,
      totalProfiles: batch?.profileIds.length ?? 0,
      nextAttemptAt: state.next_attempt_at_ms > 0
        ? new Date(state.next_attempt_at_ms).toISOString()
        : null,
      lastError: state.last_error,
    };
  }

  async alarm(): Promise<void> {
    const state = this.state();
    if (state.phase === "failed") return;

    const waitMs = state.next_attempt_at_ms - Date.now();
    if (waitMs > 0) {
      await this.ensureAlarm(waitMs);
      return;
    }

    try {
      const active = parseActiveBatch(state.active_batch_json);
      if (!active) {
        if (state.active_batch_json !== "") {
          this.updateState({ active_batch_json: "" });
        }
        await this.loadNextBatch(state);
        return;
      }

      await this.processNextPage(state, active);
    } catch (error) {
      await this.handleError(error);
    }
  }

  private async loadNextBatch(state: JourneyState): Promise<void> {
    const batch = await readPendingJourneyBatch(
      TENANT_ID,
      state.cursor_batch_version,
      state.cursor_batch_id,
      this.tinybirdConfig(),
    );
    if (!batch) {
      this.updateState({ phase: "idle", next_attempt_at_ms: 0, last_error: null });
      await this.ensureAlarm(IDLE_POLL_MS);
      return;
    }

    this.updateState({
      phase: "running",
      active_batch_json: JSON.stringify(batch),
      active_journey_version_base: Date.now(),
      conversion_index: 0,
      orphan_index: 0,
      profile_index: 0,
      page_number: 0,
      next_attempt_at_ms: 0,
      last_error: null,
    });
    await this.ensureAlarm();
  }

  // Page order matters: conversion-only pages first, profile pages last.
  // A conversion whose profile is in this batch is rebuilt twice, and the
  // profile page's higher batch version must be the one reads pick up.
  private async processNextPage(
    state: JourneyState,
    batch: PendingJourneyBatch,
  ): Promise<void> {
    const conversionIds = batch.conversionIds.slice(
      state.conversion_index,
      state.conversion_index + JOURNEY_PROFILE_BATCH_LIMIT,
    );
    if (conversionIds.length > 0) {
      await this.buildPage(state, batch, [], conversionIds);
      this.advancePage(state, {
        conversion_index: state.conversion_index + conversionIds.length,
      });
      await this.ensureAlarm(BUILD_CALL_INTERVAL_MS);
      return;
    }

    const orphanKeys = batch.orphanIdentifierKeys.slice(
      state.orphan_index,
      state.orphan_index + JOURNEY_PROFILE_BATCH_LIMIT,
    );
    if (orphanKeys.length > 0) {
      // A key that still maps to a profile must only ever be built alongside
      // its profilemates, so it is excluded here; its profile is in this
      // batch's profile list and gets rebuilt on a later page.
      const mappings = await readCurrentIdentityMappings(
        TENANT_ID,
        orphanKeys,
        this.tinybirdConfig(),
      );
      const mappedKeys = new Set(mappings.map((mapping) => mapping.identifierKey));
      const unmappedKeys = orphanKeys.filter((key) => !mappedKeys.has(key));
      if (unmappedKeys.length > 0) {
        await this.buildPage(state, batch, unmappedKeys, []);
      }
      this.advancePage(state, {
        orphan_index: state.orphan_index + orphanKeys.length,
      });
      await this.ensureAlarm(BUILD_CALL_INTERVAL_MS);
      return;
    }

    const lookaheadProfileIds = batch.profileIds.slice(
      state.profile_index,
      state.profile_index + PROFILE_PAGE_LOOKAHEAD,
    );
    if (lookaheadProfileIds.length === 0) {
      this.finishBatch(batch);
      await this.ensureAlarm();
      return;
    }

    const profiles = await readCurrentIdentityProfiles(
      TENANT_ID,
      lookaheadProfileIds,
      this.tinybirdConfig(),
    );
    const keysByProfile = new Map(profiles.map(
      (profile) => [profile.profileId, profile.memberIdentifierKeys],
    ));
    const page = takeProfilePage(lookaheadProfileIds, keysByProfile);
    if (page.identifierKeys.length > 0) {
      await this.buildPage(state, batch, page.identifierKeys, []);
    }
    this.advancePage(state, {
      profile_index: state.profile_index + page.profileIds.length,
    });
    await this.ensureAlarm(BUILD_CALL_INTERVAL_MS);
  }

  private async buildPage(
    state: JourneyState,
    batch: PendingJourneyBatch,
    identifierKeys: string[],
    conversionIds: string[],
  ): Promise<void> {
    const batchVersion = state.active_journey_version_base + state.page_number;
    await processJourneyProfileBatch({
      tenantId: batch.tenantId,
      identifierKeys,
      conversionIds,
      batchVersion,
      batchId: `${batch.batchId}_journey_${state.page_number}_${batchVersion}`,
    }, this.tinybirdConfig());
  }

  private advancePage(
    state: JourneyState,
    progress: Partial<JourneyState>,
  ): void {
    this.updateState({
      ...progress,
      page_number: state.page_number + 1,
      next_attempt_at_ms: Date.now() + BUILD_CALL_INTERVAL_MS,
      last_error: null,
    });
  }

  private finishBatch(batch: PendingJourneyBatch): void {
    this.updateState({
      phase: "idle",
      cursor_batch_version: batch.batchVersion,
      cursor_batch_id: batch.batchId,
      active_batch_json: "",
      active_journey_version_base: 0,
      conversion_index: 0,
      orphan_index: 0,
      profile_index: 0,
      page_number: 0,
      next_attempt_at_ms: 0,
      last_error: null,
    });
  }

  private async handleError(error: unknown): Promise<void> {
    const message = errorMessage(error).slice(0, 2_000);
    if (!isBackpressure(error)) {
      this.updateState({ phase: "failed", last_error: message });
      return;
    }

    const delayMs = error instanceof TinybirdRequestError
      ? error.retryAfterMs ?? DEFAULT_BACKPRESSURE_MS
      : DEFAULT_BACKPRESSURE_MS;
    this.updateState({
      phase: "running",
      last_error: message,
      next_attempt_at_ms: Date.now() + delayMs,
    });
    await this.ensureAlarm(delayMs);
  }

  private migrate(): void {
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS journey_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        phase TEXT NOT NULL,
        cursor_batch_version INTEGER NOT NULL,
        cursor_batch_id TEXT NOT NULL,
        active_batch_json TEXT NOT NULL,
        active_journey_version_base INTEGER NOT NULL,
        identifier_index INTEGER NOT NULL,
        conversion_index INTEGER NOT NULL,
        last_error TEXT,
        next_attempt_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
    `);
    this.ctx.storage.sql.exec(
      JOURNEY_STATE_INSERT_SQL,
      Date.now(),
    );
    this.ensureColumn("orphan_index", "INTEGER NOT NULL DEFAULT 0");
    this.ensureColumn("profile_index", "INTEGER NOT NULL DEFAULT 0");
    this.ensureColumn("page_number", "INTEGER NOT NULL DEFAULT 0");
  }

  private ensureColumn(name: string, definition: string): void {
    const columns = this.ctx.storage.sql.exec<{ name: string }>(
      "SELECT name FROM pragma_table_info('journey_state')",
    ).toArray();
    if (columns.some((column) => column.name === name)) return;
    this.ctx.storage.sql.exec(
      `ALTER TABLE journey_state ADD COLUMN ${name} ${definition}`,
    );
  }

  private state(): JourneyState {
    return this.ctx.storage.sql.exec<JourneyState>(
      "SELECT * FROM journey_state WHERE id = 1",
    ).one();
  }

  private updateState(values: Partial<JourneyState>): void {
    const entries = Object.entries(values).filter(([key]) => key !== "id");
    if (entries.length === 0) return;
    const assignments = entries.map(([key]) => `${key} = ?`).join(", ");
    this.ctx.storage.sql.exec(
      `UPDATE journey_state SET ${assignments}, updated_at_ms = ? WHERE id = 1`,
      ...entries.map(([, value]) => value),
      Date.now(),
    );
  }

  private async ensureAlarm(delayMs = 100): Promise<void> {
    await this.ctx.storage.setAlarm(Date.now() + Math.max(100, delayMs));
  }

  private tinybirdConfig(): TinybirdApiConfig {
    return {
      apiUrl: this.env.TINYBIRD_API_URL,
      adminToken: this.env.TINYBIRD_ADMIN_TOKEN,
      fetchTimeoutMs: positiveInteger(
        this.env.TINYBIRD_FETCH_TIMEOUT_MS,
        "TINYBIRD_FETCH_TIMEOUT_MS",
      ),
    };
  }
}

function parseActiveBatch(value: string): PendingJourneyBatch | null {
  if (!value) return null;
  const parsed = JSON.parse(value) as PendingJourneyBatch;
  if (
    !parsed.batchId
    || !Array.isArray(parsed.profileIds)
    || !Array.isArray(parsed.orphanIdentifierKeys)
    || !Array.isArray(parsed.conversionIds)
  ) {
    return null;
  }
  return parsed;
}

function isBackpressure(error: unknown): boolean {
  if (error instanceof TinybirdRequestError) {
    return error.status === 429 || error.status >= 500;
  }
  return error instanceof DOMException && error.name === "TimeoutError";
}

function positiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (Number.isSafeInteger(parsed) && parsed > 0) return parsed;
  throw new Error(`${name} must be a positive integer.`);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
