import { DurableObject } from "cloudflare:workers";
import type { WorkerEnv } from "./sync";
import { md5Hex } from "./md5";
import {
  appendConversionFactDeltas,
  appendTouchpointFactDeltas,
  readChangedConversionEntities,
  readChangedVisitors,
  readConversionFactCdcBuild,
  readConversionFactHeads,
  readCurrentIdentityMappings,
  readTouchpointFactCdcBuild,
  readTouchpointFactHeads,
  TinybirdRequestError,
  type ChangedConversionEntity,
  type ChangedVisitor,
  type TinybirdApiConfig,
} from "./tinybird-api";
import {
  conversionDeltaRows,
  expectedConversionIds,
  maxLastIngestedAt,
  nextCursorFrom,
  takeConversionEntityPage,
  takeVisitorPage,
  tinybirdDateTime64,
  touchpointDeltaRows,
  visitorAnchorKeys,
} from "./reporting-facts-worker";

const TENANT_ID = "boom";
const IDLE_POLL_MS = 120_000;
const BUILD_CALL_INTERVAL_MS = 2_000;
const DEFAULT_BACKPRESSURE_MS = 60_000;
const CHANGED_WINDOW_LIMIT = 2_000;
const JOURNEY_REPAIR_PROFILE_LIMIT = 200;
const INGESTION_SETTLE_MS = 60_000;
// The frozen fact seeds were exported at this cutoff; starting both cursors
// here makes the first pass through the CDC loop double as the catch-up
// backfill, with no separate tooling.
const CDC_EPOCH = "2026-08-26 23:05:00.000000";

type FactsPhase = "idle" | "running" | "failed";
type FactsStream = "touchpoints" | "conversions";

interface FactsState {
  [key: string]: string | number | null;
  phase: FactsPhase;
  touchpoint_cursor: string;
  conversion_cursor: string;
  active_window_json: string;
  window_index: number;
  page_number: number;
  repair_anchor_keys_json: string;
  last_error: string | null;
  next_attempt_at_ms: number;
  updated_at_ms: number;
}

interface ActiveWindow {
  stream: FactsStream;
  visitors?: ChangedVisitor[];
  entities?: ChangedConversionEntity[];
  windowEnd: string;
}

export interface ReportingFactsStatus {
  phase: FactsPhase;
  touchpointCursor: string;
  conversionCursor: string;
  activeStream: FactsStream | null;
  completedItems: number;
  totalItems: number;
  pendingRepairAnchors: number;
  nextAttemptAt: string | null;
  lastError: string | null;
}

export class ReportingFactsCoordinator extends DurableObject<WorkerEnv> {
  constructor(ctx: DurableObjectState, env: WorkerEnv) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => this.migrate());
  }

  async tick(): Promise<ReportingFactsStatus> {
    const state = this.state();
    if (state.phase !== "failed") await this.ensureAlarm();
    return this.status();
  }

  async recoverFailed(): Promise<ReportingFactsStatus> {
    const state = this.state();
    if (state.phase === "failed") {
      this.updateState({ phase: "running", next_attempt_at_ms: 0 });
      await this.ensureAlarm();
    }
    return this.status();
  }

  async status(): Promise<ReportingFactsStatus> {
    const state = this.state();
    const window = parseActiveWindow(state.active_window_json);

    return {
      phase: state.phase,
      touchpointCursor: state.touchpoint_cursor,
      conversionCursor: state.conversion_cursor,
      activeStream: window?.stream ?? null,
      completedItems: state.window_index,
      totalItems: window ? windowItems(window).length : 0,
      pendingRepairAnchors: parseAnchorKeys(state.repair_anchor_keys_json).length,
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
      const window = parseActiveWindow(state.active_window_json);
      if (!window) {
        await this.loadNextWindow(state);
        return;
      }

      await this.processNextPage(state, window);
    } catch (error) {
      await this.handleError(error);
    }
  }

  private async loadNextWindow(state: FactsState): Promise<void> {
    const windowEnd = tinybirdDateTime64(Date.now() - INGESTION_SETTLE_MS);

    if (state.touchpoint_cursor < windowEnd) {
      const visitors = await readChangedVisitors(
        state.touchpoint_cursor,
        windowEnd,
        CHANGED_WINDOW_LIMIT,
        this.tinybirdConfig(),
      );
      if (visitors.length > 0) {
        this.startWindow({ stream: "touchpoints", visitors, windowEnd });
        await this.ensureAlarm();
        return;
      }
      this.updateState({ touchpoint_cursor: windowEnd });
    }

    if (state.conversion_cursor < windowEnd) {
      const entities = await readChangedConversionEntities(
        state.conversion_cursor,
        windowEnd,
        CHANGED_WINDOW_LIMIT,
        this.tinybirdConfig(),
      );
      if (entities.length > 0) {
        this.startWindow({ stream: "conversions", entities, windowEnd });
        await this.ensureAlarm();
        return;
      }
      this.updateState({ conversion_cursor: windowEnd });
    }

    this.updateState({ phase: "idle", next_attempt_at_ms: 0, last_error: null });
    await this.ensureAlarm(IDLE_POLL_MS);
  }

  private async processNextPage(state: FactsState, window: ActiveWindow): Promise<void> {
    const remaining = windowItems(window).slice(state.window_index);
    if (remaining.length === 0) {
      await this.finishWindow(state, window);
      return;
    }

    const batch = {
      rowVersion: Date.now(),
      batchId: `facts_${window.stream}_${state.page_number}_${Date.now()}`,
      committedAt: new Date().toISOString(),
    };
    let consumed: number;
    let affectedAnchorKeys: string[];

    if (window.stream === "touchpoints") {
      const page = takeVisitorPage(remaining as ChangedVisitor[]);
      const buildRows = await readTouchpointFactCdcBuild(page, this.tinybirdConfig());
      const heads = await readTouchpointFactHeads(
        visitorAnchorKeys(page.visitors),
        this.tinybirdConfig(),
      );
      const diff = touchpointDeltaRows(buildRows, heads, batch);
      if (diff.deltaRows.length > 0) {
        await appendTouchpointFactDeltas(diff.deltaRows, this.tinybirdConfig());
      }
      consumed = page.visitors.length;
      affectedAnchorKeys = diff.affectedAnchorKeys;
    } else {
      const page = takeConversionEntityPage(remaining as ChangedConversionEntity[]);
      const buildRows = await readConversionFactCdcBuild(page, this.tinybirdConfig());
      const heads = await readConversionFactHeads(
        page.entities.flatMap(expectedConversionIds),
        this.tinybirdConfig(),
      );
      const diff = conversionDeltaRows(buildRows, heads, batch);
      if (diff.deltaRows.length > 0) {
        await appendConversionFactDeltas(diff.deltaRows, this.tinybirdConfig());
      }
      consumed = page.entities.length;
      affectedAnchorKeys = diff.affectedAnchorKeys;
    }

    this.accumulateRepairAnchors(state, affectedAnchorKeys);
    this.updateState({
      window_index: state.window_index + consumed,
      page_number: state.page_number + 1,
      next_attempt_at_ms: Date.now() + BUILD_CALL_INTERVAL_MS,
      last_error: null,
    });
    await this.ensureAlarm(BUILD_CALL_INTERVAL_MS);
  }

  // Fresh facts with stale journeys would recreate the exact staleness this
  // coordinator exists to remove, so the cursor only advances after touched
  // profiles are handed to the journey repair queue.
  private async finishWindow(state: FactsState, window: ActiveWindow): Promise<void> {
    await this.flushJourneyRepairs(state, window);

    const latest = maxLastIngestedAt(windowItems(window));
    const cursor = latest ? nextCursorFrom(latest) : window.windowEnd;
    this.updateState({
      ...(window.stream === "touchpoints"
        ? { touchpoint_cursor: cursor }
        : { conversion_cursor: cursor }),
      active_window_json: "",
      window_index: 0,
      repair_anchor_keys_json: "[]",
      next_attempt_at_ms: 0,
      last_error: null,
    });
    await this.ensureAlarm();
  }

  private async flushJourneyRepairs(state: FactsState, window: ActiveWindow): Promise<void> {
    const anchorKeys = parseAnchorKeys(state.repair_anchor_keys_json);
    if (anchorKeys.length === 0) return;

    const mappings = await readCurrentIdentityMappings(
      TENANT_ID,
      anchorKeys,
      this.tinybirdConfig(),
    );
    const repairBatches = journeyRepairBatches(
      mappings.map((mapping) => mapping.profileId),
      window.stream,
      window.windowEnd,
    );
    const journeyCoordinator = this.env.JOURNEY_COORDINATOR.getByName(TENANT_ID);

    for (const repair of repairBatches) {
      await journeyCoordinator.enqueueRepair(repair);
    }
  }

  private startWindow(window: ActiveWindow): void {
    this.updateState({
      phase: "running",
      active_window_json: JSON.stringify(window),
      window_index: 0,
      page_number: 0,
      repair_anchor_keys_json: "[]",
      next_attempt_at_ms: 0,
      last_error: null,
    });
  }

  private accumulateRepairAnchors(state: FactsState, anchorKeys: string[]): void {
    if (anchorKeys.length === 0) return;
    const merged = new Set(parseAnchorKeys(state.repair_anchor_keys_json));
    for (const key of anchorKeys) merged.add(key);
    this.updateState({ repair_anchor_keys_json: JSON.stringify([...merged].sort()) });
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
      CREATE TABLE IF NOT EXISTS facts_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        phase TEXT NOT NULL,
        touchpoint_cursor TEXT NOT NULL,
        conversion_cursor TEXT NOT NULL,
        active_window_json TEXT NOT NULL,
        window_index INTEGER NOT NULL,
        page_number INTEGER NOT NULL,
        repair_anchor_keys_json TEXT NOT NULL,
        last_error TEXT,
        next_attempt_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL
      );
    `);
    this.ctx.storage.sql.exec(
      `INSERT OR IGNORE INTO facts_state VALUES (1, 'idle', ?, ?, '', 0, 0, '[]', NULL, 0, ?)`,
      CDC_EPOCH,
      CDC_EPOCH,
      Date.now(),
    );
  }

  private state(): FactsState {
    return this.ctx.storage.sql.exec<FactsState>(
      "SELECT * FROM facts_state WHERE id = 1",
    ).one();
  }

  private updateState(values: Partial<FactsState>): void {
    const entries = Object.entries(values).filter(([key]) => key !== "id");
    if (entries.length === 0) return;
    const assignments = entries.map(([key]) => `${key} = ?`).join(", ");
    this.ctx.storage.sql.exec(
      `UPDATE facts_state SET ${assignments}, updated_at_ms = ? WHERE id = 1`,
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

export function journeyRepairBatches(
  profileIds: string[],
  stream: FactsStream,
  windowEnd: string,
): Array<{ repairId: string; profileIds: string[] }> {
  const uniqueProfileIds = [...new Set(profileIds)].sort();
  const batches: Array<{ repairId: string; profileIds: string[] }> = [];

  for (let index = 0; index < uniqueProfileIds.length; index += JOURNEY_REPAIR_PROFILE_LIMIT) {
    const batchProfileIds = uniqueProfileIds.slice(index, index + JOURNEY_REPAIR_PROFILE_LIMIT);
    const repairHash = md5Hex(`${stream}\0${windowEnd}\0${batchProfileIds.join("\0")}`);
    batches.push({
      repairId: `facts_${stream}_${repairHash}`,
      profileIds: batchProfileIds,
    });
  }

  return batches;
}

function windowItems(window: ActiveWindow): { lastIngestedAt: string }[] {
  return window.stream === "touchpoints"
    ? (window.visitors ?? [])
    : (window.entities ?? []);
}

function parseActiveWindow(value: string): ActiveWindow | null {
  if (!value) return null;
  const parsed = JSON.parse(value) as ActiveWindow;
  if (parsed.stream !== "touchpoints" && parsed.stream !== "conversions") return null;
  if (!parsed.windowEnd) return null;
  return parsed;
}

function parseAnchorKeys(value: string): string[] {
  const parsed = JSON.parse(value || "[]") as unknown;
  if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== "string")) {
    throw new Error("Stored repair anchor keys are invalid.");
  }
  return parsed as string[];
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
