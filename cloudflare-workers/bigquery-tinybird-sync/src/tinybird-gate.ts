import { DurableObject } from "cloudflare:workers";
import { MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE } from "./cadence";

const RATE_WINDOW_MS = 60_000;
const LEASE_DURATION_MS = 5 * 60_000;
const REQUEST_RETENTION_MS = 24 * 60 * 60_000;
const ATTEMPT_RETENTION_MS = 60 * 60_000;

export interface TinybirdGateEnv {
  TINYBIRD_API_URL: string;
  TINYBIRD_ADMIN_TOKEN: string;
  TINYBIRD_FETCH_TIMEOUT_MS: string;
  TINYBIRD_GATE_TIMEOUT_MS: string;
}

export interface TinybirdSyncRequest {
  requestKey: string;
  resourceName: string;
}

export interface TinybirdGateResult {
  outcome: "synced" | "already_synced" | "in_flight";
  tinybirdStatus?: number;
}

interface RequestRow {
  [key: string]: string | number | null;
  resource_name: string;
  status: "completed" | "failed" | "inflight";
  lease_expires_at_ms: number | null;
  tinybird_status: number | null;
}

interface RateWindow {
  [key: string]: number | null;
  request_count: number;
  oldest_request_ms: number | null;
}

type Claim =
  | { outcome: "acquired"; leaseId: string }
  | { outcome: "already_synced"; tinybirdStatus?: number }
  | { outcome: "in_flight" }
  | { outcome: "wait"; waitMs: number };

export class TinybirdSyncGate extends DurableObject<TinybirdGateEnv> {
  private readonly fetchTimeoutMs: number;
  private readonly gateTimeoutMs: number;

  constructor(ctx: DurableObjectState, env: TinybirdGateEnv) {
    super(ctx, env);
    this.fetchTimeoutMs = positiveInteger(
      env.TINYBIRD_FETCH_TIMEOUT_MS,
      "TINYBIRD_FETCH_TIMEOUT_MS",
    );
    this.gateTimeoutMs = positiveInteger(
      env.TINYBIRD_GATE_TIMEOUT_MS,
      "TINYBIRD_GATE_TIMEOUT_MS",
    );

    if (this.gateTimeoutMs >= LEASE_DURATION_MS) {
      throw new Error("TINYBIRD_GATE_TIMEOUT_MS must be shorter than the lease.");
    }

    ctx.blockConcurrencyWhile(async () => {
      this.migrate();
    });
  }

  async trigger(request: TinybirdSyncRequest): Promise<TinybirdGateResult> {
    validateRequest(request);
    const deadlineAt = Date.now() + this.gateTimeoutMs;

    while (true) {
      const claim = this.claim(request, Date.now());

      if (claim.outcome === "already_synced") return claim;
      if (claim.outcome === "in_flight") return claim;

      if (claim.outcome === "wait") {
        await waitBeforeDeadline(
          claim.waitMs,
          deadlineAt,
          "Tinybird rate gate deadline expired while waiting for capacity.",
        );
        continue;
      }

      return this.sendToTinybird(request, claim.leaseId, deadlineAt);
    }
  }

  private migrate(): void {
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS sync_requests (
        request_key TEXT PRIMARY KEY,
        resource_name TEXT NOT NULL,
        status TEXT NOT NULL,
        lease_id TEXT,
        lease_expires_at_ms INTEGER,
        tinybird_status INTEGER,
        last_error TEXT,
        updated_at_ms INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS rate_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_key TEXT NOT NULL,
        started_at_ms INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS rate_attempts_started_at
        ON rate_attempts(started_at_ms);
    `);

    this.ctx.storage.sql.exec(
      `
        INSERT OR IGNORE INTO schema_migrations (version, applied_at_ms)
        VALUES (1, ?)
      `,
      Date.now(),
    );
  }

  private claim(request: TinybirdSyncRequest, now: number): Claim {
    this.deleteExpiredState(now);

    const existing = this.requestRow(request.requestKey);
    if (existing && existing.resource_name !== request.resourceName) {
      throw new Error("Tinybird sync request key was reused for another resource.");
    }

    if (existing?.status === "completed") {
      return {
        outcome: "already_synced",
        tinybirdStatus: existing.tinybird_status ?? undefined,
      };
    }

    if (
      existing?.status === "inflight"
      && (existing.lease_expires_at_ms ?? 0) > now
    ) {
      return { outcome: "in_flight" };
    }

    const rateWindow = this.rateWindow(now);
    if (
      rateWindow.request_count
      >= MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE
    ) {
      return {
        outcome: "wait",
        waitMs: nextWindowWait(rateWindow.oldest_request_ms, now),
      };
    }

    const leaseId = crypto.randomUUID();
    this.reserveAttempt(request.requestKey, now);
    this.ctx.storage.sql.exec(
      `
        INSERT INTO sync_requests (
          request_key,
          resource_name,
          status,
          lease_id,
          lease_expires_at_ms,
          tinybird_status,
          last_error,
          updated_at_ms
        ) VALUES (?, ?, 'inflight', ?, ?, NULL, NULL, ?)
        ON CONFLICT(request_key) DO UPDATE SET
          resource_name = excluded.resource_name,
          status = excluded.status,
          lease_id = excluded.lease_id,
          lease_expires_at_ms = excluded.lease_expires_at_ms,
          tinybird_status = NULL,
          last_error = NULL,
          updated_at_ms = excluded.updated_at_ms
      `,
      request.requestKey,
      request.resourceName,
      leaseId,
      now + LEASE_DURATION_MS,
      now,
    );

    return { outcome: "acquired", leaseId };
  }

  private async sendToTinybird(
    request: TinybirdSyncRequest,
    leaseId: string,
    deadlineAt: number,
  ): Promise<TinybirdGateResult> {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      let response: Response;

      try {
        const fetchTimeoutMs = Math.min(
          this.fetchTimeoutMs,
          remainingTime(deadlineAt),
        );
        if (fetchTimeoutMs <= 0) {
          throw new Error("Tinybird rate gate deadline expired before the request.");
        }

        response = await fetch(this.tinybirdUrl(request.resourceName), {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.env.TINYBIRD_ADMIN_TOKEN}`,
          },
          signal: AbortSignal.timeout(fetchTimeoutMs),
        });
      } catch (error) {
        this.fail(request.requestKey, leaseId, errorMessage(error));
        throw error;
      }

      if (response.ok) {
        await response.body?.cancel().catch(() => undefined);
        this.complete(request.requestKey, leaseId, response.status);
        return { outcome: "synced", tinybirdStatus: response.status };
      }

      let details: string;

      try {
        details = (await response.text()).trim().slice(0, 2_000);
      } catch (error) {
        this.fail(request.requestKey, leaseId, errorMessage(error));
        throw error;
      }

      if (response.status === 429 && attempt === 0) {
        const retryAfterMs = parseRetryAfter(response.headers.get("Retry-After"));

        try {
          assertCanWait(
            retryAfterMs,
            deadlineAt,
            "Tinybird Retry-After exceeds the rate gate deadline.",
          );
          this.extendLease(request.requestKey, leaseId, retryAfterMs);
          await wait(retryAfterMs);
          await this.reserveRetryAttempt(
            request.requestKey,
            leaseId,
            deadlineAt,
          );
        } catch (error) {
          this.fail(request.requestKey, leaseId, errorMessage(error));
          throw error;
        }

        continue;
      }

      const message = [
        `${request.resourceName} Tinybird sync failed (${response.status})`,
        details || response.statusText,
      ].filter(Boolean).join(": ");
      this.fail(request.requestKey, leaseId, message);
      throw new Error(message);
    }

    throw new Error(`${request.resourceName} Tinybird sync exhausted its retries.`);
  }

  private async reserveRetryAttempt(
    requestKey: string,
    leaseId: string,
    deadlineAt: number,
  ): Promise<void> {
    while (true) {
      this.assertLease(requestKey, leaseId);
      const now = Date.now();
      const rateWindow = this.rateWindow(now);

      if (
        rateWindow.request_count
        < MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE
      ) {
        this.reserveAttempt(requestKey, now);
        this.extendLease(requestKey, leaseId, 0);
        return;
      }

      const waitMs = nextWindowWait(rateWindow.oldest_request_ms, now);
      assertCanWait(
        waitMs,
        deadlineAt,
        "Tinybird rate gate deadline expired before the retry.",
      );
      this.extendLease(requestKey, leaseId, waitMs);
      await wait(waitMs);
    }
  }

  private requestRow(requestKey: string): RequestRow | null {
    return this.ctx.storage.sql.exec<RequestRow>(
      `
        SELECT
          resource_name,
          status,
          lease_expires_at_ms,
          tinybird_status
        FROM sync_requests
        WHERE request_key = ?
      `,
      requestKey,
    ).toArray()[0] ?? null;
  }

  private rateWindow(now: number): RateWindow {
    return this.ctx.storage.sql.exec<RateWindow>(
      `
        SELECT
          COUNT(*) AS request_count,
          MIN(started_at_ms) AS oldest_request_ms
        FROM rate_attempts
        WHERE started_at_ms > ?
      `,
      now - RATE_WINDOW_MS,
    ).one();
  }

  private reserveAttempt(requestKey: string, now: number): void {
    this.ctx.storage.sql.exec(
      "INSERT INTO rate_attempts (request_key, started_at_ms) VALUES (?, ?)",
      requestKey,
      now,
    );
  }

  private complete(
    requestKey: string,
    leaseId: string,
    tinybirdStatus: number,
  ): void {
    const result = this.ctx.storage.sql.exec(
      `
        UPDATE sync_requests
        SET
          status = 'completed',
          lease_id = NULL,
          lease_expires_at_ms = NULL,
          tinybird_status = ?,
          last_error = NULL,
          updated_at_ms = ?
        WHERE request_key = ? AND lease_id = ?
      `,
      tinybirdStatus,
      Date.now(),
      requestKey,
      leaseId,
    );

    if (result.rowsWritten === 1) return;
    throw new Error("Tinybird sync lease was lost before completion.");
  }

  private fail(requestKey: string, leaseId: string, message: string): void {
    this.ctx.storage.sql.exec(
      `
        UPDATE sync_requests
        SET
          status = 'failed',
          lease_id = NULL,
          lease_expires_at_ms = NULL,
          last_error = ?,
          updated_at_ms = ?
        WHERE request_key = ? AND lease_id = ?
      `,
      message.slice(0, 2_000),
      Date.now(),
      requestKey,
      leaseId,
    );
  }

  private extendLease(
    requestKey: string,
    leaseId: string,
    waitMs: number,
  ): void {
    const now = Date.now();
    const result = this.ctx.storage.sql.exec(
      `
        UPDATE sync_requests
        SET lease_expires_at_ms = ?, updated_at_ms = ?
        WHERE request_key = ? AND lease_id = ? AND status = 'inflight'
      `,
      now + waitMs + LEASE_DURATION_MS,
      now,
      requestKey,
      leaseId,
    );

    if (result.rowsWritten === 1) return;
    throw new Error("Tinybird sync lease was lost while extending its deadline.");
  }

  private assertLease(requestKey: string, leaseId: string): void {
    const lease = this.ctx.storage.sql.exec<{ lease_id: string | null }>(
      "SELECT lease_id FROM sync_requests WHERE request_key = ?",
      requestKey,
    ).toArray()[0];

    if (lease?.lease_id === leaseId) return;
    throw new Error("Tinybird sync lease was lost before retry.");
  }

  private deleteExpiredState(now: number): void {
    this.ctx.storage.sql.exec(
      "DELETE FROM rate_attempts WHERE started_at_ms <= ?",
      now - ATTEMPT_RETENTION_MS,
    );
    this.ctx.storage.sql.exec(
      `
        DELETE FROM sync_requests
        WHERE status != 'inflight' AND updated_at_ms <= ?
      `,
      now - REQUEST_RETENTION_MS,
    );
  }

  private tinybirdUrl(resourceName: string): string {
    const baseUrl = this.env.TINYBIRD_API_URL.replace(/\/+$/g, "");
    return [
      baseUrl,
      "v0",
      "datasources",
      encodeURIComponent(resourceName),
      "scheduling",
      "runs",
    ].join("/");
  }
}

function validateRequest(request: TinybirdSyncRequest): void {
  if (!/^[a-zA-Z0-9_-]{1,200}$/.test(request.requestKey)) {
    throw new Error("Tinybird sync request key is invalid.");
  }

  if (!/^[a-z0-9_]{1,200}$/.test(request.resourceName)) {
    throw new Error("Tinybird resource name is invalid.");
  }
}

function nextWindowWait(oldestRequestMs: number | null, now: number): number {
  if (oldestRequestMs === null) return RATE_WINDOW_MS;
  return Math.max(1, oldestRequestMs + RATE_WINDOW_MS + 1 - now);
}

function parseRetryAfter(value: string | null): number {
  if (!value) return RATE_WINDOW_MS;

  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1_000;

  const date = Date.parse(value);
  if (Number.isNaN(date)) return RATE_WINDOW_MS;
  return Math.max(0, date - Date.now());
}

function assertCanWait(
  waitMs: number,
  deadlineAt: number,
  message: string,
): void {
  if (waitMs <= remainingTime(deadlineAt)) return;
  throw new Error(message);
}

async function waitBeforeDeadline(
  waitMs: number,
  deadlineAt: number,
  message: string,
): Promise<void> {
  assertCanWait(waitMs, deadlineAt, message);
  await wait(waitMs);
}

function remainingTime(deadlineAt: number): number {
  return Math.max(0, deadlineAt - Date.now());
}

function positiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (Number.isInteger(parsed) && parsed > 0) return parsed;
  throw new Error(`${name} must be a positive integer.`);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
