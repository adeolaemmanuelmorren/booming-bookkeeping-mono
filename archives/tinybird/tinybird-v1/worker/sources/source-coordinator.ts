import { DurableObject } from "cloudflare:workers";

import { createActiveCampaignRpcProvider } from "../conversions/activecampaign-rpc-provider.mjs";
import { runActiveCampaignSourceSlice } from "../conversions/activecampaign-source-machine.mjs";
import { ProviderReadError } from "../conversions/provider-rpc.mjs";
import { createStripeRpcProvider } from "../conversions/stripe-rpc-provider.mjs";
import { runStripeSourceSlice } from "../conversions/stripe-source-machine.mjs";
import type {
  CoordinatorStatus,
  Env,
  SourceReplacementContract,
  SourceRoute,
  StartBackfillOptions,
} from "./contracts.ts";
import { SqliteSourceStore } from "./sqlite-source-store.ts";

const SLICE_BUDGET_MS = 55_000;
const LEASE_MS = 70_000;
const RECOVERY_ALARM_MS = 60_000;
const PUBLISH_CALL_MS = 45_000;
const PUBLICATION_LIMIT = 25;
const STRIPE_INBOX_LIMIT = 25;
const ACTIVE_CAMPAIGN_INBOX_LIMIT = 25;
const ACTIVE_CAMPAIGN_READ_CALL_LIMIT = 100;

// These match the largest discovery burst allowed in one source slice.
const STRIPE_LIVE_BACKLOG_HIGH_WATER = 200;
const STRIPE_ALL_BACKLOG_HIGH_WATER = 300;
const ACTIVE_CAMPAIGN_LIVE_BACKLOG_HIGH_WATER = 100;
const ACTIVE_CAMPAIGN_ALL_BACKLOG_HIGH_WATER = 200;
const OUTBOX_BACKLOG_HIGH_WATER = 100;

export class SourceCoordinator extends DurableObject<Env> {
  private readonly store: SqliteSourceStore;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.store = new SqliteSourceStore(ctx.storage);

    ctx.blockConcurrencyWhile(async () => {
      this.store.initializeSchema();
    });
  }

  async startBackfill(
    route: SourceRoute,
    options: StartBackfillOptions = {},
  ): Promise<CoordinatorStatus> {
    this.store.bindRoute(route);

    const backfillId = requireBackfillId(options.backfillId ?? "v1");
    const stripeCreatedGte = requireStripeCreatedGte(
      options.stripeCreatedGte ?? 0,
    );
    this.store.configureBackfill(
      backfillId,
      stripeCreatedGte,
      new Date().toISOString(),
    );

    return this.wake(route);
  }

  async wake(route: SourceRoute): Promise<CoordinatorStatus> {
    this.store.bindRoute(route);
    await this.scheduleAlarmWithin(RECOVERY_ALARM_MS);
    await this.runLeasedSlice(route);
    return this.status(route);
  }

  async status(route: SourceRoute): Promise<CoordinatorStatus> {
    this.store.bindRoute(route);

    const control = this.store.getControl();
    const lease = this.store.leaseStatus(Date.now());
    const alarm = await this.ctx.storage.getAlarm();

    return {
      route,
      backfill: {
        id: control.backfill_id,
        stripeCreatedGte: control.stripe_created_gte,
        startedAt: control.backfill_started_at,
      },
      lease: {
        active: lease.active,
        expiresAt: toIsoOrNull(lease.expiresAtMs),
      },
      work: this.store.counts() as CoordinatorStatus["work"],
      storage: this.store.storageProfile(),
      lastSlice: {
        startedAt: control.last_slice_started_at,
        succeededAt: control.last_slice_succeeded_at,
        errorCode: control.last_error_code,
        errorStatus: control.last_error_status,
      },
      nextAlarmAt: toIsoOrNull(alarm),
    };
  }

  async alarm(): Promise<void> {
    const route = this.store.getRoute();

    if (!route) {
      return;
    }

    await this.ctx.storage.setAlarm(Date.now() + RECOVERY_ALARM_MS);
    await this.runLeasedSlice(route);
  }

  private async runLeasedSlice(route: SourceRoute): Promise<void> {
    const startedAtMs = Date.now();
    const sliceBudgetMs = this.sliceBudgetMs();
    const leaseId = crypto.randomUUID();
    const acquired = this.store.acquireLease(
      leaseId,
      startedAtMs,
      startedAtMs + LEASE_MS,
    );

    if (!acquired) {
      return;
    }

    this.store.recordSliceStart(new Date(startedAtMs).toISOString());

    try {
      await this.runSourceSlice(route, startedAtMs + sliceBudgetMs, sliceBudgetMs);
      this.store.recordSliceSuccess(new Date().toISOString());
    } catch (error) {
      const safeError = classifyError(error);
      this.store.recordSliceFailure(safeError.code, safeError.status);
    } finally {
      this.store.releaseLease(leaseId);
    }
  }

  private async runSourceSlice(
    route: SourceRoute,
    deadlineAtMs: number,
    sliceBudgetMs: number,
  ): Promise<void> {
    const control = this.store.getControl();
    const publishSourceReplacement = (
      contract: SourceReplacementContract,
    ): Promise<void> =>
      this.publishOneWithDeadline(contract, deadlineAtMs);
    const publishSourceReplacements = (
      contracts: SourceReplacementContract[],
    ): Promise<void> =>
      this.publishManyWithDeadline(contracts, deadlineAtMs);

    if (route.source === "stripe") {
      const limits = this.stripeDiscoveryLimits(route.account);
      const provider = createStripeRpcProvider({
        rpcRead: (account: "stripe" | "stripe_kajabi", path: string, parameters: Record<string, string>) =>
          this.env.STRIPE_SOURCE.read(account, path, parameters),
        extendedObjectPaths: this.env.STRIPE_EXTENDED_OBJECT_PATHS === "true",
      });

      await runStripeSourceSlice(
        {
          store: this.store,
          provider,
          publishSourceReplacement,
          publishSourceReplacements,
          clock: Date,
        },
        {
          account: route.account,
          maxDurationMs: sliceBudgetMs,
          backfillId: control.backfill_id,
          backfillCreatedGte: control.stripe_created_gte,
          publicationLimit: PUBLICATION_LIMIT,
          inboxLimit: STRIPE_INBOX_LIMIT,
          eventPageLimit: limits.eventPages,
          backfillPageLimit: limits.backfillPages,
        },
      );
      return;
    }

    const provider = createActiveCampaignRpcProvider({
      rpcRead: (path: string, parameters: Record<string, string>) =>
        this.env.ACTIVECAMPAIGN_SOURCE.read(path, parameters),
    });
    const limits = this.activeCampaignDiscoveryLimits();

    await runActiveCampaignSourceSlice(
      {
        store: this.store,
        provider,
        publishSourceReplacement,
        publishSourceReplacements,
        clock: Date,
      },
      {
        maxDurationMs: sliceBudgetMs,
        backfillId: control.backfill_id,
        publicationLimit: PUBLICATION_LIMIT,
        inboxLimit: ACTIVE_CAMPAIGN_INBOX_LIMIT,
        contactReadCallLimit: ACTIVE_CAMPAIGN_READ_CALL_LIMIT,
        updatePageLimit: limits.updatePages,
        backfillPageLimit: limits.backfillPages,
        reconciliationPageLimit: limits.reconciliationPages,
      },
    );
  }

  private stripeDiscoveryLimits(account: "main" | "kajabi"): {
    eventPages: number;
    backfillPages: number;
  } {
    const pendingLive = this.store.pendingInboxCount({
      source: "stripe",
      sourceAccount: account,
      maximumPriority: 10,
    });
    const work = this.store.counts();

    return {
      eventPages: pendingLive < STRIPE_LIVE_BACKLOG_HIGH_WATER ? 2 : 0,
      backfillPages:
        work.pendingInbox < STRIPE_ALL_BACKLOG_HIGH_WATER &&
        work.pendingOutbox < OUTBOX_BACKLOG_HIGH_WATER
          ? 1
          : 0,
    };
  }

  private activeCampaignDiscoveryLimits(): {
    updatePages: number;
    backfillPages: number;
    reconciliationPages: number;
  } {
    const pendingLive = this.store.pendingInboxCount({
      source: "activecampaign",
      sourceAccount: "default",
      maximumPriority: 10,
    });
    const work = this.store.counts();
    const allowHistoricalDiscovery =
      work.pendingInbox < ACTIVE_CAMPAIGN_ALL_BACKLOG_HIGH_WATER &&
      work.pendingOutbox < OUTBOX_BACKLOG_HIGH_WATER;

    return {
      updatePages:
        pendingLive < ACTIVE_CAMPAIGN_LIVE_BACKLOG_HIGH_WATER ? 1 : 0,
      backfillPages: allowHistoricalDiscovery ? 1 : 0,
      reconciliationPages: allowHistoricalDiscovery ? 1 : 0,
    };
  }

  private async publishManyWithDeadline(
    contracts: SourceReplacementContract[],
    deadlineAtMs: number,
  ): Promise<void> {
    const publishMany = this.env.SOURCE_PUBLISHER.publishSourceReplacements;

    if (typeof publishMany === "function") {
      await this.callPublisherWithDeadline(
        () =>
          this.env.SOURCE_PUBLISHER.publishSourceReplacements!(contracts),
        deadlineAtMs,
      );
      return;
    }

    for (const contract of contracts) {
      await this.publishOneWithDeadline(contract, deadlineAtMs);
    }
  }

  private async publishOneWithDeadline(
    contract: SourceReplacementContract,
    deadlineAtMs: number,
  ): Promise<void> {
    const publishOne = this.env.SOURCE_PUBLISHER.publishSourceReplacement;

    if (typeof publishOne !== "function") {
      throw new SourcePublisherError("publisher_method_missing");
    }

    await this.callPublisherWithDeadline(
      () => this.env.SOURCE_PUBLISHER.publishSourceReplacement!(contract),
      deadlineAtMs,
    );
  }

  private async callPublisherWithDeadline(
    operation: () => Promise<void>,
    deadlineAtMs: number,
  ): Promise<void> {
    const remainingMs = deadlineAtMs - Date.now();

    if (remainingMs <= 0) {
      throw new SourcePublisherError("publisher_deadline_exceeded");
    }

    const timeoutMs = Math.min(remainingMs, PUBLISH_CALL_MS);
    let timer: ReturnType<typeof setTimeout> | null = null;
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(
        () => reject(new SourcePublisherError("publisher_deadline_exceeded")),
        timeoutMs,
      );
    });

    try {
      await Promise.race([
        operation(),
        timeout,
      ]);
    } catch (error) {
      if (error instanceof SourcePublisherError) {
        throw error;
      }

      throw new SourcePublisherError("publisher_call_failed", error);
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
    }
  }

  private async scheduleAlarmWithin(delayMs: number): Promise<void> {
    const requestedAt = Date.now() + delayMs;
    const current = await this.ctx.storage.getAlarm();

    if (current !== null && current <= requestedAt) {
      return;
    }

    await this.ctx.storage.setAlarm(requestedAt);
  }

  private sliceBudgetMs(): number {
    const configured = this.env.SOURCE_SLICE_BUDGET_MS;

    if (!configured) {
      return SLICE_BUDGET_MS;
    }

    const value = Number(configured);
    if (!Number.isSafeInteger(value) || value < 1 || value > SLICE_BUDGET_MS) {
      throw new TypeError("SOURCE_SLICE_BUDGET_MS must be from 1 through 55000");
    }

    return value;
  }

}

class SourcePublisherError extends Error {
  readonly code: string;

  constructor(code: string, cause?: unknown) {
    super("source publisher failed", { cause });
    this.name = "SourcePublisherError";
    this.code = code;
  }
}

function classifyError(error: unknown): { code: string; status: number | null } {
  if (error instanceof ProviderReadError || hasErrorName(error, "ProviderReadError")) {
    const providerError = error as {
      code?: unknown;
      status?: unknown;
    };
    const code =
      typeof providerError.code === "string"
        ? providerError.code
        : "provider_read_failed";

    return {
      code: `provider_${code}`,
      status: Number.isSafeInteger(providerError.status)
        ? (providerError.status as number)
        : null,
    };
  }

  if (
    error instanceof SourcePublisherError ||
    hasErrorName(error, "SourcePublisherError")
  ) {
    const code = (error as { code?: unknown }).code;
    return {
      code: typeof code === "string" ? code : "publisher_call_failed",
      status: null,
    };
  }

  if (error instanceof TypeError) {
    return { code: "invalid_source_response", status: null };
  }

  return { code: "source_slice_failed", status: null };
}

function hasErrorName(error: unknown, name: string): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  return (error as { name?: unknown }).name === name;
}

function requireBackfillId(value: string): string {
  const id = requireNonEmptyString(value, "backfillId");

  if (id.length > 128 || !/^[A-Za-z0-9._:-]+$/.test(id)) {
    throw new TypeError("backfillId contains unsupported characters");
  }

  return id;
}

function requireStripeCreatedGte(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError("stripeCreatedGte must be non-negative Unix seconds");
  }

  return value;
}

function requireNonEmptyString(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }

  return value.trim();
}

function toIsoOrNull(value: number | null): string | null {
  return value === null ? null : new Date(value).toISOString();
}
