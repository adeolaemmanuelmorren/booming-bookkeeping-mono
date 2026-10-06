import type { SourceCoordinator } from "./source-coordinator.ts";

export type StripeAccount = "main" | "kajabi";

export type SourceRoute =
  | { source: "stripe"; account: StripeAccount }
  | { source: "activecampaign"; account: "default" };

export interface ProviderRpcResponse {
  status: number;
  retryAfter: string | null;
  body: unknown;
}

export interface StripeSourceBinding {
  read(
    account: "stripe" | "stripe_kajabi",
    path: string,
    parameters?: Record<string, string>,
  ): Promise<ProviderRpcResponse>;
}

export interface ActiveCampaignSourceBinding {
  read(
    path: string,
    parameters?: Record<string, string>,
  ): Promise<ProviderRpcResponse>;
}

export interface SourceReplacementContract {
  source: string;
  source_account: string;
  scope_id: string;
  replacement_id: string;
  observed_at: string;
  observation_sequence: number;
  rows: unknown[];
  evidence_inbox_ids: string[];
  source_evidence: Record<string, unknown>;
}

export interface SourcePublisherBinding {
  publishSourceReplacements?(
    contracts: SourceReplacementContract[],
  ): Promise<void>;
  publishSourceReplacement?(
    contract: SourceReplacementContract,
  ): Promise<void>;
}

export interface Env {
  SOURCE_COORDINATOR: DurableObjectNamespace<SourceCoordinator>;
  STRIPE_SOURCE: StripeSourceBinding;
  ACTIVECAMPAIGN_SOURCE: ActiveCampaignSourceBinding;
  SOURCE_PUBLISHER: SourcePublisherBinding;
  STRIPE_EXTENDED_OBJECT_PATHS?: string;
  SOURCE_SLICE_BUDGET_MS?: string;
}

export interface SourceCoordinatorRpc {
  startBackfill(
    route: SourceRoute,
    options?: StartBackfillOptions,
  ): Promise<CoordinatorStatus>;
  wake(route: SourceRoute): Promise<CoordinatorStatus>;
  status(route: SourceRoute): Promise<CoordinatorStatus>;
}

export interface StartBackfillOptions {
  backfillId?: string;
  stripeCreatedGte?: number;
}

export interface CoordinatorStatus {
  route: SourceRoute;
  backfill: {
    id: string;
    stripeCreatedGte: number;
    startedAt: string | null;
  };
  lease: {
    active: boolean;
    expiresAt: string | null;
  };
  work: {
    pendingInbox: number;
    outboxedInbox: number;
    processedInbox: number;
    pendingOutbox: number;
    publishedOutbox: number;
    cursorCount: number;
    currentScopeCount: number;
    currentRowCount: number;
    immutableVersionCount: number;
  };
  storage: {
    databaseBytes: number;
    storedJsonBytes: number;
  };
  lastSlice: {
    startedAt: string | null;
    succeededAt: string | null;
    errorCode: string | null;
    errorStatus: number | null;
  };
  nextAlarmAt: string | null;
}
