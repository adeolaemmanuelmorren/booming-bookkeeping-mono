import type { WorkerEnv } from "../src/sync";
import type {
  TinybirdSyncGate,
  TinybirdSyncRequest,
  TinybirdGateResult,
} from "../src/tinybird-gate";
import type {
  CoordinatorStatus,
  EnqueueRawRunInput,
  EnqueueRawRunResult,
  PublicationCoordinator,
} from "../src/publication-coordinator";
import type {
  JourneyCoordinator,
  JourneyCoordinatorStatus,
} from "../src/journey-coordinator";
import type {
  ReportingFactsCoordinator,
  ReportingFactsStatus,
} from "../src/reporting-facts-coordinator";

export function testEnv(overrides: Partial<WorkerEnv> = {}): WorkerEnv {
  return {
    BIGQUERY_PROJECT_ID: "able-folio-499722",
    BIGQUERY_LOCATION: "US",
    GCS_BUCKET: "booming-data",
    GCS_PREFIX: "tinybird",
    TINYBIRD_API_URL: "https://api.us-east.tinybird.co",
    TINYBIRD_FETCH_TIMEOUT_MS: "50",
    TINYBIRD_GATE_TIMEOUT_MS: "500",
    COPY_POLL_INTERVAL_MS: "5",
    COPY_JOB_TIMEOUT_MS: "500",
    IDENTITY_BOOTSTRAP_SOURCE_INGESTED_FROM: "2026-08-26 23:05:00",
    EXPORT_OVERLAP_MINUTES: "20",
    BIGQUERY_POLL_INTERVAL_MS: "5",
    BIGQUERY_JOB_TIMEOUT_MS: "50",
    GCP_SERVICE_ACCOUNT_JSON: "test-service-account-json",
    TINYBIRD_ADMIN_TOKEN: "test-tinybird-admin-token",
    SYNC_ADMIN_TOKEN: "test-admin-token",
    TINYBIRD_SYNC_GATE: testTinybirdGate(),
    PUBLICATION_COORDINATOR: testPublicationCoordinator(),
    JOURNEY_COORDINATOR: testJourneyCoordinator(),
    REPORTING_FACTS_COORDINATOR: testReportingFactsCoordinator(),
    ...overrides,
  };
}

export function testJourneyCoordinator(): DurableObjectNamespace<JourneyCoordinator> {
  const status: JourneyCoordinatorStatus = {
    phase: "idle",
    cursorBatchVersion: 0,
    cursorBatchId: "",
    activeIdentityBatchId: null,
    activeRepairId: null,
    pendingRepairBatches: 0,
    completedRepairBatches: 0,
    completedConversions: 0,
    totalConversions: 0,
    completedOrphanKeys: 0,
    totalOrphanKeys: 0,
    completedProfiles: 0,
    totalProfiles: 0,
    nextAttemptAt: null,
    lastError: null,
  };
  return {
    getByName: () => ({
      tick: async () => status,
      status: async () => status,
      recoverFailed: async () => status,
      enqueueRepair: async () => status,
    }),
  } as unknown as DurableObjectNamespace<JourneyCoordinator>;
}

export function testReportingFactsCoordinator(): DurableObjectNamespace<ReportingFactsCoordinator> {
  const status: ReportingFactsStatus = {
    phase: "idle",
    touchpointCursor: "2026-08-26 23:05:00.000000",
    conversionCursor: "2026-08-26 23:05:00.000000",
    serverConversionCursor: "2026-08-26 23:05:00.000000",
    activeStream: null,
    completedItems: 0,
    totalItems: 0,
    pendingRepairAnchors: 0,
    pendingRepairConversions: 0,
    nextAttemptAt: null,
    lastError: null,
  };
  return {
    getByName: () => ({
      tick: async () => status,
      status: async () => status,
      recoverFailed: async () => status,
    }),
  } as unknown as DurableObjectNamespace<ReportingFactsCoordinator>;
}

export function testPublicationCoordinator(overrides: Partial<{
  enqueueRawRun: (
    input: EnqueueRawRunInput,
  ) => Promise<EnqueueRawRunResult>;
  status: () => Promise<CoordinatorStatus>;
}> = {}): DurableObjectNamespace<PublicationCoordinator> {
  const enqueueRawRun = overrides.enqueueRawRun ?? (async (input) => ({
    accepted: true,
    requestKey: "raw_20260826123400000_slot_4",
    status: "queued",
    slot: input.slot ?? 4,
    scheduledAt: input.scheduledAt,
  }));

  return {
    getByName: () => ({
      enqueueRawRun,
      status: overrides.status ?? (async () => testCoordinatorStatus()),
      setOperatorPaused: async () => testCoordinatorStatus(),
      pauseAfterPublication: async () => testCoordinatorStatus(),
      configureBootstrap: async () => testCoordinatorStatus(),
      startJourneyBackfill: async () => testCoordinatorStatus(),
      recoverFailed: async () => testCoordinatorStatus(),
    }),
  } as unknown as DurableObjectNamespace<PublicationCoordinator>;
}

function testCoordinatorStatus(): CoordinatorStatus {
  return {
    operatorPaused: false,
    operatorPauseAfterPublication: false,
    phase: "collecting_raw",
    bootstrapStatus: "complete",
    activeRawGeneration: null,
    completedRawSlots: [],
    queuedRawRuns: 0,
    publicationKind: null,
    publicationSection: null,
    publicationIndex: 0,
    identityPhase: "enqueue",
    identityEnqueue: null,
    identityBatch: null,
    journeyBatch: null,
    journeyBackfill: {
      status: "not_requested",
      afterProfileId: "",
      completedProfiles: 0,
      completedBatches: 0,
    },
    activeCopyPipe: null,
    activeCopyJobId: null,
    activeCopyParameters: {},
    lastError: null,
    latestPublication: null,
  };
}

export function testTinybirdGate(
  trigger: (
    request: TinybirdSyncRequest,
  ) => Promise<TinybirdGateResult> = async () => ({
    outcome: "synced",
    tinybirdStatus: 202,
  }),
): DurableObjectNamespace<TinybirdSyncGate> {
  return {
    getByName: () => ({ trigger }),
  } as unknown as DurableObjectNamespace<TinybirdSyncGate>;
}
