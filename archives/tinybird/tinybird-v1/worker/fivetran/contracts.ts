import type { PendingIdentityFact } from "../identity/engine.ts";

export type JsonRecord = Record<string, unknown>;

export type PipelineId =
  | "stripe_main"
  | "stripe_kajabi"
  | "activecampaign";

export type StripeAccount = "main" | "kajabi";

export type ConversionSourceTable =
  | "raw_stripe_charge"
  | "raw_stripe_customer"
  | "raw_stripe_payment_intent"
  | "raw_stripe_kajabi_charge"
  | "raw_stripe_kajabi_customer"
  | "raw_stripe_kajabi_payment_intent"
  | "raw_activecampaign_contact"
  | "raw_activecampaign_contact_tag"
  | "raw_activecampaign_tags";

export const PIPELINE_TABLES = {
  stripe_main: [
    "raw_stripe_charge",
    "raw_stripe_customer",
    "raw_stripe_payment_intent",
  ],
  stripe_kajabi: [
    "raw_stripe_kajabi_charge",
    "raw_stripe_kajabi_customer",
    "raw_stripe_kajabi_payment_intent",
  ],
  activecampaign: [
    "raw_activecampaign_contact",
    "raw_activecampaign_contact_tag",
    "raw_activecampaign_tags",
  ],
} as const satisfies Readonly<
  Record<PipelineId, readonly ConversionSourceTable[]>
>;

export interface SourceReplacement {
  source: "stripe" | "activecampaign";
  source_account: "main" | "kajabi" | "default";
  scope_id: string;
  replacement_id: string;
  observed_at: string;
  observation_sequence: number;
  rows: JsonRecord[];
  evidence_inbox_ids: string[];
  source_evidence: JsonRecord;
}

export interface SourceReplacementPublisher {
  publishSourceReplacements(replacements: SourceReplacement[]): Promise<void>;
}

/** Bootstrap writes source records and commits without calling the live identity queue. */
export interface BootstrapSourceReplacementPublisher {
  publishBootstrapSourceReplacements(input: {
    bootstrapId: string;
    replacements: SourceReplacement[];
  }): Promise<void>;
}

export interface BootstrapIdentityManifest {
  snapshotId: string;
  snapshotAt: string;
  expectedDistinctFactCount: number;
  /** SHA-256 over canonical facts sorted by `(factKind, factKey)`. */
  canonicalHash: string;
  sealedAt: string;
}

export interface BootstrapIdentitySnapshotSealer {
  sealBootstrapIdentitySnapshot(input: {
    snapshotId: string;
    snapshotAt: string;
    expectedDistinctFactCount: number;
    sealedAt: string;
  }): Promise<BootstrapIdentityManifest>;
}

export interface BulkBootstrapCheckpoint {
  snapshotId: string;
  snapshotAt: string;
  pipeline: PipelineId;
  afterCursor: string;
  complete: boolean;
  publishedScopes: number;
  identityFactCount: number;
}

export interface BulkBootstrapCheckpointStore {
  loadBulkBootstrapCheckpoint(input: {
    snapshotId: string;
    pipeline: PipelineId;
  }): Promise<BulkBootstrapCheckpoint | null>;

  saveBulkBootstrapCheckpoint(checkpoint: BulkBootstrapCheckpoint): Promise<void>;
}

export interface BulkBootstrapStateSeeder {
  initializePipeline(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void>;

  seedBulkBootstrapScopes(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    prepared: PreparedScope[];
  }): Promise<void>;

  finalizeBulkBootstrap(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void>;
}

export interface ChangeWindow {
  id: string;
  pipeline: PipelineId;
  afterExclusive: string;
  throughInclusive: string;
}

export interface DiscoveryPage {
  /** Fully expanded replacement scopes, not raw dependency IDs. */
  scopeIds: string[];
  nextCursor: string;
  eof: boolean;
}

export interface BootstrapScopePage {
  scopeIds: string[];
  nextCursor: string;
  eof: boolean;
}

export interface StripeRawScope {
  account: StripeAccount;
  chargeId: string;
  chargeVersions: JsonRecord[];
  customerVersions: JsonRecord[];
  paymentIntentVersions: JsonRecord[];
  evidenceRecordIds: string[];
}

export interface ActiveCampaignRawScope {
  contactId: string;
  contactVersions: JsonRecord[];
  assignmentVersions: JsonRecord[];
  referencedTagVersions: JsonRecord[];
  evidenceRecordIds: string[];
}

export interface FivetranFactReader {
  /**
   * Read the globally verified nine-table transport barrier. Completeness is
   * taken from its receipt, never inferred from source timestamps.
   */
  verifiedObservationThrough(
    requiredTables: readonly ConversionSourceTable[],
  ): Promise<string>;

  /**
   * Read changed replacement scopes for one dependency. The reader expands
   * customer/payment-intent IDs to charges and tag IDs to contacts.
   */
  readChangedScopePage(input: {
    pipeline: PipelineId;
    table: ConversionSourceTable;
    afterExclusive: string;
    throughInclusive: string;
    afterCursor: string;
    limit: number;
  }): Promise<DiscoveryPage>;

  /**
   * Read every scope that can emit a conversion from the immutable snapshot.
   * Raw tables remain the proof for non-qualifying source rows.
   */
  readBootstrapScopePage(input: {
    pipeline: PipelineId;
    snapshotAt: string;
    afterCursor: string;
    limit: number;
  }): Promise<BootstrapScopePage>;

  /** Hydrate a bounded charge batch in a constant number of Tinybird queries. */
  readStripeScopes(input: {
    account: StripeAccount;
    chargeIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<StripeRawScope[]>;

  /** Hydrate a bounded contact batch in a constant number of Tinybird queries. */
  readActiveCampaignScopes(input: {
    contactIds: string[];
    throughInclusive: string;
    snapshotOnly: boolean;
  }): Promise<ActiveCampaignRawScope[]>;
}

export interface ScopeState {
  rows: JsonRecord[];
  compactState: JsonRecord;
  observationSequence: number;
}

export interface PreparedScope {
  windowId: string;
  scopeId: string;
  inputHash: string;
  replacement: SourceReplacement;
  compactState: JsonRecord;
}

export interface PipelineState {
  pipeline: PipelineId;
  snapshotAt: string | null;
  bootstrapComplete: boolean;
  completedObservationAt: string | null;
  activeWindow: ChangeWindow | null;
}

/**
 * All page, reservation, and commit methods must be atomic transactions.
 * A production implementation is supplied by `SqliteFactCoordinatorStore`.
 */
export interface FactCoordinatorStore {
  initializePipeline(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<void>;

  getPipeline(pipeline: PipelineId): Promise<PipelineState>;

  beginBootstrap(input: {
    pipeline: PipelineId;
    snapshotAt: string;
  }): Promise<ChangeWindow>;

  beginIncrementalWindow(input: {
    pipeline: PipelineId;
    afterExclusive: string;
    throughInclusive: string;
  }): Promise<ChangeWindow>;

  getDiscovery(input: {
    windowId: string;
    table: ConversionSourceTable | "bootstrap_scopes";
  }): Promise<{ cursor: string; eof: boolean }>;

  recordDiscoveryPage(input: {
    window: ChangeWindow;
    table: ConversionSourceTable | "bootstrap_scopes";
    expectedCursor: string;
    page: DiscoveryPage;
  }): Promise<void>;

  listUnpreparedScopes(windowId: string, limit: number): Promise<string[]>;

  getScopeState(input: {
    pipeline: PipelineId;
    scopeId: string;
  }): Promise<ScopeState | null>;

  /** Returns the same sequence on retry, including after a process restart. */
  reserveScope(input: {
    windowId: string;
    scopeId: string;
  }): Promise<number>;

  savePreparedScope(prepared: PreparedScope): Promise<void>;

  listPreparedScopes(windowId: string, limit: number): Promise<PreparedScope[]>;

  /** Atomically installs current state and discards published payload copies. */
  markPublished(prepared: PreparedScope[]): Promise<void>;

  finishWindow(window: ChangeWindow): Promise<void>;
}

export interface StripeChargeNormalizer {
  (input: JsonRecord): JsonRecord;
}

export interface ActiveCampaignReplacementNormalizer {
  (input: {
    contact: JsonRecord;
    assignments: JsonRecord[];
    tags: JsonRecord[];
    previousFacts: JsonRecord[];
    replacementVersion: string;
    replacementId: string;
  }): {
    replacement_scope_id: string;
    rows: JsonRecord[];
  };
}

export interface FactBuilders {
  stripeNormalizer: StripeChargeNormalizer;
  activeCampaignNormalizer: ActiveCampaignReplacementNormalizer;
  stripeIdentityProjector?: (raw: StripeRawScope, observedAt: string) => Promise<PendingIdentityFact[]>;
  activeCampaignIdentityProjector?: (raw: ActiveCampaignRawScope, observedAt: string) => Promise<PendingIdentityFact[]>;
  replaceIdentityScope?: (current: PendingIdentityFact[], previous: PendingIdentityFact[], observedAt: string) => Promise<PendingIdentityFact[]>;
}
