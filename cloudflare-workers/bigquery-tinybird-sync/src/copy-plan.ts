export type PublicationKind = "bootstrap" | "recurring";

export type PublicationSection = "pre_identity" | "identity" | "post_identity";

export type IdentityPhase =
  | "enqueue"
  | "compute"
  | "validate"
  | "activate"
  | "journey";

export interface IdentityEnqueueBatch {
  producerId: string;
  usesGenerationOverlapCutoff: boolean;
}

export interface CopyPlanStep {
  pipeName: string;
  parameters: Readonly<Record<string, string>>;
}

export const IDENTITY_ENQUEUE_BATCHES = [
  {
    producerId: "source_identity:segment_identify",
    usesGenerationOverlapCutoff: true,
  },
  {
    producerId: "source_identity:segment_form",
    usesGenerationOverlapCutoff: true,
  },
  {
    producerId: "source_identity:segment_order_completed",
    usesGenerationOverlapCutoff: true,
  },
  {
    producerId: "source_identity:segment_page_view",
    usesGenerationOverlapCutoff: true,
  },
  {
    producerId: "source_identity:segment_attribution",
    usesGenerationOverlapCutoff: true,
  },
  {
    producerId: "source_identity:activecampaign",
    usesGenerationOverlapCutoff: true,
  },
  {
    producerId: "source_identity:stripe",
    usesGenerationOverlapCutoff: true,
  },
  {
    producerId: "source_identity:stripe_kajabi",
    usesGenerationOverlapCutoff: true,
  },
] as const satisfies readonly IdentityEnqueueBatch[];

export const BOOTSTRAP_IDENTITY_ENQUEUE_BATCHES =
  IDENTITY_ENQUEUE_BATCHES.filter(
    // The immutable bootstrap seed already contains every ActiveCampaign version.
    ({ producerId }) => producerId !== "source_identity:activecampaign",
  );

export function identityEnqueueBatchesForKind(
  kind: PublicationKind,
): readonly IdentityEnqueueBatch[] {
  if (kind === "bootstrap") return BOOTSTRAP_IDENTITY_ENQUEUE_BATCHES;
  return IDENTITY_ENQUEUE_BATCHES;
}

export const BOOTSTRAP_PAGE_VIEW_COPIES = [
  pageViewBackfill("boom_domains", "2026-06-22", "2026-06-24"),
  pageViewBackfill("boom_domains", "2026-06-24", "2026-06-26"),
] as const satisfies readonly CopyPlanStep[];

export const BOOTSTRAP_PRE_IDENTITY_COPIES = [
  copyStep("backfill_activecampaign_contacts_current"),
  copyStep("backfill_activecampaign_tags_current"),
  copyStep("backfill_activecampaign_tag_semantic_versions"),
  copyStep("backfill_activecampaign_contact_tags_current"),
  copyStep("snapshot_activecampaign_registrations"),
  copyStep("backfill_jitsu_attribution_param_versions"),
  copyStep("backfill_jitsu_form_submission_versions"),
  copyStep("backfill_jitsu_identify_versions"),
  copyStep("backfill_jitsu_order_completed_versions"),
  ...BOOTSTRAP_PAGE_VIEW_COPIES,
  copyStep("snapshot_all_stripe_payments"),
] as const satisfies readonly CopyPlanStep[];

export const BOOTSTRAP_POST_IDENTITY_COPIES = [] as const;

export const RECURRING_PRE_IDENTITY_COPIES = [] as const;

export const RECURRING_POST_IDENTITY_COPIES = [] as const;

export const RETIRED_REPORTING_COPIES = [
  copyStep("migrate_mart_touchpoints_all_facts_v3"),
  copyStep("migrate_reporting_conversion_facts_v2"),
  copyStep("seed_mart_form_submissions_server_side_facts"),
  copyStep("seed_mart_touchpoints_all_facts"),
  copyStep("seed_reporting_conversion_identity_base"),
  copyStep("seed_reporting_identity_facts"),
  copyStep("snapshot_int_stripe_browser_product_resolution"),
  copyStep("snapshot_mart_ad_performance"),
  copyStep("snapshot_mart_form_submissions_client_side"),
  copyStep("snapshot_mart_form_submissions_server_side"),
  copyStep("snapshot_mart_manual_attribution_profile_search_candidates"),
  copyStep("snapshot_mart_payments"),
  copyStep("snapshot_mart_touchpoints_all"),
  copyStep("snapshot_mart_touchpoints_all_facts"),
  copyStep("snapshot_reporting_latest_ad_names"),
  copyStep("snapshot_mart_payments_client_side"),
  copyStep("snapshot_int_payment_plan_timing"),
  copyStep("snapshot_segretl_repeatable_conversions"),
] as const satisfies readonly CopyPlanStep[];

export const RETIRED_IDENTITY_COMPACTION_COPIES = [
  "prepare_identity_compaction",
  "classify_identity_compaction_changes",
  "seed_identity_compaction_profiles",
  "freeze_identity_compaction_touched_profiles",
  "scope_identity_compaction",
  "select_identity_compaction_fact_keys",
  "expand_identity_compaction_facts",
  "build_identity_compaction_components",
  "compact_identity_state",
] as const;

export const RETIRED_IDENTITY_ENQUEUE_COPIES = [
  "enqueue_identity_changes",
] as const;

export const SHADOW_REBUILD_COPIES = [
  "snapshot_identity_rebuild_manifest",
  "snapshot_identity_rebuild_facts",
  "initialize_identity_rebuild_labels",
  "propagate_identity_rebuild_labels",
  "snapshot_identity_rebuild_commit",
  "snapshot_identity_rebuild_components",
  "stage_identity_rebuild_mappings",
  "stage_identity_rebuild_profiles",
  "stage_identity_rebuild_facts",
  "snapshot_identity_rebuild_ready_commit",
  "commit_identity_rebuild",
] as const;

export const RETIRED_IDENTITY_COPIES = [
  "seed_identity_state",
  "snapshot_live_pending_identity_resolution",
  "snapshot_pending_identity_events",
  "snapshot_pending_identity_components",
  ...RETIRED_IDENTITY_ENQUEUE_COPIES,
  ...RETIRED_IDENTITY_COMPACTION_COPIES,
  ...SHADOW_REBUILD_COPIES,
] as const;

export const AUTOMATED_COPY_PIPES = unique([
  ...BOOTSTRAP_PRE_IDENTITY_COPIES.map(({ pipeName }) => pipeName),
  ...BOOTSTRAP_POST_IDENTITY_COPIES.map(({ pipeName }) => pipeName),
  ...RECURRING_PRE_IDENTITY_COPIES.map(({ pipeName }) => pipeName),
  ...RECURRING_POST_IDENTITY_COPIES.map(({ pipeName }) => pipeName),
]);

export function copiesForSection(
  kind: PublicationKind,
  section: Exclude<PublicationSection, "identity">,
): readonly CopyPlanStep[] {
  if (kind === "bootstrap" && section === "pre_identity") {
    return BOOTSTRAP_PRE_IDENTITY_COPIES;
  }

  if (kind === "bootstrap") return BOOTSTRAP_POST_IDENTITY_COPIES;
  if (section === "pre_identity") return RECURRING_PRE_IDENTITY_COPIES;
  return RECURRING_POST_IDENTITY_COPIES;
}

function unique(values: readonly string[]): readonly string[] {
  return [...new Set(values)];
}

function copyStep(pipeName: string): CopyPlanStep {
  return { pipeName, parameters: {} };
}

function pageViewBackfill(
  sourceSystem: "boom_domains",
  start: string,
  end: string,
): CopyPlanStep {
  return {
    pipeName: "backfill_jitsu_page_view_versions",
    parameters: {
      p_source_system: sourceSystem,
      p_start: `${start} 00:00:00`,
      p_end: `${end} 00:00:00`,
    },
  };
}
