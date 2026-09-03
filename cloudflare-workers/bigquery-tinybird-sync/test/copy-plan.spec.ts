import { readdirSync } from "node:fs";
import { URL as NodeUrl, fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  AUTOMATED_COPY_PIPES,
  BOOTSTRAP_IDENTITY_ENQUEUE_BATCHES,
  BOOTSTRAP_PAGE_VIEW_COPIES,
  BOOTSTRAP_POST_IDENTITY_COPIES,
  BOOTSTRAP_PRE_IDENTITY_COPIES,
  IDENTITY_ENQUEUE_BATCHES,
  RECURRING_POST_IDENTITY_COPIES,
  RECURRING_PRE_IDENTITY_COPIES,
  RETIRED_REPORTING_COPIES,
  RETIRED_IDENTITY_ENQUEUE_COPIES,
  RETIRED_IDENTITY_COPIES,
  RETIRED_IDENTITY_COMPACTION_COPIES,
  SHADOW_REBUILD_COPIES,
  identityEnqueueBatchesForKind,
} from "../src/copy-plan";

describe("Copy publication plans", () => {
  it("encodes the exact serial bootstrap order", () => {
    expect(pipeNames(BOOTSTRAP_PRE_IDENTITY_COPIES.slice(0, 9))).toEqual([
      "backfill_activecampaign_contacts_current",
      "backfill_activecampaign_tags_current",
      "backfill_activecampaign_tag_semantic_versions",
      "backfill_activecampaign_contact_tags_current",
      "snapshot_activecampaign_registrations",
      "backfill_jitsu_attribution_param_versions",
      "backfill_jitsu_form_submission_versions",
      "backfill_jitsu_identify_versions",
      "backfill_jitsu_order_completed_versions",
    ]);
    expect(BOOTSTRAP_PRE_IDENTITY_COPIES[9]).toEqual(
      BOOTSTRAP_PAGE_VIEW_COPIES[0],
    );
    expect(BOOTSTRAP_PRE_IDENTITY_COPIES[10]).toEqual(
      BOOTSTRAP_PAGE_VIEW_COPIES[1],
    );
    expect(BOOTSTRAP_PRE_IDENTITY_COPIES[11]).toEqual({
      pipeName: "snapshot_all_stripe_payments",
      parameters: {},
    });
    expect(BOOTSTRAP_PRE_IDENTITY_COPIES.at(-1)).toEqual({
      pipeName: "snapshot_all_stripe_payments",
      parameters: {},
    });
    expect(BOOTSTRAP_POST_IDENTITY_COPIES).toEqual([]);
  });

  it("keeps only the two completed page-view windows", () => {
    expect(BOOTSTRAP_PAGE_VIEW_COPIES).toEqual([
      {
        pipeName: "backfill_jitsu_page_view_versions",
        parameters: {
          p_source_system: "boom_domains",
          p_start: "2026-06-22 00:00:00",
          p_end: "2026-06-24 00:00:00",
        },
      },
      {
        pipeName: "backfill_jitsu_page_view_versions",
        parameters: {
          p_source_system: "boom_domains",
          p_start: "2026-06-24 00:00:00",
          p_end: "2026-06-26 00:00:00",
        },
      },
    ]);
  });

  it("encodes the recurring dependency order around identity maintenance", () => {
    expect(RECURRING_PRE_IDENTITY_COPIES).toEqual([]);
    expect(RECURRING_POST_IDENTITY_COPIES).toEqual([]);
  });

  it("keeps every retired identity Copy outside automatic execution", () => {
    expect(AUTOMATED_COPY_PIPES).toHaveLength(11);
    expect(SHADOW_REBUILD_COPIES).toHaveLength(11);
    expect(RETIRED_IDENTITY_ENQUEUE_COPIES).toEqual(["enqueue_identity_changes"]);
    expect(RETIRED_IDENTITY_COMPACTION_COPIES).toHaveLength(9);
    expect(RETIRED_IDENTITY_COPIES).toHaveLength(25);
    expect(new Set([
      ...AUTOMATED_COPY_PIPES,
      ...RETIRED_IDENTITY_COPIES,
      ...RETIRED_REPORTING_COPIES.map(({ pipeName }) => pipeName),
    ]).size).toBe(54);

    for (const pipeName of RETIRED_IDENTITY_COPIES) {
      expect(AUTOMATED_COPY_PIPES).not.toContain(pipeName);
    }
  });

  it("classifies every checked-in Tinybird Copy definition", () => {
    const copiesDirectory = fileURLToPath(new NodeUrl(
      "../../../tinybird-production/copies",
      import.meta.url,
    ));
    const checkedInCopies = readdirSync(copiesDirectory, {
      recursive: true,
      withFileTypes: true,
    })
      .filter((entry) => entry.isFile() && entry.name.endsWith(".pipe"))
      .map((entry) => entry.name.replace(/\.pipe$/, ""))
      .sort();
    const classifiedCopies = [
      ...AUTOMATED_COPY_PIPES,
      ...RETIRED_IDENTITY_ENQUEUE_COPIES,
      ...RETIRED_IDENTITY_COMPACTION_COPIES,
      ...RETIRED_REPORTING_COPIES.map(({ pipeName }) => pipeName),
    ].sort();

    expect(classifiedCopies).toEqual(checkedInCopies);
  });

  it("keeps graph compaction out of the automatic Copy plan", () => {
    expect(RETIRED_IDENTITY_ENQUEUE_COPIES).toContain("enqueue_identity_changes");
    expect(RETIRED_IDENTITY_COMPACTION_COPIES).toContain("compact_identity_state");
    expect(RETIRED_IDENTITY_COPIES).toContain("snapshot_pending_identity_components");
    expect(RETIRED_IDENTITY_COPIES).toContain("snapshot_live_pending_identity_resolution");
  });

  it("keeps all eight producers in recurring identity enqueue", () => {
    const recurringBatches = identityEnqueueBatchesForKind("recurring");

    expect(recurringBatches).toBe(IDENTITY_ENQUEUE_BATCHES);
    expect(recurringBatches.map((batch) => batch.producerId)).toEqual([
      "source_identity:segment_identify",
      "source_identity:segment_form",
      "source_identity:segment_order_completed",
      "source_identity:segment_page_view",
      "source_identity:segment_attribution",
      "source_identity:activecampaign",
      "source_identity:stripe",
      "source_identity:stripe_kajabi",
    ]);
    expect(recurringBatches.map((batch) => (
      batch.usesGenerationOverlapCutoff
    ))).toEqual([
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
    ]);
  });

  it("skips only the already-seeded ActiveCampaign producer during bootstrap", () => {
    const bootstrapBatches = identityEnqueueBatchesForKind("bootstrap");

    expect(bootstrapBatches).toBe(BOOTSTRAP_IDENTITY_ENQUEUE_BATCHES);
    expect(bootstrapBatches.map((batch) => batch.producerId)).toEqual([
      "source_identity:segment_identify",
      "source_identity:segment_form",
      "source_identity:segment_order_completed",
      "source_identity:segment_page_view",
      "source_identity:segment_attribution",
      "source_identity:stripe",
      "source_identity:stripe_kajabi",
    ]);
    expect(bootstrapBatches[5]?.producerId).toBe("source_identity:stripe");
  });
});

function pipeNames(steps: readonly { pipeName: string }[]): string[] {
  return steps.map(({ pipeName }) => pipeName);
}
