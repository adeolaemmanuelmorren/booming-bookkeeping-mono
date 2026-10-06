import { describe, expect, it } from "vitest";
import type { SourceTable } from "../src/table-manifest.generated";
import { SOURCE_NAMES, SOURCE_TABLES, landingName } from "../src/sources";
import {
  buildExportPlan,
  buildBridgeExportPlan,
  buildSnapshotExportPlan,
  checkBigQueryExport,
  nextCatchupTarget,
} from "../src/bigquery";

describe("V1 Fivetran source contract", () => {
  it("contains exactly the nine conversion dependencies", () => {
    expect(SOURCE_NAMES).toHaveLength(9);
    expect(SOURCE_TABLES).toHaveLength(9);
    expect(new Set(SOURCE_NAMES).size).toBe(9);
    expect(SOURCE_NAMES.some((name) => name.includes("report"))).toBe(false);
    expect(SOURCE_NAMES.some((name) => name.includes("attribution"))).toBe(
      false,
    );
  });

  it("uses separate mechanical live landing names", () => {
    expect(landingName("raw_activecampaign_contact")).toBe(
      "v1_fivetran_activecampaign_contact",
    );
    expect(
      SOURCE_NAMES.map(landingName).every((name) =>
        name.startsWith("v1_fivetran_"),
      ),
    ).toBe(true);
  });

  it("preserves Fivetran deletion and sync metadata where supplied", () => {
    const activeCampaign = SOURCE_TABLES.filter((table) =>
      table.resourceName.startsWith("raw_activecampaign_"),
    );
    expect(activeCampaign).toHaveLength(3);
    expect(
      activeCampaign.every((table) =>
        table.columns.includes("_fivetran_synced"),
      ),
    ).toBe(true);
    expect(
      activeCampaign.every((table) =>
        table.columns.includes("_fivetran_deleted"),
      ),
    ).toBe(true);
  });

  it("builds the common fixed-time snapshot without a watermark filter", () => {
    const snapshot = buildSnapshotExportPlan(
      SOURCE_TABLES[0],
      new Date("2026-09-05T22:30:00.000Z"),
      { bucket: "booming-data", prefix: "tinybird/v1-live", overlapMinutes: 2 },
    );

    expect(snapshot.query).toContain(
      "FOR SYSTEM_TIME AS OF TIMESTAMP '2026-09-05 22:30:00.000+00'",
    );
    expect(snapshot.query).not.toContain("_fivetran_synced` >=");
    expect(snapshot.uri).toContain("/snapshot-2026090522300000000-*.parquet");
    expect(snapshot.uri).toContain("/raw_activecampaign_contact/");
  });

  it("exports one cutoff state for each id changed in the closed window", () => {
    for (const table of SOURCE_TABLES) {
      const plan = buildExportPlan(
        table,
        new Date("2026-09-05T22:40:00.000Z"),
        {
          bucket: "booming-data",
          prefix: "tinybird/v1-live",
          overlapMinutes: 20,
          changeHistory: historyGuard(),
        },
      );

      expect(plan.query).toContain(".`_fivetran_synced` AS `_fivetran_synced`");
      expect(plan.query).toContain("FROM CHANGES(TABLE");
      expect(plan.query).toContain("SELECT DISTINCT `id`");
      expect(plan.query).toContain("FOR SYSTEM_TIME AS OF");
      expect(plan.query).toContain("FALSE AS `_v1_deleted`");
      expect(plan.query).toContain("TRUE AS `_v1_deleted`");
      expect(plan.query).toContain("ORDER BY `id`");
      expect(plan.uri).toMatch(/\/commit-\d{8}-\d{6}\/part-\*\.parquet$/);
      expect(plan.query).toContain("is_change_history_enabled = 'YES'");
    }
  });

  it("stamps a batch at the fixed-time read boundary", () => {
    const table = SOURCE_TABLES[0];
    const plan = buildExportPlan(table, new Date("2026-09-05T22:40:00.000Z"), {
      bucket: "booming-data",
      prefix: "tinybird/v1-live",
      overlapMinutes: 32,
      changeHistory: historyGuard(),
    });

    expect(plan.query).toContain(
      "TIMESTAMP '2026-09-05 22:40:00.000+00' AS `_v1_observed_at`",
    );
    expect(plan.query).toContain(
      "FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(TIMESTAMP '2026-09-05 22:40:00.000+00', INTERVAL 1 MICROSECOND)",
    );
    expect(plan.query).toContain("overwrite=false");
  });

  it("fails closed when the requested window predates retained history", () => {
    expect(() =>
      buildExportPlan(
        SOURCE_TABLES[0],
        new Date("2026-09-05T22:40:00.000Z"),
        {
          bucket: "booming-data",
          prefix: "tinybird/v1-live",
          overlapMinutes: 20,
          changeHistory: {
            ...historyGuard(),
            checkedAt: new Date("2026-09-20T00:00:00.000Z"),
          },
        },
      ),
    ).toThrow("outside retained history");
  });

  it("builds a one-time snapshot-diff bridge with tombstones", () => {
    const plan = buildBridgeExportPlan(
      SOURCE_TABLES[0],
      new Date("2026-09-05T23:30:00.000Z"),
      {
        bucket: "booming-data",
        prefix: "tinybird/v1-live",
        bridgeStart: new Date("2026-09-05T22:28:00.000Z"),
        sourceMetadata: historyGuard(),
      },
    );

    expect(plan.query).toContain("CREATE TEMP TABLE at_start");
    expect(plan.query).toContain("CREATE TEMP TABLE at_end");
    expect(plan.query).toContain(
      "TIMESTAMP_SUB(TIMESTAMP '2026-09-05 23:30:00.000+00', INTERVAL 1 MICROSECOND)",
    );
    expect(plan.query).toContain("TO_JSON_STRING(STRUCT(");
    expect(plan.query).toContain("TRUE AS `_v1_deleted`");
    expect(plan.query).toContain("FALSE AS `_v1_deleted`");
    expect(plan.query).toContain("overwrite=false");
    expect(plan.uri).toContain("bridge-20260905233000/part-*.parquet");
  });

  it("supports a minimal synthetic primary-id table", () => {
    const table: SourceTable = {
      resourceName: "raw_test_records",
      exportKind: "table",
      source: { project: "test", dataset: "raw", table: "records" },
      unionSources: [],
      sourcePartitioning: null,
      versionColumns: [{ name: "_fivetran_synced", bigqueryType: "TIMESTAMP" }],
      watermarkColumns: [{ name: "_fivetran_synced", bigqueryType: "TIMESTAMP" }],
      columns: ["id", "payload", "_fivetran_synced"],
      columnTypes: {
        id: "STRING",
        payload: "STRING",
        _fivetran_synced: "TIMESTAMP",
      },
      jsonColumns: [],
      geographyColumns: [],
    };

    const plan = buildExportPlan(
      table,
      new Date("2026-09-05T23:42:00.000Z"),
      {
        bucket: "test",
        prefix: "changes",
        overlapMinutes: 1,
        changeHistory: historyGuard(),
      },
    );

    expect(plan.query).toContain("CREATE TEMP TABLE changed AS");
    expect(plan.query).toContain("`_CHANGE_TYPE` AS `v1_change_type`");
    expect(plan.query).toContain("WHERE `v1_change_type` = 'DELETE'");
    expect(plan.query).toContain("NOT COALESCE(`v1_is_for_update`, FALSE)");
    expect(plan.query).toContain("ORDER BY `v1_commit_at` DESC");
    expect(plan.query).not.toContain("_ADJUSTED_CHANGE_TIMESTAMP");
    expect(plan.query).not.toContain("_CHANGE_SEQUENCE_NUMBER");
    expect(plan.query).toContain("cutoff_row.`payload` AS `payload`");
    expect(plan.query).toContain("fallback.`payload` AS `payload`");
  });

  it("resolves export statistics from the single script child job", async () => {
    const requests: string[] = [];
    const fetcher = async (input: Request | string | URL) => {
      const url = String(input);
      requests.push(url);
      if (url.includes("parentJobId=")) {
        return Response.json({
          jobs: [
            {
              jobReference: { jobId: "export-child" },
              configuration: { query: { query: "EXPORT DATA OPTIONS(...)" } },
              status: { state: "DONE" },
              statistics: {
                query: {
                  exportDataStatistics: { fileCount: "2", rowCount: "3" },
                },
              },
            },
          ],
        });
      }
      return Response.json({
        jobReference: { jobId: "parent", projectId: "test", location: "US" },
        configuration: { query: { query: "script" } },
        status: { state: "DONE" },
        statistics: { query: {} },
      });
    };
    const result = await checkBigQueryExport(
      { resourceName: "raw_test", jobId: "parent", query: "script", uri: "gs://b/x" },
      { projectId: "test", location: "US", pollIntervalMs: 1, jobTimeoutMs: 1000 },
      "token",
      fetcher,
    );

    expect(result.status).toBe("done");
    if (result.status === "done") {
      expect(result.job.jobReference?.jobId).toBe("export-child");
    }
    expect(requests.some((url) => url.includes("parentJobId=parent"))).toBe(true);
  });

  it("catches up faster than time arrives and stops at the final boundary", () => {
    const completed = new Date("2026-09-05T22:28:00.000Z");
    const target = nextCatchupTarget(
      completed,
      new Date("2026-09-06T00:28:00.000Z"),
    );

    expect(target?.toISOString()).toBe("2026-09-05T23:28:00.000Z");
    expect(nextCatchupTarget(target!, target!)).toBeNull();

    // Three five-minute runs must clear a two-hour backlog, including new time.
    let cursor = completed;
    let now = new Date("2026-09-06T00:28:00.000Z");
    for (let run = 0; run < 3; run++) {
      const end = nextCatchupTarget(cursor, now)!;
      expect(end.valueOf()).toBeGreaterThan(cursor.valueOf());
      expect(end.valueOf()).toBeLessThanOrEqual(now.valueOf());
      expect(end.valueOf() - cursor.valueOf()).toBeLessThanOrEqual(60 * 60_000);
      cursor = end;
      now = new Date(now.valueOf() + 5 * 60_000);
    }
    expect(now.valueOf() - cursor.valueOf()).toBe(5 * 60_000);
  });
});

function historyGuard() {
  return {
    enabledAt: new Date("2026-09-05T22:00:00.000Z"),
    sourceCreatedAt: new Date("2025-01-01T00:00:00.000Z"),
    retentionHours: 168,
    checkedAt: new Date("2026-09-05T23:00:00.000Z"),
  };
}
