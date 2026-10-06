import { describe, expect, it } from "vitest";
import {
  verifyFailedExportRecovery,
  type ExportPlan,
  type Fetcher,
} from "../src/bigquery";

const plan: ExportPlan = {
  resourceName: "raw_activecampaign_contact",
  jobId: "tinybird_bridge_20260905234200_raw_activecampaign_contact",
  query: "SELECT 1",
  uri: "gs://booming-data/tinybird/v1-live/raw_activecampaign_contact/bridge-20260905234200/part-*.parquet",
};
const config = {
  projectId: "able-folio-499722",
  location: "US",
  pollIntervalMs: 100,
  jobTimeoutMs: 1_000,
};

describe("failed export recovery proof", () => {
  it("accepts only the exact failed job with an empty destination", async () => {
    await expect(
      verifyFailedExportRecovery(plan, config, "token", fixture()),
    ).resolves.toBeUndefined();
  });

  it("rejects a running job", async () => {
    await expect(
      verifyFailedExportRecovery(plan, config, "token", fixture({ state: "RUNNING" })),
    ).rejects.toThrow("not a terminal failure");
  });

  it("rejects a successful DONE job", async () => {
    await expect(
      verifyFailedExportRecovery(plan, config, "token", fixture({ successful: true })),
    ).rejects.toThrow("not a terminal failure");
  });

  it("rejects a mismatched saved query", async () => {
    await expect(
      verifyFailedExportRecovery(plan, config, "token", fixture({ query: "SELECT 2" })),
    ).rejects.toThrow("does not match saved plan");
  });

  it("rejects a nonempty immutable destination", async () => {
    await expect(
      verifyFailedExportRecovery(plan, config, "token", fixture({ objects: [{}] })),
    ).rejects.toThrow("destination is not empty");
  });

  it("rejects a paginated destination response", async () => {
    await expect(
      verifyFailedExportRecovery(plan, config, "token", fixture({ nextPageToken: "more" })),
    ).rejects.toThrow("destination is not empty");
  });
});

function fixture(
  overrides: {
    state?: string;
    query?: string;
    objects?: unknown[];
    nextPageToken?: string;
    successful?: boolean;
  } = {},
): Fetcher {
  return async (input) => {
    const url = String(input);
    if (url.startsWith("https://storage.googleapis.com/")) {
      return Response.json({
        items: overrides.objects ?? [],
        nextPageToken: overrides.nextPageToken,
      });
    }
    return Response.json({
      jobReference: {
        jobId: plan.jobId,
        projectId: config.projectId,
        location: config.location,
      },
      configuration: { query: { query: overrides.query ?? plan.query } },
      status: {
        state: overrides.state ?? "DONE",
        errorResult: overrides.successful ? undefined : { reason: "accessDenied" },
      },
    });
  };
}
