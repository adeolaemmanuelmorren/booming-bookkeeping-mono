import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { SourceExportCoordinator } from "../src/coordinator";

const request = {
  expectedJobId: "failed-job",
  expectedSource: "raw_activecampaign_contact",
  expectedWindowStart: "2026-09-05T22:00:00.000Z",
  expectedWindowEnd: "2026-09-05T23:00:00.000Z",
};

describe("failed export Durable Object recovery gate", () => {
  it("rejects a request whose expected job does not match persisted state", async () => {
    const target = await seeded("recovery-mismatch", "bq_pending", "another-job");
    await expectRecoveryRejection(target);
  });

  it("rejects a repeated request after the plan has moved to retry1", async () => {
    const target = await seeded("recovery-repeat", "idle", "failed-job_retry1");
    await expectRecoveryRejection(target);
  });
});

async function seeded(name: string, phase: string, jobId: string) {
  const target = env.SOURCE_EXPORT_COORDINATOR.getByName(name);
  await target.status();
  await runInDurableObject(
    target,
    async (_instance: SourceExportCoordinator, state) => {
      state.storage.sql.exec(
        "UPDATE source_state SET target_end=?,phase=?,plan_json=? WHERE singleton=1",
        request.expectedWindowEnd,
        phase,
        JSON.stringify({
          resourceName: request.expectedSource,
          jobId,
          query: "SELECT 1",
          uri: "gs://booming-data/tinybird/v1-live/raw_activecampaign_contact/bridge-20260905234200/part-*.parquet",
        }),
      );
    },
  );
  return target;
}

async function expectRecoveryRejection(
  target: DurableObjectStub<SourceExportCoordinator>,
) {
  await runInDurableObject(target, async (instance: SourceExportCoordinator) => {
    await expect(instance.recoverFailedExport(request)).rejects.toThrow(
      "does not match recovery request",
    );
  });
}
