import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { SourceExportCoordinator } from "../src/coordinator";
import { nextCatchupTarget } from "../src/bigquery";

function stub(name: string) {
  return env.SOURCE_EXPORT_COORDINATOR.getByName(name);
}

describe("durable source transport", () => {
  it("initializes an empty object on first start", async () => {
    const target = stub("first-start");
    const status = await target.status();
    expect(status.completedThrough).toBe("2026-09-05T22:00:00.000Z");
    await runInDurableObject(
      target,
      async (_instance: SourceExportCoordinator, state) => {
        expect(
          state.storage.sql
            .exec<{ count: number }>("SELECT count(*) count FROM source_state")
            .one().count,
        ).toBe(1);
      },
    );
  });

  it("keeps persisted ambiguous import state across another start", async () => {
    const target = stub("ambiguous");
    await target.status();
    await runInDurableObject(
      target,
      async (_instance: SourceExportCoordinator, state) => {
        state.storage.sql.exec(
          "UPDATE source_state SET target_end=?,phase='import_unknown',import_attempted_at_ms=? WHERE singleton=1",
          "2026-09-05T23:00:00.000Z",
          Date.now(),
        );
      },
    );
    expect((await target.start()).phase).toBe("import_unknown");
  });

  it("persists retry quota in the same object", async () => {
    const target = stub("quota");
    await target.status();
    await runInDurableObject(
      target,
      async (_instance: SourceExportCoordinator, state) => {
        for (let i = 0; i < 5; i++)
          state.storage.sql.exec(
            "INSERT INTO import_attempts VALUES(?)",
            Date.now(),
          );
        expect(
          state.storage.sql
            .exec<{
              count: number;
            }>("SELECT count(*) count FROM import_attempts")
            .one().count,
        ).toBe(5);
      },
    );
  });

  it("retains a pending BigQuery job for a later alarm", async () => {
    const target = stub("long-job");
    await target.status();
    await runInDurableObject(
      target,
      async (_instance: SourceExportCoordinator, state) =>
        state.storage.sql.exec(
          "UPDATE source_state SET target_end=?,phase='bq_pending',plan_json=? WHERE singleton=1",
          "2026-09-05T23:00:00.000Z",
          JSON.stringify({
            jobId: "stable-job",
            resourceName: "raw_activecampaign_contact",
            query: "q",
            uri: "gs://b/x",
          }),
        ),
    );
    expect((await target.status()).phase).toBe("bq_pending");
  });

  it("chooses catch-up windows faster than new time arrives", () => {
    const start = new Date("2026-09-05T00:00:00Z"),
      now = new Date("2026-09-05T10:00:00Z");
    expect(nextCatchupTarget(start, now)?.valueOf()).toBe(
      start.valueOf() + 60 * 60_000,
    );
  });

  it("does not advance the barrier for an import failure state", async () => {
    const target = stub("import-failure");
    await target.status();
    await runInDurableObject(
      target,
      async (_instance: SourceExportCoordinator, state) =>
        state.storage.sql.exec(
          "UPDATE source_state SET target_end=?,phase='import_pending',import_job_id='failed-job' WHERE singleton=1",
          "2026-09-05T23:00:00.000Z",
        ),
    );
    const status = await target.status();
    expect(status.completedThrough).toBe("2026-09-05T22:00:00.000Z");
    expect(status.phase).toBe("import_pending");
  });
});
