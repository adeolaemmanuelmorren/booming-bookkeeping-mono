import { env, fetchMock, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { JourneyCoordinator } from "../src/journey-coordinator";

const tinybirdOrigin = "https://api.us-east.tinybird.co";

beforeAll(() => {
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

afterEach(() => {
  fetchMock.assertNoPendingInterceptors();
});

interface BuildCall {
  identifierKeys: string[];
  identifierProfileIds: string[];
  conversionIds: string[];
  batchVersion: number;
}

describe("journey coordinator", () => {
  it("durably repairs whole profiles without advancing the identity cursor", async () => {
    const stub = env.JOURNEY_COORDINATOR.getByName("journey-profile-repair-test");
    const pool = fetchMock.get(tinybirdOrigin);

    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/identity_worker_profile_heads.json"),
      })
      .reply(200, {
        data: [identityProfile("profile-a", ["anonymous_id:a1", "user_id:a2"])],
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_profile_journey_window_build.json"),
      })
      .reply(200, { data: [] });

    await runInDurableObject(stub, async (instance: JourneyCoordinator) => {
      await instance.enqueueRepair({
        repairId: "pre-fix-0001",
        profileIds: ["profile-a"],
      });
      const queued = await instance.enqueueRepair({
        repairId: "pre-fix-0001",
        profileIds: ["profile-a"],
      });
      expect(queued.pendingRepairBatches).toBe(1);

      await instance.alarm();
      expect((await instance.status()).activeRepairId).toBe("pre-fix-0001");

      await instance.alarm();
      await clearPacingDelay(instance);
      await instance.alarm();

      const completed = await instance.status();
      expect(completed.phase).toBe("idle");
      expect(completed.activeRepairId).toBeNull();
      expect(completed.pendingRepairBatches).toBe(0);
      expect(completed.completedRepairBatches).toBe(1);
      expect(completed.cursorBatchVersion).toBe(0);
      expect(completed.cursorBatchId).toBe("");
    });
  });

  it("pages conversions, orphan keys, then whole profiles in that order", async () => {
    const stub = env.JOURNEY_COORDINATOR.getByName("journey-page-order-test");
    const buildCalls: BuildCall[] = [];
    const pool = fetchMock.get(tinybirdOrigin);

    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_journey_pending_batch.json"),
      })
      .reply(200, {
        data: [{
          tenant_id: "boom",
          batch_version: 7,
          batch_id: "identity-7",
          profile_ids: ["profile-a", "profile-b"],
          identifier_keys: ["anonymous_id:orphan", "anonymous_id:still-mapped"],
          conversion_ids: ["server_payment:stripe:pay-1"],
        }],
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/identity_worker_mapping_heads.json"),
      })
      .reply(200, {
        data: [{
          identifier_type: "anonymous_id",
          identifier_value: "still-mapped",
          identifier_key: "anonymous_id:still-mapped",
          profile_id: "profile-a",
          first_seen_at: "2026-08-01 00:00:00.000000",
          last_seen_at: "2026-08-30 00:00:00.000000",
        }],
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/identity_worker_profile_heads.json"),
      })
      .reply(200, {
        data: [
          identityProfile("profile-a", [
            "anonymous_id:a1",
            "anonymous_id:still-mapped",
            "user_id:a2",
          ]),
          identityProfile("profile-b", ["anonymous_id:b1", "user_id:b2"]),
        ],
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_profile_journey_window_build.json"),
      })
      .reply(200, (request) => {
        const query = new URLSearchParams(request.path.split("?")[1] ?? "");
        buildCalls.push({
          identifierKeys: splitArrayParameter(query.get("p_identifier_keys")),
          identifierProfileIds: splitArrayParameter(
            query.get("p_identifier_profile_ids"),
          ),
          conversionIds: splitArrayParameter(query.get("p_conversion_ids")),
          batchVersion: 0,
        });
        return { data: [] };
      })
      .times(3);
    // Only the conversion page appends: it writes a zero-row commit marker.
    // The orphan and profile pages return no rows, so nothing is appended.
    pool
      .intercept({
        method: "POST",
        path: (path) => path.startsWith("/v0/events"),
      })
      .reply(200, { successful_rows: 1, quarantined_rows: 0 });

    await runInDurableObject(stub, async (instance: JourneyCoordinator) => {
      // First alarm loads the pending batch; each following alarm runs one
      // page after clearing the pacing delay left by the previous page.
      await instance.alarm();
      for (let page = 0; page < 4; page += 1) {
        await clearPacingDelay(instance);
        await instance.alarm();
      }

      const status = await instance.status();
      expect(status.phase).toBe("idle");
      expect(status.cursorBatchVersion).toBe(7);
      expect(status.cursorBatchId).toBe("identity-7");
    });

    expect(buildCalls.map((call) => call.conversionIds)).toEqual([
      ["server_payment:stripe:pay-1"],
      [],
      [],
    ]);
    expect(buildCalls[1].identifierKeys).toEqual(["anonymous_id:orphan"]);
    expect(buildCalls[2].identifierKeys).toEqual([
      "anonymous_id:a1",
      "anonymous_id:b1",
      "anonymous_id:still-mapped",
      "user_id:a2",
      "user_id:b2",
    ]);
    expect(buildCalls[2].identifierProfileIds).toEqual([
      "profile-a",
      "profile-b",
      "profile-a",
      "profile-a",
      "profile-b",
    ]);
  });

  it("treats a 429 as backpressure and keeps the checkpoint", async () => {
    const stub = env.JOURNEY_COORDINATOR.getByName("journey-backpressure-test");
    const pool = fetchMock.get(tinybirdOrigin);

    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_journey_pending_batch.json"),
      })
      .reply(200, {
        data: [{
          tenant_id: "boom",
          batch_version: 9,
          batch_id: "identity-9",
          profile_ids: [],
          identifier_keys: [],
          conversion_ids: ["server_payment:stripe:pay-2"],
        }],
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_profile_journey_window_build.json"),
      })
      .reply(429, { error: "rate limited" }, {
        headers: { "Retry-After": "17" },
      });

    await runInDurableObject(stub, async (instance: JourneyCoordinator) => {
      await instance.alarm();
      await clearPacingDelay(instance);
      await instance.alarm();

      const status = await instance.status();
      expect(status.phase).toBe("running");
      expect(status.completedConversions).toBe(0);
      expect(status.nextAttemptAt).not.toBeNull();
      expect(status.lastError).toContain("429");
    });
  });

  it("discards a stale legacy active batch instead of crashing", async () => {
    const stub = env.JOURNEY_COORDINATOR.getByName("journey-legacy-batch-test");
    const pool = fetchMock.get(tinybirdOrigin);

    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_journey_pending_batch.json"),
      })
      .reply(200, { data: [] });

    await runInDurableObject(stub, async (instance: JourneyCoordinator) => {
      const legacyBatch = JSON.stringify({
        tenantId: "boom",
        batchVersion: 3,
        batchId: "identity-3",
        identifierKeys: ["anonymous_id:a1"],
        conversionIds: [],
      });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (instance as any).ctx.storage.sql.exec(
        "UPDATE journey_state SET active_batch_json = ? WHERE id = 1",
        legacyBatch,
      );

      await instance.alarm();

      const status = await instance.status();
      expect(status.phase).toBe("idle");
      expect(status.activeIdentityBatchId).toBeNull();
    });
  });
});

function splitArrayParameter(value: string | null): string[] {
  return value ? value.split(",") : [];
}

async function clearPacingDelay(instance: JourneyCoordinator): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (instance as any).ctx.storage.sql.exec(
    "UPDATE journey_state SET next_attempt_at_ms = 0 WHERE id = 1",
  );
}

function identityProfile(
  profileId: string,
  memberIdentifierKeys: string[],
): Record<string, unknown> {
  return {
    profile_id: profileId,
    profile_key: `profile:${profileId}`,
    winner_identifier_key: memberIdentifierKeys[0],
    member_identifier_keys: memberIdentifierKeys,
    historical_profile_ids: [],
    first_name: "",
    last_name: "",
    first_seen_at: "2026-08-01 00:00:00.000000",
    last_seen_at: "2026-08-30 00:00:00.000000",
  };
}
