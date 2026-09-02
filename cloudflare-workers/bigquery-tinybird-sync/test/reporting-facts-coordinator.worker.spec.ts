import { env, fetchMock, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { ReportingFactsCoordinator } from "../src/reporting-facts-coordinator";

const tinybirdOrigin = "https://api.us-east.tinybird.co";

beforeAll(() => {
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

afterEach(() => {
  fetchMock.assertNoPendingInterceptors();
});

describe("reporting facts coordinator", () => {
  it("rebuilds changed visitors, tombstones ghosts, and repairs journeys", async () => {
    const stub = env.REPORTING_FACTS_COORDINATOR.getByName("facts-window-test");
    const pool = fetchMock.get(tinybirdOrigin);
    const appended: Record<string, unknown>[][] = [];

    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_cdc_changed_visitors.json"),
      })
      .reply(200, {
        data: [{
          visitor_kind: "anonymous",
          visitor_value: "anon-1",
          last_ingested_at: "2026-09-01 10:00:05.000000",
        }],
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_touchpoint_facts_cdc_build.json"),
      })
      .reply(200, {
        data: [{
          identity_anchor_key: "anonymous_id:anon-1",
          touchpoint_id: "tp-live",
          session_id: "session-live",
        }],
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_touchpoint_fact_heads.json"),
      })
      .reply(200, {
        data: [
          { identity_anchor_key: "anonymous_id:anon-1", touchpoint_id: "tp-live" },
          { identity_anchor_key: "anonymous_id:anon-1", touchpoint_id: "tp-ghost" },
        ],
      });
    pool
      .intercept({
        method: "POST",
        path: (path) => path.startsWith("/v0/events"),
      })
      .reply(200, (request) => {
        appended.push(
          String(request.body)
            .trim()
            .split("\n")
            .map((line) => JSON.parse(line) as Record<string, unknown>),
        );
        return { successful_rows: 2, quarantined_rows: 0 };
      });
    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/identity_worker_mapping_heads.json"),
      })
      .reply(200, {
        data: [{
          identifier_type: "anonymous_id",
          identifier_value: "anon-1",
          identifier_key: "anonymous_id:anon-1",
          profile_id: "profile-1",
          first_seen_at: "2026-08-01 00:00:00.000000",
          last_seen_at: "2026-08-30 00:00:00.000000",
        }],
      });

    await runInDurableObject(stub, async (instance: ReportingFactsCoordinator) => {
      await instance.alarm();
      await clearPacing(instance);
      await instance.alarm();
      await clearPacing(instance);
      await instance.alarm();

      const status = await instance.status();
      expect(status.activeStream).toBeNull();
      expect(status.lastError).toBeNull();
      expect(status.touchpointCursor).toBe("2026-09-01 10:00:04.000000");
    });

    const rows = appended.flat();
    expect(rows).toHaveLength(2);
    expect(rows.find((row) => row.touchpoint_id === "tp-live")).toMatchObject({
      is_deleted: 0,
      identity_anchor_key: "anonymous_id:anon-1",
    });
    expect(rows.find((row) => row.touchpoint_id === "tp-ghost")).toMatchObject({
      is_deleted: 1,
    });
  });

  it("treats a 429 as backpressure and keeps the window", async () => {
    const stub = env.REPORTING_FACTS_COORDINATOR.getByName("facts-backpressure-test");
    const pool = fetchMock.get(tinybirdOrigin);

    pool
      .intercept({
        method: "GET",
        path: (path) => path.startsWith("/v0/pipes/reporting_cdc_changed_visitors.json"),
      })
      .reply(429, { error: "rate limited" }, { headers: { "Retry-After": "23" } });

    await runInDurableObject(stub, async (instance: ReportingFactsCoordinator) => {
      await instance.alarm();

      const status = await instance.status();
      expect(status.phase).toBe("running");
      expect(status.lastError).toContain("429");
      expect(status.nextAttemptAt).not.toBeNull();
    });
  });
});

async function clearPacing(instance: ReportingFactsCoordinator): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (instance as any).ctx.storage.sql.exec(
    "UPDATE facts_state SET next_attempt_at_ms = 0 WHERE id = 1",
  );
}
