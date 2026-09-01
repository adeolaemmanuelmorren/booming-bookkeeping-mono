import { describe, expect, it, vi } from "vitest";
import {
  MAX_TABLES_PER_RUN,
  RAW_GENERATION_COVERAGE_MINUTES,
  RAW_TRIGGER_INTERVAL_MINUTES,
  SHARD_COUNT,
  runSync,
  shardSizes,
  slotForTime,
  tablesForSlot,
  type SyncDependencies,
} from "../src/sync";
import { TABLE_MANIFEST } from "../src/table-manifest.generated";
import { testEnv, testTinybirdGate } from "./helpers";

describe("ten-minute source sharding", () => {
  it("assigns all 50 resources exactly once with exactly five per minute", () => {
    const shards = Array.from(
      { length: SHARD_COUNT },
      (_, slot) => tablesForSlot(slot),
    );
    const resources = shards.flatMap((tables) => (
      tables.map((table) => table.resourceName)
    ));

    expect(shardSizes()).toEqual([5, 5, 5, 5, 5, 5, 5, 5, 5, 5]);
    expect(Math.max(...shardSizes())).toBe(MAX_TABLES_PER_RUN);
    expect(new Set(resources).size).toBe(50);
    expect(resources.slice().sort()).toEqual(
      TABLE_MANIFEST.map((table) => table.resourceName).slice().sort(),
    );
  });

  it("covers every shard once across ten consecutive scheduled minutes", () => {
    const firstMinute = new Date("2026-08-26T12:34:00.000Z");
    const slots = Array.from(
      { length: SHARD_COUNT },
      (_, offset) => slotForTime(new Date(
        firstMinute.valueOf() + offset * RAW_TRIGGER_INTERVAL_MINUTES * 60_000,
      )),
    );

    expect(RAW_GENERATION_COVERAGE_MINUTES).toBe(10);
    expect(slots.slice().sort((left, right) => left - right)).toEqual(
      Array.from({ length: SHARD_COUNT }, (_, slot) => slot),
    );
    expect(slotForTime(new Date(
      firstMinute.valueOf() + RAW_GENERATION_COVERAGE_MINUTES * 60_000,
    ))).toBe(slots[0]);
  });
});

describe("sync ordering", () => {
  it("uses the current clock for OAuth when replaying an older export window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-27T02:00:00.000Z"));

    try {
      const done = new Set<string>();
      const dependencies = mockDependencies({ done });
      let oauthSigningTime: Date | undefined;

      dependencies.getAccessToken = async (_json, _fetcher, now) => {
        oauthSigningTime = now;
        return "google-token";
      };

      const summary = await runSync(
        testEnv(),
        new Date("2026-06-01T12:34:00.000Z"),
        9,
        dependencies,
      );

      expect(summary.scheduledAt).toBe("2026-06-01T12:34:00.000Z");
      expect(oauthSigningTime?.toISOString()).toBe("2026-08-27T02:00:00.000Z");
    } finally {
      vi.useRealTimers();
    }
  });

  it("calls Tinybird only after each corresponding BigQuery job is DONE", async () => {
    const done = new Set<string>();
    const tinybirdCalls: string[] = [];
    const dependencies = mockDependencies({ done });
    const env = testEnv({
      TINYBIRD_SYNC_GATE: mockTinybirdGate(done, tinybirdCalls),
    });

    const summary = await runSync(
      env,
      new Date("2026-08-26T12:34:56.789Z"),
      9,
      dependencies,
    );

    expect(summary.ok).toBe(true);
    expect(summary.scheduledAt).toBe("2026-08-26T12:34:00.000Z");
    expect(summary.tableCount).toBe(5);
    expect(summary.succeeded).toBe(5);
    expect(tinybirdCalls).toHaveLength(5);
    expect(summary.results.every((result) => (
      result.uri.includes("/incremental/run_date=2026-08-26/run_time=123400/")
    ))).toBe(true);
  });

  it("does not trigger Tinybird for a failed BigQuery export", async () => {
    const done = new Set<string>();
    const tinybirdCalls: string[] = [];
    const failedResource = tablesForSlot(9)[0].resourceName;
    const dependencies = mockDependencies({
      done,
      failedResource,
    });
    const env = testEnv({
      TINYBIRD_SYNC_GATE: mockTinybirdGate(done, tinybirdCalls),
    });

    const summary = await runSync(
      env,
      new Date("2026-08-26T12:34:00.000Z"),
      9,
      dependencies,
    );

    expect(summary.ok).toBe(false);
    expect(summary.failed).toBe(1);
    expect(tinybirdCalls).not.toContain(failedResource);
    expect(tinybirdCalls).toHaveLength(4);
  });

  it("does not mark a raw shard complete while a duplicate Tinybird request is in flight", async () => {
    const done = new Set<string>();
    const dependencies = mockDependencies({ done });
    const inFlightResource = tablesForSlot(9)[0].resourceName;
    const env = testEnv({
      TINYBIRD_SYNC_GATE: testTinybirdGate(async ({ resourceName }) => ({
        outcome: resourceName === inFlightResource ? "in_flight" : "synced",
        tinybirdStatus: resourceName === inFlightResource ? undefined : 202,
      })),
    });

    const summary = await runSync(
      env,
      new Date("2026-08-26T12:34:00.000Z"),
      9,
      dependencies,
    );

    expect(summary.ok).toBe(false);
    expect(summary.inFlight).toBe(1);
    expect(summary.succeeded).toBe(4);
  });
});

function mockDependencies(options: {
  done: Set<string>;
  failedResource?: string;
}): SyncDependencies {
  const jobs = new Map<string, string>();
  const fetcher = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
    const url = new URL(String(input));

    if (url.hostname === "bigquery.googleapis.com" && init?.method === "POST") {
      const body = JSON.parse(String(init.body)) as {
        jobReference: { jobId: string };
      };
      const resource = resourceFromJobId(body.jobReference.jobId);
      jobs.set(body.jobReference.jobId, resource);
      return Response.json({});
    }

    if (url.hostname === "bigquery.googleapis.com") {
      const jobId = decodeURIComponent(url.pathname.split("/").at(-1) || "");
      const resource = jobs.get(jobId);
      if (!resource) return Response.json({ error: "unknown job" }, { status: 404 });

      options.done.add(resource);
      if (resource === options.failedResource) {
        return Response.json({
          status: {
            state: "DONE",
            errorResult: { reason: "invalidQuery", message: "test failure" },
          },
        });
      }

      return Response.json({ status: { state: "DONE" } });
    }

    return Response.json({ error: "unexpected URL" }, { status: 500 });
  });

  return {
    fetcher,
    getAccessToken: async () => "google-token",
    sleep: async () => undefined,
  };
}

function mockTinybirdGate(
  done: Set<string>,
  calls: string[],
) {
  return testTinybirdGate(async ({ resourceName }) => {
    if (!done.has(resourceName)) {
      throw new Error(`Tinybird ran before BigQuery completed for ${resourceName}.`);
    }

    calls.push(resourceName);
    return { outcome: "synced", tinybirdStatus: 202 };
  });
}

function resourceFromJobId(jobId: string): string {
  return TABLE_MANIFEST
    .map((table) => table.resourceName)
    .find((resource) => jobId.endsWith(resource)) || "";
}
