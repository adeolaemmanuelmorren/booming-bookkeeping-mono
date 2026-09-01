import {
  env,
  fetchMock,
  runInDurableObject,
} from "cloudflare:test";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE } from "../src/cadence";
import type { TinybirdSyncGate } from "../src/tinybird-gate";

const tinybirdOrigin = "https://api.us-east.tinybird.co";

beforeAll(() => {
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

afterEach(() => {
  fetchMock.assertNoPendingInterceptors();
});

describe("Tinybird workspace rate gate", () => {
  it("initializes SQLite and sends a request key only once", async () => {
    const resourceName = "raw_test_orders";
    mockTinybird(resourceName, 202);
    const stub = env.TINYBIRD_SYNC_GATE.getByName("idempotency-test");
    const request = {
      requestKey: "job_20260826_raw_test_orders",
      resourceName,
    };

    const first = await stub.trigger(request);
    const duplicate = await stub.trigger(request);

    expect(first).toEqual({ outcome: "synced", tinybirdStatus: 202 });
    expect(duplicate).toEqual({
      outcome: "already_synced",
      tinybirdStatus: 202,
    });

    await runInDurableObject(stub, async (_instance: TinybirdSyncGate, state) => {
      const version = state.storage.sql
        .exec<{ version: number }>("SELECT MAX(version) AS version FROM schema_migrations")
        .one().version;
      const attempts = state.storage.sql
        .exec<{ count: number }>("SELECT COUNT(*) AS count FROM rate_attempts")
        .one().count;

      expect(version).toBe(1);
      expect(attempts).toBe(1);
    });
  });

  it("reserves no more than five calls in one rolling-minute window", async () => {
    const stub = env.TINYBIRD_SYNC_GATE.getByName("rate-window-test");
    const requests = Array.from(
      { length: MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE },
      (_, index) => {
        const resourceName = `raw_test_${index}`;
        mockTinybird(resourceName, 202);

        return {
          requestKey: `job_20260826_raw_test_${index}`,
          resourceName,
        };
      },
    );

    const results = await Promise.all(
      requests.map((request) => stub.trigger(request)),
    );

    expect(results.every((result) => result.outcome === "synced")).toBe(true);
    await runInDurableObject(stub, async (_instance: TinybirdSyncGate, state) => {
      const attempts = state.storage.sql
        .exec<{ count: number }>("SELECT COUNT(*) AS count FROM rate_attempts")
        .one().count;

      expect(attempts).toBe(MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE);
    });
  });

  it("waits for the oldest rate slot before admitting a sixth call", async () => {
    const resourceName = "raw_test_sixth";
    const stub = env.TINYBIRD_SYNC_GATE.getByName("sixth-call-test");
    const seededAt = Date.now() - 59_800;

    await runInDurableObject(stub, async (_instance: TinybirdSyncGate, state) => {
      for (
        let index = 0;
        index < MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE;
        index += 1
      ) {
        state.storage.sql.exec(
          "INSERT INTO rate_attempts (request_key, started_at_ms) VALUES (?, ?)",
          `seeded-${index}`,
          seededAt,
        );
      }
    });

    mockTinybird(resourceName, 202);
    const startedAt = Date.now();
    const result = await stub.trigger({
      requestKey: "job_20260826_raw_test_sixth",
      resourceName,
    });

    expect(result.outcome).toBe("synced");
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(100);
    await runInDurableObject(stub, async (_instance: TinybirdSyncGate, state) => {
      const activeAttempts = state.storage.sql.exec<{ count: number }>(
        "SELECT COUNT(*) AS count FROM rate_attempts WHERE started_at_ms > ?",
        Date.now() - 60_000,
      ).one().count;

      expect(activeAttempts).toBeLessThanOrEqual(
        MAX_TINYBIRD_INGESTION_CALLS_PER_MINUTE,
      );
    });
  });

  it("coalesces overlapping delivery of the same completed BigQuery job", async () => {
    const resourceName = "raw_test_overlap";
    mockTinybird(resourceName, 202, {}, 20);
    const stub = env.TINYBIRD_SYNC_GATE.getByName("overlap-test");
    const request = {
      requestKey: "job_20260826_raw_test_overlap",
      resourceName,
    };

    const results = await Promise.all([
      stub.trigger(request),
      stub.trigger(request),
    ]);

    expect(results.map(({ outcome }) => outcome).sort()).toEqual([
      "in_flight",
      "synced",
    ]);
    await runInDurableObject(stub, async (_instance: TinybirdSyncGate, state) => {
      const attempts = state.storage.sql
        .exec<{ count: number }>("SELECT COUNT(*) AS count FROM rate_attempts")
        .one().count;

      expect(attempts).toBe(1);
    });
  });

  it("honors Retry-After and counts the retry as another rate attempt", async () => {
    const resourceName = "raw_test_retry";
    mockTinybird(resourceName, 429, { "Retry-After": "0.02" });
    mockTinybird(resourceName, 202);
    const stub = env.TINYBIRD_SYNC_GATE.getByName("retry-test");
    const startedAt = Date.now();

    const result = await stub.trigger({
      requestKey: "job_20260826_raw_test_retry",
      resourceName,
    });

    expect(result).toEqual({ outcome: "synced", tinybirdStatus: 202 });
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(15);
    await runInDurableObject(stub, async (_instance: TinybirdSyncGate, state) => {
      const attempts = state.storage.sql
        .exec<{ count: number }>("SELECT COUNT(*) AS count FROM rate_attempts")
        .one().count;

      expect(attempts).toBe(2);
    });
  });

});

function mockTinybird(
  resourceName: string,
  status: number,
  headers: Record<string, string> = {},
  delayMs = 0,
): void {
  const response = fetchMock.get(tinybirdOrigin)
    .intercept({
      method: "POST",
      path: `/v0/datasources/${resourceName}/scheduling/runs`,
    })
    .reply(status, status === 429 ? "rate limited" : "", { headers });

  if (delayMs > 0) response.delay(delayMs);
}
