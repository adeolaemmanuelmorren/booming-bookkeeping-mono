import { describe, expect, it } from "vitest";
import { handleRequest } from "../src/http";
import { testEnv } from "./helpers";

describe("operator endpoints", () => {
  it("protects the health endpoint with the admin bearer token", async () => {
    const unauthorized = await handleRequest(
      new Request("https://sync.example.com/health"),
      testEnv(),
    );
    const authorized = await handleRequest(
      new Request("https://sync.example.com/health", {
        headers: { Authorization: "Bearer test-admin-token" },
      }),
      testEnv(),
    );
    const body = await authorized.json() as {
      tableCount: number;
      shardSizes: number[];
      cadence: {
        plannedTriggerIntervalMinutes: number;
        rawSlotsPerGeneration: number;
        healthyPathRawCoverageMinutes: number;
        maximumTinybirdIngestionCallsPerRollingMinute: number;
        endToEndPublicationIntervalGuaranteed: boolean;
      };
      configured: { gcpServiceAccount: boolean; tinybirdAdminToken: boolean };
    };

    expect(unauthorized.status).toBe(401);
    expect(authorized.status).toBe(200);
    expect(body.tableCount).toBe(50);
    expect(body.shardSizes).toEqual([5, 5, 5, 5, 5, 5, 5, 5, 5, 5]);
    expect(body.cadence).toEqual({
      plannedTriggerIntervalMinutes: 1,
      rawSlotsPerGeneration: 10,
      healthyPathRawCoverageMinutes: 10,
      maximumTinybirdIngestionCallsPerRollingMinute: 5,
      endToEndPublicationIntervalGuaranteed: false,
    });
    expect(body.configured).toMatchObject({
      gcpServiceAccount: true,
      tinybirdAdminToken: true,
    });
  });

  it("queues manual raw work instead of bypassing publication coordination", async () => {
    const response = await handleRequest(
      new Request("https://sync.example.com/run", {
        method: "POST",
        headers: {
          Authorization: "Bearer test-admin-token",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          scheduledAt: "2026-08-26T12:34:00.000Z",
          slot: 4,
        }),
      }),
      testEnv(),
    );

    expect(response.status).toBe(202);
    await expect(response.json()).resolves.toMatchObject({
      accepted: true,
      status: "queued",
      slot: 4,
    });
  });

  it("rejects an invalid manual shard before starting a run", async () => {
    const response = await handleRequest(
      new Request("https://sync.example.com/run", {
        method: "POST",
        headers: {
          Authorization: "Bearer test-admin-token",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ slot: 10 }),
      }),
      testEnv(),
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({
      error: "slot must be an integer from 0 through 9.",
    });
  });

  it("requires an explicit boolean when pausing publication", async () => {
    const response = await handleRequest(
      new Request("https://sync.example.com/publication/pause", {
        method: "POST",
        headers: {
          Authorization: "Bearer test-admin-token",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ paused: "yes" }),
      }),
      testEnv(),
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({
      error: "paused must be true or false.",
    });
  });

  it("starts the resumable journey backfill through the coordinator", async () => {
    const response = await handleRequest(
      new Request("https://sync.example.com/publication/journey-backfill", {
        method: "POST",
        headers: { Authorization: "Bearer test-admin-token" },
      }),
      testEnv(),
    );

    expect(response.status).toBe(202);
    await expect(response.json()).resolves.toMatchObject({
      journeyBackfill: { status: "not_requested" },
    });
  });
});
