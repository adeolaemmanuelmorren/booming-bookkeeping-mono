import { describe, expect, it, vi } from "vitest";
import {
  JOURNEY_PROFILE_BATCH_LIMIT,
  processJourneyProfileBatch,
  takeProfilePage,
} from "../src/journey-worker";
import type { Fetcher } from "../src/bigquery";
import type { TinybirdApiConfig } from "../src/tinybird-api";
import {
  affectedJourneyConversionIds,
  orphanedJourneyIdentifierKeys,
} from "../src/identity-worker";
import type { IdentityEngineResult } from "../src/identity-engine";

const config: TinybirdApiConfig = {
  apiUrl: "https://api.us-east.tinybird.co",
  adminToken: "test-token",
  fetchTimeoutMs: 30_000,
};

describe("incremental journey Worker", () => {
  it("targets conversion records directly when identity facts change", () => {
    const changedFacts = [
      identityFact("segment_form", "form-1"),
      identityFact("segment_order_completed", "order-1"),
      identityFact("stripe", "payment-1"),
    ];
    const engine = { changedFacts } as unknown as IdentityEngineResult;

    expect(affectedJourneyConversionIds(engine)).toEqual([
      "client_form:form-1",
      "client_payment:historical_form_form-1",
      "client_payment:segment_order_order-1",
      "server_payment:stripe:payment-1",
    ]);
  });

  it("writes journey rows before their atomic conversion commit", async () => {
    const requests: { url: URL; body: string }[] = [];
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      requests.push({ url, body: String(init?.body ?? "") });

      if (url.pathname.endsWith("/reporting_profile_journey_window_build.json")) {
        return jsonResponse({
          data: [
            journeyRow("conversion-1", "profile-1", "touchpoint-1"),
            journeyRow("conversion-1", "profile-1", "touchpoint-2"),
          ],
        });
      }
      return jsonResponse({ successful_rows: 1, quarantined_rows: 0 });
    }) as unknown as Fetcher;

    const result = await processJourneyProfileBatch({
      tenantId: "boom",
      identifierKeys: ["email:two@example.com", "email:one@example.com"],
      identifierProfileIds: ["profile-1", "profile-1"],
      batchVersion: 1_787_878_380_010,
      batchId: "identity-1_journey_0_1787878380010",
    }, config, fetcher);

    expect(result).toEqual({
      identifierCount: 2,
      conversionCount: 1,
      journeyRowCount: 2,
    });
    expect(requests[0].url.pathname).toBe(
      "/v0/pipes/reporting_profile_journey_window_build.json",
    );
    const journeyParameters = new URLSearchParams(requests[0].body);
    expect(journeyParameters.get("p_identifier_keys_delimited")).toBe(
      "email:one@example.com|email:two@example.com",
    );
    expect(journeyParameters.get("p_identifier_profile_ids_delimited")).toBe(
      "profile-1|profile-1",
    );
    expect(requests[1].url.searchParams.get("name")).toBe(
      "reporting_journey_versions",
    );
    expect(requests[2].url.searchParams.get("name")).toBe(
      "reporting_journey_commits",
    );

    const versions = ndjsonRows(requests[1].body);
    const commits = ndjsonRows(requests[2].body);
    expect(versions).toHaveLength(2);
    expect(versions[0]).toMatchObject({
      tenant_id: "boom",
      batch_version: 1_787_878_380_010,
      journey_row_key: "touchpoint-1",
    });
    expect(commits).toEqual([
      expect.objectContaining({
        conversion_id: "conversion-1",
        profile_id: "profile-1",
        journey_row_count: 2,
        is_deleted: 0,
      }),
    ]);
  });

  it("preserves comma-bearing identity keys in the delimited request body", async () => {
    const requests: { url: URL; body: string }[] = [];
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      requests.push({ url, body: String(init?.body ?? "") });

      if (url.pathname.endsWith("/reporting_profile_journey_window_build.json")) {
        return jsonResponse({ data: [] });
      }
      return jsonResponse({ successful_rows: 1, quarantined_rows: 0 });
    }) as unknown as Fetcher;

    await processJourneyProfileBatch({
      tenantId: "boom",
      identifierKeys: ["email:last,first@example.com"],
      identifierProfileIds: ["profile-1"],
      batchVersion: 1,
      batchId: "comma-key",
    }, config, fetcher);

    const parameters = new URLSearchParams(requests[0].body);
    expect(parameters.get("p_identifier_keys_delimited")).toBe(
      "email:last,first@example.com",
    );
    expect(parameters.get("p_identifier_profile_ids_delimited")).toBe("profile-1");
  });

  it("keeps the complete profile page in one request body", async () => {
    const requests: { url: URL; method: string; body: string }[] = [];
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      requests.push({
        url,
        method: String(init?.method ?? "GET"),
        body: String(init?.body ?? ""),
      });
      return jsonResponse({ data: [] });
    }) as unknown as Fetcher;
    const profileAKeys = Array.from(
      { length: 10 },
      (_, index) => `anonymous_id:a-${index}-${"x".repeat(700)}`,
    );
    const profileBKeys = Array.from(
      { length: 10 },
      (_, index) => `anonymous_id:b-${index}-${"y".repeat(700)}`,
    );

    await processJourneyProfileBatch({
      tenantId: "boom",
      identifierKeys: [...profileAKeys, ...profileBKeys],
      identifierProfileIds: [
        ...profileAKeys.map(() => "profile-a"),
        ...profileBKeys.map(() => "profile-b"),
      ],
      batchVersion: 1,
      batchId: "whole-profile-url-chunks",
    }, config, fetcher);

    expect(requests).toHaveLength(1);
    expect(requests[0].method).toBe("POST");
    const parameters = new URLSearchParams(requests[0].body);
    expect(
      (parameters.get("p_identifier_keys_delimited") ?? "").split("|"),
    ).toHaveLength(20);
    expect(new Set(
      (parameters.get("p_identifier_profile_ids_delimited") ?? "").split("|"),
    )).toEqual(new Set(["profile-a", "profile-b"]));
  });

  it("never writes a commit when the journey row append fails", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/reporting_profile_journey_window_build.json")) {
        return jsonResponse({
          data: [journeyRow("conversion-1", "profile-1", "touchpoint-1")],
        });
      }
      return new Response("append failed", { status: 500 });
    }) as unknown as Fetcher;

    await expect(processJourneyProfileBatch({
      tenantId: "boom",
      identifierKeys: ["email:one@example.com"],
      batchVersion: 1,
      batchId: "failed-journey",
    }, config, fetcher)).rejects.toThrow("append failed");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("rejects profile batches larger than the endpoint contract", async () => {
    const identifierKeys = Array.from(
      { length: JOURNEY_PROFILE_BATCH_LIMIT + 1 },
      (_, index) => `email:user-${index}@example.com`,
    );

    await expect(processJourneyProfileBatch({
      tenantId: "boom",
      identifierKeys,
      batchVersion: 1,
      batchId: "too-wide",
    }, config)).rejects.toThrow("exceeds 500 keys");
  });

  it("commits a tombstone when an explicitly changed conversion disappeared", async () => {
    const requests: { url: URL; body: string }[] = [];
    const fetcher = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      requests.push({ url, body: String(init?.body ?? "") });
      if (url.pathname.endsWith("/reporting_profile_journey_window_build.json")) {
        return jsonResponse({ data: [] });
      }
      return jsonResponse({ successful_rows: 1, quarantined_rows: 0 });
    }) as unknown as Fetcher;

    const result = await processJourneyProfileBatch({
      tenantId: "boom",
      identifierKeys: [],
      conversionIds: ["server_payment:stripe:payment-1"],
      batchVersion: 2,
      batchId: "deleted-conversion",
    }, config, fetcher);

    expect(result).toEqual({
      identifierCount: 0,
      conversionCount: 1,
      journeyRowCount: 0,
    });
    expect(requests).toHaveLength(2);
    expect(requests[0].url.searchParams.getAll("p_conversion_ids")).toEqual([
      "server_payment:stripe:payment-1",
    ]);
    expect(ndjsonRows(requests[1].body)).toEqual([
      expect.objectContaining({
        conversion_id: "server_payment:stripe:payment-1",
        journey_row_count: 0,
        is_deleted: 1,
      }),
    ]);
  });
});

describe("journey profile paging", () => {
  it("never splits one profile's identifier keys across pages", () => {
    const keysByProfile = new Map([
      ["profile-a", ["anonymous_id:a1", "user_id:a2"]],
      ["profile-b", ["anonymous_id:b1", "user_id:b2", "email:b3"]],
      ["profile-c", ["anonymous_id:c1"]],
    ]);

    const page = takeProfilePage(
      ["profile-a", "profile-b", "profile-c"],
      keysByProfile,
      4,
    );

    expect(page.profileIds).toEqual(["profile-a"]);
    expect(page.identifierKeys).toEqual(["anonymous_id:a1", "user_id:a2"]);
    expect(page.identifierProfileIds).toEqual(["profile-a", "profile-a"]);

    const nextPage = takeProfilePage(["profile-b", "profile-c"], keysByProfile, 4);
    expect(nextPage.profileIds).toEqual(["profile-b", "profile-c"]);
    expect(nextPage.identifierKeys).toEqual([
      "anonymous_id:b1",
      "anonymous_id:c1",
      "email:b3",
      "user_id:b2",
    ]);
    expect(nextPage.identifierProfileIds).toEqual([
      "profile-b",
      "profile-c",
      "profile-b",
      "profile-b",
    ]);
  });

  it("consumes profiles with no current mapping without stalling", () => {
    const page = takeProfilePage(
      ["profile-gone", "profile-a"],
      new Map([["profile-a", ["anonymous_id:a1"]]]),
      10,
    );

    expect(page.profileIds).toEqual(["profile-gone", "profile-a"]);
    expect(page.identifierKeys).toEqual(["anonymous_id:a1"]);
  });

  it("refuses a single profile wider than the page limit", () => {
    const keysByProfile = new Map([
      ["profile-wide", ["k1", "k2", "k3"]],
    ]);

    expect(() => takeProfilePage(["profile-wide"], keysByProfile, 2)).toThrow(
      /profile-wide has 3 identifier keys/,
    );
  });

  it("defaults the page limit to the build batch limit", () => {
    const manyKeys = Array.from({ length: 400 }, (_, index) => `key:${index}`);
    const keysByProfile = new Map([
      ["profile-a", manyKeys],
      ["profile-b", ["anonymous_id:b1", "user_id:b2"]],
      ["profile-c", Array.from({ length: 200 }, (_, index) => `c:${index}`)],
    ]);

    const page = takeProfilePage(
      ["profile-a", "profile-b", "profile-c"],
      keysByProfile,
    );

    expect(page.profileIds).toEqual(["profile-a", "profile-b"]);
    expect(page.identifierKeys).toHaveLength(402);
    expect(JOURNEY_PROFILE_BATCH_LIMIT).toBe(500);
  });
});

describe("orphaned journey identifier keys", () => {
  it("returns only deleted keys that no current profile still owns", () => {
    const engine = {
      rows: [
        mappingRow("anonymous_id:kept", ["anonymous_id:kept", "user_id:kept"], 0),
        mappingRow("anonymous_id:moved", [], 1),
        mappingRow("user_id:kept", [], 1),
        mappingRow("anonymous_id:orphaned", [], 1),
      ],
    } as unknown as IdentityEngineResult;

    // anonymous_id:moved is deleted as its own mapping but absent from every
    // live row's membership, so it counts as orphaned; user_id:kept survives
    // inside the kept profile's member list.
    expect(orphanedJourneyIdentifierKeys(engine)).toEqual([
      "anonymous_id:moved",
      "anonymous_id:orphaned",
    ]);
  });
});

function mappingRow(
  identifierKey: string,
  memberIdentifierKeys: string[],
  isDeleted: number,
): Record<string, unknown> {
  return {
    identifier_key: identifierKey,
    member_identifier_keys: memberIdentifierKeys,
    is_deleted: isDeleted,
  };
}

function journeyRow(
  conversionId: string,
  profileId: string,
  touchpointId: string,
): Record<string, unknown> {
  return {
    conversion_id: conversionId,
    profile_id: profileId,
    touchpoint_id: touchpointId,
  };
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function ndjsonRows(value: string): Record<string, unknown>[] {
  return value.trim().split("\n").map((line) => (
    JSON.parse(line) as Record<string, unknown>
  ));
}

function identityFact(sourceSystem: string, sourceRecordId: string) {
  return {
    factPayload: JSON.stringify({
      source_system: sourceSystem,
      source_record_id: sourceRecordId,
    }),
  };
}
