import { describe, expect, it, vi } from "vitest";
import {
  appendIdentityActivation,
  appendIdentityPendingFacts,
  countActiveIngestionJobs,
  getCopyJob,
  readCurrentIdentityFacts,
  readIdentityCompactionCursor,
  readIdentityCompactionManifest,
  readSourceIdentityFacts,
  submitCopyJob,
  type IdentityCompactionManifest,
  type TinybirdApiConfig,
} from "../src/tinybird-api";

const config: TinybirdApiConfig = {
  apiUrl: "https://api.us-east.tinybird.co",
  adminToken: "test-admin-token",
  fetchTimeoutMs: 500,
};

describe("Tinybird Copy API", () => {
  it("submits a Copy with the Tinybird admin token and returns its job ID", async () => {
    const fetcher = vi.fn(async (_input: Request | string | URL, init?: RequestInit) => {
      expect(init?.method).toBe("POST");
      expect(new Headers(init?.headers).get("Authorization")).toBe(
        "Bearer test-admin-token",
      );
      return Response.json({ job: { job_id: "copy-job-1" } });
    });

    await expect(submitCopyJob(
      "snapshot_mart_payments",
      config,
      {},
      fetcher,
    )).resolves.toBe("copy-job-1");
    expect(String(fetcher.mock.calls[0][0])).toBe(
      "https://api.us-east.tinybird.co/v0/pipes/snapshot_mart_payments/copy",
    );
  });

  it("sends sorted, URL-encoded Copy parameters", async () => {
    const fetcher = vi.fn(async (_input: Request | string | URL) => Response.json({
      job: { job_id: "copy-job-parameterized" },
    }));

    await expect(submitCopyJob(
      "enqueue_identity_changes",
      config,
      {
        p_source_ingested_from: "2026-08-26 12:14:00",
        p_identity_producer: "source_identity:segment_page_view",
      },
      fetcher,
    )).resolves.toBe("copy-job-parameterized");
    expect(String(fetcher.mock.calls[0][0])).toBe(
      "https://api.us-east.tinybird.co/v0/pipes/enqueue_identity_changes/copy"
      + "?p_identity_producer=source_identity%3Asegment_page_view"
      + "&p_source_ingested_from=2026-08-26+12%3A14%3A00",
    );
  });

  it("polls a submitted Copy job until the coordinator sees a terminal status", async () => {
    const fetcher = vi.fn(async (_input: Request | string | URL) => Response.json({
      status: "done",
      progress_percentage: 100,
    }));

    await expect(getCopyJob("copy-job-1", config, fetcher)).resolves.toEqual({
      id: "copy-job-1",
      status: "done",
      error: undefined,
    });
  });

  it("waits for active GCS and import jobs", async () => {
    const fetcher = vi.fn(async (input: Request | string | URL) => {
      const url = new URL(String(input));
      const active = url.searchParams.get("status") === "working"
        && url.searchParams.get("kind") === "import";
      return Response.json({ jobs: active ? [{ id: "import-1" }] : [] });
    });

    await expect(countActiveIngestionJobs(config, fetcher)).resolves.toBe(1);
    expect(fetcher).toHaveBeenCalledTimes(4);
  });

  it("reads the last activated identity cursor", async () => {
    const fetcher = vi.fn(async (_input: Request | string | URL) => Response.json({
      data: [{
        active_batch_version: "1724691600000",
        active_batch_id: "batch-1724691600000",
        checkpoint_ingested_at: "2026-08-26 12:14:00.000000",
        checkpoint_event_id: "a".repeat(64),
      }],
    }));

    await expect(readIdentityCompactionCursor("boom", config, fetcher)).resolves.toEqual({
      activeBatchVersion: 1724691600000,
      activeBatchId: "batch-1724691600000",
      checkpointIngestedAt: "2026-08-26 12:14:00.000000",
      checkpointEventId: "a".repeat(64),
    });
    expect(String(fetcher.mock.calls[0][0])).toBe(
      "https://api.us-east.tinybird.co/v0/pipes/current_identity_compaction_cursor.json"
      + "?p_tenant_id=boom",
    );
  });

  it("passes literal fact keys as Tinybird array parameters", async () => {
    const fetcher = vi.fn(async (_input: Request | string | URL) => Response.json({
      data: [{
        producer_id: "fixture",
        fact_kind: "identity_observation",
        fact_key: "fact-a",
        source_fact_version: 1,
        fact_deleted: 0,
        fact_observed_at: "2026-08-26 12:14:00.000000",
        fact_payload_hash: "a".repeat(64),
        evidence_keys: ["email:a@example.com"],
        first_name: "Ada",
        last_name: "Lovelace",
        is_deleted: 0,
      }],
    }));

    await expect(readCurrentIdentityFacts(
      "boom",
      ["fact-b", "fact-a"],
      config,
      fetcher,
    )).resolves.toHaveLength(1);
    const url = new URL(String(fetcher.mock.calls[0][0]));
    expect(url.searchParams.get("p_fact_keys")).toBe("fact-a,fact-b");
    expect(url.searchParams.get("p_state_keys")).toBe("fact:fact-a,fact:fact-b");
  });

  it("pages normalized source facts and appends their complete queue rows", async () => {
    const payload = JSON.stringify({
      anonymous_id: "anon-1",
      user_id: "user@example.com",
      email: "user@example.com",
      phone: "+18085550101",
      first_name: "Ada",
      last_name: "Lovelace",
    });
    const sourceRow = {
      event_id: "e".repeat(64),
      producer_id: "source_identity:segment_identify",
      observed_at: "2026-08-28 11:00:00.000000",
      ingested_at: "2026-08-28 11:40:00.000000",
      fact_kind: "identity_observation",
      fact_key: "16:segment_identify:6:fact-1",
      source_fact_version: 42,
      fact_deleted: 0,
      fact_payload_hash: "f".repeat(64),
      fact_payload: payload,
      evidence_keys: ["email:user@example.com"],
    };
    const fetcher = vi.fn(async (_input: Request | string | URL, init?: RequestInit) => {
      if (init?.method !== "POST") return Response.json({ data: [sourceRow] });

      const event = JSON.parse(String(init.body).trim()) as Record<string, unknown>;
      expect(event).toMatchObject({
        tenant_id: "boom",
        event_id: sourceRow.event_id,
        email: "user@example.com",
        first_name: "Ada",
        fact_key: sourceRow.fact_key,
      });
      return Response.json({ successful_rows: 1 });
    });

    const facts = await readSourceIdentityFacts(
      sourceRow.producer_id,
      "2026-08-28 10:54:00",
      "prior-fact",
      500,
      config,
      fetcher,
    );
    await appendIdentityPendingFacts(facts, config, fetcher);

    const sourceUrl = new URL(String(fetcher.mock.calls[0][0]));
    expect(sourceUrl.pathname).toBe("/v0/pipes/identity_worker_source_facts.json");
    expect(sourceUrl.searchParams.get("p_after_fact_key")).toBe("prior-fact");
    expect(sourceUrl.searchParams.get("p_identity_producer")).toBe(sourceRow.producer_id);
    expect(String(fetcher.mock.calls[1][0])).toContain(
      "/v0/events?name=identity_events_cursor&wait=true",
    );
  });

  it("validates the exact identity compaction batch manifest", async () => {
    const manifest = testManifest();
    const fetcher = vi.fn(async (_input: Request | string | URL) => Response.json({
      data: [{
        tenant_id: manifest.tenantId,
        batch_version: String(manifest.batchVersion),
        batch_id: manifest.batchId,
        input_event_count: String(manifest.inputEventCount),
        input_hash: manifest.inputHash,
        checkpoint_ingested_at: manifest.checkpointIngestedAt,
        checkpoint_event_id: manifest.checkpointEventId,
        expected_output_row_count: String(manifest.expectedOutputRowCount),
        expected_output_hash: manifest.expectedOutputHash,
        actual_output_row_count: String(manifest.actualOutputRowCount),
        actual_output_hash: manifest.actualOutputHash,
        is_valid: 1,
      }],
    }));

    await expect(readIdentityCompactionManifest(
      manifest.tenantId,
      manifest.batchVersion,
      manifest.batchId,
      config,
      fetcher,
    )).resolves.toEqual(manifest);
    expect(String(fetcher.mock.calls[0][0])).toContain(
      "/v0/pipes/identity_compaction_manifest.json?",
    );
  });

  it("appends one complete activation marker with wait=true", async () => {
    const manifest = testManifest();
    const fetcher = vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      if (init?.method !== "POST") {
        return Response.json({
          data: [{
            active_batch_version: manifest.batchVersion,
            active_batch_id: manifest.batchId,
            checkpoint_ingested_at: manifest.checkpointIngestedAt,
            checkpoint_event_id: manifest.checkpointEventId,
          }],
        });
      }

      expect(init?.method).toBe("POST");
      expect(new Headers(init?.headers).get("Content-Type")).toContain(
        "application/x-ndjson",
      );
      const event = JSON.parse(String(init?.body).trim()) as Record<string, unknown>;
      expect(event).toMatchObject({
        tenant_id: "boom",
        state_kind: "activation_audit",
        batch_version: manifest.batchVersion,
        batch_id: manifest.batchId,
        checkpoint_event_id: manifest.checkpointEventId,
        output_row_count: manifest.actualOutputRowCount,
      });
      return Response.json({ successful_rows: 1 });
    });

    await expect(appendIdentityActivation(
      { tenantId: "boom", manifest },
      config,
      fetcher,
    )).resolves.toBeUndefined();
    expect(String(fetcher.mock.calls[0][0])).toBe(
      "https://api.us-east.tinybird.co/v0/events"
      + "?name=identity_state_delta_versions&wait=true",
    );
    expect(String(fetcher.mock.calls[1][0])).toBe(
      "https://api.us-east.tinybird.co/v0/pipes/current_identity_compaction_cursor.json"
      + "?p_tenant_id=boom",
    );
  });
});

function testManifest(): IdentityCompactionManifest {
  return {
    tenantId: "boom",
    batchVersion: 1724691600001,
    batchId: "generation_raw_20260826121400000_slot_1_identity_1724691600001",
    inputEventCount: 17,
    inputHash: "b".repeat(64),
    checkpointIngestedAt: "2026-08-26 12:14:59.123456",
    checkpointEventId: "c".repeat(64),
    expectedOutputRowCount: 43,
    expectedOutputHash: "d".repeat(64),
    actualOutputRowCount: 43,
    actualOutputHash: "d".repeat(64),
    isValid: true,
  };
}
