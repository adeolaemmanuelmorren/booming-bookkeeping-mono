import { generateKeyPairSync, verify } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
  buildBackfillExportPlan,
  buildExportPlan,
  createGoogleAccessToken,
  runBigQueryExport,
  type BigQueryConfig,
  type Fetcher,
} from "../src/bigquery";
import {
  TABLE_MANIFEST,
  type SourceTable,
} from "../src/table-manifest.generated";

const runAt = new Date("2026-08-26T12:34:00.000Z");

describe("BigQuery export", () => {
  it("builds the immutable incremental path, time window, and Parquet-safe casts", () => {
    const plan = buildExportPlan(testTable(), runAt, {
      bucket: "booming-data",
      prefix: "tinybird",
      overlapMinutes: 20,
    });

    expect(plan.uri).toBe(
      "gs://booming-data/tinybird/example_data/order_events/incremental/run_date=2026-08-26/run_time=123400/part-*.parquet",
    );
    expect(plan.jobId).toBe(
      "tinybird_incremental_20260826123400_raw_example_order_events",
    );
    expect(plan.query).toContain("TO_JSON_STRING(`payload`) AS `payload`");
    expect(plan.query).toContain("ST_ASWKT(`location`) AS `location`");
    expect(plan.query).toContain("FROM `able-folio-499722.Example Data.Order Events`");
    expect(plan.query).toContain(
      "COALESCE(`updated_at`, TIMESTAMP '1970-01-01 00:00:00+00') >= TIMESTAMP '2026-08-26 12:14:00.000+00'",
    );
    expect(plan.query).toContain(
      "COALESCE(`updated_at`, TIMESTAMP '1970-01-01 00:00:00+00') < TIMESTAMP '2026-08-26 12:34:00.000+00'",
    );
    expect(plan.query).not.toContain("COALESCE(`version_id`");
  });

  it("exports ingestion-time partition metadata into a versioned object prefix", () => {
    const table = testTable();
    table.sourcePartitioning = {
      type: "DAY",
      field: null,
      pseudoColumn: "_PARTITIONTIME",
      bigqueryType: "TIMESTAMP",
    };
    const plan = buildExportPlan(table, runAt, exportConfig());

    expect(plan.uri).toBe(
      "gs://booming-data/tinybird/example_data/order_events/partition_time_v1/incremental/run_date=2026-08-26/run_time=123400/part-*.parquet",
    );
    expect(plan.query).toContain(
      "`_PARTITIONTIME` AS `source_partition_time`",
    );
  });

  it("reconciles one hourly shard of old ActiveCampaign assignment deletions", () => {
    const table = TABLE_MANIFEST.find(({ resourceName }) => (
      resourceName === "raw_activecampaign_contact_tag"
    ));
    if (!table) throw new Error("raw_activecampaign_contact_tag is missing");

    const plan = buildExportPlan(table, runAt, exportConfig());

    expect(plan.query).toContain("COALESCE(`_fivetran_deleted`, FALSE)");
    expect(plan.query).toContain("FARM_FINGERPRINT(CAST(`id` AS STRING))");
    expect(plan.query).toContain("TIMESTAMP '2026-08-26 12:34:00.000+00'");
    expect(plan.query).toContain("AS `_fivetran_synced`");
    expect(plan.query).toContain("  OR (");
  });

  it("exports all seven missing Stripe tables as one typed auxiliary union", () => {
    const table = TABLE_MANIFEST.find(({ resourceName }) => (
      resourceName === "raw_stripe_auxiliary"
    ));
    if (!table) throw new Error("raw_stripe_auxiliary is missing");

    const plan = buildExportPlan(table, runAt, exportConfig());

    expect(plan.uri).toBe(
      "gs://booming-data/tinybird/stripe_combined/auxiliary/incremental/run_date=2026-08-26/run_time=123400/part-*.parquet",
    );
    expect(plan.query.match(/UNION ALL/g)).toHaveLength(6);
    expect(plan.query).toContain("FROM `able-folio-499722.stripe.subscription_history`");
    expect(plan.query).toContain("FROM `able-folio-499722.stripe_kajabi.subscription_history`");
    expect(plan.query).toContain("FROM `able-folio-499722.stripe_kajabi.checkout_session`");
    expect(plan.query).toContain("FROM `able-folio-499722.stripe_kajabi.checkout_session_line_item`");
    expect(plan.query).toContain("FROM `able-folio-499722.stripe_kajabi.price`");
    expect(plan.query).toContain("FROM `able-folio-499722.stripe_kajabi.plan`");
    expect(plan.query).toContain("FROM `able-folio-499722.stripe_kajabi.product`");
    expect(plan.query).toContain("'subscription_history' AS `record_type`");
    expect(plan.query).toContain("'checkout_session_line_item' AS `record_type`");
    expect(plan.query).toContain("CAST(NULL AS TIMESTAMP) AS `subscription_started_at`");
    expect(plan.query).toContain("`amount_total` AS `amount_total_cents`");
    expect(plan.query).toContain("CAST(NULL AS FLOAT64) AS `amount_total_cents`");
  });

  it("builds one full auxiliary backfill without incremental predicates", () => {
    const table = TABLE_MANIFEST.find(({ resourceName }) => (
      resourceName === "raw_stripe_auxiliary"
    ));
    if (!table) throw new Error("raw_stripe_auxiliary is missing");

    const plan = buildBackfillExportPlan(table, runAt, exportConfig());

    expect(plan.uri).toBe(
      "gs://booming-data/tinybird/stripe_combined/auxiliary/backfill/snapshot_at=20260826123400/part-*.parquet",
    );
    expect(plan.jobId).toBe(
      "tinybird_backfill_20260826123400_raw_stripe_auxiliary",
    );
    expect(plan.query.match(/UNION ALL/g)).toHaveLength(6);
    expect(plan.query).not.toContain("WHERE COALESCE(");
  });

  it("reuses a deterministic job after a duplicate insert and polls until DONE", async () => {
    const responses = [
      new Response("duplicate", { status: 409 }),
      Response.json({ status: { state: "RUNNING" } }),
      Response.json({ status: { state: "DONE" } }),
    ];
    const fetcher = vi.fn<Fetcher>(async () => responses.shift()!);
    const sleep = vi.fn(async () => undefined);

    await runBigQueryExport(
      buildExportPlan(testTable(), runAt, exportConfig()),
      bigQueryConfig(),
      "google-token",
      fetcher,
      sleep,
    );

    expect(fetcher).toHaveBeenCalledTimes(3);
    expect(sleep).toHaveBeenCalledOnce();
    const insert = fetcher.mock.calls[0];
    expect(insert[1]?.method).toBe("POST");
    expect(insert[1]?.headers).toMatchObject({
      Authorization: "Bearer google-token",
    });
  });

  it("fails a completed BigQuery job that has an error result", async () => {
    const fetcher = vi.fn<Fetcher>(async (_input, init) => {
      if (init?.method === "POST") return Response.json({});

      return Response.json({
        status: {
          state: "DONE",
          errorResult: { reason: "accessDenied", message: "bucket denied" },
        },
      });
    });

    await expect(runBigQueryExport(
      buildExportPlan(testTable(), runAt, exportConfig()),
      bigQueryConfig(),
      "google-token",
      fetcher,
    )).rejects.toThrow("accessDenied: bucket denied");
  });
});

describe("Google service-account OAuth", () => {
  it("signs an RS256 assertion and exchanges it for one access token", async () => {
    const { privateKey, publicKey } = generateKeyPairSync("rsa", {
      modulusLength: 2_048,
    });
    const privateKeyPem = privateKey.export({
      type: "pkcs8",
      format: "pem",
    }).toString();
    let assertion = "";
    const fetcher = vi.fn<Fetcher>(async (input, init) => {
      expect(String(input)).toBe("https://oauth2.googleapis.com/token");
      const body = new URLSearchParams(String(init?.body));
      assertion = body.get("assertion") || "";
      expect(body.get("grant_type")).toBe(
        "urn:ietf:params:oauth:grant-type:jwt-bearer",
      );

      return Response.json({ access_token: "google-access-token" });
    });

    const token = await createGoogleAccessToken(JSON.stringify({
      client_email: "sync@able-folio-499722.iam.gserviceaccount.com",
      private_key: privateKeyPem,
      private_key_id: "key-1",
      token_uri: "https://oauth2.googleapis.com/token",
    }), fetcher, runAt);

    const [encodedHeader, encodedClaims, encodedSignature] = assertion.split(".");
    const unsigned = `${encodedHeader}.${encodedClaims}`;
    const header = decodeJwtPart(encodedHeader);
    const claims = decodeJwtPart(encodedClaims);

    expect(token).toBe("google-access-token");
    expect(header).toMatchObject({ alg: "RS256", typ: "JWT", kid: "key-1" });
    expect(claims).toMatchObject({
      aud: "https://oauth2.googleapis.com/token",
      iss: "sync@able-folio-499722.iam.gserviceaccount.com",
      scope: "https://www.googleapis.com/auth/cloud-platform",
    });
    expect(verify(
      "RSA-SHA256",
      Buffer.from(unsigned),
      publicKey,
      Buffer.from(encodedSignature, "base64url"),
    )).toBe(true);
  });
});

function testTable(): SourceTable {
  return {
    resourceName: "raw_example_order_events",
    exportKind: "table",
    source: {
      project: "able-folio-499722",
      dataset: "Example Data",
      table: "Order Events",
    },
    unionSources: [],
    sourcePartitioning: null,
    versionColumns: [
      { name: "updated_at", bigqueryType: "TIMESTAMP" },
      { name: "version_id", bigqueryType: "STRING" },
    ],
    watermarkColumns: [
      { name: "updated_at", bigqueryType: "TIMESTAMP" },
    ],
    columns: ["id", "updated_at", "version_id", "payload", "location"],
    columnTypes: {
      id: "STRING",
      updated_at: "TIMESTAMP",
      version_id: "STRING",
      payload: "JSON",
      location: "GEOGRAPHY",
    },
    jsonColumns: ["payload"],
    geographyColumns: ["location"],
  };
}

function exportConfig() {
  return {
    bucket: "booming-data",
    prefix: "tinybird",
    overlapMinutes: 20,
  };
}

function bigQueryConfig(): BigQueryConfig {
  return {
    projectId: "able-folio-499722",
    location: "US",
    pollIntervalMs: 5,
    jobTimeoutMs: 50,
  };
}

function decodeJwtPart(value: string): Record<string, unknown> {
  return JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
}
