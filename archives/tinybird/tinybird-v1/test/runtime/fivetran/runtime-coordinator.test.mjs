import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

import { buildActiveCampaignFivetranScope } from "../../../worker/fivetran/activecampaign-fivetran.ts";

const { buildActiveCampaignContactReplacement } = await import(
  "../../../worker/conversions/activecampaign.mjs"
);

const directory = dirname(fileURLToPath(import.meta.url));
const bundled = await build({
  entryPoints: [join(directory, "test-worker.ts")],
  bundle: true,
  write: false,
  format: "esm",
  platform: "browser",
  target: "es2022",
  external: ["cloudflare:workers", "node:crypto"],
});
const script = bundled.outputFiles[0].text;
const SNAPSHOT_ID = "fivetran-20260905T222800Z";
const SNAPSHOT_AT = "2026-09-05T22:28:00.000000Z";
const SCOPE_ID = "activecampaign:contact:100";
const ALL_SOURCES = [
  "raw_activecampaign_contact",
  "raw_activecampaign_contact_tag",
  "raw_activecampaign_tags",
  "raw_stripe_charge",
  "raw_stripe_customer",
  "raw_stripe_kajabi_charge",
  "raw_stripe_kajabi_customer",
  "raw_stripe_kajabi_payment_intent",
  "raw_stripe_payment_intent",
];

test("seeded baseline survives restart, then an alarm deletes and resurrects it", async (t) => {
  const runtime = await createRuntime(t);
  const prepared = baselinePreparedScope();
  await runtime.call("/initialize", {
    pipeline: "activecampaign",
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
  });
  await runtime.call("/seed", {
    pipeline: "activecampaign",
    snapshotAt: SNAPSHOT_AT,
    prepared: [prepared],
  });
  await runtime.call("/checkpoint-save", {
    pipeline: "activecampaign",
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
    afterCursor: SCOPE_ID,
    complete: true,
    publishedScopes: 1,
    identityFactCount: 1,
  });
  const finalized = await runtime.call("/finalize", {
    pipeline: "activecampaign",
    snapshotAt: SNAPSHOT_AT,
  });
  assert.equal(finalized.bootstrapComplete, true);
  assert.equal(finalized.nextAlarmAt, null);

  await runtime.restart();
  const restarted = await runtime.call("/status");
  assert.equal(restarted.storedScopes, 1);
  assert.equal(restarted.activated, false);
  assert.equal(restarted.nextAlarmAt, null);
  const checkpoint = await runtime.call("/checkpoint-load", {
    pipeline: "activecampaign",
    snapshotId: SNAPSHOT_ID,
  });
  assert.equal(checkpoint.complete, true);

  runtime.tinybird.stage = "deleted";
  await runtime.call("/start", {
    pipeline: "activecampaign",
    snapshotId: SNAPSHOT_ID,
    activationId: "browser-baseline-v1",
  });
  await waitFor(async () => {
    const status = await runtime.call("/status");
    return runtime.publications.length === 1 && !status.running;
  });
  assert.equal(runtime.publications.length, 1);
  assert.equal(runtime.publications[0][0].observation_sequence, 2);
  assert.equal(runtime.publications[0][0].rows[0].is_deleted, true);

  runtime.tinybird.stage = "resurrected";
  await runtime.call("/run");
  assert.equal(runtime.publications.length, 2);
  assert.equal(runtime.publications[1][0].observation_sequence, 3);
  assert.equal(runtime.publications[1][0].rows[0].is_deleted, false);
  assert.equal(runtime.publications[1][0].rows[0].form_submission_id, "assignment-primary");

  await runtime.call("/clear-alarm", {});
  await runtime.restart();
  assert.ok((await runtime.call("/status")).nextAlarmAt);

  await assert.rejects(
    runtime.call("/seed", {
      pipeline: "activecampaign",
      snapshotAt: SNAPSHOT_AT,
      prepared: [prepared],
    }),
    /closed after activation/,
  );
});

async function createRuntime(t) {
  const storage = await mkdtemp(join(tmpdir(), "fivetran-fact-runtime-"));
  const tinybird = fakeTinybird();
  const publications = [];
  const options = {
    ...convertV4MiniflareOptions({
      name: "fivetran-fact-test",
      modules: true,
      script,
      compatibilityDate: "2026-09-05",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: {
        FACTS: { className: "TestFivetranFactCoordinator", useSQLite: true },
      },
      bindings: {
        TENANT_ID: "boom",
        TINYBIRD_URL: "https://api.us-east.tinybird.co",
        TINYBIRD_TOKEN: "local-test-only",
        FIVETRAN_SNAPSHOT_AT: SNAPSHOT_AT,
        INGESTION_ENABLED: "true",
      },
      serviceBindings: {
        SOURCE_PUBLISHER: {
          name: "fivetran-fact-test",
          entrypoint: "TestSourcePublisher",
        },
        TEST_CONTROL: async (request) => {
          publications.push(await request.json());
          return Response.json({ accepted: true });
        },
      },
      outboundService: (request) => tinybird.fetch(request),
    }),
    unsafeInspectDurableObjects: true,
    // Miniflare 5 drops the old durableObjectsPersist setting.
    isolatedResourcePersistencePath: storage,
    resourcePersistencePath: storage,
  };
  let miniflare = new Miniflare(options);
  t.after(async () => {
    await miniflare.dispose();
    await rm(storage, { recursive: true, force: true });
  });

  return {
    tinybird,
    publications,
    async call(path, body) {
      const response = await miniflare.dispatchFetch(
        `https://test.invalid${path}?pipeline=activecampaign`,
        {
          method: body === undefined ? "GET" : "POST",
          body: body === undefined ? undefined : JSON.stringify(body),
        },
      );
      if (!response.ok) throw new Error(await response.text());
      return response.json();
    },
    async restart() {
      await miniflare.dispose();
      miniflare = new Miniflare(options);
    },
  };
}

function fakeTinybird() {
  return {
    stage: "snapshot",
    async fetch(request) {
      const url = new URL(request.url);
      assert.equal(url.pathname, "/v0/sql");
      const sql = new URLSearchParams(await request.text()).get("q");

      if (sql.includes("FROM v1_fivetran_source_receipts")) {
        const through = this.stage === "resurrected"
          ? "2026-09-06 00:28:00.000000"
          : "2026-09-05 23:28:00.000000";
        return rows([{
          complete_through_exclusive: through,
          source_count: 9,
          manifest_json: JSON.stringify(
            ALL_SOURCES.map((source_name) => ({ source_name })),
          ),
        }]);
      }
      if (sql.includes("FROM v1_fivetran_reconciliation_receipts")) {
        return rows([]);
      }
      if (sql.includes("SELECT concat") || sql.includes("WITH changed_tags")) {
        if (
          sql.includes("FROM v1_fivetran_activecampaign_contact_tag") &&
          !sql.includes("WITH changed_tags")
        ) {
          return rows([{ scope_id: SCOPE_ID }]);
        }
        return rows([]);
      }
      if (sql.includes("FROM v1_snapshot_activecampaign_contact_tag")) {
        return rows([snapshotAssignment()]);
      }
      if (sql.includes("FROM v1_fivetran_activecampaign_contact_tag")) {
        return rows(this.stage === "resurrected"
          ? [reconciliationDelete(), resurrection()]
          : [reconciliationDelete()]);
      }
      if (sql.includes("FROM v1_snapshot_activecampaign_contact")) {
        return rows([snapshotContact()]);
      }
      if (sql.includes("FROM v1_fivetran_activecampaign_contact")) {
        return rows([]);
      }
      if (sql.includes("FROM v1_snapshot_activecampaign_tags")) {
        return rows([snapshotTag()]);
      }
      if (sql.includes("FROM v1_fivetran_activecampaign_tags")) {
        return rows([]);
      }
      throw new Error(`Unexpected Tinybird query: ${sql}`);
    },
  };
}

function rows(data) {
  return Response.json({ data });
}

async function waitFor(predicate, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for the Durable Object alarm");
}

function baselinePreparedScope() {
  return buildActiveCampaignFivetranScope({
    raw: {
      contactId: "100",
      contactVersions: [snapshotContact()],
      assignmentVersions: [snapshotAssignment()],
      referencedTagVersions: [snapshotTag()],
      evidenceRecordIds: ["contact:100", "assignment:assignment-primary", "tag:11"],
    },
    throughInclusive: SNAPSHOT_AT,
    windowId: `bootstrap:${SNAPSHOT_ID}`,
    observationSequence: 1,
    previous: null,
    normalizeContactReplacement: buildActiveCampaignContactReplacement,
    snapshotOnly: true,
  });
}

function snapshotContact() {
  return {
    id: 100,
    email: "person@example.com",
    first_name: "Ada",
    last_name: "Person",
    deleted: 0,
    _fivetran_deleted: false,
    _fivetran_synced: "2026-09-05T22:26:00.000000Z",
  };
}

function snapshotAssignment() {
  return {
    id: "assignment-primary",
    contact: 100,
    tags: 11,
    c_date: "2026-08-02T18:04:05.000000Z",
    _fivetran_deleted: false,
    _fivetran_synced: "2026-09-05T22:27:00.000000Z",
  };
}

function snapshotTag() {
  return {
    id: "11",
    tags: "[KRC] Registered - August 2026",
    _fivetran_deleted: false,
    _fivetran_synced: "2026-09-05T22:27:30.000000Z",
  };
}

function reconciliationDelete() {
  return {
    ...snapshotAssignment(),
    _fivetran_deleted: true,
    _v1_observed_at: "2026-09-05T23:28:00.000000Z",
    _v1_observation_kind: "deletion_reconciliation",
  };
}

function resurrection() {
  return {
    ...snapshotAssignment(),
    _fivetran_deleted: false,
    _fivetran_synced: "2026-09-06T00:20:00.000000Z",
    _v1_observed_at: "2026-09-06T00:28:00.000000Z",
    _v1_observation_kind: "incremental",
  };
}
