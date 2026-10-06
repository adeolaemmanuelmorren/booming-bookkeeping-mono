import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { mkdtemp, readFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Miniflare } from "../node_modules/@cloudflare/vitest-pool-workers/node_modules/miniflare/dist/src/index.js";
const key = generateKeyPairSync("rsa", { modulusLength: 2048 })
  .privateKey.export({ type: "pkcs8", format: "pem" })
  .toString();
const remote = {
  bq: "done",
  receipt: false,
  job: "working",
  throwSchedule: false,
  schedules: 0,
  throwInsert: false,
  missingLookup: false,
  inserts: 0,
  lastSubmittedQuery: "",
  lastSubmittedJobId: "",
};
const persistencePath = await mkdtemp(join(tmpdir(), "fivetran-miniflare-"));
const options = {
  modules: true,
  script: await readFile(
    process.env.TEST_BUNDLE ?? "/private/tmp/boom-v1-integrated-fivetran-bundle/index.js",
    "utf8",
  ),
  compatibilityDate: "2026-08-26",
  compatibilityFlags: ["nodejs_compat"],
  bindings: {
    BIGQUERY_PROJECT_ID: "able-folio-499722",
    BIGQUERY_LOCATION: "US",
    GCS_BUCKET: "booming-data",
    GCS_PREFIX: "tinybird/v1-live",
    COMPLETE_THROUGH: "2026-09-05T22:28:00.000Z",
    GCP_SERVICE_ACCOUNT_JSON: JSON.stringify({
      client_email: "test@example.iam.gserviceaccount.com",
      private_key: key,
      token_uri: "https://oauth2.googleapis.com/token",
    }),
    TINYBIRD_API_URL: "https://api.us-east.tinybird.co",
    TINYBIRD_ADMIN_TOKEN: "test",
    EXPORT_OVERLAP_MINUTES: "20",
    BIGQUERY_POLL_INTERVAL_MS: "5000",
    BIGQUERY_JOB_TIMEOUT_MS: "480000",
    TEST_MODE: "true",
    CHANGE_HISTORY_ENABLED_AT: "2026-09-05T23:38:46.234Z",
    SOURCE_BRIDGE_THROUGH: "2026-09-05T23:42:00.000Z",
    SOURCE_CREATED_AT_JSON: JSON.stringify({
      raw_activecampaign_contact: "2026-07-15T09:40:22.635Z",
      raw_activecampaign_contact_tag: "2026-07-15T09:40:41.170Z",
      raw_activecampaign_tags: "2026-07-16T04:07:17.322Z",
      raw_stripe_charge: "2026-06-23T17:56:17.085Z",
      raw_stripe_customer: "2026-06-23T17:56:17.834Z",
      raw_stripe_kajabi_charge: "2026-07-22T05:24:50.089Z",
      raw_stripe_kajabi_customer: "2026-07-22T05:24:55.757Z",
      raw_stripe_kajabi_payment_intent: "2026-07-22T05:24:50.928Z",
      raw_stripe_payment_intent: "2026-06-23T17:56:16.745Z",
    }),
  },
  durableObjects: {
    SOURCE_EXPORT_COORDINATOR: {
      className: "SourceExportCoordinator",
      useSQLite: true,
    },
  },
  durableObjectsPersist: persistencePath,
  outboundService: async (request) => {
    const u = new URL(request.url);
    if (u.hostname === "oauth2.googleapis.com")
      return Response.json({ access_token: "token" });
    if (u.hostname === "bigquery.googleapis.com" && request.method === "POST") {
      remote.inserts++;
      const body = await request.clone().json();
      remote.lastSubmittedQuery = body.configuration?.query?.query ?? "";
      remote.lastSubmittedJobId = body.jobReference?.jobId ?? "";
      if (remote.throwInsert) {
        remote.throwInsert = false;
        throw new Error("lost insert acknowledgement");
      }
      return Response.json({});
    }
    if (u.hostname === "bigquery.googleapis.com") {
      if (remote.missingLookup) {
        remote.missingLookup = false;
        return new Response("", { status: 404 });
      }
      return Response.json(
        remote.bq === "done"
          ? {
              jobReference: {
                jobId: remote.lastSubmittedJobId,
                projectId: "able-folio-499722",
                location: "US",
              },
              configuration: { query: { query: remote.lastSubmittedQuery } },
              status: { state: "DONE" },
              statistics: {
                query: {
                  exportDataStatistics: { fileCount: "1", rowCount: "2" },
                },
              },
            }
          : {
              jobReference: {
                jobId: remote.lastSubmittedJobId,
                projectId: "able-folio-499722",
                location: "US",
              },
              configuration: { query: { query: remote.lastSubmittedQuery } },
              status: { state: "RUNNING" },
            },
      );
    }
    if (u.pathname === "/v0/sql")
      return Response.json({
        data: [
          remote.receipt
            ? { files: 1, rows: 2, quarantine: 0 }
            : { files: 0, rows: 0, quarantine: 0 },
        ],
      });
    if (u.pathname.includes("/scheduling/runs")) {
      remote.schedules++;
      if (remote.throwSchedule) throw new Error("ambiguous");
      return Response.json({ job: { job_id: "import-1" } });
    }
    if (u.pathname.includes("/v0/jobs/"))
      return Response.json({ status: remote.job });
    throw new Error(`unexpected ${request.method} ${u}`);
  },
};
let mf = new Miniflare(options);
try {
  const ns = await mf.getDurableObjectNamespace("SOURCE_EXPORT_COORDINATOR");
  const first = ns.get(ns.idFromName("first"));
  const initial = await first.status();
  assert.equal(initial.phase, "idle");
  const initialCompletedThrough = initial.completedThrough;
  await mf.dispose();
  mf = new Miniflare(options);
  const restartedNamespace = await mf.getDurableObjectNamespace(
    "SOURCE_EXPORT_COORDINATOR",
  );
  const restarted = restartedNamespace.get(
    restartedNamespace.idFromName("first"),
  );
  assert.equal(
    (await restarted.status()).completedThrough,
    initialCompletedThrough,
  );
  const long = restartedNamespace.get(restartedNamespace.idFromName("long"));
  remote.bq = "pending";
  await long.status();
  await long.runTestStep();
  assert.match(remote.lastSubmittedQuery, /CREATE TEMP TABLE at_start/);
  await long.runTestStep();
  assert.equal((await long.status()).phase, "bq_pending");

  const lostInsert = restartedNamespace.get(
    restartedNamespace.idFromName("lost-insert"),
  );
  remote.bq = "done";
  remote.throwInsert = true;
  remote.missingLookup = true;
  const insertsBefore = remote.inserts;
  await lostInsert.status();
  await lostInsert.runTestStep().catch(() => {});
  assert.equal((await lostInsert.status()).phase, "bq_pending");
  await lostInsert.runTestStep();
  assert.equal(remote.inserts - insertsBefore, 2);
  const ambiguous = restartedNamespace.get(
    restartedNamespace.idFromName("ambiguous"),
  );
  remote.bq = "done";
  remote.throwSchedule = true;
  await ambiguous.status();
  await ambiguous.runTestStep();
  await ambiguous.runTestStep();
  await ambiguous.runTestStep().catch(() => {});
  assert.equal((await ambiguous.status()).phase, "import_unknown");
  remote.throwSchedule = false;
  remote.receipt = true;
  await ambiguous.runTestStep();
  assert.equal(
    (await ambiguous.status()).completedThrough,
    "2026-09-05T22:28:00.000Z",
  );
  const failed = restartedNamespace.get(
    restartedNamespace.idFromName("failed"),
  );
  remote.receipt = false;
  remote.job = "error";
  await failed.status();
  await failed.runTestStep();
  await failed.runTestStep();
  await failed.runTestStep();
  await failed.runTestStep().catch(() => {});
  assert.equal(
    (await failed.status()).completedThrough,
    "2026-09-05T22:28:00.000Z",
  );
  assert.ok(remote.schedules <= 2);
  await mf.dispose();
  remote.lastSubmittedQuery = "";
  mf = new Miniflare({
    ...options,
    bindings: {
      ...options.bindings,
      COMPLETE_THROUGH: "2026-09-05T23:42:00.000Z",
    },
    durableObjectsPersist: await mkdtemp(
      join(tmpdir(), "fivetran-miniflare-changes-"),
    ),
  });
  const changesNamespace = await mf.getDurableObjectNamespace(
    "SOURCE_EXPORT_COORDINATOR",
  );
  const changes = changesNamespace.get(changesNamespace.idFromName("changes"));
  await changes.status();
  await changes.runTestStep();
  assert.match(remote.lastSubmittedQuery, /FROM CHANGES\(TABLE/);
  assert.doesNotMatch(remote.lastSubmittedQuery, /CREATE TEMP TABLE at_start/);
  const callsBeforeDisable = remote.inserts;
  await mf.dispose();
  const { TEST_MODE: _testMode, ...productionBindings } = options.bindings;
  mf = new Miniflare({
    ...options,
    bindings: { ...productionBindings, INGESTION_ENABLED: "false" },
    durableObjectsPersist: await mkdtemp(
      join(tmpdir(), "fivetran-miniflare-disabled-"),
    ),
  });
  const disabledNamespace = await mf.getDurableObjectNamespace(
    "SOURCE_EXPORT_COORDINATOR",
  );
  await disabledNamespace.get(disabledNamespace.idFromName("disabled")).start();
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(remote.inserts, callsBeforeDisable);
  console.log("Miniflare runtime tests passed");
} finally {
  await mf.dispose();
}
