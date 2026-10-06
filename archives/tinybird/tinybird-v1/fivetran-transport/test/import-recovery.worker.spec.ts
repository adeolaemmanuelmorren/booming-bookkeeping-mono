import { createScheduledController, env, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SourceExportCoordinator, Env as CoordinatorEnv } from "../src/coordinator";
import worker from "../src/index";
import { SOURCE_TABLES, landingName } from "../src/sources";

const completedThrough = "2026-09-05T22:00:00.000Z";
const targetEnd = "2026-09-05T23:00:00.000Z";
const resource = landingName(SOURCE_TABLES[0].resourceName);
const plan = {
  resourceName: SOURCE_TABLES[0].resourceName,
  jobId: "synthetic-saved-export",
  query: "synthetic immutable export query",
  uri: "gs://synthetic-bucket/recovery/part-*.parquet",
};
type Stub = DurableObjectStub<SourceExportCoordinator>;
type Job = { id: string; datasource: { name: string } };
type RequestSummary = { method: string; url: URL };
let requests: RequestSummary[];
let jobs: Job[];
let importStatus: string;
let receipt: { files: number; rows: number; quarantine: number };

beforeEach(() => {
  requests = [];
  jobs = [];
  importStatus = "working";
  receipt = { files: 0, rows: 0, quarantine: 0 };
  // Every outbound request is intercepted. Unexpected methods and paths fail.
  vi.stubGlobal("fetch", vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const method = init?.method ?? (input instanceof Request ? input.method : "GET");
    requests.push({ method, url });
    if (method !== "GET" || url.origin !== "https://api.us-east.tinybird.co") {
      throw new Error(`Unexpected outbound ${method} ${url.origin}${url.pathname}`);
    }
    if (url.pathname === "/v0/sql") {
      expect(url.searchParams.get("q")).toMatch(/FORMAT JSON$/);
      return Response.json({ data: [receipt] });
    }
    if (url.pathname === "/v0/jobs") return Response.json({ jobs });
    if (url.pathname === "/v0/jobs/known-import") return Response.json({ status: importStatus });
    throw new Error(`Unexpected outbound path ${url.pathname}`);
  }));
});

afterEach(() => vi.unstubAllGlobals());

async function seed(name: string, phase = "import_unknown", attemptedAt = Date.now()): Promise<Stub> {
  const target = env.SOURCE_EXPORT_COORDINATOR.getByName(name);
  await target.status();
  await runInDurableObject(target, async (_instance: SourceExportCoordinator, state) => {
    state.storage.sql.exec(
      "UPDATE source_state SET target_end=?, phase=?, plan_json=?, file_count=1, row_count=2, import_job_id=?, import_attempted_at_ms=? WHERE singleton=1",
      targetEnd, phase, JSON.stringify(plan), phase === "import_pending" ? "known-import" : null, attemptedAt,
    );
    state.storage.sql.exec("INSERT INTO import_attempts VALUES(?)", attemptedAt);
  });
  return target;
}

async function alarm(target: Stub): Promise<void> {
  await runInDurableObject(target, async (instance: SourceExportCoordinator, state) => {
    // Enable the real alarm only within this test's object invocation.
    const bindings = (instance as unknown as { env: CoordinatorEnv }).env;
    const previous = bindings.INGESTION_ENABLED;
    bindings.INGESTION_ENABLED = "true";
    try {
      await instance.alarm();
    } finally {
      bindings.INGESTION_ENABLED = previous;
      await state.storage.deleteAlarm();
    }
  });
}

async function savedState(target: Stub) {
  return runInDurableObject(target, async (_instance: SourceExportCoordinator, state) => ({
    row: state.storage.sql.exec<{
      completed_through: string; phase: string; table_index: number;
      import_job_id: string | null; plan_json: string | null;
    }>("SELECT * FROM source_state").one(),
    attempts: state.storage.sql.exec<{ count: number }>("SELECT count(*) count FROM import_attempts").one().count,
    receipts: state.storage.sql.exec<{ count: number }>("SELECT count(*) count FROM verified_receipts").one().count,
  }));
}

function expectReadOnlyRecovery() {
  expect(requests.every(({ method }) => method === "GET")).toBe(true);
  expect(requests.some(({ url }) => url.pathname.includes("/scheduling/runs"))).toBe(false);
}

describe("native import recovery without another submission", () => {
  it.each([
    { name: "top-level job ID", body: { job_id: "known-import" }, known: true },
    { name: "nested job ID", body: { job: { job_id: "known-import" } }, known: true },
    { name: "accepted without job ID", body: {}, known: false },
    { name: "accepted without a response body", body: null, known: false },
  ])("handles $name without delaying or repeating the import", async ({ name, body, known }) => {
    const target = await seed(`accepted-${name}`, "exported");
    vi.stubGlobal("fetch", vi.fn(async (input: Request | string | URL, init?: RequestInit) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      const method = init?.method ?? "GET";
      requests.push({ method, url });
      if (method === "GET" && url.pathname === "/v0/sql") {
        expect(url.searchParams.get("q")).toMatch(/FORMAT JSON$/);
        return Response.json({ data: [receipt] });
      }
      if (method === "POST" && url.pathname === `/v0/datasources/${resource}/scheduling/runs`) {
        return body === null ? new Response(null, { status: 202 }) : Response.json(body);
      }
      if (method === "GET" && url.pathname === "/v0/jobs") {
        return Response.json({ jobs: [{ id: "known-import", datasource: { name: resource } }] });
      }
      throw new Error(`Unexpected outbound ${method} ${url.pathname}`);
    }));
    await alarm(target);
    expect((await savedState(target)).row).toMatchObject({
      phase: known ? "import_pending" : "import_unknown",
      import_job_id: known ? "known-import" : null,
      completed_through: completedThrough,
    });
    if (!known) {
      await alarm(target);
      expect((await savedState(target)).row).toMatchObject({ phase: "import_pending", import_job_id: "known-import" });
    }
    receipt = { files: 1, rows: 2, quarantine: 0 };
    await alarm(target);
    expect((await savedState(target)).row).toMatchObject({ phase: "idle", table_index: 1 });
    expect(requests.filter(({ method }) => method === "POST")).toHaveLength(1);
  });

  it("keeps zero matching candidates unknown across repeated alarms and start", async () => {
    const attemptedAt = Date.now();
    const target = await seed("missing-import", "import_unknown", attemptedAt);
    jobs = [{ id: "unrelated", datasource: { name: "another-source" } }];
    await alarm(target);
    expect((await target.start()).phase).toBe("import_unknown");
    await alarm(target);
    const saved = await savedState(target);
    expect(saved.row).toMatchObject({ phase: "import_unknown", import_job_id: null, completed_through: completedThrough });
    expect(saved.attempts).toBe(1);
    const lookups = requests.filter(({ url }) => url.pathname === "/v0/jobs");
    // start() can also deliver its immediate alarm; every retry must stay read-only.
    expect(lookups.length).toBeGreaterThanOrEqual(2);
    for (const { url } of lookups) {
      expect(url.searchParams.get("kind")).toBe("gcs_sync");
      expect(url.searchParams.get("created_after")).toBe(new Date(attemptedAt - 5_000).toISOString());
    }
    expectReadOnlyRecovery();
  });

  it("stops for attention when an unknown submission remains missing for ten minutes", async () => {
    const target = await seed("missing-import-expired", "import_unknown", Date.now() - 10 * 60_000 - 1);
    await alarm(target);
    expect((await target.status()).phase).toBe("attention");
    const count = requests.length;
    await alarm(target);
    expect(requests).toHaveLength(count);
    expect((await savedState(target)).attempts).toBe(1);
    expectReadOnlyRecovery();
  });

  it("adopts one matching job, then checks that exact job without another import", async () => {
    const target = await seed("found-import");
    jobs = [
      { id: "unrelated", datasource: { name: "another-source" } },
      { id: "known-import", datasource: { name: resource } },
    ];
    await alarm(target);
    expect((await savedState(target)).row).toMatchObject({ phase: "import_pending", import_job_id: "known-import" });
    await alarm(target);
    expect(requests.map(({ url }) => url.pathname)).toEqual(["/v0/sql", "/v0/jobs", "/v0/sql", "/v0/jobs/known-import"]);
    expect((await savedState(target)).attempts).toBe(1);
    expectReadOnlyRecovery();
  });

  it("requires attention when two matching imports could own the missing acknowledgment", async () => {
    const target = await seed("multiple-imports");
    jobs = ["first", "second"].map((id) => ({ id, datasource: { name: resource } }));
    await alarm(target);
    expect((await savedState(target)).row).toMatchObject({ phase: "attention", import_job_id: null, completed_through: completedThrough });
    await target.start();
    const count = requests.length;
    await alarm(target);
    expect(requests).toHaveLength(count);
    expect((await savedState(target)).attempts).toBe(1);
    expectReadOnlyRecovery();
  });

  it.each(["error", "cancelled"])("a known %s job remains stopped through cron, admin start, and alarm", async (status) => {
    const target = await seed("v1-source-export", "import_pending");
    importStatus = status;
    await alarm(target);
    expect((await target.status()).phase).toBe("attention");
    const count = requests.length;
    const enabledEnv = { ...env, INGESTION_ENABLED: "true", ADMIN_TOKEN: "synthetic-admin" };
    await worker.scheduled(createScheduledController(), enabledEnv);
    const response = await worker.fetch(new Request("https://synthetic.test/admin/source-export/start", {
      method: "POST", headers: { Authorization: "Bearer synthetic-admin" },
    }), enabledEnv);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ phase: "attention", completedThrough });
    await alarm(target);
    const saved = await savedState(target);
    expect(saved.row).toMatchObject({ phase: "attention", import_job_id: "known-import", completed_through: completedThrough, plan_json: JSON.stringify(plan) });
    expect(saved.attempts).toBe(1);
    expect(saved.receipts).toBe(0);
    expect(requests).toHaveLength(count);
    expectReadOnlyRecovery();
  });

  it("waits for receipt visibility after done, then requires attention after five minutes", async () => {
    const target = await seed("done-without-receipt", "import_pending");
    importStatus = "done";
    await alarm(target);
    expect((await target.status()).phase).toBe("import_pending");
    await runInDurableObject(target, async (_instance: SourceExportCoordinator, state) => {
      state.storage.sql.exec("UPDATE source_state SET completion_seen_at_ms=?", Date.now() - 5 * 60_000 - 1);
    });
    await alarm(target);
    const saved = await savedState(target);
    expect(saved.row).toMatchObject({ phase: "attention", completed_through: completedThrough });
    expect(saved.receipts).toBe(0);
    expect(saved.attempts).toBe(1);
    expectReadOnlyRecovery();
  });

  it("uses an exact visible receipt to finish the source even when the import acknowledgment was lost", async () => {
    const target = await seed("receipt-without-job");
    receipt = { files: 1, rows: 2, quarantine: 0 };
    await alarm(target);
    const saved = await savedState(target);
    expect(saved.row).toMatchObject({ phase: "idle", table_index: 1, import_job_id: null, completed_through: completedThrough });
    expect(saved.receipts).toBe(1);
    expect(saved.attempts).toBe(1);
    expect(requests.map(({ url }) => url.pathname)).toEqual(["/v0/sql"]);
    expectReadOnlyRecovery();
  });
});
