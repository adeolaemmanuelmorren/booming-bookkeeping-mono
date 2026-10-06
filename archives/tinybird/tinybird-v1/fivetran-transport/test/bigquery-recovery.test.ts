import { describe, expect, it, vi } from "vitest";
import {
  checkBigQueryExport,
  submitBigQueryExport,
  type BigQueryConfig,
  type BigQueryJob,
  type ExportPlan,
  type Fetcher,
} from "../src/bigquery";

const config: BigQueryConfig = {
  projectId: "synthetic-project",
  location: "US",
  pollIntervalMs: 5_000,
  jobTimeoutMs: 480_000,
};
const plan: ExportPlan = {
  resourceName: "raw_activecampaign_contact",
  jobId: "saved-export-job",
  query: "EXPORT DATA OPTIONS(uri='gs://synthetic/exact-*.parquet', format='PARQUET') AS SELECT 1",
  uri: "gs://synthetic/exact-*.parquet",
};

function savedJob(state = "DONE"): BigQueryJob {
  return {
    jobReference: { jobId: plan.jobId, projectId: config.projectId, location: config.location },
    configuration: { query: { query: plan.query } },
    status: { state },
    statistics: { query: { exportDataStatistics: { fileCount: "1", rowCount: "2" } } },
  };
}

const mismatches: Array<[string, (job: BigQueryJob) => void]> = [
  ["query", (job) => { job.configuration!.query!.query = `${plan.query} `; }],
  ["project", (job) => { job.jobReference!.projectId = "another-project"; }],
  ["location", (job) => { job.jobReference!.location = "EU"; }],
  ["job ID", (job) => { job.jobReference!.jobId = "another-job"; }],
  ["missing job identity", (job) => { delete job.jobReference; }],
  ["missing query", (job) => { delete job.configuration; }],
];

describe("saved BigQuery job recovery", () => {
  it.each(["RUNNING", "DONE"])("accepts the exact saved %s job using one read", async (state) => {
    const job = savedJob(state);
    const fetcher = vi.fn<Fetcher>(async () => Response.json(job));
    const result = await checkBigQueryExport(plan, config, "synthetic-token", fetcher);
    expect(result).toEqual(state === "DONE" ? { status: "done", job } : { status: "pending" });
    expect(fetcher).toHaveBeenCalledTimes(1);
    const [url, init] = fetcher.mock.calls[0];
    expect(String(url)).toBe(`https://bigquery.googleapis.com/bigquery/v2/projects/${config.projectId}/jobs/${plan.jobId}?location=US`);
    expect(init?.method ?? "GET").toBe("GET");
  });

  for (const state of ["RUNNING", "DONE"]) {
    it.each(mismatches)(`rejects %s mismatch before adopting an existing ${state} job`, async (_field, change) => {
      const job = savedJob(state);
      change(job);
      const fetcher = vi.fn<Fetcher>(async () => Response.json(job));
      await expect(checkBigQueryExport(plan, config, "synthetic-token", fetcher))
        .rejects.toThrow("does not match saved plan");
      expect(fetcher).toHaveBeenCalledTimes(1);
      expect(fetcher.mock.calls.every(([, init]) => (init?.method ?? "GET") === "GET")).toBe(true);
    });
  }

  it.each(mismatches)("rejects %s mismatch after a 409 without resubmitting", async (_field, change) => {
    const job = savedJob();
    change(job);
    const fetcher = vi.fn<Fetcher>(async (_input, init) => {
      if (init?.method === "POST") return new Response(null, { status: 409 });
      return Response.json(job);
    });
    await expect(submitBigQueryExport(plan, config, "synthetic-token", fetcher))
      .rejects.toThrow("does not match saved plan");
    expect(fetcher.mock.calls.map(([, init]) => init?.method ?? "GET")).toEqual(["POST", "GET"]);
    expect(JSON.parse(String(fetcher.mock.calls[0][1]?.body))).toMatchObject({
      jobReference: { projectId: config.projectId, location: config.location, jobId: plan.jobId },
      configuration: { query: { query: plan.query, useLegacySql: false } },
    });
  });

  it("adopts an exact 409 job using its original immutable submission", async () => {
    const fetcher = vi.fn<Fetcher>(async (_input, init) => {
      if (init?.method === "POST") return new Response(null, { status: 409 });
      return Response.json(savedJob());
    });
    await submitBigQueryExport(plan, config, "synthetic-token", fetcher);
    expect(fetcher.mock.calls.map(([, init]) => init?.method ?? "GET")).toEqual(["POST", "GET"]);
  });
});
