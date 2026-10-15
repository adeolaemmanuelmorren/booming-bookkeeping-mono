import { ApiError, identifier, integer } from "./errors.js";
import { loadMetaReconciliation } from "./checks/meta-reconciliation.js";

export function createGoogleProvider({ auth, bigquery, projectId, datasets, repository, location = "US" }) {
  const allowedDatasets = new Set(datasets);

  async function get(path, params = {}) {
    const client = await auth.getClient();
    const { data } = await client.request({
      url: `https://dataform.googleapis.com/v1/${repository}/${path}`,
      params, timeout: 25_000, retry: false,
    });
    return data;
  }

  function dataset(id) {
    if (!allowedDatasets.has(id)) throw new ApiError(403, "Dataset is outside this API's configured scope.");
    return bigquery.dataset(id);
  }

  function table(datasetId, tableId) {
    if (!/^[a-zA-Z0-9_ -]{1,200}$/.test(tableId)) throw new ApiError(400, "Invalid table ID.");
    return dataset(datasetId).table(tableId);
  }

  async function query({ sql, limit = 100, pageToken, jobId }) {
    limit = integer(limit, 100);
    if (jobId) {
      identifier(jobId);
      // Only jobs created by this service can be resumed, and its SA can only read its own jobs.
      if (!jobId.startsWith("boom_health_")) throw new ApiError(400, "Not an operations API query job.");
      return results(bigquery.job(jobId, { location }), limit, pageToken);
    }
    if (typeof sql !== "string" || !sql.trim() || sql.length > 30_000) {
      throw new ApiError(400, "Provide SQL up to 30,000 characters.");
    }
    const options = {
      query: sql, location, useLegacySql: false,
      maximumBytesBilled: "1000000000", jobTimeoutMs: 25_000,
    };
    const [dryRun] = await bigquery.createQueryJob({ ...options, dryRun: true });
    const stats = dryRun.metadata.statistics?.query;
    if (stats?.statementType !== "SELECT") throw new ApiError(400, "Only a single SELECT query is allowed.");
    if (Number(stats.totalBytesProcessed ?? 0) > 1_000_000_000) {
      throw new ApiError(400, "Query exceeds the 1 GB scan limit. Narrow the date range or columns.");
    }
    for (const reference of stats.referencedTables ?? []) {
      if (reference.projectId !== projectId || !allowedDatasets.has(reference.datasetId)) {
        throw new ApiError(403, "Query references a dataset outside this API's configured scope.");
      }
    }
    const [job] = await bigquery.createQueryJob({ ...options, jobPrefix: "boom_health_" });
    return results(job, limit);
  }

  async function results(job, limit, pageToken) {
    const [rows, next, response] = await job.getQueryResults({
      maxResults: limit, autoPaginate: false, timeoutMs: 25_000, pageToken,
    });
    return { jobId: job.id, rows, totalRows: response?.totalRows,
      nextPageToken: next?.pageToken ?? null, maximumBytesBilled: 1_000_000_000 };
  }

  async function invocations(params) {
    return get("workflowInvocations", {
      pageSize: integer(params.get("limit"), 50),
      // Explicit ordering keeps the first page useful for current health checks.
      orderBy: "create_time desc",
      ...(params.get("pageToken") ? { pageToken: params.get("pageToken") } : {}),
    });
  }

  return {
    metaReconciliation: () => loadMetaReconciliation(query, projectId),
    invocations,
    invocation: id => get(`workflowInvocations/${identifier(id)}`),
    actions: (id, params) => get(`workflowInvocations/${identifier(id)}:query`, {
      pageSize: integer(params.get("limit"), 100),
      ...(params.get("pageToken") ? { pageToken: params.get("pageToken") } : {}),
    }),
    compilation: id => get(`compilationResults/${identifier(id)}`),
    workflows: () => get("workflowConfigs", { pageSize: 100 }),
    async datasets() {
      return { projectId, datasets: await Promise.all(datasets.map(async id => {
        const [metadata] = await dataset(id).getMetadata();
        return { id, location: metadata.location, lastModifiedTime: metadata.lastModifiedTime };
      })) };
    },
    async tables(id, params) {
      const [tables, next] = await dataset(id).getTables({
        maxResults: integer(params.get("limit"), 100), autoPaginate: false,
        pageToken: params.get("pageToken") ?? undefined,
      });
      return { dataset: id, tables: tables.map(item => ({ id: item.id, ...item.metadata })),
        nextPageToken: next?.pageToken ?? null };
    },
    async table(id, tableId) {
      const [metadata] = await table(id, tableId).getMetadata();
      return metadata;
    },
    async rows(id, tableId, params) {
      table(id, tableId);
      const limit = integer(params.get("limit"), 20);
      return query({ sql: `SELECT * FROM \`${projectId}.${id}.${tableId}\` LIMIT ${limit}`, limit });
    },
    query,
    async status() {
      const page = await invocations(new URLSearchParams({ limit: "100" }));
      const runs = page.workflowInvocations ?? [];
      const scheduled = runs.filter(run => run.workflowConfig?.endsWith("/workflowConfigs/main"));
      const latest = scheduled[0];
      const completed = scheduled.find(run => run.invocationTiming?.endTime);
      const ageHours = completed ? (Date.now() - Date.parse(completed.invocationTiming.endTime)) / 3_600_000 : null;
      const ok = !!completed && completed.state === "SUCCEEDED" && ageHours <= 3;
      return { status: ok ? "ok" : "degraded", latest, latestCompleted: completed,
        completedAgeHours: ageHours, staleAfterHours: 3, checkedRuns: runs.length,
        note: "Main hourly workflow only. Failed assertions remain failures." };
    },
    async bigqueryStatus() {
      const [metadata] = await dataset("booming_data_analytics").getMetadata();
      return { status: "ok", projectId, dataset: metadata.datasetReference,
        note: "Dataset access verified. Use table metadata or a timestamp query to check data freshness." };
    },
  };
}
