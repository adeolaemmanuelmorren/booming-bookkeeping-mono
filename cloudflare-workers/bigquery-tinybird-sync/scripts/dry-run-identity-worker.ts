import { computeIdentityWorkerBatch } from "../src/identity-worker";
import { readIdentityCompactionCursor } from "../src/tinybird-api";

const token = process.env.TB_TOKEN;
if (!token) throw new Error("TB_TOKEN is required.");

const batchLimit = Number(process.argv[2] ?? "500");
if (!Number.isSafeInteger(batchLimit) || batchLimit < 1 || batchLimit > 5_000) {
  throw new Error("Batch limit must be an integer from 1 to 5000.");
}

const config = {
  apiUrl: "https://api.us-east.tinybird.co",
  adminToken: token,
  fetchTimeoutMs: 30_000,
};
const cursor = await readIdentityCompactionCursor("boom", config);
const batchVersion = cursor.activeBatchVersion + 1;
const startedAt = performance.now();
const tracedFetch: typeof fetch = async (input, init) => {
  const response = await fetch(input, init);
  if (process.env.IDENTITY_DRY_RUN_TRACE !== "1") return response;

  const url = new URL(String(input));
  if (!url.pathname.includes("identity_worker_fact_heads")) return response;
  const body = await response.clone().json() as { data?: unknown[] };
  console.error(JSON.stringify({
    endpoint: "fact_heads",
    factKeys: url.searchParams.getAll("p_fact_keys").length,
    stateKeys: url.searchParams.getAll("p_state_keys").length,
    rows: body.data?.length ?? 0,
  }));
  return response;
};
const result = await computeIdentityWorkerBatch({
  tenantId: "boom",
  batchVersion,
  batchId: `identity_worker_dry_run_${batchVersion}`,
  batchLimit,
  cursor,
}, config, tracedFetch);

console.log(JSON.stringify({
  status: "ok",
  batchLimit,
  elapsedMs: Math.round(performance.now() - startedAt),
  inputEvents: result.manifest.input_event_count,
  changedFacts: result.changedFacts.length,
  outputRows: result.rows.length,
  rowsByKind: Object.fromEntries(
    [...Map.groupBy(result.rows, (row) => row.state_kind)]
      .map(([kind, rows]) => [kind, rows.length]),
  ),
  inputHash: result.inputHash,
  diagnostics: result.diagnostics,
  outputHash: result.outputHash,
}, null, 2));
