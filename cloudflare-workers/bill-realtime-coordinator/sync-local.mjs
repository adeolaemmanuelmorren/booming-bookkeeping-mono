import { readFile, writeFile, mkdir } from "node:fs/promises";
import { createRuntime } from "../../bill-realtime-reporting-api/src/runtime.js";
import { withJobLease } from "../../bill-realtime-reporting-api/src/job-lease.js";
import { createPublicationSync } from "../../bill-realtime-reporting-api/src/publication-sync.js";

// Operator verification only. The deployed consumer must use its own Google
// service account and a publication-scoped secret, not local admin credentials.
const config = await readFile(new URL("./.dev.vars", import.meta.url), "utf8");
const token = config.match(/^ADMIN_TOKEN=([a-f0-9]{64})$/m)?.[1];
const workerUrl = "https://bill-realtime-coordinator.bill-3e3.workers.dev";
const results = [];
try {
  const { query, store } = await createRuntime();
  const sync = createPublicationSync({ query, workerUrl, token, hydrateHistory: process.env.HYDRATE_HISTORY === "true" });
  let baseline;
  const leaseResult = await withJobLease(query, "publication", async () => {
  const deadline = Date.now() + 300_000;
  for (let index = 0; index < 40 && Date.now() < deadline; index++) {
    baseline = await store.latestBaseline();
    const result = await sync(baseline.id);
    results.push(result);
    console.log(JSON.stringify(result));
    if (result.status === "idle") break;
  }
  });
  if (leaseResult?.status === "already_running") console.log(JSON.stringify(leaseResult));
  await mkdir(new URL("./evidence", import.meta.url), { recursive: true });
  await writeFile(new URL("./evidence/latest-publication-run.json", import.meta.url),
    JSON.stringify({ checkedAt: new Date().toISOString(), baselineId: baseline?.id, results }, null, 2) + "\n");
} catch (error) {
  // SDK error objects can contain authenticated requests and staged source rows.
  console.error(JSON.stringify({ error: error.message, code: error.code }));
  process.exitCode = 1;
}
