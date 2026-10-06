import { readFile, writeFile } from "node:fs/promises";
import { createRuntime } from "../../bill-realtime-reporting-api/src/runtime.js";

// Aggregate counters only. Never persist provider records or authentication data.
try {
  const config = await readFile(new URL("./.dev.vars", import.meta.url), "utf8");
  const token = config.match(/^ADMIN_TOKEN=([a-f0-9]{64})$/m)?.[1];
  if (!token) throw new Error("Missing local admin token.");
  async function call(path, body = {}) {
    const response = await fetch(`https://bill-realtime-coordinator.bill-3e3.workers.dev/admin/${path}`, {
      method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(body), signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) throw new Error(`Status returned HTTP ${response.status}.`);
    return response.json();
  }
  const publication = await call("publication/status");
  const sources = [];
  for (const route of [{ source: "stripe", account: "main" }, { source: "stripe", account: "kajabi" },
    { source: "activecampaign", account: "default" }]) {
    const status = await call("sources/status", { route });
    sources.push({ route, work: status.work, pending: status.pending, discoveredThrough: status.discoveredThrough,
      lastSlice: status.lastSlice, paused: status.paused });
  }
  const { query } = await createRuntime();
  const commits = await query({ query: `SELECT batch_version, baseline_id, committed_at,
    JSON_VALUE(manifest, '$.historyComplete') = 'true' history_complete,
    CAST(JSON_VALUE(manifest, '$.sourceCount') AS INT64) source_count,
    CAST(JSON_VALUE(manifest, '$.identityCount') AS INT64) identity_count
    FROM \`able-folio-499722.bill_reports_realtime.ingest_commits\`
    ORDER BY batch_version DESC LIMIT 10` });
  const result = { checkedAt: new Date().toISOString(), publication, sources, commits };
  await writeFile(new URL("./evidence/pipeline-progress.json", import.meta.url), JSON.stringify(result, null, 2) + "\n");
  console.log(JSON.stringify(result));
} catch (error) {
  console.error(JSON.stringify({ error: error.message }));
  process.exitCode = 1;
}
