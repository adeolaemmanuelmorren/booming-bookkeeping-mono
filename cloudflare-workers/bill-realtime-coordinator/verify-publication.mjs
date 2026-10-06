import { readFile, writeFile, mkdir } from "node:fs/promises";
import { createRuntime } from "../../bill-realtime-reporting-api/src/runtime.js";

const base = "https://bill-realtime-coordinator.bill-3e3.workers.dev";
const config = await readFile(new URL("./.dev.vars", import.meta.url), "utf8");
const token = config.match(/^ADMIN_TOKEN=([a-f0-9]{64})$/m)?.[1];
async function status(path, body = {}) {
  const response = await fetch(`${base}/admin/${path}/status`, {
    method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(body), signal: AbortSignal.timeout(60_000),
  });
  if (!response.ok) throw new Error(`Status returned HTTP ${response.status}.`);
  return response.json();
}

try {
  const { query } = await createRuntime();
  const commits = await query({ query: `
    WITH stored AS (
      SELECT batch_id, batch_version, COUNT(*) row_count,
        COUNT(DISTINCT position) positions,
        LOWER(TO_HEX(SHA256(STRING_AGG(row_text, '\\n' ORDER BY position)))) content_hash
      FROM \`able-folio-499722.bill_reports_realtime.ingest_rows\` GROUP BY 1, 2
    )
    SELECT COUNT(*) commits, MAX(c.batch_version) latest_version,
      SUM(c.row_count) published_rows, COUNTIF(c.report_ready) report_ready_commits,
      COUNTIF(s.row_count IS NULL OR c.row_count != s.row_count
        OR s.positions != s.row_count OR c.content_hash != s.content_hash) invalid_commits
    FROM \`able-folio-499722.bill_reports_realtime.ingest_commits\` c
    LEFT JOIN stored s USING (batch_id, batch_version)` });
  const sources = await query({ query: `SELECT
      JSON_VALUE(replacement, '$.source') source,
      JSON_VALUE(replacement, '$.source_account') account, COUNT(*) replacements
    FROM \`able-folio-499722.bill_reports_realtime.ingested_source_replacements\`
    GROUP BY 1, 2 ORDER BY 1, 2` });
  const identities = await query({ query: `SELECT
      JSON_VALUE(identity_record, '$.state_kind') kind, COUNT(*) records
    FROM \`able-folio-499722.bill_reports_realtime.ingested_identity_records\`
    GROUP BY 1 ORDER BY 1` });
  const publication = await status("publication");
  const sourceStates = [];
  for (const route of [{ source: "stripe", account: "main" }, { source: "stripe", account: "kajabi" },
    { source: "activecampaign", account: "default" }]) {
    sourceStates.push(await status("sources", { route }));
  }
  const complete = commits[0].invalid_commits === 0 && commits[0].report_ready_commits === 0
    && commits[0].latest_version === publication.version && publication.pending === 0
    && !publication.activeBatch && sources.length === 3 && identities.length > 0
    && sourceStates.every((source) => source.paused && !source.lease.active && source.nextAlarmAt === null);
  const result = { checkedAt: new Date().toISOString(), integrationVerified: complete,
    realtimeReportsReady: false, commits: commits[0], sources, identities, publication, sourceStates };
  await mkdir(new URL("./evidence", import.meta.url), { recursive: true });
  await writeFile(new URL("./evidence/publication-verification.json", import.meta.url), JSON.stringify(result, null, 2) + "\n");
  console.log(JSON.stringify(result));
  if (!complete) process.exitCode = 1;
} catch (error) {
  console.error(JSON.stringify({ error: error.message, code: error.code }));
  process.exitCode = 1;
}
