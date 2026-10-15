import { readFileSync } from "node:fs";

const sql = readFileSync(new URL("./meta-reconciliation.sql", import.meta.url), "utf8");
const accepted = JSON.parse(readFileSync(new URL("./meta-accepted-differences.json", import.meta.url), "utf8"));

export function metaReconciliationSql(projectId) {
  return sql.replaceAll("__PROJECT__", projectId)
    .replace("__ACCEPTED_FINGERPRINTS__", accepted.map(row => `'${row.fingerprint}'`).join(", "));
}

export async function loadMetaReconciliation(query, projectId) {
  const result = await query({ sql: metaReconciliationSql(projectId), limit: 200 });
  const mismatchCount = Number(result.totalRows ?? result.rows.length);
  return {
    status: mismatchCount ? "warning" : "ok",
    checkedAt: new Date().toISOString(),
    mismatchCount,
    differences: result.rows,
    truncated: mismatchCount > result.rows.length,
    note: "Advisory comparison of Meta daily and hourly source totals. Excludes the last three days and preserves accepted historical differences. Does not block Dataform builds.",
  };
}
