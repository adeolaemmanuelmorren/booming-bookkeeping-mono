// Executes extracted Dataform SELECT logic against literals only. No workflow or source-table reads.
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { attributeProfile, conversionMetrics } from "./engine.mjs";
import { namedMetrics } from "./named-metrics.mjs";
import { scenario } from "./fixtures.mjs";
const source = await readFile(
  new URL(
    "../../../dataform/definitions/output/attribution/mart_conversions_with_touchpoints.sqlx",
    import.meta.url,
  ),
  "utf8",
);
const extracted = source
  .slice(source.indexOf("joined as ("), source.lastIndexOf("\nselect\n"))
  .replace('${ref("mart_touchpoints_all")}', "touchpoints");
const quote = (value) => JSON.stringify(value);
const str = (value) => (value == null ? "CAST(NULL AS STRING)" : quote(value));
const ts = (value) =>
  value == null ? "CAST(NULL AS TIMESTAMP)" : `TIMESTAMP_MILLIS(${value})`;
const touches = [],
  conversions = [],
  wanted = new Map();
const merge = (row) => {
  if (!wanted.has(row.key))
    wanted.set(row.key, {
      key: row.key,
      values: Array(144).fill(0),
      sessions: 0,
    });
  const target = wanted.get(row.key);
  target.sessions += row.sessions;
  row.values.forEach((v, i) => (target.values[i] += v));
};
for (let seed = 0; seed < 25; seed++) {
  const input = scenario(seed, 15, 20);
  // One currency/account makes comparison at the existing Dataform grain unambiguous.
  input.conversions.forEach((c) => (c.currency = "USD"));
  input.touches.forEach(
    (t) => (t.sessionId = t.sessionId ? `${seed}:${t.sessionId}` : ""),
  );
  // Empty-string session IDs are globally shared. Use unique non-null IDs for this aggregation test.
  input.touches.forEach((t, i) => {
    if (t.sessionId === "") t.sessionId = `${seed}:empty:${i}`;
  });
  input.touches.forEach((t) =>
    touches.push({ ...t, id: `${seed}:${t.id}`, profile: `p${seed}` }),
  );
  input.conversions
    .filter((c) => c.time !== null)
    .forEach((c) =>
      conversions.push({ ...c, id: `${seed}:${c.id}`, profile: `p${seed}` }),
    );
  // Prefix touch IDs before both calculations to preserve tie ordering.
  const prefixed = input.touches.map((t) => ({ ...t, id: `${seed}:${t.id}` }));
  attributeProfile(prefixed, input.conversions).forEach(merge);
}
const touchSql = touches
  .map(
    (
      t,
    ) => `SELECT ${str(t.id)} touchpoint_id,${str(t.profile)} profile_id,${ts(t.time)} touchpoint_time,
  ${str(t.sessionId)} session_id,${ts(t.sessionTime)} session_start_timestamp,
  ${str(t.source)} utm_source,${str(t.medium)} utm_medium,CAST(NULL AS STRING) channel_source,CAST(NULL AS STRING) channel_medium,
  CAST(NULL AS STRING) utm_campaign,CAST(NULL AS STRING) utm_content,CAST(NULL AS STRING) utm_term,
  ${str(t.campaign)} campaign_id,${str(t.adset)} adset_id,${str(t.ad)} ad_id,
  CAST(NULL AS STRING) first_page_host,CAST(NULL AS STRING) first_page_path,TIMESTAMP('2000-01-01') _dataform_updated_at`,
  )
  .join("\nUNION ALL\n");
const eventSql = conversions
  .map(
    (
      c,
    ) => `SELECT ${str(c.id)} conversion_id,${str(c.profile)} profile_id,${ts(c.time)} conversion_time,
  ${conversionMetrics.map((m, i) => `CAST(${c.metrics[i]} AS FLOAT64) ${m}`).join(",")},TIMESTAMP('2000-01-01') _dataform_updated_at`,
  )
  .join("\nUNION ALL\n");
const weights = {
  ft: "first_touch_weight",
  lt: "last_touch_weight",
  mt: "multi_touch_weight",
  paid_ft: "IF(is_paid_ad_touch,first_touch_weight,0)",
  paid_lt: "IF(is_paid_ad_touch,last_touch_weight,0)",
  paid_mt: "IF(is_paid_ad_touch,multi_touch_weight,0)",
};
const metrics = conversionMetrics.flatMap((m) =>
  ["mt", "ft", "lt", "paid_mt", "paid_ft", "paid_lt"].map(
    (s) => `SUM(${m}*${weights[s]}) AS ${m}_${s}`,
  ),
);
const sql = `WITH conversion_events AS (${eventSql}),touchpoints AS (${touchSql}),
 ad_names AS (SELECT CAST(NULL AS STRING) source,CAST(NULL AS STRING) campaign_id,CAST(NULL AS STRING) adset_id,CAST(NULL AS STRING) ad_id,
 CAST(NULL AS STRING) campaign_name,CAST(NULL AS STRING) adset_name,CAST(NULL AS STRING) ad_name,TIMESTAMP('2000-01-01') _dataform_updated_at FROM UNNEST([1]) WHERE FALSE),
 ${extracted},
 labeled AS (SELECT *,TO_JSON_STRING([CAST(DATE(COALESCE(session_start_timestamp,conversion_time),'America/Los_Angeles') AS STRING),
 source,medium,IF(touchpoint_id IS NULL,'','account:1'),campaign_id,adset_id,ad_id,campaign_name,adset_name,ad_name,
 COALESCE(utm_content,'unknown'),COALESCE(utm_term,'unknown'),COALESCE(landing_page_host,'offline'),COALESCE(landing_page_path,'offline'),'USD']) group_key FROM weighted)
 SELECT group_key,COUNT(DISTINCT session_id) sessions,${metrics.join(",")} FROM labeled GROUP BY group_key`;
await writeFile(
  new URL("results/bigquery-reference.sql", import.meta.url),
  sql,
);
let token;
try {
  token = execFileSync(
    "/Users/adeola/google-cloud-sdk/bin/gcloud",
    ["auth", "print-access-token", "--project", "able-folio-499722"],
    { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
  ).trim();
} catch {
  try {
    token = execFileSync(
      "/Users/adeola/google-cloud-sdk/bin/gcloud",
      ["auth", "application-default", "print-access-token"],
      { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    ).trim();
  } catch {
    await writeFile(
      new URL("results/bigquery-reference.json", import.meta.url),
      JSON.stringify(
        {
          verified: false,
          blocked: "Google Cloud reauthentication required",
          syntheticOnly: true,
        },
        null,
        2,
      ),
    );
    throw new Error(
      "Google Cloud reauthentication required. The SQL comparison has not run.",
    );
  }
}

const response = await fetch(
  "https://bigquery.googleapis.com/bigquery/v2/projects/able-folio-499722/queries",
  {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: sql,
      useLegacySql: false,
      location: "US",
      timeoutMs: 200000,
      maxResults: 10000,
      maximumBytesBilled: "10000000",
    }),
  },
);
const result = await response.json();
if (!response.ok) throw new Error(JSON.stringify(result.error));
assert.ok(result.jobComplete, "Reference query did not finish");
assert.ok(!result.pageToken, "Read all reference results");
let maxDifference = 0;
assert.equal(result.rows.length, wanted.size);
for (const row of result.rows) {
  const data = Object.fromEntries(
    result.schema.fields.map((field, i) => [field.name, row.f[i].v]),
  );
  const normalized = JSON.stringify(JSON.parse(data.group_key));
  const target = wanted.get(normalized);
  assert.ok(target, normalized);
  assert.equal(Number(data.sessions), target.sessions);
  for (const [name, value] of Object.entries(namedMetrics(target.values))) {
    const difference = Math.abs(Number(data[name]) - value);
    maxDifference = Math.max(maxDifference, difference);
    assert.ok(difference < 1e-6, `${name}: ${difference}`);
  }
}
const evidence = {
  verified: true,
  profiles: 25,
  touches: touches.length,
  conversions: conversions.length,
  reportRows: wanted.size,
  namedMetrics: 144,
  maxDifference,
  bytesProcessed: result.totalBytesProcessed,
  jobId: result.jobReference.jobId,
  sourceSha256: createHash("sha256").update(source).digest("hex"),
  scope:
    "Extracted joined-through-weighted Dataform SELECT on synthetic normalized literals; source normalization and ad-name enrichment not tested",
};
await writeFile(
  new URL("results/bigquery-reference.json", import.meta.url),
  JSON.stringify(evidence, null, 2),
);
console.log(JSON.stringify(evidence));
