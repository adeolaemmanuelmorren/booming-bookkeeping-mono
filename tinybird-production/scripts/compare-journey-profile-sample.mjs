import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";

const PROJECT_ID = "able-folio-499722";
const DATASET = "booming_data_analytics";
const DEFAULT_REPAIR_CUTOFF = 1788249409811;

const [cutoff = "2026-09-01 19:00:00", sampleSizeText = "50"] = process.argv.slice(2);
const sampleSize = Number(sampleSizeText);
if (!Number.isSafeInteger(sampleSize) || sampleSize < 1 || sampleSize > 100) {
  throw new Error("sample size must be an integer from 1 through 100.");
}

const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const profileIds = await sampleProfiles(tinybirdConfig, sampleSize);
if (profileIds.length !== sampleSize) {
  throw new Error(`Expected ${sampleSize} sample profiles but found ${profileIds.length}.`);
}

const [bigQueryRows, tinybirdRows] = await Promise.all([
  queryBigQuery(profileIds, cutoff),
  queryTinybird(tinybirdConfig, profileIds, cutoff),
]);
const expected = digestByProfile(bigQueryRows);
const actual = digestByProfile(tinybirdRows);
const mismatches = profileIds.flatMap((profileId) => {
  const bigQuery = expected.get(profileId) ?? emptyDigest();
  const tinybird = actual.get(profileId) ?? emptyDigest();
  if (bigQuery.hash === tinybird.hash && bigQuery.rows === tinybird.rows) return [];
  return [{ profileId, bigQuery, tinybird }];
});

console.log(JSON.stringify({
  cutoff,
  sampleSize,
  bigQueryRows: bigQueryRows.length,
  tinybirdRows: tinybirdRows.length,
  matchedProfiles: sampleSize - mismatches.length,
  mismatchedProfiles: mismatches.length,
  mismatches: mismatches.slice(0, 10),
}, null, 2));

if (mismatches.length > 0) process.exitCode = 1;

async function sampleProfiles(config, limit) {
  const rows = await queryTinybirdSql(config, `
    SELECT profiles.profile_id
    FROM current_identity_profiles AS profiles
    WHERE length(profiles.member_identifier_keys) >= 3
      AND profiles.profile_id IN (
        SELECT profile_id
        FROM reporting_journey_versions
        WHERE tenant_id = 'boom'
          AND batch_version > 0
          AND batch_version < ${DEFAULT_REPAIR_CUTOFF}
          AND profile_id IS NOT NULL
          AND profile_id != ''
      )
    ORDER BY cityHash64(profiles.profile_id)
    LIMIT ${limit}
  `);
  return rows.map((row) => String(row.profile_id));
}

function queryBigQuery(profileIds, conversionCutoff) {
  const sql = `
    SELECT
      ${bigQueryColumns().join(",\n      ")}
    FROM \`${PROJECT_ID}.${DATASET}.mart_conversions_with_touchpoints\`
    WHERE profile_id IN (${sqlStrings(profileIds)})
      AND conversion_time < TIMESTAMP('${conversionCutoff}+00')
    ORDER BY profile_id, conversion_id, touchpoint_id
  `;
  const output = execFileSync(
    "bq",
    [
      "query",
      "--quiet",
      "--use_legacy_sql=false",
      `--project_id=${PROJECT_ID}`,
      "--format=json",
      "--max_rows=100000",
      sql,
    ],
    { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 },
  );
  return JSON.parse(output);
}

async function queryTinybird(config, profileIds, conversionCutoff) {
  return queryTinybirdSql(config, `
    SELECT
      ${tinybirdColumns().join(",\n      ")}
    FROM reporting_current_journey_base_rows
    WHERE profile_id IN (${sqlStrings(profileIds)})
      AND conversion_time < parseDateTime64BestEffort('${conversionCutoff}', 6)
    ORDER BY profile_id, conversion_id, touchpoint_id
  `);
}

async function queryTinybirdSql(config, sql) {
  const response = await fetch(`${config.host}/v0/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ q: `${sql} FORMAT JSON` }),
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(`Tinybird journey parity query failed with HTTP ${response.status}: ${message}`);
  }
  return (await response.json()).data;
}

function bigQueryColumns() {
  return commonColumns().map((column) => {
    if (column === "conversion_time") return "UNIX_MICROS(conversion_time) AS conversion_time";
    if (column === "session_start_timestamp") {
      return "UNIX_MICROS(session_start_timestamp) AS session_start_timestamp";
    }
    return column;
  });
}

function tinybirdColumns() {
  return commonColumns().map((column) => {
    if (column === "conversion_time") {
      return "toUnixTimestamp64Micro(conversion_time) AS conversion_time";
    }
    if (column === "session_start_timestamp") {
      return "toUnixTimestamp64Micro(session_start_timestamp) AS session_start_timestamp";
    }
    return column;
  });
}

function commonColumns() {
  return [
    "profile_id",
    "conversion_id",
    "conversion_time",
    "conversion_source",
    "conversion_type",
    "form_submissions_server_side",
    "krc_registrations_server_side",
    "form_submissions_client_side",
    "order_completed_server_side",
    "order_completed_client_side",
    "payments_server_side",
    "payments_client_side",
    "revenue_server_side",
    "revenue_client_side",
    "customers_server_side",
    "bbb_buyers_server_side",
    "bbb_revenue_server_side",
    "payments_vip_server_side",
    "payments_book_server_side",
    "payments_mentorship_server_side",
    "payments_kajabi_server_side",
    "payments_catalog_server_side",
    "payments_unknown_server_side",
    "revenue_vip_server_side",
    "revenue_book_server_side",
    "revenue_mentorship_server_side",
    "revenue_kajabi_server_side",
    "revenue_catalog_server_side",
    "revenue_unknown_server_side",
    "touchpoint_id",
    "session_id",
    "session_start_timestamp",
    "landing_page_host",
    "landing_page_path",
    "source",
    "medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "campaign_id",
    "adset_id",
    "ad_id",
    "is_direct_touch",
    "is_paid_ad_touch",
    "total_touchpoints",
    "touchpoint_number",
    "is_excluded_direct_touch",
    "non_direct_touchpoints",
    "non_direct_touchpoint_number",
    "attribution_touchpoints",
    "attribution_touchpoint_number",
    "first_touch_is_direct",
    "excluded_direct_touchpoints",
    "paid_ad_touchpoints",
    "paid_ad_touchpoint_number",
    "journey_direct_status",
    "multi_touch_weight",
    "first_touch_weight",
    "last_touch_weight",
    "paid_multi_touch_weight",
    "paid_first_touch_weight",
    "paid_last_touch_weight",
  ];
}

function digestByProfile(rows) {
  const rowsByProfile = new Map();
  for (const row of rows) {
    const profileId = String(row.profile_id);
    const profileRows = rowsByProfile.get(profileId) ?? [];
    profileRows.push(canonicalRow(row));
    rowsByProfile.set(profileId, profileRows);
  }

  return new Map([...rowsByProfile].map(([profileId, profileRows]) => {
    profileRows.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
    return [profileId, {
      rows: profileRows.length,
      hash: createHash("sha256")
        .update(profileRows.map((row) => JSON.stringify(row)).join("\n"))
        .digest("hex"),
    }];
  }));
}

function canonicalRow(row) {
  return Object.fromEntries(commonColumns().map((column) => [
    column,
    canonicalValue(column, row[column]),
  ]));
}

function canonicalValue(column, value) {
  if (value === null || value === undefined) return null;
  if (numericColumns().has(column)) return Number(value).toFixed(9);
  if (booleanColumns().has(column)) return value === true || Number(value) === 1 ? "1" : "0";
  return String(value);
}

function numericColumns() {
  return new Set([
    "form_submissions_server_side",
    "krc_registrations_server_side",
    "form_submissions_client_side",
    "order_completed_server_side",
    "order_completed_client_side",
    "payments_server_side",
    "payments_client_side",
    "revenue_server_side",
    "revenue_client_side",
    "customers_server_side",
    "bbb_buyers_server_side",
    "bbb_revenue_server_side",
    "payments_vip_server_side",
    "payments_book_server_side",
    "payments_mentorship_server_side",
    "payments_kajabi_server_side",
    "payments_catalog_server_side",
    "payments_unknown_server_side",
    "revenue_vip_server_side",
    "revenue_book_server_side",
    "revenue_mentorship_server_side",
    "revenue_kajabi_server_side",
    "revenue_catalog_server_side",
    "revenue_unknown_server_side",
    "total_touchpoints",
    "touchpoint_number",
    "non_direct_touchpoints",
    "non_direct_touchpoint_number",
    "attribution_touchpoints",
    "attribution_touchpoint_number",
    "excluded_direct_touchpoints",
    "paid_ad_touchpoints",
    "paid_ad_touchpoint_number",
    "multi_touch_weight",
    "first_touch_weight",
    "last_touch_weight",
    "paid_multi_touch_weight",
    "paid_first_touch_weight",
    "paid_last_touch_weight",
  ]);
}

function booleanColumns() {
  return new Set([
    "is_direct_touch",
    "is_paid_ad_touch",
    "is_excluded_direct_touch",
    "first_touch_is_direct",
  ]);
}

function sqlStrings(values) {
  return values.map((value) => `'${value.replaceAll("'", "''")}'`).join(", ");
}

function emptyDigest() {
  return { rows: 0, hash: null };
}
