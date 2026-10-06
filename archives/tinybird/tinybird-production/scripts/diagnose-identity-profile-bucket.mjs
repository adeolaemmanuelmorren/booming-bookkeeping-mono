import { execFileSync } from "node:child_process";

import { buildIdentitySurfaces } from "./lib/identity-parity.mjs";

const bucket = numberArgument("bucket");
const snapshot = stringArgument("snapshot");
const surface = buildIdentitySurfaces().find((candidate) => (
  candidate.name === "profiles"
));

if (!surface) throw new Error("The profiles parity surface is missing.");

const bigqueryRows = queryBigQuery(bigqueryProfileBucketQuery(
  surface.bigqueryQuery,
  bigqueryBucketExpression(),
));
const tinybirdRows = queryTinybird(tinybirdProfileBucketQuery(
  surface.tinybirdQuery,
  tinybirdBucketExpression(),
));

console.log(JSON.stringify(compareRows(bigqueryRows, tinybirdRows), null, 2));

function bigqueryProfileBucketQuery(sourceQuery, bucketExpression) {
  return `
    SELECT
      * EXCEPT(first_seen_at, last_seen_at),
      FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', first_seen_at, 'UTC') AS first_seen_at,
      FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', last_seen_at, 'UTC') AS last_seen_at
    FROM (${sourceQuery})
    WHERE ${bucketExpression} = ${bucket}
    ORDER BY profile_id
  `;
}

function tinybirdProfileBucketQuery(sourceQuery, bucketExpression) {
  return `
    SELECT
      * EXCEPT(first_seen_at, last_seen_at),
      formatDateTime(toTimeZone(first_seen_at, 'UTC'), '%Y-%m-%dT%H:%i:%S.%fZ')
        AS first_seen_at,
      formatDateTime(toTimeZone(last_seen_at, 'UTC'), '%Y-%m-%dT%H:%i:%S.%fZ')
        AS last_seen_at
    FROM (${sourceQuery})
    WHERE ${bucketExpression} = ${bucket}
    ORDER BY profile_id
  `;
}

function bigqueryBucketExpression() {
  return `MOD(
    MOD(
      FARM_FINGERPRINT(CONCAT(
        'V', CAST(BYTE_LENGTH(tenant_id) AS STRING), ':', tenant_id,
        'V', CAST(BYTE_LENGTH(profile_id) AS STRING), ':', profile_id
      )),
      256
    ) + 256,
    256
  )`;
}

function tinybirdBucketExpression() {
  return `farmFingerprint64(concat(
    'V', toString(length(tenant_id)), ':', tenant_id,
    'V', toString(length(profile_id)), ':', profile_id
  )) % 256`;
}

function queryBigQuery(sql) {
  const output = execFileSync("bq", [
    "query",
    "--quiet",
    "--use_legacy_sql=false",
    "--project_id=able-folio-499722",
    "--location=US",
    "--format=json",
    "--max_rows=20000",
    "--maximum_bytes_billed=20000000000",
    `--parameter=snapshot_at:STRING:${snapshot}`,
    sql,
  ], {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
  return JSON.parse(output);
}

function queryTinybird(sql) {
  const output = execFileSync("tb", [
    "--cloud",
    "--output",
    "json",
    "sql",
    "--rows-limit",
    "20000",
    sql,
  ], {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
  return parseJsonOutput(output).data;
}

function compareRows(bigqueryRows, tinybirdRows) {
  const bigquery = rowsByProfile(bigqueryRows);
  const tinybird = rowsByProfile(tinybirdRows);
  const profileIds = [...new Set([...bigquery.keys(), ...tinybird.keys()])].sort();
  const fieldMismatchCounts = {};
  const mismatches = [];

  for (const profileId of profileIds) {
    const expected = bigquery.get(profileId);
    const actual = tinybird.get(profileId);
    const fields = differingFields(expected, actual);
    if (fields.length === 0) continue;

    for (const field of fields) {
      fieldMismatchCounts[field] = (fieldMismatchCounts[field] ?? 0) + 1;
    }
    if (mismatches.length < 10) {
      mismatches.push({
        profile_id: profileId,
        fields,
        values: Object.fromEntries(fields.map((field) => [field, {
          bigquery: expected?.[field] ?? null,
          tinybird: actual?.[field] ?? null,
        }])),
      });
    }
  }

  return {
    bucket,
    bigquery_rows: bigqueryRows.length,
    tinybird_rows: tinybirdRows.length,
    mismatch_count: profileIds.filter((profileId) => (
      differingFields(bigquery.get(profileId), tinybird.get(profileId)).length > 0
    )).length,
    field_mismatch_counts: fieldMismatchCounts,
    sample_mismatches: mismatches,
  };
}

function rowsByProfile(rows) {
  return new Map(rows.map((row) => [String(row.profile_id), normalizeRow(row)]));
}

function normalizeRow(row) {
  return Object.fromEntries(Object.entries(row).map(([name, value]) => {
    if (Array.isArray(value)) return [name, [...value].map(String).sort()];
    if (name.endsWith("_at")) return [name, normalizeTimestamp(String(value))];
    return [name, value === null ? null : String(value)];
  }));
}

function normalizeTimestamp(value) {
  const normalized = value.replace(" ", "T").replace(/Z$/, "");
  const [whole, fraction = ""] = normalized.split(".");
  return `${whole}.${fraction.padEnd(6, "0").slice(0, 6)}Z`;
}

function differingFields(expected, actual) {
  if (!expected) return ["missing_in_bigquery"];
  if (!actual) return ["missing_in_tinybird"];

  const fields = [...new Set([...Object.keys(expected), ...Object.keys(actual)])].sort();
  return fields.filter((field) => (
    JSON.stringify(expected[field]) !== JSON.stringify(actual[field])
  ));
}

function parseJsonOutput(output) {
  for (const match of output.matchAll(/[\[{]/g)) {
    try {
      return JSON.parse(output.slice(match.index).trim());
    } catch {
      continue;
    }
  }
  throw new Error("Command did not return JSON.");
}

function numberArgument(name) {
  const value = Number(stringArgument(name));
  if (!Number.isInteger(value) || value < 0 || value > 255) {
    throw new Error(`--${name} must be an integer from 0 through 255.`);
  }
  return value;
}

function stringArgument(name) {
  const prefix = `--${name}=`;
  const values = process.argv.filter((argument) => argument.startsWith(prefix));
  if (values.length !== 1) throw new Error(`Supply --${name}=... exactly once.`);
  return values[0].slice(prefix.length);
}
