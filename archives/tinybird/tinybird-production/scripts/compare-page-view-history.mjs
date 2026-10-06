import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  buildBigQueryDigestSql,
  buildTinybirdDigestSql,
} from "./lib/parity-sql.mjs";

const snapshotCutoff = "2026-08-28 00:53:00";
const importedTailStart = "2026-08-26 00:00:00";
const bucketCount = 256;
const sampleMode = process.argv.includes("--sample");
const tinybirdShardCount = sampleMode ? 1 : 8;
const floatScale = 9;
const bigQueryTable = "able-folio-499722.booming_data_analytics.stg_page_views";
const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const columns = queryBigQuery(`
  SELECT
    column_name AS name,
    data_type
  FROM \`able-folio-499722.booming_data_analytics.INFORMATION_SCHEMA.COLUMNS\`
  WHERE table_name = 'stg_page_views'
  ORDER BY ordinal_position
`);
const columnNames = columns.map(({ name }) => `\`${name}\``).join(", ");
const bigQuerySource = {
  query: `
    WITH raw_versions AS (
      SELECT
        'boom_domains' AS source_system,
        id AS page_view_id,
        MAX(COALESCE(loaded_at, received_at, sent_at, timestamp,
          TIMESTAMP '1970-01-01 00:00:00+00')) AS source_version
      FROM \`able-folio-499722.boom_domains.pages\`
      GROUP BY id

      UNION ALL

      SELECT
        'jitsu_data' AS source_system,
        message_id AS page_view_id,
        MAX(COALESCE(received_at, sent_at, timestamp,
          TIMESTAMP '1970-01-01 00:00:00+00')) AS source_version
      FROM \`able-folio-499722.jitsu_data.pages\`
      GROUP BY message_id
    )
    SELECT ${columnNames}
    FROM \`${bigQueryTable}\` AS page_views
    JOIN raw_versions USING (source_system, page_view_id)
    WHERE source_version < TIMESTAMP '${snapshotCutoff}+00'
      ${sampleMode ? `AND source_version >= TIMESTAMP '${importedTailStart}+00'` : ""}
      ${sampleMode ? "AND MOD(FARM_FINGERPRINT(page_view_id), 16) = 0" : ""}
  `,
};
const options = {
  columns,
  source: bigQuerySource,
  bucketCount,
  floatScale,
};

const bigQueryRows = queryBigQuery(buildBigQueryDigestSql(options));
const tinybirdBuckets = new Map();

for (let shard = 0; shard < tinybirdShardCount; shard += 1) {
  const resource = {
    query: `
      SELECT ${columnNames}
      FROM (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY tenant_id, page_view_id
            ORDER BY
              source_priority DESC,
              source_fact_version DESC,
              source_ingested_at DESC,
              payload_hash DESC
          ) AS current_version_rank
        FROM jitsu_page_view_versions
        WHERE source_system IN ('boom_domains', 'jitsu_data')
          AND source_version < toDateTime64('${snapshotCutoff}', 6, 'UTC')
          ${sampleMode ? `AND source_version >= toDateTime64('${importedTailStart}', 6, 'UTC')` : ""}
          ${sampleMode ? "AND farmFingerprint64(ifNull(page_view_id, '')) % 16 = 0" : ""}
          AND farmFingerprint64(ifNull(page_view_id, '')) % ${tinybirdShardCount} = ${shard}
      )
      WHERE current_version_rank = 1 AND source_deleted = 0
    `,
  };
  const shardRows = await queryTinybird(
    buildTinybirdDigestSql({ ...options, resource }),
  );
  mergeDigestRows(tinybirdBuckets, shardRows);
  console.error(`Verified Tinybird shard ${shard + 1}/${tinybirdShardCount}`);
}

const tinybirdRows = [...tinybirdBuckets.values()];
const normalizedBigQuery = normalizeRows(bigQueryRows);
const normalizedTinybird = normalizeRows(tinybirdRows);
const tinybirdByBucket = new Map(
  normalizedTinybird.map((row) => [row.bucket, row]),
);
const mismatchedBuckets = normalizedBigQuery
  .filter((row) => JSON.stringify(row) !== JSON.stringify(tinybirdByBucket.get(row.bucket)))
  .map((row) => Number(row.bucket));
const mismatchedFields = findMismatchedFields(
  normalizedBigQuery,
  normalizedTinybird,
);

for (const row of normalizedTinybird) {
  if (!normalizedBigQuery.some((candidate) => candidate.bucket === row.bucket)) {
    mismatchedBuckets.push(Number(row.bucket));
  }
}

const result = {
  matches: mismatchedBuckets.length === 0,
  sample_mode: sampleMode,
  sample_start: sampleMode ? `${importedTailStart}Z` : null,
  snapshot_cutoff: `${snapshotCutoff}Z`,
  columns_compared: columns.length,
  buckets_compared: bucketCount,
  bigquery_rows: sumRows(normalizedBigQuery),
  tinybird_rows: sumRows(normalizedTinybird),
  bigquery_digest_sha256: digest(normalizedBigQuery),
  tinybird_digest_sha256: digest(normalizedTinybird),
  mismatched_buckets: [...new Set(mismatchedBuckets)].sort((left, right) => left - right),
  mismatched_fields: mismatchedFields,
};

console.log(JSON.stringify(result, null, 2));

if (!result.matches) {
  process.exitCode = 1;
}

function queryBigQuery(sql) {
  const output = execFileSync(
    "bq",
    [
      "query",
      "--quiet",
      "--use_legacy_sql=false",
      "--project_id=able-folio-499722",
      "--format=json",
      "--max_rows=300",
      sql,
    ],
    { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 },
  );

  return JSON.parse(output);
}

async function queryTinybird(sql) {
  const response = await fetch(`${tinybirdConfig.host}/v0/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${tinybirdConfig.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ q: `${sql} FORMAT JSON` }),
  });
  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(`Tinybird parity query failed with HTTP ${response.status}`);
  }

  return JSON.parse(responseText).data;
}

function normalizeRows(rows) {
  return rows
    .map((row) => Object.fromEntries(Object.entries(row)
      .map(([key, value]) => [key, String(value)])
      .sort(([left], [right]) => left.localeCompare(right))))
    .sort((left, right) => Number(left.bucket) - Number(right.bucket));
}

function mergeDigestRows(buckets, rows) {
  for (const row of rows) {
    const bucket = String(row.bucket);
    const merged = buckets.get(bucket) ?? { bucket };

    for (const [key, value] of Object.entries(row)) {
      if (key === "bucket") {
        continue;
      }

      const current = BigInt(merged[key] ?? "0");
      const incoming = BigInt(value);
      merged[key] = key === "fingerprint_xor"
        ? (current ^ incoming).toString()
        : (current + incoming).toString();
    }

    buckets.set(bucket, merged);
  }
}

function sumRows(rows) {
  return rows.reduce((total, row) => total + BigInt(row.row_count), 0n).toString();
}

function digest(rows) {
  return createHash("sha256")
    .update(rows.map((row) => JSON.stringify(row)).join("\n"))
    .digest("hex");
}

function findMismatchedFields(bigQueryRows, tinybirdRows) {
  const bigQueryByBucket = new Map(
    bigQueryRows.map((row) => [row.bucket, row]),
  );
  const tinybirdByBucket = new Map(
    tinybirdRows.map((row) => [row.bucket, row]),
  );
  const buckets = new Set([
    ...bigQueryByBucket.keys(),
    ...tinybirdByBucket.keys(),
  ]);
  const mismatchCounts = new Map();

  for (const bucket of buckets) {
    const bigQueryRow = bigQueryByBucket.get(bucket) ?? {};
    const tinybirdRow = tinybirdByBucket.get(bucket) ?? {};
    const fields = new Set([
      ...Object.keys(bigQueryRow),
      ...Object.keys(tinybirdRow),
    ]);

    for (const field of fields) {
      if (bigQueryRow[field] === tinybirdRow[field]) {
        continue;
      }

      mismatchCounts.set(field, (mismatchCounts.get(field) ?? 0) + 1);
    }
  }

  return Object.fromEntries(
    [...mismatchCounts.entries()].sort(([left], [right]) =>
      left.localeCompare(right)),
  );
}
