import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { resolveTinybirdTarget } from "./lib/tinybird-target.mjs";

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);

const argumentsByName = parseArguments(process.argv.slice(2));
const tinybirdTarget = resolveTinybirdTarget(argumentsByName);
const snapshotAt =
  argumentsByName.snapshot ?? "2026-08-26 19:50:00+00";

const bigQueryRows = normalizeBuckets(queryBigQuery());
const tinybirdRows = normalizeBuckets(queryTinybird());
const firstMismatch = findFirstMismatch(bigQueryRows, tinybirdRows);
const matches = firstMismatch === null;
const columnMismatches = matches ? [] : diagnoseColumnMismatches();

console.log(
  JSON.stringify(
    {
      model: "activecampaign_registration_adapter",
      authoritative_bigquery_model:
        "booming_data_analytics.int_activecampaign_form_submissions",
      snapshot_at: snapshotAt,
      target: tinybirdTarget.name,
      branch: tinybirdTarget.branch,
      matches,
      bigquery: summarize(bigQueryRows),
      tinybird: summarize(tinybirdRows),
      first_mismatch: firstMismatch,
      column_mismatches: columnMismatches,
    },
    null,
    2,
  ),
);

if (!matches) {
  process.exitCode = 1;
}

function queryBigQuery() {
  const sql = `
    WITH signed_fingerprints AS (
      SELECT FARM_FINGERPRINT(CONCAT(
        COALESCE(form_submission_id, '␀'), CHR(31),
        COALESCE(
          FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', submitted_at, 'UTC'),
          '␀'
        ), CHR(31),
        COALESCE(contact_id, '␀'), CHR(31),
        COALESCE(email, '␀'), CHR(31),
        COALESCE(phone, '␀'), CHR(31),
        COALESCE(first_name, '␀'), CHR(31),
        COALESCE(last_name, '␀'), CHR(31),
        COALESCE(full_name, '␀'), CHR(31),
        COALESCE(tag_id, '␀'), CHR(31),
        COALESCE(tag_name, '␀'), CHR(31),
        COALESCE(registration_type, '␀'), CHR(31),
        COALESCE(content_name, '␀'), CHR(31),
        COALESCE(canonical_event_id, '␀'), CHR(31),
        COALESCE(form_type, '␀'), CHR(31),
        COALESCE(event_source, '␀'), CHR(31),
        COALESCE(registration_tag_type, '␀')
      )) AS signed_fingerprint
      FROM \`able-folio-499722.booming_data_analytics.int_activecampaign_form_submissions\`
        FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
    ),
    fingerprints AS (
      SELECT
        signed_fingerprint,
        IF(
          signed_fingerprint < 0,
          CAST(signed_fingerprint AS BIGNUMERIC)
            + CAST('18446744073709551616' AS BIGNUMERIC),
          CAST(signed_fingerprint AS BIGNUMERIC)
        ) AS fingerprint
      FROM signed_fingerprints
    )
    SELECT
      CAST(MOD(fingerprint, 16) AS STRING) AS bucket,
      COUNT(*) AS row_count,
      CAST(SUM(fingerprint) AS STRING) AS fingerprint_sum,
      CAST(
        IF(
          BIT_XOR(signed_fingerprint) < 0,
          CAST(BIT_XOR(signed_fingerprint) AS BIGNUMERIC)
            + CAST('18446744073709551616' AS BIGNUMERIC),
          CAST(BIT_XOR(signed_fingerprint) AS BIGNUMERIC)
        ) AS STRING
      ) AS fingerprint_xor
    FROM fingerprints
    GROUP BY bucket
    ORDER BY bucket
  `;

  const output = execFileSync(
    "bq",
    [
      "query",
      "--quiet",
      "--use_legacy_sql=false",
      "--project_id=able-folio-499722",
      "--location=US",
      "--format=json",
      "--max_rows=1000",
      `--parameter=snapshot_at:STRING:${snapshotAt}`,
      sql,
    ],
    { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );

  return parseJsonOutput(output);
}

function queryTinybird() {
  const sql = String.raw`
    SELECT
      toString(fingerprint % 16) AS bucket,
      count() AS row_count,
      toString(sum(toUInt128(fingerprint))) AS fingerprint_sum,
      toString(groupBitXor(fingerprint)) AS fingerprint_xor
    FROM (
      SELECT farmFingerprint64(concat(
        ifNull(form_submission_id, '␀'), '\x1F',
        if(
          submitted_at IS NULL,
          '␀',
          formatDateTime(
            toTimeZone(assumeNotNull(submitted_at), 'UTC'),
            '%Y-%m-%dT%H:%i:%S.%fZ'
          )
        ), '\x1F',
        ifNull(contact_id, '␀'), '\x1F',
        ifNull(email, '␀'), '\x1F',
        ifNull(phone, '␀'), '\x1F',
        ifNull(first_name, '␀'), '\x1F',
        ifNull(last_name, '␀'), '\x1F',
        ifNull(full_name, '␀'), '\x1F',
        ifNull(tag_id, '␀'), '\x1F',
        ifNull(tag_name, '␀'), '\x1F',
        ifNull(registration_type, '␀'), '\x1F',
        ifNull(content_name, '␀'), '\x1F',
        ifNull(canonical_event_id, '␀'), '\x1F',
        ifNull(form_type, '␀'), '\x1F',
        ifNull(event_source, '␀'), '\x1F',
        ifNull(registration_tag_type, '␀')
      )) AS fingerprint
      FROM activecampaign_registration_adapter
    )
    GROUP BY bucket
    ORDER BY bucket
  `;

  const output = execFileSync(
    "tb",
    [...tinybirdTarget.cliArguments, "--output", "json", "sql", sql],
    {
      cwd: projectRoot,
      encoding: "utf8",
      maxBuffer: 64 * 1024 * 1024,
    },
  );

  return parseJsonOutput(output).data;
}

function diagnoseColumnMismatches() {
  const bigQueryStats = queryBigQueryColumnStats();
  const tinybirdStats = queryTinybirdColumnStats();
  const mismatches = [];

  for (const column of comparableColumns()) {
    const metrics = ["nulls", "fingerprint_sum", "fingerprint_xor"];

    for (const metric of metrics) {
      const key = `${column.name}_${metric}`;
      const expected = String(bigQueryStats[key]);
      const actual = String(tinybirdStats[key]);

      if (expected === actual) {
        continue;
      }

      mismatches.push({ column: column.name, metric, expected, actual });
    }
  }

  return mismatches;
}

function queryBigQueryColumnStats() {
  const metrics = comparableColumns().flatMap((column) => {
    const fingerprint =
      `FARM_FINGERPRINT(COALESCE(${column.bigquery}, '__NULL__'))`;
    const unsignedFingerprint = `IF(
      ${fingerprint} < 0,
      CAST(${fingerprint} AS BIGNUMERIC)
        + CAST('18446744073709551616' AS BIGNUMERIC),
      CAST(${fingerprint} AS BIGNUMERIC)
    )`;

    return [
      `COUNTIF(${column.bigquery} IS NULL) AS ${column.name}_nulls`,
      `CAST(SUM(${unsignedFingerprint}) AS STRING)
        AS ${column.name}_fingerprint_sum`,
      `CAST(IF(
        BIT_XOR(${fingerprint}) < 0,
        CAST(BIT_XOR(${fingerprint}) AS BIGNUMERIC)
          + CAST('18446744073709551616' AS BIGNUMERIC),
        CAST(BIT_XOR(${fingerprint}) AS BIGNUMERIC)
      ) AS STRING) AS ${column.name}_fingerprint_xor`,
    ];
  });

  const sql = `
    SELECT
      ${metrics.join(",\n      ")}
    FROM \`able-folio-499722.booming_data_analytics.int_activecampaign_form_submissions\`
      FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
  `;
  const output = execFileSync(
    "bq",
    [
      "query",
      "--quiet",
      "--use_legacy_sql=false",
      "--project_id=able-folio-499722",
      "--location=US",
      "--format=json",
      "--max_rows=10",
      `--parameter=snapshot_at:STRING:${snapshotAt}`,
      sql,
    ],
    { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );

  return parseJsonOutput(output)[0];
}

function queryTinybirdColumnStats() {
  const metrics = comparableColumns().flatMap((column) => {
    const fingerprint =
      `farmFingerprint64(ifNull(${column.tinybird}, '__NULL__'))`;

    return [
      `countIf(${column.tinybird} IS NULL) AS ${column.name}_nulls`,
      `toString(sum(toUInt128(${fingerprint})))
        AS ${column.name}_fingerprint_sum`,
      `toString(groupBitXor(${fingerprint}))
        AS ${column.name}_fingerprint_xor`,
    ];
  });
  const sql = `
    SELECT
      ${metrics.join(",\n      ")}
    FROM activecampaign_registration_adapter
  `;
  const output = execFileSync(
    "tb",
    [...tinybirdTarget.cliArguments, "--output", "json", "sql", sql],
    {
      cwd: projectRoot,
      encoding: "utf8",
      maxBuffer: 64 * 1024 * 1024,
    },
  );

  return parseJsonOutput(output).data[0];
}

function comparableColumns() {
  const timestampInBigQuery =
    "FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', submitted_at, 'UTC')";
  const timestampInTinybird = `if(
    submitted_at IS NULL,
    CAST(NULL AS Nullable(String)),
    formatDateTime(
      toTimeZone(assumeNotNull(submitted_at), 'UTC'),
      '%Y-%m-%dT%H:%i:%S.%fZ'
    )
  )`;
  const stringColumns = [
    "form_submission_id",
    "contact_id",
    "email",
    "phone",
    "first_name",
    "last_name",
    "full_name",
    "tag_id",
    "tag_name",
    "registration_type",
    "content_name",
    "canonical_event_id",
    "form_type",
    "event_source",
    "registration_tag_type",
  ];

  return [
    ...stringColumns.map((name) => ({
      name,
      bigquery: name,
      tinybird: name,
    })),
    {
      name: "submitted_at",
      bigquery: timestampInBigQuery,
      tinybird: timestampInTinybird,
    },
  ];
}

function normalizeBuckets(rows) {
  return rows
    .map((row) => ({
      bucket: String(row.bucket),
      row_count: String(row.row_count),
      fingerprint_sum: String(row.fingerprint_sum),
      fingerprint_xor: String(row.fingerprint_xor),
    }))
    .sort((left, right) => Number(left.bucket) - Number(right.bucket));
}

function summarize(rows) {
  const rowCount = rows.reduce(
    (sum, row) => sum + BigInt(row.row_count),
    0n,
  );
  const digest = createHash("sha256")
    .update(rows.map((row) => JSON.stringify(row)).join("\n"))
    .digest("hex");

  return {
    row_count: rowCount.toString(),
    bucket_count: rows.length,
    fingerprint_sha256: digest,
  };
}

function findFirstMismatch(expectedRows, actualRows) {
  const comparedRows = Math.max(expectedRows.length, actualRows.length);

  for (let index = 0; index < comparedRows; index += 1) {
    const expected = expectedRows[index] ?? null;
    const actual = actualRows[index] ?? null;

    if (JSON.stringify(expected) === JSON.stringify(actual)) {
      continue;
    }

    return { index, expected, actual };
  }

  return null;
}

function parseArguments(values) {
  const parsed = {};

  for (const value of values) {
    if (!value.startsWith("--")) {
      throw new Error(`Unknown argument ${value}`);
    }

    const [name, argumentValue = "true"] = value.slice(2).split("=", 2);
    parsed[name] = argumentValue;
  }

  return parsed;
}

function parseJsonOutput(output) {
  const candidateIndexes = [...output.matchAll(/[\[{]/g)].map(
    (match) => match.index,
  );

  for (const candidateIndex of candidateIndexes) {
    try {
      return JSON.parse(output.slice(candidateIndex).trim());
    } catch {
      continue;
    }
  }

  throw new Error("Command did not return a complete JSON value");
}
