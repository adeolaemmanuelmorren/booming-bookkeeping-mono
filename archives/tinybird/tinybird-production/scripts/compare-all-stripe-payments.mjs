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
  argumentsByName.snapshot ?? "2026-08-26 20:10:00+00";

const columns = [
  column("payment_source", "string"),
  column("payment_id", "string"),
  column("payment_time", "timestamp"),
  column("payment_date", "date"),
  column("customer_id", "string"),
  column("payment_email", "string"),
  column("payment_name", "string"),
  column("payment_phone", "string"),
  column("billing_address_line_1", "string"),
  column("billing_address_city", "string"),
  column("billing_address_region", "string"),
  column("billing_address_zip", "string"),
  column("billing_address_country", "string"),
  column("currency", "string"),
  column("amount_paid", "decimal"),
  column("refund_amount", "decimal"),
  column("net_amount", "decimal"),
  column("has_refund", "boolean"),
  column("product_id", "string"),
  column("product_name", "string"),
  column("product_variant", "string"),
  column("price_id", "string"),
  column("plan_id", "string"),
  column("product_rule", "string"),
  column("product_classification_method", "string"),
  column("is_product_classified", "boolean"),
  column("is_bbb_mentorship_installment", "boolean"),
  column("payment_sequence_number", "integer"),
  column("payment_occurrence_type", "string"),
  column("is_repeat_payment", "boolean"),
  column("payment_intent_id", "string"),
  column("payment_method_id", "string"),
  column("invoice_id", "string"),
  column("subscription_id", "string"),
  column("checkout_session_id", "string"),
  column("payment_link_id", "string"),
  column("invoice_billing_reason", "string"),
  column("checkout_mode", "string"),
  column("has_discount", "boolean"),
  column("discount_amount", "decimal"),
  column("discount_ids", "unordered_string_array"),
  column("coupon_ids", "unordered_string_array"),
  column("coupon_names", "unordered_string_array"),
  column("promotion_code_ids", "unordered_string_array"),
  column("promotion_codes", "unordered_string_array"),
  column("coupon_amount_off", "decimal"),
  column("coupon_percent_off", "float"),
  column("charge_description", "string"),
  column("calculated_statement_descriptor", "string"),
  column("statement_descriptor", "string"),
  column("payment_intent_description", "string"),
  column("invoice_description", "string"),
  column("book_product_text", "string"),
  column("receipt_url", "string"),
  column("livemode", "boolean"),
  column("product_complete_name", "string"),
  column("content_ids", "ordered_string_array"),
];

main();

function main() {
  try {
    if (argumentsByName["tinybird-only"] === "true") {
      const tinybirdRows = normalizeRows(queryTinybird());

      console.log(
        JSON.stringify(
          {
            model: "all_stripe_payments_current",
            target: tinybirdTarget.name,
            branch: tinybirdTarget.branch,
            tinybird: summarize(tinybirdRows),
          },
          null,
          2,
        ),
      );
      return;
    }

    const bigQueryRows = normalizeRows(queryBigQuery());
    const tinybirdRows = normalizeRows(queryTinybird());
    const firstMismatch = findFirstMismatch(bigQueryRows, tinybirdRows);
    const matches = firstMismatch === null;

    console.log(
      JSON.stringify(
        {
          model: "all_stripe_payments_current",
          authoritative_bigquery_model:
            "booming_data_analytics.int_all_stripe_payments",
          snapshot_at: snapshotAt,
          target: tinybirdTarget.name,
          branch: tinybirdTarget.branch,
          matches,
          bigquery: summarize(bigQueryRows),
          tinybird: summarize(tinybirdRows),
          first_mismatch: firstMismatch,
        },
        null,
        2,
      ),
    );

    if (!matches) {
      process.exitCode = 1;
    }
  } catch (error) {
    console.error(sanitizeSecrets(error instanceof Error ? error.message : error));
    process.exitCode = 1;
  }
}

function queryBigQuery() {
  const canonicalFields = columns
    .map(
      (field) =>
        `${bigQueryCanonicalValue(field)} AS canonical_${field.name}`,
    )
    .join(",\n        ");
  const encodedFields = columns
    .map((field) => bigQueryLengthPrefix(`canonical_${field.name}`))
    .join(",\n          ");

  const sql = `
    WITH canonical_fields AS (
      SELECT
        payment_source,
        payment_id,
        ${canonicalFields}
      FROM \`able-folio-499722.booming_data_analytics.int_all_stripe_payments\`
        FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
    ),
    canonical_rows AS (
      SELECT
        payment_source,
        payment_id,
        CONCAT(
          ${encodedFields}
        ) AS canonical_row
      FROM canonical_fields
    )
    SELECT
      payment_source,
      payment_id,
      TO_HEX(SHA256(canonical_row)) AS row_fingerprint
    FROM canonical_rows
    ORDER BY payment_source, payment_id, row_fingerprint
  `;

  const output = runCommand(
    "bq",
    [
      "query",
      "--quiet",
      "--use_legacy_sql=false",
      "--project_id=able-folio-499722",
      "--location=US",
      "--format=json",
      "--max_rows=1000000",
      `--parameter=snapshot_at:STRING:${snapshotAt}`,
      sql,
    ],
    { maxBuffer: 256 * 1024 * 1024 },
  );

  return parseJsonOutput(output);
}

function queryTinybird() {
  const canonicalFields = columns
    .map(
      (field) =>
        `${tinybirdCanonicalValue(field)} AS canonical_${field.name}`,
    )
    .join(",\n          ");
  const encodedFields = columns
    .map((field) => tinybirdLengthPrefix(`canonical_${field.name}`))
    .join(",\n            ");

  const sql = `
    SELECT
      payment_source,
      payment_id,
      hex(SHA256(canonical_row)) AS row_fingerprint
    FROM (
      SELECT
        payment_source,
        payment_id,
        concat(
          ${encodedFields}
        ) AS canonical_row
      FROM (
        SELECT
          payment_source,
          payment_id,
          ${canonicalFields}
        FROM all_stripe_payments_adapter
      )
    )
    ORDER BY payment_source, payment_id, row_fingerprint
  `;

  const output = runCommand(
    "tb",
    [
      ...tinybirdTarget.cliArguments,
      "--output",
      "json",
      "sql",
      "--rows-limit",
      "1000000",
      sql,
    ],
    { cwd: projectRoot, maxBuffer: 256 * 1024 * 1024 },
  );

  return parseJsonOutput(output).data;
}

function column(name, kind) {
  return { name, kind };
}

function bigQueryCanonicalValue(field) {
  const name = field.name;

  if (field.kind === "string") {
    return name;
  }

  if (field.kind === "timestamp") {
    return `FORMAT_TIMESTAMP('%FT%H:%M:%E6SZ', ${name}, 'UTC')`;
  }

  if (field.kind === "date") {
    return `FORMAT_DATE('%F', ${name})`;
  }

  if (field.kind === "decimal") {
    return `IF(
          ${name} IS NULL,
          NULL,
          FORMAT('%.0f', ${name} * NUMERIC '1000000000')
        )`;
  }

  if (field.kind === "float") {
    return `IF(
          ${name} IS NULL,
          NULL,
          FORMAT('%.0f', ROUND(${name} * 1000000000))
        )`;
  }

  if (field.kind === "integer") {
    return `CAST(${name} AS STRING)`;
  }

  if (field.kind === "boolean") {
    return `CASE
          WHEN ${name} IS NULL THEN NULL
          WHEN ${name} THEN '1'
          ELSE '0'
        END`;
  }

  const orderBy =
    field.kind === "unordered_string_array" ? "item" : "item_offset";
  const withOffset =
    field.kind === "ordered_string_array" ? " WITH OFFSET AS item_offset" : "";

  return `ARRAY_TO_STRING(
          ARRAY(
            SELECT CONCAT(CAST(BYTE_LENGTH(item) AS STRING), ':', item)
            FROM UNNEST(${name}) AS item${withOffset}
            ORDER BY ${orderBy}
          ),
          ''
        )`;
}

function tinybirdCanonicalValue(field) {
  const name = field.name;

  if (field.kind === "string") {
    return name;
  }

  if (field.kind === "timestamp") {
    return `if(
            ${name} IS NULL,
            CAST(NULL AS Nullable(String)),
            formatDateTime(
              toTimeZone(assumeNotNull(${name}), 'UTC'),
              '%Y-%m-%dT%H:%i:%S.%fZ'
            )
          )`;
  }

  if (field.kind === "date") {
    return `if(
            ${name} IS NULL,
            CAST(NULL AS Nullable(String)),
            toString(assumeNotNull(${name}))
          )`;
  }

  if (field.kind === "decimal") {
    return `if(
            ${name} IS NULL,
            CAST(NULL AS Nullable(String)),
            toString(toInt128(assumeNotNull(${name}) * 1000000000))
          )`;
  }

  if (field.kind === "float") {
    return `if(
            ${name} IS NULL,
            CAST(NULL AS Nullable(String)),
            toString(toInt64(round(assumeNotNull(${name}) * 1000000000)))
          )`;
  }

  if (field.kind === "integer") {
    return `if(
            ${name} IS NULL,
            CAST(NULL AS Nullable(String)),
            toString(assumeNotNull(${name}))
          )`;
  }

  if (field.kind === "boolean") {
    return `if(
            ${name} IS NULL,
            CAST(NULL AS Nullable(String)),
            if(${name} = 1, '1', '0')
          )`;
  }

  const arrayName =
    field.kind === "unordered_string_array" ? `arraySort(${name})` : name;

  return `arrayStringConcat(
            arrayMap(
              item -> concat(toString(length(item)), ':', item),
              ${arrayName}
            ),
            ''
          )`;
}

function bigQueryLengthPrefix(expression) {
  return `IF(
            ${expression} IS NULL,
            '-1:',
            CONCAT(
              CAST(BYTE_LENGTH(COALESCE(${expression}, '')) AS STRING),
              ':',
              COALESCE(${expression}, '')
            )
          )`;
}

function tinybirdLengthPrefix(expression) {
  return `if(
              ${expression} IS NULL,
              '-1:',
              concat(
                toString(length(ifNull(${expression}, ''))),
                ':',
                ifNull(${expression}, '')
              )
            )`;
}

function normalizeRows(rows) {
  return rows
    .map((row) => ({
      payment_source: String(row.payment_source),
      payment_id: row.payment_id === null ? null : String(row.payment_id),
      row_fingerprint: String(row.row_fingerprint).toUpperCase(),
    }))
    .sort(compareRows);
}

function compareRows(left, right) {
  return rowSortKey(left).localeCompare(rowSortKey(right));
}

function rowSortKey(row) {
  return JSON.stringify([
    row.payment_source,
    row.payment_id,
    row.row_fingerprint,
  ]);
}

function summarize(rows) {
  const rowsBySource = {};
  const seenKeys = new Set();
  let duplicateKeyCount = 0;

  for (const row of rows) {
    rowsBySource[row.payment_source] ??= 0;
    rowsBySource[row.payment_source] += 1;

    const key = JSON.stringify([row.payment_source, row.payment_id]);

    if (seenKeys.has(key)) {
      duplicateKeyCount += 1;
      continue;
    }

    seenKeys.add(key);
  }

  return {
    row_count: rows.length,
    distinct_key_count: seenKeys.size,
    duplicate_key_count: duplicateKeyCount,
    rows_by_source: rowsBySource,
    row_fingerprint_sha256: createHash("sha256")
      .update(rows.map(rowSortKey).join("\n"))
      .digest("hex"),
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

function runCommand(command, argumentsList, options = {}) {
  try {
    return execFileSync(command, argumentsList, {
      encoding: "utf8",
      ...options,
    });
  } catch (error) {
    const stdout = error?.stdout ? String(error.stdout) : "";
    const stderr = error?.stderr ? String(error.stderr) : "";
    const details = sanitizeSecrets([stderr, stdout].filter(Boolean).join("\n"));
    throw new Error(`${command} failed${details ? `\n${details}` : ""}`);
  }
}

function sanitizeSecrets(value) {
  return String(value)
    .replace(/([?&]token=)[^&\s]+/gi, "$1[REDACTED]")
    .replace(/(authorization:\s*bearer\s+)[^\s]+/gi, "$1[REDACTED]")
    .replace(/\b[pj]\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g, "[REDACTED_TOKEN]");
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
