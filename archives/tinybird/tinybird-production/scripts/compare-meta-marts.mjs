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
const rangeStart = argumentsByName.start ?? "2026-08-17 10:00:00";
const rangeEnd = argumentsByName.end ?? "2026-08-24 10:00:00";
const bigQueryOnly = argumentsByName["bigquery-only"] === "true";

const martDefinitions = [
  {
    name: "mart_ad_performance_hourly",
    fields: [
      "hour_start",
      "date",
      "hour_bucket",
      "hour_breakdown_type",
      "account_id",
      "account_name",
      "source",
      "formatted_source",
      "campaign_id",
      "campaign_name",
      "adset_id",
      "adset_name",
      "ad_id",
      "ad_name",
      "impressions",
      "clicks",
      "spend",
      "currency",
    ],
    integerFields: ["impressions", "clicks"],
    decimalFields: ["spend"],
    timestampFields: ["hour_start"],
    totals: ["impressions", "clicks", "spend"],
    bigQuerySql: `
      SELECT
        hour_start,
        date,
        hour_bucket,
        hour_breakdown_type,
        account_id,
        account_name,
        source,
        formatted_source,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        impressions,
        clicks,
        spend,
        currency
      FROM \`able-folio-499722.booming_data_analytics.mart_ad_performance_hourly\`
      WHERE hour_start >= TIMESTAMP(@range_start, 'America/Los_Angeles')
        AND hour_start < TIMESTAMP(@range_end, 'America/Los_Angeles')
      ORDER BY hour_start, account_id, ad_id
    `,
  },
  {
    name: "mart_meta_delivery_daily",
    fields: [
      "date",
      "level",
      "account_id",
      "campaign_id",
      "adset_id",
      "ad_id",
      "campaign_name",
      "adset_name",
      "ad_name",
      "impressions",
      "spend",
      "reach",
      "frequency",
      "currency",
    ],
    integerFields: ["impressions", "reach"],
    decimalFields: ["spend"],
    floatingFields: ["frequency"],
    totals: ["impressions", "spend", "reach"],
    bigQuerySql: `
      SELECT
        date,
        level,
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        campaign_name,
        adset_name,
        ad_name,
        impressions,
        spend,
        reach,
        frequency,
        currency
      FROM \`able-folio-499722.booming_data_analytics.mart_meta_delivery_daily\`
      WHERE date >= DATE(TIMESTAMP(@range_start, 'America/Los_Angeles'), 'America/Los_Angeles')
        AND date < DATE(TIMESTAMP(@range_end, 'America/Los_Angeles'), 'America/Los_Angeles')
      ORDER BY date, level, account_id, campaign_id, adset_id, ad_id
    `,
  },
];

let failed = false;

for (const definition of martDefinitions) {
  const bigQueryRows = normalizeRows(
    queryBigQuery(definition.bigQuerySql),
    definition,
  );
  const bigQuerySummary = summarize(bigQueryRows, definition);

  if (bigQueryOnly) {
    console.log(
      JSON.stringify(
        { mart: definition.name, bigquery: bigQuerySummary },
        null,
        2,
      ),
    );
    continue;
  }

  const tinybirdRows = normalizeRows(
    queryTinybird(definition.name),
    definition,
  );
  const tinybirdSummary = summarize(tinybirdRows, definition);
  const firstMismatch = findFirstMismatch(bigQueryRows, tinybirdRows);
  const matches = firstMismatch === null;

  failed ||= !matches;
  console.log(
    JSON.stringify(
      {
        mart: definition.name,
        target: tinybirdTarget.name,
        branch: tinybirdTarget.branch,
        matches,
        bigquery: bigQuerySummary,
        tinybird: tinybirdSummary,
        first_mismatch: firstMismatch,
      },
      null,
      2,
    ),
  );
}

if (failed) {
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
      "--location=US",
      "--format=json",
      "--max_rows=1000000",
      `--parameter=range_start:STRING:${rangeStart}`,
      `--parameter=range_end:STRING:${rangeEnd}`,
      sql,
    ],
    { encoding: "utf8", maxBuffer: 128 * 1024 * 1024 },
  );

  return parseJsonOutput(output);
}

function queryTinybird(endpoint) {
  const output = execFileSync(
    "tb",
    [
      ...tinybirdTarget.cliArguments,
      "endpoint",
      "data",
      endpoint,
      "--p_range_start_pacific",
      rangeStart,
      "--p_range_end_pacific",
      rangeEnd,
      "--format",
      "json",
    ],
    {
      cwd: projectRoot,
      encoding: "utf8",
      maxBuffer: 128 * 1024 * 1024,
    },
  );
  const response = parseJsonOutput(output);

  return response.data;
}

function normalizeRows(rows, definition) {
  const normalizedRows = rows.map((row) => {
    const normalized = Object.create(null);

    for (const field of definition.fields) {
      normalized[field] = normalizeValue(row[field], field, definition);
    }

    return normalized;
  });

  normalizedRows.sort((left, right) =>
    JSON.stringify(left).localeCompare(JSON.stringify(right)),
  );
  return normalizedRows;
}

function normalizeValue(value, field, definition) {
  if (value === null || value === undefined) {
    return null;
  }

  if (definition.timestampFields?.includes(field)) {
    return normalizeTimestamp(String(value));
  }

  if (definition.integerFields?.includes(field)) {
    return String(value);
  }

  if (definition.decimalFields?.includes(field)) {
    return Number(value).toFixed(9);
  }

  if (definition.floatingFields?.includes(field)) {
    return Number(value).toFixed(9);
  }

  return String(value);
}

function normalizeTimestamp(value) {
  const withTimeSeparator = value.replace(" ", "T");
  const withTimezone = /(?:Z|[+-]\d\d:?\d\d)$/.test(withTimeSeparator)
    ? withTimeSeparator
    : `${withTimeSeparator}Z`;
  const parsed = new Date(withTimezone);

  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Cannot normalize timestamp ${value}`);
  }

  return parsed.toISOString();
}

function summarize(rows, definition) {
  const lines = rows.map((row) => JSON.stringify(row));
  const totals = {};

  for (const field of definition.totals) {
    const scale = definition.integerFields?.includes(field) ? 0 : 9;
    totals[field] = sumFixedPrecision(rows, field, scale);
  }

  return {
    row_count: rows.length,
    sha256: createHash("sha256").update(lines.join("\n")).digest("hex"),
    totals,
  };
}

function sumFixedPrecision(rows, field, scale) {
  const total = rows.reduce(
    (sum, row) => sum + fixedPrecisionInteger(row[field] ?? "0", scale),
    0n,
  );

  if (scale === 0) {
    return total.toString();
  }

  const sign = total < 0n ? "-" : "";
  const absolute = total < 0n ? -total : total;
  const digits = absolute.toString().padStart(scale + 1, "0");
  const whole = digits.slice(0, -scale);
  const fraction = digits.slice(-scale);
  return `${sign}${whole}.${fraction}`;
}

function fixedPrecisionInteger(value, scale) {
  const normalized = Number(value).toFixed(scale);
  return BigInt(normalized.replace(".", ""));
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
