#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const tinybirdDirectory = dirname(scriptDirectory);
const manifestPath = join(
  tinybirdDirectory,
  "contracts",
  "raw-bigquery-schemas.json",
);
const outputDirectory = join(tinybirdDirectory, "datasources", "raw");
const expectedTableCount = 49;

const scalarTypeMap = new Map([
  ["STRING", "String"],
  ["BYTES", "String"],
  ["INTEGER", "Int64"],
  ["INT64", "Int64"],
  ["FLOAT", "Float64"],
  ["FLOAT64", "Float64"],
  ["BOOLEAN", "Bool"],
  ["BOOL", "Bool"],
  ["TIMESTAMP", "DateTime64(6)"],
  ["DATE", "Date"],
  ["DATETIME", "DateTime64(6)"],
  ["TIME", "String"],
  ["NUMERIC", "Decimal(38,9)"],
  ["DECIMAL", "Decimal(38,9)"],
  ["BIGNUMERIC", "Decimal(76,38)"],
  ["BIGDECIMAL", "Decimal(76,38)"],
  ["JSON", "String"],
  ["GEOGRAPHY", "String"],
]);

function main() {
  const mode = normalizeMode(process.argv[2] ?? "render");

  if (process.argv.length > 3) {
    failWithUsage();
  }

  const manifest = loadManifest();
  validateManifest(manifest);

  if (mode === "render") {
    renderAll(manifest);
    return;
  }

  if (mode === "check") {
    checkRenderedFiles(manifest);
    return;
  }

  if (mode === "check-live") {
    checkLiveSchemas(manifest);
    return;
  }

  failWithUsage();
}

function loadManifest() {
  return JSON.parse(readFileSync(manifestPath, "utf8"));
}

function validateManifest(manifest) {
  if (manifest.contractVersion !== 1) {
    throw new Error("Expected raw schema contractVersion 1.");
  }

  if (manifest.deploymentBinding?.status !== "bound") {
    throw new Error("Raw sources must have an explicit deployment binding.");
  }

  for (const property of ["connectionName", "bucketPrefix", "format", "schedule"]) {
    if (!manifest.deploymentBinding[property]) {
      throw new Error(`Raw source deployment binding is missing ${property}.`);
    }
  }

  if (manifest.tables?.length !== expectedTableCount) {
    throw new Error(
      `Expected ${expectedTableCount} raw tables; found ${manifest.tables?.length ?? 0}.`,
    );
  }

  const resourceNames = new Set();

  for (const table of manifest.tables) {
    validateTable(table, resourceNames);
    resourceNames.add(table.resourceName);
  }
}

function validateTable(table, resourceNames) {
  assertSafeIdentifier(table.resourceName, "resource name");

  if (resourceNames.has(table.resourceName)) {
    throw new Error(`Duplicate resource name: ${table.resourceName}`);
  }

  if (!table.source?.project || !table.source.dataset || !table.source.table) {
    throw new Error(`${table.resourceName} is missing its BigQuery source.`);
  }

  if (!table.sourceRole) {
    throw new Error(`${table.resourceName} is missing its source role.`);
  }

  if (!table.fields?.length) {
    throw new Error(`${table.resourceName} has no fields.`);
  }

  const fieldNames = new Set();
  const targetFieldNames = new Set();
  let previousOrdinal = 0;

  for (const field of table.fields) {
    validateField(field, `${table.resourceName}.${field.name}`);

    if (field.ordinal <= previousOrdinal) {
      throw new Error(`${table.resourceName}.${field.name} has an out-of-order ordinal.`);
    }

    previousOrdinal = field.ordinal;

    if (fieldNames.has(field.name)) {
      throw new Error(`${table.resourceName} repeats field ${field.name}.`);
    }

    fieldNames.add(field.name);
    const targetName = targetFieldName(field);
    assertSafeIdentifier(targetName, `target field at ${table.resourceName}.${field.name}`);

    if (targetFieldNames.has(targetName)) {
      throw new Error(`${table.resourceName} repeats target field ${targetName}.`);
    }

    targetFieldNames.add(targetName);
  }

  for (const key of table.sortingKey ?? []) {
    if (!fieldNames.has(key)) {
      throw new Error(`${table.resourceName} sorting key ${key} is not a source field.`);
    }
  }

  for (const pseudoColumn of table.pseudoColumns ?? []) {
    if (fieldNames.has(pseudoColumn.name)) {
      throw new Error(
        `${table.resourceName}.${pseudoColumn.name} cannot be both a field and pseudo-column.`,
      );
    }
  }
}

function validateField(field, path) {
  assertSafeIdentifier(field.name, `field at ${path}`);

  if (!Number.isInteger(field.ordinal) || field.ordinal < 1) {
    throw new Error(`${path} has an invalid ordinal.`);
  }

  if (!["NULLABLE", "REQUIRED", "REPEATED"].includes(field.mode)) {
    throw new Error(`${path} has unsupported mode ${field.mode}.`);
  }

  const type = field.bigqueryType.toUpperCase();
  const isRecord = type === "RECORD" || type === "STRUCT";

  if (!isRecord && !scalarTypeMap.has(type)) {
    throw new Error(`${path} has unsupported BigQuery type ${field.bigqueryType}.`);
  }

  if (isRecord && !field.fields?.length) {
    throw new Error(`${path} is a record without nested fields.`);
  }

  for (const child of field.fields ?? []) {
    validateField(child, `${path}.${child.name}`);
  }
}

function renderAll(manifest) {
  mkdirSync(outputDirectory, { recursive: true });

  for (const table of manifest.tables) {
    const outputPath = datasourcePath(table);
    writeFileSync(outputPath, renderDatasource(table), "utf8");
  }

  console.log(`Rendered ${manifest.tables.length} raw Data Sources in ${outputDirectory}.`);
}

function checkRenderedFiles(manifest) {
  const expectedNames = new Set(
    manifest.tables.map((table) => `${table.resourceName}.datasource`),
  );
  const actualNames = new Set(
    readdirSync(outputDirectory).filter((name) => name.endsWith(".datasource")),
  );
  const problems = [];

  for (const table of manifest.tables) {
    const fileName = `${table.resourceName}.datasource`;

    if (!actualNames.has(fileName)) {
      problems.push(`missing ${fileName}`);
      continue;
    }

    const actual = readFileSync(datasourcePath(table), "utf8");
    const expected = renderDatasource(table);

    if (actual !== expected) {
      problems.push(`out of date ${fileName}`);
    }
  }

  for (const actualName of actualNames) {
    if (!expectedNames.has(actualName)) {
      problems.push(`unexpected ${actualName}`);
    }
  }

  if (problems.length > 0) {
    throw new Error(`Raw Data Source check failed:\n- ${problems.join("\n- ")}`);
  }

  console.log(`Checked ${manifest.tables.length} generated raw Data Sources: no drift.`);
}

function renderDatasource(table) {
  const manifest = loadManifest();
  const binding = manifest.deploymentBinding;
  const source = `${table.source.project}.${table.source.dataset}.${table.source.table}`;
  const schemaFields = [
    ...table.fields.map(renderSchemaField),
    ...renderInternalSortingFields(table),
  ];
  const schema = schemaFields.join(",\n");
  const sortingKey = table.sortingKey
    .map((fieldName, index) => renderSortingKey(table, fieldName, index))
    .join(", ");

  return [
    "DESCRIPTION >",
    `    Raw schema mirror of ${source}.`,
    `    Source role: ${table.sourceRole}.`,
    "    Nullable source keys use non-null internal DEFAULT projections for MergeTree sorting.",
    "",
    "SCHEMA >",
    schema,
    "",
    'ENGINE "MergeTree"',
    `ENGINE_SORTING_KEY "${sortingKey}"`,
    "",
    `IMPORT_CONNECTION_NAME ${binding.connectionName}`,
    `IMPORT_BUCKET_URI ${binding.bucketPrefix}/${normalizeObjectSegment(table.source.dataset)}/${normalizeObjectSegment(table.source.table)}/**/*.${binding.format}`,
    `IMPORT_SCHEDULE '${binding.schedule}'`,
    `IMPORT_FORMAT ${binding.format}`,
    "",
  ].join("\n");
}

function normalizeObjectSegment(value) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function renderInternalSortingFields(table) {
  return table.sortingKey.flatMap((fieldName, index) => {
    const field = table.fields.find((candidate) => candidate.name === fieldName);

    if (!field) {
      throw new Error(`${table.resourceName} sorting key ${fieldName} is not a source field.`);
    }

    if (field.mode === "REQUIRED") {
      return [];
    }

    if (field.mode === "REPEATED") {
      throw new Error(`${table.resourceName} cannot sort by repeated field ${fieldName}.`);
    }

    const name = internalSortingFieldName(index);
    const expression = renderNonNullableExpression(field);
    return [
      `    \`${name}\` ${tinybirdBaseType(field)} \`json:$.${name}\` DEFAULT ${expression}`,
    ];
  });
}

function renderSortingKey(table, fieldName, index) {
  const field = table.fields.find((candidate) => candidate.name === fieldName);

  if (!field) {
    throw new Error(`${table.resourceName} sorting key ${fieldName} is not a source field.`);
  }

  if (field.mode === "REQUIRED") {
    return targetFieldName(field);
  }

  return internalSortingFieldName(index);
}

function internalSortingFieldName(index) {
  return `__tb_sort_${index + 1}`;
}

function renderNonNullableExpression(field) {
  const fieldName = targetFieldName(field);
  const type = field.bigqueryType.toUpperCase();
  if (["STRING", "BYTES", "JSON", "GEOGRAPHY", "TIME"].includes(type)) {
    return `ifNull(${fieldName}, '')`;
  }

  if (["TIMESTAMP", "DATETIME"].includes(type)) {
    return `ifNull(${fieldName}, toDateTime64(0, 6))`;
  }

  if (type === "DATE") {
    return `ifNull(${fieldName}, toDate(0))`;
  }

  if (["INTEGER", "INT64"].includes(type)) {
    return `ifNull(${fieldName}, toInt64(0))`;
  }

  if (["FLOAT", "FLOAT64"].includes(type)) {
    return `ifNull(${fieldName}, toFloat64(0))`;
  }

  if (["BOOLEAN", "BOOL"].includes(type)) {
    return `ifNull(${fieldName}, false)`;
  }

  if (["NUMERIC", "DECIMAL"].includes(type)) {
    return `ifNull(${fieldName}, toDecimal128(0, 9))`;
  }

  if (["BIGNUMERIC", "BIGDECIMAL"].includes(type)) {
    return `ifNull(${fieldName}, toDecimal256(0, 38))`;
  }

  throw new Error(
    `No non-null sorting projection exists for ${field.name} (${field.bigqueryType}).`,
  );
}

function renderSchemaField(field) {
  const jsonPath = field.mode === "REPEATED" ? `$.${field.name}[:]` : `$.${field.name}`;
  return `    \`${targetFieldName(field)}\` ${tinybirdType(field)} \`json:${jsonPath}\``;
}

function targetFieldName(field) {
  return field.targetName ?? field.name;
}

function tinybirdType(field) {
  const baseType = tinybirdBaseType(field);

  if (field.mode === "REPEATED") {
    return `Array(${baseType})`;
  }

  if (field.mode === "NULLABLE") {
    return `Nullable(${baseType})`;
  }

  return baseType;
}

function tinybirdBaseType(field) {
  const type = field.bigqueryType.toUpperCase();

  if (type !== "RECORD" && type !== "STRUCT") {
    return scalarTypeMap.get(type);
  }

  const children = field.fields.map(
    (child) => `\`${child.name}\` ${tinybirdType(child)}`,
  );
  return `Tuple(${children.join(", ")})`;
}

function checkLiveSchemas(manifest) {
  const liveColumns = fetchLiveColumns(manifest);
  const differences = compareLiveColumns(manifest, liveColumns);

  if (differences.length > 0) {
    throw new Error(`Live BigQuery schema drift detected:\n- ${differences.join("\n- ")}`);
  }

  console.log(
    `Compared ${manifest.tables.length} manifest schemas with live BigQuery INFORMATION_SCHEMA: no drift.`,
  );
}

function fetchLiveColumns(manifest) {
  const datasets = groupTablesByDataset(manifest.tables);
  const rows = [];

  for (const [datasetKey, tables] of datasets) {
    const [project, dataset] = datasetKey.split(".");
    const tableNames = tables.map((table) => table.source.table);
    const query = informationSchemaQuery(project, dataset, tableNames);
    const output = execFileSync(
      "bq",
      [
        "query",
        `--project_id=${project}`,
        "--use_legacy_sql=false",
        "--max_rows=10000",
        "--format=prettyjson",
        query,
      ],
      { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
    );

    const datasetRows = JSON.parse(output).map((row) => ({
      ...row,
      _source_project: project,
      _source_dataset: dataset,
    }));
    rows.push(...datasetRows);
  }

  return rows;
}

function groupTablesByDataset(tables) {
  const groups = new Map();

  for (const table of tables) {
    const key = `${table.source.project}.${table.source.dataset}`;
    const group = groups.get(key) ?? [];
    group.push(table);
    groups.set(key, group);
  }

  return groups;
}

function informationSchemaQuery(project, dataset, tableNames) {
  const names = tableNames.map((name) => `'${escapeSqlString(name)}'`).join(", ");

  return `
SELECT
  table_name,
  ordinal_position,
  column_name,
  data_type,
  is_nullable,
  is_hidden,
  is_system_defined,
  is_partitioning_column
FROM \`${project}.${dataset}.INFORMATION_SCHEMA.COLUMNS\`
WHERE table_name IN (${names})
ORDER BY table_name, ordinal_position, column_name
`.trim();
}

function compareLiveColumns(manifest, liveColumns) {
  const liveByTable = new Map();

  for (const column of liveColumns) {
    const key = columnKey(
      column._source_project,
      column._source_dataset,
      column.table_name,
      column.column_name,
    );
    liveByTable.set(key, column);
  }

  const expectedKeys = new Set();
  const differences = [];

  for (const table of manifest.tables) {
    for (const field of table.fields) {
      const key = columnKey(
        table.source.project,
        table.source.dataset,
        table.source.table,
        field.name,
      );
      expectedKeys.add(key);
      compareField(table, field, liveByTable.get(key), differences);
    }

    for (const pseudoColumn of table.pseudoColumns ?? []) {
      const key = columnKey(
        table.source.project,
        table.source.dataset,
        table.source.table,
        pseudoColumn.name,
      );
      expectedKeys.add(key);
      comparePseudoColumn(table, pseudoColumn, liveByTable.get(key), differences);
    }
  }

  for (const column of liveColumns) {
    const table = manifest.tables.find(
      (candidate) =>
        candidate.source.project === column._source_project &&
        candidate.source.dataset === column._source_dataset &&
        candidate.source.table === column.table_name,
    );

    if (!table) {
      continue;
    }

    const key = columnKey(
      column._source_project,
      column._source_dataset,
      column.table_name,
      column.column_name,
    );

    if (!expectedKeys.has(key)) {
      differences.push(`${table.resourceName} has new live column ${column.column_name}`);
    }
  }

  return differences;
}

function compareField(table, field, live, differences) {
  if (!live) {
    differences.push(`${table.resourceName} is missing live column ${field.name}`);
    return;
  }

  compareValue(table, field, "ordinal", field.ordinal, numberOrNull(live.ordinal_position), differences);
  compareValue(
    table,
    field,
    "type",
    normalizeInformationSchemaType(field.informationSchemaType),
    normalizeInformationSchemaType(live.data_type),
    differences,
  );
  compareValue(
    table,
    field,
    "mode",
    field.mode,
    informationSchemaMode(live),
    differences,
  );

  if (live.is_hidden !== "NO" || live.is_system_defined !== "NO") {
    differences.push(`${table.resourceName}.${field.name} unexpectedly became a pseudo-column`);
  }
}

function comparePseudoColumn(table, column, live, differences) {
  if (!live) {
    differences.push(`${table.resourceName} is missing pseudo-column ${column.name}`);
    return;
  }

  compareValue(
    table,
    column,
    "type",
    normalizeInformationSchemaType(column.informationSchemaType),
    normalizeInformationSchemaType(live.data_type),
    differences,
  );
  compareValue(
    table,
    column,
    "mode",
    column.mode,
    informationSchemaMode(live),
    differences,
  );
  compareValue(
    table,
    column,
    "partitioning",
    column.partitioning,
    live.is_partitioning_column === "YES",
    differences,
  );

  if (live.is_hidden !== "YES" || live.is_system_defined !== "YES") {
    differences.push(`${table.resourceName}.${column.name} is no longer a system pseudo-column`);
  }
}

function compareValue(table, field, property, expected, actual, differences) {
  if (expected === actual) {
    return;
  }

  differences.push(
    `${table.resourceName}.${field.name} ${property}: expected ${expected}; live ${actual}`,
  );
}

function informationSchemaMode(column) {
  if (normalizeInformationSchemaType(column.data_type).startsWith("ARRAY<")) {
    return "REPEATED";
  }

  return column.is_nullable === "YES" ? "NULLABLE" : "REQUIRED";
}

function normalizeInformationSchemaType(value) {
  return value.replace(/\s+/g, " ").replace(/\s*([<>,])\s*/g, "$1").trim().toUpperCase();
}

function numberOrNull(value) {
  if (value === null || value === undefined) {
    return null;
  }

  return Number(value);
}

function columnKey(project, dataset, tableName, columnName) {
  return `${project}\u0000${dataset}\u0000${tableName}\u0000${columnName}`;
}

function datasourcePath(table) {
  return join(outputDirectory, `${table.resourceName}.datasource`);
}

function assertSafeIdentifier(value, label) {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    return;
  }

  throw new Error(`Unsafe ${label}: ${value}`);
}

function escapeSqlString(value) {
  return value.replaceAll("'", "''");
}

function normalizeMode(value) {
  const aliases = new Map([
    ["render", "render"],
    ["--render", "render"],
    ["check", "check"],
    ["--check", "check"],
    ["check-live", "check-live"],
    ["--check-live", "check-live"],
  ]);
  return aliases.get(value) ?? value;
}

function failWithUsage() {
  throw new Error(
    "Usage: node render-raw-datasources.mjs [render|check|check-live]",
  );
}

main();
