#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { validateSourceRegistration } from "./source-registration.mjs";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const workerDirectory = dirname(scriptDirectory);
const repositoryDirectory = resolve(workerDirectory, "..", "..");
const contractPath = join(
  repositoryDirectory,
  "tinybird",
  "contracts",
  "raw-bigquery-schemas.json",
);
const derivedContractPath = join(
  repositoryDirectory,
  "tinybird",
  "contracts",
  "derived-bigquery-exports.json",
);
const sourceRegistryPath = join(
  repositoryDirectory,
  "tinybird",
  "project",
  "source-registry.json",
);
const outputPath = join(workerDirectory, "src", "table-manifest.generated.ts");

const contractText = await readFile(contractPath, "utf8");
const contract = JSON.parse(contractText);
const derivedContractText = await readFile(derivedContractPath, "utf8");
const derivedContract = JSON.parse(derivedContractText);
const sourceRegistry = JSON.parse(await readFile(sourceRegistryPath, "utf8"));

try {
  validateSourceRegistration(contract, derivedContract, sourceRegistry);
} catch (error) {
  fail(error.message);
}

const manifest = buildManifest(contract, derivedContract);
const generated = renderManifest(manifest, contractText, derivedContractText);

if (process.argv.includes("--check")) {
  await checkGeneratedFile(generated);
  process.exit(0);
}

await writeFile(outputPath, generated);
console.log(`Generated ${manifest.length} table entries at ${outputPath}.`);

function buildManifest(contract, derivedContract) {
  if (!Array.isArray(contract.tables)) {
    fail("The raw BigQuery contract has no tables array.");
  }

  if (!Array.isArray(derivedContract.exports)) {
    fail("The derived BigQuery export contract has no exports array.");
  }

  const manifest = [
    ...contract.tables.map(compactTable),
    ...derivedContract.exports.map(compactDerivedExport),
  ].sort((left, right) => left.resourceName.localeCompare(right.resourceName));

  return manifest;
}

function compactTable(table) {
  const fields = new Map(table.fields.map((field) => [field.name, field]));
  const versionColumns = table.versionColumns.map((name) => {
    const field = fields.get(name);
    if (!field) fail(`${table.resourceName} has an unknown version column: ${name}`);

    return { name, bigqueryType: field.bigqueryType };
  });
  const watermarkColumns = versionColumns.filter(({ bigqueryType }) => (
    ["DATE", "DATETIME", "TIMESTAMP"].includes(bigqueryType)
  ));

  if (watermarkColumns.length === 0) {
    fail(`${table.resourceName} has no temporal version column.`);
  }

  return {
    resourceName: table.resourceName,
    exportKind: "table",
    source: table.source,
    unionSources: [],
    sourcePartitioning: compactPartitioning(table, fields),
    versionColumns,
    watermarkColumns,
    columns: table.fields.map((field) => field.name),
    columnTypes: Object.fromEntries(
      table.fields.map((field) => [field.name, field.bigqueryType]),
    ),
    jsonColumns: table.fields
      .filter((field) => field.bigqueryType === "JSON")
      .map((field) => field.name),
    geographyColumns: table.fields
      .filter((field) => field.bigqueryType === "GEOGRAPHY")
      .map((field) => field.name),
  };
}

function compactDerivedExport(exportDefinition) {
  if (exportDefinition.exportKind !== "typed_union") {
    fail(`Unsupported derived export kind: ${exportDefinition.exportKind}`);
  }

  if (!exportDefinition.unionSources?.length) {
    fail(`${exportDefinition.resourceName} must combine at least one source table.`);
  }

  const fields = new Map(
    exportDefinition.fields.map((field) => [field.name, field]),
  );
  const versionColumns = exportDefinition.versionColumns.map((name) => {
    const field = fields.get(name);
    if (!field) {
      fail(`${exportDefinition.resourceName} has an unknown version column: ${name}`);
    }

    return { name, bigqueryType: field.bigqueryType };
  });
  const watermarkColumns = versionColumns.filter(({ bigqueryType }) => (
    ["DATE", "DATETIME", "TIMESTAMP"].includes(bigqueryType)
  ));

  if (watermarkColumns.length === 0) {
    fail(`${exportDefinition.resourceName} has no temporal version column.`);
  }

  return {
    resourceName: exportDefinition.resourceName,
    exportKind: exportDefinition.exportKind,
    source: exportDefinition.source,
    unionSources: exportDefinition.unionSources,
    sourcePartitioning: null,
    versionColumns,
    watermarkColumns,
    columns: exportDefinition.fields.map((field) => field.name),
    columnTypes: Object.fromEntries(
      exportDefinition.fields.map((field) => [field.name, field.bigqueryType]),
    ),
    jsonColumns: [],
    geographyColumns: [],
  };
}

function compactPartitioning(table, fields) {
  if (!table.sourcePartitioning) return null;

  const fieldName = table.sourcePartitioning.field;
  const field = fieldName
    ? fields.get(fieldName)
    : table.pseudoColumns.find((column) => column.partitioning);

  if (!field) {
    fail(`${table.resourceName} has partition metadata with no partition column.`);
  }

  return {
    type: table.sourcePartitioning.type,
    field: fieldName,
    pseudoColumn: fieldName ? null : field.name,
    bigqueryType: field.bigqueryType || field.informationSchemaType,
  };
}

function renderManifest(manifest, contractText, derivedContractText) {
  const sourceSha256 = createHash("sha256")
    .update(contractText)
    .update(derivedContractText)
    .digest("hex");
  const entries = manifest.map((table) => `  ${JSON.stringify(table)},`).join("\n");

  return [
    "// Generated by scripts/generate-table-manifest.mjs. Do not edit by hand.",
    `// Source SHA-256: ${sourceSha256}`,
    "",
    "export interface SourceTable {",
    "  resourceName: string;",
    "  exportKind: \"table\" | \"typed_union\";",
    "  source: { project: string; dataset: string; table: string };",
    "  unionSources: ReadonlyArray<{",
    "    recordType: string;",
    "    paymentSource: string;",
    "    source: { project: string; dataset: string; table: string };",
    "    watermarkColumn: string;",
    "    columnMappings: Readonly<Record<string, string>>;",
    "  }>;",
    "  sourcePartitioning: {",
    "    type: string;",
    "    field: string | null;",
    "    pseudoColumn: string | null;",
    "    bigqueryType: string;",
    "  } | null;",
    "  versionColumns: ReadonlyArray<{ name: string; bigqueryType: string }>;",
    "  watermarkColumns: ReadonlyArray<{ name: string; bigqueryType: string }>;",
    "  columns: readonly string[];",
    "  columnTypes: Readonly<Record<string, string>>;",
    "  jsonColumns: readonly string[];",
    "  geographyColumns: readonly string[];",
    "}",
    "",
    "export const TABLE_MANIFEST: readonly SourceTable[] = [",
    entries,
    "];",
    "",
  ].join("\n");
}

async function checkGeneratedFile(expected) {
  let current;

  try {
    current = await readFile(outputPath, "utf8");
  } catch {
    fail("The generated Worker table manifest is missing. Run npm run manifest:generate.");
  }

  if (current !== expected) {
    fail("The generated Worker table manifest is stale. Run npm run manifest:generate.");
  }

  console.log(
    `Worker table manifest matches ${manifest.length} registered sync resources.`,
  );
}

function fail(message) {
  console.error(message);
  process.exit(1);
}
