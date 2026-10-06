#!/usr/bin/env node

import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const projectDirectory = dirname(scriptDirectory);
const contractPath = join(projectDirectory, "contracts", "raw-bigquery-schemas.json");
const partitionTimeExportPath = "partition_time_v1";
const partitionTimeTargetColumn = "source_partition_time";

const options = parseArguments(process.argv.slice(2));
const contract = JSON.parse(await readFile(contractPath, "utf8"));
const tables = selectTables(contract.tables, options.resources);
const runAt = options.runAt ?? new Date();
const exports = tables.map((table) => buildExport(contract, table, options, runAt));

if (options.planOnly) {
  printPlan(exports, options);
  process.exit(0);
}

await runPool(exports, options.concurrency, runExport);
console.log(`Exported ${exports.length} BigQuery tables to GCS.`);

function parseArguments(arguments_) {
  const parsed = {
    concurrency: 5,
    mode: "backfill",
    overlapMinutes: 20,
    planOnly: false,
    resources: [],
    runAt: null,
  };

  for (const argument of arguments_) {
    if (argument === "--plan") {
      parsed.planOnly = true;
      continue;
    }

    const [name, value] = splitArgument(argument);

    if (name === "--mode") {
      if (!['backfill', 'incremental'].includes(value)) fail(`Unsupported mode: ${value}`);
      parsed.mode = value;
      continue;
    }

    if (name === "--resource") {
      parsed.resources.push(value);
      continue;
    }

    if (name === "--run-at") {
      const runAt = new Date(value);
      if (Number.isNaN(runAt.valueOf())) fail(`Invalid --run-at value: ${value}`);
      parsed.runAt = runAt;
      continue;
    }

    if (name === "--overlap-minutes") {
      parsed.overlapMinutes = positiveInteger(value, name);
      continue;
    }

    if (name === "--concurrency") {
      parsed.concurrency = positiveInteger(value, name);
      continue;
    }

    fail(`Unknown argument: ${argument}`);
  }

  return parsed;
}

function splitArgument(argument) {
  const separator = argument.indexOf('=');
  if (separator === -1) fail(`Expected --name=value, received: ${argument}`);

  const name = argument.slice(0, separator);
  const value = argument.slice(separator + 1);
  if (!value) fail(`${name} requires a value.`);

  return [name, value];
}

function positiveInteger(value, name) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) fail(`${name} must be a positive integer.`);
  return parsed;
}

function selectTables(tables, resources) {
  if (resources.length === 0) return tables;

  const requested = new Set(resources);
  const selected = tables.filter((table) => requested.has(table.resourceName));
  const found = new Set(selected.map((table) => table.resourceName));
  const missing = resources.filter((resource) => !found.has(resource));

  if (missing.length > 0) fail(`Unknown resources: ${missing.join(', ')}`);
  return selected;
}

function buildExport(contract, table, options, runAt) {
  const runDate = formatDate(runAt);
  const runTime = formatTime(runAt);
  const dataset = normalizeObjectSegment(table.source.dataset);
  const sourceTable = normalizeObjectSegment(table.source.table);
  const modePath = options.mode === 'backfill' ? 'backfill' : 'incremental';
  const uri = [
    contract.deploymentBinding.bucketPrefix,
    dataset,
    sourceTable,
    partitionExportPath(table),
    modePath,
    `run_date=${runDate}`,
    `run_time=${runTime}`,
    'part-*.parquet',
  ].filter(Boolean).join('/');
  const source = quoteTable(table.source);
  const selectList = buildSelectList(table);
  const predicate = incrementalPredicate(table, options, runAt);
  const query = [
    'EXPORT DATA OPTIONS(',
    `  uri='${escapeSqlString(uri)}',`,
    "  format='PARQUET',",
    '  overwrite=true',
    ') AS',
    'SELECT',
    selectList,
    `FROM ${source}`,
    predicate ? `WHERE ${predicate}` : '',
  ].filter(Boolean).join('\n');

  return { query, resourceName: table.resourceName, uri };
}

function buildSelectList(table) {
  const projections = table.fields.map((field) => `  ${exportExpression(field)}`);
  const pseudoColumn = table.pseudoColumns?.find((column) => column.partitioning);

  if (pseudoColumn) {
    projections.push(
      `  ${quoteIdentifier(pseudoColumn.name)} AS ${quoteIdentifier(partitionTimeTargetColumn)}`,
    );
  }

  return projections.join(',\n');
}

function partitionExportPath(table) {
  const pseudoColumn = table.pseudoColumns?.find((column) => column.partitioning);
  if (!pseudoColumn) return '';

  return partitionTimeExportPath;
}

function exportExpression(field) {
  const identifier = quoteIdentifier(field.name);
  const type = field.bigqueryType.toUpperCase();

  if (type === 'JSON') {
    return `TO_JSON_STRING(${identifier}) AS ${identifier}`;
  }

  if (type === 'GEOGRAPHY') {
    return `ST_ASWKT(${identifier}) AS ${identifier}`;
  }

  return identifier;
}

function incrementalPredicate(table, options, runAt) {
  if (options.mode !== 'incremental') return '';

  const start = new Date(runAt.valueOf() - options.overlapMinutes * 60_000);
  const version = greatestTimestamp(table.versionColumns);

  return [
    `${version} >= TIMESTAMP '${formatTimestamp(start)}'`,
    `${version} < TIMESTAMP '${formatTimestamp(runAt)}'`,
  ].join('\n  AND ');
}

function greatestTimestamp(columns) {
  if (!columns?.length) fail('Every incremental table needs at least one version column.');

  const expressions = columns.map((column) => (
    `COALESCE(${quoteIdentifier(column)}, TIMESTAMP '1970-01-01 00:00:00+00')`
  ));

  if (expressions.length === 1) return expressions[0];
  return `GREATEST(${expressions.join(', ')})`;
}

async function runPool(items, concurrency, worker) {
  let cursor = 0;
  const failures = [];

  async function consume() {
    while (cursor < items.length) {
      const item = items[cursor];
      cursor += 1;

      try {
        await worker(item);
      } catch (error) {
        failures.push(`${item.resourceName}: ${error.message}`);
      }
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    consume,
  );
  await Promise.all(workers);

  if (failures.length > 0) {
    throw new Error(`BigQuery exports failed:\n- ${failures.join('\n- ')}`);
  }
}

function runExport(item) {
  console.log(`Exporting ${item.resourceName} -> ${item.uri}`);

  return new Promise((resolve, reject) => {
    const child = spawn('bq', [
      'query',
      `--project_id=${contract.sourceProject}`,
      '--location=US',
      '--use_legacy_sql=false',
      '--format=none',
      item.query,
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });

    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      const details = [stdout.trim(), stderr.trim()].filter(Boolean).join('\n');
      reject(new Error(details || `bq exited with status ${code}`));
    });
  });
}

function printPlan(exports, options) {
  console.log(`Mode: ${options.mode}`);
  console.log(`Tables: ${exports.length}`);
  console.log(`Concurrency: ${options.concurrency}`);

  for (const item of exports) {
    console.log(`${item.resourceName}\t${item.uri}`);
  }
}

function quoteTable(source) {
  const path = [source.project, source.dataset, source.table]
    .map((part) => part.replaceAll('`', ''))
    .join('.');
  return `\`${path}\``;
}

function quoteIdentifier(value) {
  return `\`${value.replaceAll('`', '')}\``;
}

function normalizeObjectSegment(value) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function formatTime(date) {
  return date.toISOString().slice(11, 19).replaceAll(':', '');
}

function formatTimestamp(date) {
  return date.toISOString().replace('T', ' ').replace('Z', '+00');
}

function escapeSqlString(value) {
  return value.replaceAll("'", "''");
}

function fail(message) {
  console.error(message);
  console.error('Usage: node scripts/export-bigquery-to-gcs.mjs [--mode=backfill|incremental] [--resource=name] [--run-at=ISO] [--overlap-minutes=20] [--concurrency=5] [--plan]');
  process.exit(1);
}
