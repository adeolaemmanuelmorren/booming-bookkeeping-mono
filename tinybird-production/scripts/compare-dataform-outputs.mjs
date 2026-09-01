import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadOutputContract,
  selectOutputs,
} from "./lib/parity-contract.mjs";
import {
  buildBigQueryDiagnosticSql,
  buildBigQueryDigestSql,
  buildBigQuerySchemaSql,
  buildBigQueryUniquenessSql,
  buildTinybirdDiagnosticSql,
  buildTinybirdDigestSql,
  buildTinybirdUniquenessSql,
  metricDefinitions,
  parseBigQueryTableReference,
} from "./lib/parity-sql.mjs";
import {
  areTypesCompatible,
  parseBigQueryType,
  parseTinybirdType,
} from "./lib/parity-types.mjs";
import { resolveTinybirdTarget } from "./lib/tinybird-target.mjs";

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const defaultContractPath = path.join(
  projectRoot,
  "scripts",
  "parity",
  "output-contract.json",
);

main();

function main() {
  try {
    const argumentsByName = parseArguments(process.argv.slice(2));
    validateArgumentNames(argumentsByName);
    const contractPath = path.resolve(
      projectRoot,
      firstArgument(argumentsByName, "contract") ?? defaultContractPath,
    );
    const contract = loadOutputContract(projectRoot, contractPath);
    const config = buildRunConfig(argumentsByName, contract);

    if (config.planOnly) {
      printPlan(config, contractPath);
      return;
    }

    const result = runComparison(config, contract, contractPath);
    console.log(JSON.stringify(result, null, 2));

    if (result.status !== "passed") {
      process.exitCode = 1;
    }
  } catch (error) {
    console.error(sanitizeError(error));
    process.exitCode = 1;
  }
}

function buildRunConfig(argumentsByName, contract) {
  const target = resolveTinybirdTarget(flattenArguments(argumentsByName));
  const outputs = selectOutputs(contract, {
    outputs: argumentsByName.output ?? [],
    layer: integerArgument(argumentsByName, "layer"),
  });
  const planOnly = booleanArgument(argumentsByName, "plan", false);
  const snapshotAt = firstArgument(argumentsByName, "snapshot");
  const generation = firstArgument(argumentsByName, "generation");
  const quietCutConfirmed = booleanArgument(
    argumentsByName,
    "quiet-cut-confirmed",
    false,
  );
  const diagnostics = firstArgument(argumentsByName, "diagnostics") ?? "summary";
  const diagnosticLimit = integerArgument(
    argumentsByName,
    "diagnostic-limit",
  ) ?? 200;
  const viewSnapshots = parseViewSnapshots(argumentsByName["view-snapshot"] ?? []);
  const evidenceDirectory = path.resolve(
    projectRoot,
    firstArgument(argumentsByName, "evidence-dir") ?? ".parity-evidence",
  );

  if (!["summary", "hashed-rows"].includes(diagnostics)) {
    throw new Error("--diagnostics must be summary or hashed-rows");
  }

  if (!Number.isInteger(diagnosticLimit) || diagnosticLimit < 1 || diagnosticLimit > 10000) {
    throw new Error("--diagnostic-limit must be from 1 through 10000");
  }

  validateViewSnapshotNames(viewSnapshots, contract.outputs);

  if (!planOnly) {
    validateExecutionIdentity({
      snapshotAt,
      generation,
      quietCutConfirmed,
      target,
    });
    requireFrozenViews(outputs, viewSnapshots);
  }

  return {
    target,
    outputs,
    planOnly,
    snapshotAt,
    generation,
    quietCutConfirmed,
    diagnostics,
    diagnosticLimit,
    viewSnapshots,
    evidenceDirectory,
  };
}

function printPlan(config, contractPath) {
  const views = config.outputs.filter((output) => output.bigquery_kind === "view");
  const plan = {
    mode: "plan",
    target: config.target.name,
    branch: config.target.branch,
    contract: contractPath,
    output_count: config.outputs.length,
    layers: [...new Set(config.outputs.map((output) => output.layer))],
    outputs: config.outputs.map((output) => ({
      name: output.name,
      layer: output.layer,
      bigquery_kind: output.bigquery_kind,
      unique_key: output.unique_key,
      diagnostic_key: output.diagnostic_key,
    })),
    required_frozen_views: views.map((output) => ({
      output: output.name,
      supplied_table: config.viewSnapshots.get(output.name) ?? null,
    })),
    execution_requirements: [
      "--snapshot=<BigQuery quiet-cut timestamp>",
      "--generation=<quiet-cut or refresh generation identifier>",
      "--quiet-cut-confirmed=true for production",
      "one --view-snapshot=<output>=<project.dataset.table> for each selected BigQuery view",
    ],
  };

  console.log(JSON.stringify(plan, null, 2));
}

function runComparison(config, contract, contractPath) {
  const startedAt = new Date().toISOString();
  const evidencePath = prepareEvidencePath(config);
  const runResult = {
    contract_version: contract.version,
    contract_sha256: sha256(readFileSync(contractPath, "utf8")),
    generation: config.generation,
    snapshot_at: config.snapshotAt,
    quiet_cut_confirmed: config.quietCutConfirmed,
    target: config.target.name,
    branch: config.target.branch,
    diagnostics: config.diagnostics,
    started_at: startedAt,
    completed_at: null,
    status: "running",
    stopped_after_layer: null,
    outputs: [],
  };

  saveEvidence(evidencePath, runResult);

  for (const layer of selectedLayers(config.outputs)) {
    const layerOutputs = config.outputs.filter((output) => output.layer === layer);
    let layerFailed = false;

    for (const output of layerOutputs) {
      const result = safelyCompareOutput(output, config, contract);
      runResult.outputs.push(result);
      layerFailed ||= result.status !== "passed";
      saveEvidence(evidencePath, runResult);
    }

    if (layerFailed) {
      runResult.stopped_after_layer = layer;
      break;
    }
  }

  const comparedAll = runResult.outputs.length === config.outputs.length;
  const allPassed = runResult.outputs.every((output) => output.status === "passed");
  runResult.status = comparedAll && allPassed ? "passed" : "failed";
  runResult.completed_at = new Date().toISOString();
  runResult.duration_ms = Date.parse(runResult.completed_at) - Date.parse(startedAt);
  runResult.evidence_file = evidencePath;
  saveEvidence(evidencePath, runResult);
  return runResult;
}

function safelyCompareOutput(output, config, contract) {
  const startedAt = new Date().toISOString();

  try {
    const result = compareOutput(output, config, contract);
    return {
      ...result,
      started_at: startedAt,
      completed_at: new Date().toISOString(),
    };
  } catch (error) {
    return {
      name: output.name,
      layer: output.layer,
      status: "error",
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      error: sanitizeError(error),
    };
  }
}

function compareOutput(output, config, contract) {
  const source = resolveBigQuerySource(output, config, contract);
  const bigQuerySchema = queryBigQuerySchema(source, config, contract);
  const tinybirdSchema = queryTinybirdSchema(output, config);
  const schema = compareSchemas(bigQuerySchema.columns, tinybirdSchema);
  const result = {
    name: output.name,
    layer: output.layer,
    status: "running",
    bigquery_source: source.reference,
    bigquery_source_kind: source.frozen ? "frozen_table" : output.bigquery_kind,
    tinybird_resource: output.tinybird_resource,
    unique_key: output.unique_key,
    diagnostic_key: output.diagnostic_key,
    schema,
    uniqueness: null,
    data: null,
    diagnostics: null,
  };

  if (!schema.structural_match) {
    result.status = "failed";
    return result;
  }

  validateContractColumns(output, bigQuerySchema.columns);

  if (output.unique_key.length > 0) {
    result.uniqueness = compareUniqueness(
      output,
      source,
      config,
      contract,
    );
  }

  const dataComparison = compareDigest(
    output,
    source,
    bigQuerySchema.columns,
    config,
    contract,
  );
  result.data = dataComparison;

  if (config.diagnostics === "hashed-rows" && !dataComparison.matches) {
    result.diagnostics = compareHashedRows(
      output,
      source,
      bigQuerySchema.columns,
      dataComparison.mismatched_bucket_numbers,
      config,
      contract,
    );
  }

  const uniquenessMatches = result.uniqueness?.matches ?? true;
  const matches = schema.matches && uniquenessMatches && dataComparison.matches;
  result.status = matches ? "passed" : "failed";
  return result;
}

function queryBigQuerySchema(source, config, contract) {
  const table = parseBigQueryTableReference(source.reference);
  const rows = queryBigQuery(
    buildBigQuerySchemaSql(source.reference),
    {
      contract,
      parameters: [`table_name:STRING:${table.table}`],
      maxRows: 1000,
    },
  );

  if (rows.length === 0) {
    throw new Error(`BigQuery source ${source.reference} has no columns`);
  }

  const tableTypes = new Set(rows.map((row) => row.table_type));
  if (source.frozen && (tableTypes.size !== 1 || tableTypes.has("VIEW"))) {
    throw new Error(`${source.reference} must be a frozen table, clone, or snapshot`);
  }

  return {
    columns: rows.map((row) => ({
      name: row.column_name,
      data_type: row.data_type,
      is_nullable: row.is_nullable,
      ordinal_position: Number(row.ordinal_position),
    })),
    table_types: [...tableTypes],
  };
}

function queryTinybirdSchema(output, config) {
  const sql = `SELECT * FROM \`${output.tinybird_resource}\` LIMIT 0 FORMAT JSON`;
  const response = config.target.name === "cloud"
    ? queryTinybirdCloudSql(sql)
    : queryTinybird(
      `SELECT * FROM \`${output.tinybird_resource}\` LIMIT 1`,
      config,
      1,
    );

  if (!Array.isArray(response.meta) || response.meta.length === 0) {
    throw new Error(`${output.tinybird_resource} returned no Tinybird schema`);
  }

  return response.meta.map((column, index) => ({
    name: column.name,
    type: column.type,
    ordinal_position: index + 1,
  }));
}

function queryTinybirdCloudSql(sql) {
  const tinybird = JSON.parse(
    readFileSync(path.join(projectRoot, ".tinyb"), "utf8"),
  );
  const output = execFileSync(
    "curl",
    [
      "-sS",
      "-G",
      "-H",
      `Authorization: Bearer ${tinybird.token}`,
      "--data-urlencode",
      `q=${sql}`,
      `${tinybird.host}/v0/sql`,
    ],
    {
      encoding: "utf8",
      maxBuffer: 256 * 1024 * 1024,
    },
  );
  return parseJsonOutput(output);
}

function compareSchemas(bigQueryColumns, tinybirdColumns) {
  const mismatches = [];
  const comparedColumns = Math.max(bigQueryColumns.length, tinybirdColumns.length);

  for (let index = 0; index < comparedColumns; index += 1) {
    const expected = bigQueryColumns[index] ?? null;
    const actual = tinybirdColumns[index] ?? null;

    if (!expected || !actual) {
      mismatches.push({
        ordinal_position: index + 1,
        kind: "missing_column",
        bigquery: expected,
        tinybird: actual,
      });
      continue;
    }

    if (expected.name !== actual.name) {
      mismatches.push({
        ordinal_position: index + 1,
        kind: "column_name",
        bigquery: expected.name,
        tinybird: actual.name,
      });
      continue;
    }

    const typeResult = areTypesCompatible(expected.data_type, actual.type);
    if (!typeResult.compatible) {
      mismatches.push({
        column: expected.name,
        kind: "type",
        bigquery: expected.data_type,
        tinybird: actual.type,
        reason: typeResult.reason,
      });
    }

    const tinybirdType = parseTinybirdType(actual.type);
    const nullabilityCompatible = expected.is_nullable === "YES"
      || tinybirdType.nullable === false;
    if (!nullabilityCompatible) {
      mismatches.push({
        column: expected.name,
        kind: "nullability",
        bigquery: "required",
        tinybird: "nullable",
      });
    }
  }

  const structuralMismatchKinds = new Set(["missing_column", "column_name", "type"]);
  return {
    matches: mismatches.length === 0,
    structural_match: !mismatches.some((mismatch) =>
      structuralMismatchKinds.has(mismatch.kind)
    ),
    bigquery_column_count: bigQueryColumns.length,
    tinybird_column_count: tinybirdColumns.length,
    bigquery_schema_sha256: sha256(JSON.stringify(bigQueryColumns)),
    tinybird_schema_sha256: sha256(JSON.stringify(tinybirdColumns)),
    mismatches,
  };
}

function compareUniqueness(output, source, config, contract) {
  const bigQueryRows = queryBigQuery(
    buildBigQueryUniquenessSql({ source, key: output.unique_key }),
    bigQueryQueryOptions(source, config, contract, 10),
  );
  const tinybirdRows = queryTinybird(
    buildTinybirdUniquenessSql({
      resource: output.tinybird_resource,
      key: output.unique_key,
    }),
    config,
    10,
  ).data;
  const bigQuery = normalizeUniqueness(bigQueryRows[0]);
  const tinybird = normalizeUniqueness(tinybirdRows[0]);
  const matches = bigQuery.duplicate_key_count === "0"
    && tinybird.duplicate_key_count === "0"
    && JSON.stringify(bigQuery) === JSON.stringify(tinybird);

  return { matches, bigquery: bigQuery, tinybird };
}

function compareDigest(output, source, columns, config, contract) {
  const baseOptions = {
    columns,
    source,
    resource: output.tinybird_resource,
    bucketCount: contract.bucket_count,
    floatScale: contract.float_scale,
    arrayPolicies: output.array_policies ?? {},
  };
  const shardCount = output.digest_shards ?? 1;
  const bigQueryRows = [];
  const tinybirdRows = [];

  for (let shardNumber = 0; shardNumber < shardCount; shardNumber++) {
    const options = {
      ...baseOptions,
      shardCount,
      shardNumber,
      shardKey: output.digest_shard_key,
      shardStrategy: output.digest_shard_strategy ?? "hash",
    };
    const expected = queryBigQuery(
      buildBigQueryDigestSql(options),
      bigQueryQueryOptions(source, config, contract, contract.bucket_count + 10),
    );
    const actual = queryTinybird(
      buildTinybirdDigestSql(options),
      config,
      contract.bucket_count + 10,
    ).data;

    bigQueryRows.push(...labelDigestShard(expected, shardNumber));
    tinybirdRows.push(...labelDigestShard(actual, shardNumber));
  }

  const metrics = metricDefinitions(columns);
  const bigQuery = summarizeDigest(bigQueryRows, metrics, contract.float_scale);
  const tinybird = summarizeDigest(tinybirdRows, metrics, contract.float_scale);
  const bucketMismatches = compareBuckets(bigQueryRows, tinybirdRows, metrics);
  const nullMismatches = compareNamedValues(
    bigQuery.null_counts,
    tinybird.null_counts,
  );
  const numericMismatches = compareNamedValues(
    bigQuery.numeric_totals_scaled,
    tinybird.numeric_totals_scaled,
  );
  const mismatchedBucketNumbers = bucketMismatches.map((mismatch) =>
    Number(mismatch.bucket.split("/").at(-1))
  );

  return {
    matches: bucketMismatches.length === 0
      && nullMismatches.length === 0
      && numericMismatches.length === 0,
    float_and_decimal_scale: contract.float_scale,
    digest_shards: shardCount,
    bigquery: bigQuery,
    tinybird,
    bucket_mismatches: bucketMismatches,
    null_count_mismatches: nullMismatches,
    numeric_total_mismatches: numericMismatches,
    mismatched_bucket_numbers: mismatchedBucketNumbers,
  };
}

function labelDigestShard(rows, shardNumber) {
  return rows.map((row) => ({
    ...row,
    bucket: `${shardNumber}/${row.bucket}`,
  }));
}

function compareHashedRows(
  output,
  source,
  columns,
  mismatchedBuckets,
  config,
  contract,
) {
  if (mismatchedBuckets.length === 0) {
    return {
      mode: "hashed-rows",
      note: "No row-hash bucket mismatch was available for row diagnostics.",
      differences: [],
    };
  }

  const options = {
    columns,
    source,
    resource: output.tinybird_resource,
    diagnosticKey: output.diagnostic_key,
    buckets: mismatchedBuckets,
    bucketCount: contract.bucket_count,
    floatScale: contract.float_scale,
    arrayPolicies: output.array_policies ?? {},
    limit: config.diagnosticLimit,
  };
  const bigQueryRows = queryBigQuery(
    buildBigQueryDiagnosticSql(options),
    bigQueryQueryOptions(source, config, contract, config.diagnosticLimit),
  );
  const tinybirdRows = queryTinybird(
    buildTinybirdDiagnosticSql(options),
    config,
    config.diagnosticLimit,
  ).data;
  const differences = compareHashedRowSets(bigQueryRows, tinybirdRows);

  return {
    mode: "hashed-rows",
    pii_policy: "Only SHA-256 key and row hashes are emitted. Raw field values never leave either query.",
    row_limit_per_system: config.diagnosticLimit,
    bigquery_rows_returned: bigQueryRows.length,
    tinybird_rows_returned: tinybirdRows.length,
    limit_reached: bigQueryRows.length === config.diagnosticLimit
      || tinybirdRows.length === config.diagnosticLimit,
    differences,
  };
}

function summarizeDigest(rows, metrics, scale) {
  const normalizedRows = normalizeDigestRows(rows, metrics);
  const coreRows = normalizedRows.map((row) => ({
    bucket: row.bucket,
    row_count: row.row_count,
    fingerprint_sum: row.fingerprint_sum,
    fingerprint_xor: row.fingerprint_xor,
  }));
  const nullCounts = {};
  const numericTotals = {};

  for (const metric of metrics) {
    nullCounts[metric.name] = sumField(normalizedRows, metric.null_alias);
    if (metric.numeric_alias) {
      numericTotals[metric.name] = sumField(normalizedRows, metric.numeric_alias);
    }
  }

  return {
    row_count: sumField(normalizedRows, "row_count"),
    populated_bucket_count: normalizedRows.length,
    bucket_digest_sha256: sha256(coreRows.map((row) => JSON.stringify(row)).join("\n")),
    null_counts: nullCounts,
    numeric_totals_scaled: numericTotals,
    numeric_scale: scale,
  };
}

function normalizeDigestRows(rows, metrics) {
  return rows.map((row) => {
    const normalized = {
      bucket: String(row.bucket),
      row_count: String(row.row_count),
      fingerprint_sum: String(row.fingerprint_sum),
      fingerprint_xor: String(row.fingerprint_xor),
    };

    for (const metric of metrics) {
      normalized[metric.null_alias] = String(row[metric.null_alias] ?? "0");
      if (metric.numeric_alias) {
        normalized[metric.numeric_alias] = String(row[metric.numeric_alias] ?? "0");
      }
    }

    return normalized;
  }).sort((left, right) => compareBucketLabels(left.bucket, right.bucket));
}

function compareBucketLabels(left, right) {
  const leftParts = left.split("/").map(Number);
  const rightParts = right.split("/").map(Number);
  const length = Math.max(leftParts.length, rightParts.length);

  for (let index = 0; index < length; index++) {
    const difference = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (difference !== 0) return difference;
  }

  return 0;
}

function compareBuckets(bigQueryRows, tinybirdRows, metrics) {
  const bigQuery = new Map(
    normalizeDigestRows(bigQueryRows, metrics).map((row) => [row.bucket, row]),
  );
  const tinybird = new Map(
    normalizeDigestRows(tinybirdRows, metrics).map((row) => [row.bucket, row]),
  );
  const buckets = [...new Set([...bigQuery.keys(), ...tinybird.keys()])]
    .sort(compareBucketLabels);
  const mismatches = [];

  for (const bucket of buckets) {
    const expected = bigQuery.get(bucket) ?? null;
    const actual = tinybird.get(bucket) ?? null;
    if (JSON.stringify(expected) === JSON.stringify(actual)) continue;

    mismatches.push({
      bucket,
      bigquery: summarizeBucket(expected, metrics),
      tinybird: summarizeBucket(actual, metrics),
    });
  }

  return mismatches;
}

function summarizeBucket(row, metrics) {
  if (!row) return null;
  const nullDifferences = {};
  const numericDifferences = {};

  for (const metric of metrics) {
    nullDifferences[metric.name] = row[metric.null_alias];
    if (metric.numeric_alias) {
      numericDifferences[metric.name] = row[metric.numeric_alias];
    }
  }

  return {
    row_count: row.row_count,
    fingerprint_sum: row.fingerprint_sum,
    fingerprint_xor: row.fingerprint_xor,
    null_counts: nullDifferences,
    numeric_totals_scaled: numericDifferences,
  };
}

function compareNamedValues(expected, actual) {
  const names = [...new Set([...Object.keys(expected), ...Object.keys(actual)])];
  return names.flatMap((name) => {
    if (expected[name] === actual[name]) return [];
    return [{ name, bigquery: expected[name] ?? null, tinybird: actual[name] ?? null }];
  });
}

function compareHashedRowSets(bigQueryRows, tinybirdRows) {
  const rowKey = (row) => [
    row.bucket,
    row.key_hash,
    row.row_hash,
    row.occurrence_count,
  ].map(String).join("|");
  const expected = new Set(bigQueryRows.map(rowKey));
  const actual = new Set(tinybirdRows.map(rowKey));

  return {
    only_in_bigquery: [...expected].filter((value) => !actual.has(value)),
    only_in_tinybird: [...actual].filter((value) => !expected.has(value)),
  };
}

function normalizeUniqueness(row) {
  if (!row) {
    return { row_count: "0", duplicate_key_count: "0", duplicate_rows: "0" };
  }

  return {
    row_count: String(row.row_count),
    duplicate_key_count: String(row.duplicate_key_count),
    duplicate_rows: String(row.duplicate_rows),
  };
}

function validateContractColumns(output, columns) {
  const names = new Set(columns.map((column) => column.name));
  const requiredNames = new Set([
    ...output.unique_key,
    ...output.diagnostic_key,
    ...(output.digest_shard_key ? [output.digest_shard_key] : []),
    ...Object.keys(output.array_policies ?? {}).map((name) => name.split(".")[0]),
  ]);
  const missing = [...requiredNames].filter((name) => !names.has(name));

  if (missing.length > 0) {
    throw new Error(`${output.name} contract columns are missing: ${missing.join(", ")}`);
  }

  for (const column of columns) {
    parseBigQueryType(column.data_type);
  }
}

function resolveBigQuerySource(output, config, contract) {
  const frozenReference = config.viewSnapshots.get(output.name);
  if (frozenReference) {
    parseBigQueryTableReference(frozenReference);
    return { reference: frozenReference, timeTravel: false, frozen: true };
  }

  const { project, dataset } = contract.bigquery;
  return {
    reference: `${project}.${dataset}.${output.name}`,
    timeTravel: output.bigquery_kind === "table",
    frozen: false,
  };
}

function queryBigQuery(sql, options) {
  const argumentsForBq = [
    "query",
    "--quiet",
    "--use_legacy_sql=false",
    `--project_id=${options.contract.bigquery.project}`,
    `--location=${options.contract.bigquery.location}`,
    "--format=json",
    `--max_rows=${options.maxRows}`,
    ...(options.parameters ?? []).map((parameter) => `--parameter=${parameter}`),
    sql,
  ];
  const output = execFileSync("bq", argumentsForBq, {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
  return parseJsonOutput(output);
}

function queryTinybird(sql, config, rowsLimit) {
  const output = execFileSync(
    "tb",
    [
      ...config.target.cliArguments,
      "--output",
      "json",
      "sql",
      "--rows-limit",
      String(rowsLimit),
      sql,
    ],
    {
      cwd: projectRoot,
      encoding: "utf8",
      maxBuffer: 256 * 1024 * 1024,
    },
  );
  return parseJsonOutput(output);
}

function bigQueryQueryOptions(source, config, contract, maxRows) {
  const parameters = source.timeTravel
    ? [`snapshot_at:STRING:${config.snapshotAt}`]
    : [];
  return { contract, parameters, maxRows };
}

function prepareEvidencePath(config) {
  const generationDirectory = path.join(
    config.evidenceDirectory,
    config.generation,
  );
  mkdirSync(generationDirectory, { recursive: true, mode: 0o700 });
  return path.join(generationDirectory, "run.json");
}

function saveEvidence(evidencePath, result) {
  writeFileSync(evidencePath, `${JSON.stringify(result, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
}

function selectedLayers(outputs) {
  return [...new Set(outputs.map((output) => output.layer))]
    .sort((left, right) => left - right);
}

function validateExecutionIdentity(values) {
  if (!values.snapshotAt || Number.isNaN(Date.parse(values.snapshotAt))) {
    throw new Error("Execution requires --snapshot=<ISO timestamp>");
  }

  if (!values.generation || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(values.generation)) {
    throw new Error("Execution requires a filesystem-safe --generation identifier");
  }

  if (values.target.name === "cloud" && !values.quietCutConfirmed) {
    throw new Error("Production parity requires --quiet-cut-confirmed=true");
  }
}

function requireFrozenViews(outputs, viewSnapshots) {
  const missing = outputs
    .filter((output) => output.bigquery_kind === "view")
    .filter((output) => !viewSnapshots.has(output.name))
    .map((output) => output.name);

  if (missing.length > 0) {
    throw new Error(
      `Frozen BigQuery tables are required for views: ${missing.join(", ")}`,
    );
  }
}

function validateViewSnapshotNames(viewSnapshots, outputs) {
  const outputsByName = new Map(outputs.map((output) => [output.name, output]));

  for (const [name, reference] of viewSnapshots) {
    const output = outputsByName.get(name);
    if (!output) throw new Error(`Unknown view snapshot output ${name}`);
    if (output.bigquery_kind !== "view") {
      throw new Error(`${name} is a table and does not need --view-snapshot`);
    }
    parseBigQueryTableReference(reference);
  }
}

function parseViewSnapshots(values) {
  const snapshots = new Map();

  for (const value of values) {
    const separatorIndex = value.indexOf("=");
    if (separatorIndex <= 0) {
      throw new Error("--view-snapshot must be <output>=<project.dataset.table>");
    }

    const name = value.slice(0, separatorIndex);
    const reference = value.slice(separatorIndex + 1);
    if (snapshots.has(name)) throw new Error(`Duplicate view snapshot for ${name}`);
    snapshots.set(name, reference);
  }

  return snapshots;
}

function parseArguments(values) {
  const parsed = {};

  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!value.startsWith("--")) throw new Error(`Unknown argument ${value}`);

    const equalsIndex = value.indexOf("=");
    let name;
    let argumentValue;

    if (equalsIndex !== -1) {
      name = value.slice(2, equalsIndex);
      argumentValue = value.slice(equalsIndex + 1);
    } else {
      name = value.slice(2);
      const nextValue = values[index + 1];
      if (nextValue && !nextValue.startsWith("--")) {
        argumentValue = nextValue;
        index += 1;
      } else {
        argumentValue = "true";
      }
    }

    parsed[name] ??= [];
    parsed[name].push(argumentValue);
  }

  return parsed;
}

function validateArgumentNames(argumentsByName) {
  const repeatableArguments = new Set(["output", "view-snapshot"]);
  const allowedArguments = new Set([
    "branch",
    "contract",
    "diagnostic-limit",
    "diagnostics",
    "evidence-dir",
    "generation",
    "layer",
    "output",
    "plan",
    "quiet-cut-confirmed",
    "snapshot",
    "target",
    "view-snapshot",
  ]);

  for (const [name, values] of Object.entries(argumentsByName)) {
    if (!allowedArguments.has(name)) {
      throw new Error(`Unknown argument --${name}`);
    }

    if (!repeatableArguments.has(name) && values.length > 1) {
      throw new Error(`--${name} can only be supplied once`);
    }
  }
}

function flattenArguments(argumentsByName) {
  return Object.fromEntries(
    Object.entries(argumentsByName).map(([name, values]) => [name, values.at(-1)]),
  );
}

function firstArgument(argumentsByName, name) {
  const values = argumentsByName[name];
  if (!values || values.length === 0) return null;
  if (values.length > 1) throw new Error(`--${name} can only be supplied once`);
  return values[0];
}

function integerArgument(argumentsByName, name) {
  const value = firstArgument(argumentsByName, name);
  if (value === null) return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) throw new Error(`--${name} must be an integer`);
  return parsed;
}

function booleanArgument(argumentsByName, name, defaultValue) {
  const value = firstArgument(argumentsByName, name);
  if (value === null) return defaultValue;
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error(`--${name} must be true or false`);
}

function sumField(rows, field) {
  return rows.reduce((total, row) => total + BigInt(row[field] ?? "0"), 0n)
    .toString();
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
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

function sanitizeError(error) {
  const message = error instanceof Error ? error.message : String(error);
  const sanitized = message
    .replace(/([?&](?:token|authorization)=)[^&\s]+/gi, "$1[REDACTED]")
    .replace(/(Bearer\s+)[A-Za-z0-9._~-]+/gi, "$1[REDACTED]");

  if (sanitized.length <= 4000) return sanitized;
  return `${sanitized.slice(0, 4000)}\n[TRUNCATED]`;
}
