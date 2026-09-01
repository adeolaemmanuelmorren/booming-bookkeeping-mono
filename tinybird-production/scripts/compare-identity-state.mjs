import { execFileSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  buildIdentityGenerationProofSql,
  buildIdentitySurfaceQueries,
  buildIdentitySurfaces,
  compareIdentitySurface,
  identityBucketCount,
  isCoordinatorGenerationId,
  validateIdentityGenerationProof,
} from "./lib/identity-parity.mjs";
import { resolveTinybirdTarget } from "./lib/tinybird-target.mjs";

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const defaultEvidenceDirectory = path.join(
  projectRoot,
  ".identity-parity-evidence",
);
const bigqueryProject = "able-folio-499722";
const bigqueryLocation = "US";
const maximumBytesBilledPerQuery = 20_000_000_000;

export function parseIdentityParityArguments(values) {
  const parsed = {};

  for (let index = 0; index < values.length; index += 1) {
    const argument = values[index];
    if (!argument.startsWith("--")) {
      throw new Error(`Unknown argument ${argument}`);
    }

    const equalsIndex = argument.indexOf("=");
    const name = equalsIndex === -1
      ? argument.slice(2)
      : argument.slice(2, equalsIndex);
    let value = equalsIndex === -1
      ? null
      : argument.slice(equalsIndex + 1);

    if (value === null) {
      const next = values[index + 1];
      if (next && !next.startsWith("--")) {
        value = next;
        index += 1;
      } else {
        value = "true";
      }
    }

    if (Object.hasOwn(parsed, name)) {
      throw new Error(`--${name} can only be supplied once`);
    }

    parsed[name] = value;
  }

  validateArgumentNames(parsed);
  return parsed;
}

export function buildIdentityParityConfig(argumentsByName) {
  const execute = booleanArgument(argumentsByName, "execute", false);
  const help = booleanArgument(argumentsByName, "help", false);
  const quietCutConfirmed = booleanArgument(
    argumentsByName,
    "quiet-cut-confirmed",
    false,
  );
  const snapshotAt = argumentsByName.snapshot ?? null;
  const generation = argumentsByName.generation ?? null;
  const target = resolveTinybirdTarget({
    target: argumentsByName.target,
    branch: argumentsByName.branch,
  });
  const evidenceDirectory = path.resolve(
    projectRoot,
    argumentsByName["evidence-dir"] ?? defaultEvidenceDirectory,
  );

  if (help || !execute) {
    return {
      execute: false,
      help,
      target,
      snapshotAt,
      generation,
      quietCutConfirmed,
      evidenceDirectory,
    };
  }

  if (!argumentsByName.target) {
    throw new Error("Execution requires an explicit --target=cloud, staging, or branch");
  }

  if (!snapshotAt || Number.isNaN(Date.parse(snapshotAt))) {
    throw new Error("Execution requires --snapshot=<ISO timestamp>");
  }

  if (!isCoordinatorGenerationId(generation)) {
    throw new Error(
      "Execution requires --generation=<exact coordinator generation ID>",
    );
  }

  if (target.name === "cloud" && !quietCutConfirmed) {
    throw new Error("Production identity parity requires --quiet-cut-confirmed=true");
  }

  return {
    execute,
    help,
    target,
    snapshotAt,
    generation,
    quietCutConfirmed,
    evidenceDirectory,
  };
}

export function identityParityPlan(config) {
  return {
    mode: "plan",
    execution_is_opt_in: true,
    target: config.target.name,
    branch: config.target.branch,
    snapshot_at: config.snapshotAt,
    coordinator_generation_id: config.generation,
    tenant_id: "boom",
    bucket_count: identityBucketCount,
    maximum_bigquery_bytes_billed_per_query: maximumBytesBilledPerQuery,
    tinybird_query_scope:
      "One generation-lineage proof, then four read-only full-state aggregate scans; Tinybird has no client-side bytes-read cap.",
    surfaces: buildIdentitySurfaces().map((surface) => ({
      name: surface.name,
      description: surface.description,
      unique_key: surface.key,
      bigquery_relations: surface.bigqueryRelations,
      tinybird_resource: surface.tinybirdResource,
    })),
    execution_requirements: [
      "--execute=true",
      "--target=cloud, staging, or branch",
      "--snapshot=<BigQuery quiet-cut timestamp>",
      "--generation=<exact coordinator generation ID>",
      "--quiet-cut-confirmed=true when target is cloud",
    ],
  };
}

function main() {
  try {
    const argumentsByName = parseIdentityParityArguments(process.argv.slice(2));
    const config = buildIdentityParityConfig(argumentsByName);

    if (config.help) {
      printHelp();
      return;
    }

    if (!config.execute) {
      console.log(JSON.stringify(identityParityPlan(config), null, 2));
      return;
    }

    const result = runIdentityParity(config);
    console.log(JSON.stringify(result, null, 2));
    if (result.status !== "passed") process.exitCode = 1;
  } catch (error) {
    console.error(sanitizeError(error));
    process.exitCode = 1;
  }
}

function runIdentityParity(config) {
  const startedAt = new Date().toISOString();
  const evidencePath = prepareEvidencePath(config);
  const result = {
    schema_version: 1,
    status: "running",
    coordinator_generation_id: config.generation,
    snapshot_at: config.snapshotAt,
    quiet_cut_confirmed: config.quietCutConfirmed,
    target: config.target.name,
    branch: config.target.branch,
    tenant_id: "boom",
    bucket_count: identityBucketCount,
    hash: "FarmHash64 with length-prefixed canonical fields",
    bucket_metrics: [
      "row_count",
      "key_count",
      "duplicate_key_count",
      "duplicate_rows",
      "fingerprint_sum",
      "fingerprint_xor",
    ],
    started_at: startedAt,
    completed_at: null,
    active_generation_before: null,
    active_generation_after: null,
    active_generation_stable: null,
    coordinator_generation_proof: null,
    surfaces: [],
  };
  saveEvidence(evidencePath, result);

  try {
    result.active_generation_before = readActiveGeneration(config);
    const generationRecords = queryTinybird(
      buildIdentityGenerationProofSql(config.generation),
      config,
      100001,
    );
    result.coordinator_generation_proof = validateIdentityGenerationProof(
      config.generation,
      generationRecords,
      result.active_generation_before,
    );
    saveEvidence(evidencePath, result);

    if (result.coordinator_generation_proof.status === "passed") {
      for (const surface of buildIdentitySurfaces()) {
        result.surfaces.push(compareSurfaceSafely(surface, config));
        saveEvidence(evidencePath, result);
      }
    }

    result.active_generation_after = readActiveGeneration(config);
    result.active_generation_stable = generationsMatch(
      result.active_generation_before,
      result.active_generation_after,
    );
  } catch (error) {
    result.run_error = sanitizeError(error);
  }

  const allSurfacesPassed = result.surfaces.length === 4
    && result.surfaces.every((surface) => surface.status === "passed");
  const generationProofPassed =
    result.coordinator_generation_proof?.status === "passed";
  result.status = allSurfacesPassed
    && generationProofPassed
    && result.active_generation_stable
    ? "passed"
    : "failed";
  result.completed_at = new Date().toISOString();
  result.duration_ms = Date.parse(result.completed_at) - Date.parse(startedAt);
  result.evidence_file = evidencePath;
  saveEvidence(evidencePath, result);
  return result;
}

function compareSurfaceSafely(surface, config) {
  const startedAt = new Date().toISOString();

  try {
    const queries = buildIdentitySurfaceQueries(surface);
    const bigqueryRows = queryBigQuery(queries.bigqueryDigest, config);
    const tinybirdRows = queryTinybird(
      queries.tinybirdDigest,
      config,
      identityBucketCount + 10,
    );
    const comparison = compareIdentitySurface(
      surface,
      bigqueryRows,
      tinybirdRows,
    );

    return {
      ...comparison,
      bigquery_relations: surface.bigqueryRelations,
      tinybird_resource: surface.tinybirdResource,
      started_at: startedAt,
      completed_at: new Date().toISOString(),
    };
  } catch (error) {
    return {
      name: surface.name,
      status: "error",
      bigquery_relations: surface.bigqueryRelations,
      tinybird_resource: surface.tinybirdResource,
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      error: sanitizeError(error),
    };
  }
}

function readActiveGeneration(config) {
  const rows = queryTinybird(
    `
      SELECT
        tenant_id,
        toString(batch_version) AS batch_version,
        batch_id,
        toString(committed_at) AS committed_at,
        toString(row_hash) AS row_hash,
        producer_id,
        toString(checkpoint_sequence) AS checkpoint_sequence,
        toString(checkpoint_ingested_at) AS checkpoint_ingested_at,
        toString(checkpoint_event_id) AS checkpoint_event_id,
        toString(input_event_count) AS input_event_count,
        toString(input_hash) AS input_hash,
        toString(output_row_count) AS output_row_count,
        toString(output_hash) AS output_hash
      FROM activated_identity_batches
      WHERE tenant_id = 'boom'
      ORDER BY batch_version DESC, committed_at DESC, row_hash DESC, batch_id DESC
      LIMIT 1
    `,
    config,
    2,
  );

  if (rows.length !== 1) {
    throw new Error(`Expected one active identity generation, received ${rows.length}`);
  }

  return Object.fromEntries(
    Object.entries(rows[0]).map(([name, value]) => [name, String(value)]),
  );
}

function queryBigQuery(sql, config) {
  const output = execFileSync(
    "bq",
    [
      "query",
      "--quiet",
      "--use_legacy_sql=false",
      `--project_id=${bigqueryProject}`,
      `--location=${bigqueryLocation}`,
      "--format=json",
      `--max_rows=${identityBucketCount + 10}`,
      `--maximum_bytes_billed=${maximumBytesBilledPerQuery}`,
      `--parameter=snapshot_at:STRING:${config.snapshotAt}`,
      sql,
    ],
    {
      encoding: "utf8",
      maxBuffer: 256 * 1024 * 1024,
    },
  );

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
  const response = parseJsonOutput(output);

  if (!Array.isArray(response.data)) {
    throw new Error("Tinybird did not return a data array");
  }

  return response.data;
}

function generationsMatch(before, after) {
  if (!before || !after) return false;
  return JSON.stringify(before) === JSON.stringify(after);
}

function prepareEvidencePath(config) {
  const directory = path.join(config.evidenceDirectory, config.generation);
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const evidencePath = path.join(directory, "run.json");

  if (existsSync(evidencePath)) {
    throw new Error(
      `Evidence already exists for generation ${config.generation}; use a new generation label`,
    );
  }

  return evidencePath;
}

function saveEvidence(evidencePath, result) {
  writeFileSync(evidencePath, `${JSON.stringify(result, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
}

function parseJsonOutput(output) {
  const candidateIndexes = [...output.matchAll(/[\[{]/g)]
    .map((match) => match.index);

  for (const candidateIndex of candidateIndexes) {
    try {
      return JSON.parse(output.slice(candidateIndex).trim());
    } catch {
      continue;
    }
  }

  throw new Error("Command did not return a complete JSON value");
}

function validateArgumentNames(argumentsByName) {
  const allowed = new Set([
    "branch",
    "evidence-dir",
    "execute",
    "generation",
    "help",
    "quiet-cut-confirmed",
    "snapshot",
    "target",
  ]);

  for (const name of Object.keys(argumentsByName)) {
    if (!allowed.has(name)) throw new Error(`Unknown argument --${name}`);
  }
}

function booleanArgument(argumentsByName, name, defaultValue) {
  const value = argumentsByName[name];
  if (value === undefined) return defaultValue;
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error(`--${name} must be true or false`);
}

function sanitizeError(error) {
  const message = error instanceof Error ? error.message : String(error);
  const sanitized = message
    .replace(/([?&](?:token|authorization)=)[^&\s]+/gi, "$1[REDACTED]")
    .replace(/(Bearer\s+)[A-Za-z0-9._~-]+/gi, "$1[REDACTED]");

  if (sanitized.length <= 4000) return sanitized;
  return `${sanitized.slice(0, 4000)}\n[TRUNCATED]`;
}

function printHelp() {
  console.log(`Usage:
  node scripts/compare-identity-state.mjs
  node scripts/compare-identity-state.mjs --execute=true \\
    --target=cloud \\
    --snapshot=2026-08-26T23:05:00Z \\
    --generation=generation_raw_20260827000000000_slot_0 \\
    --quiet-cut-confirmed=true

Without --execute=true, the command prints a no-scan plan.`);
}

const isMain = process.argv[1]
  && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) main();
