#!/usr/bin/env node

import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const options = parseArguments(process.argv.slice(2));
const runAt = options.runAt ?? new Date();
const snapshotSegment = formatSnapshotSegment(runAt);
const seedId = `identity_seed_${snapshotSegment}`;
const exports = selectExports(options.part).map((definition) => ({
  ...definition,
  gcsUri: buildGcsUri(definition.gcsFolder, snapshotSegment),
}));

console.log(`Identity seed: ${seedId}`);

for (const item of exports) {
  console.log(`${item.name} destination: ${item.gcsUri}`);
}

if (options.planOnly) {
  process.exit(0);
}

for (const item of exports) {
  const template = await readFile(
    path.join(projectRoot, "scripts", "sql", item.templateName),
    "utf8",
  );
  const query = renderTemplate(template, {
    __COMMITTED_AT__: formatBigQueryTimestamp(runAt),
    __GCS_URI__: item.gcsUri,
    __SEED_ID__: seedId,
  });

  await runBigQuery(query, options.dryRun);
}

console.log(options.dryRun ? "BigQuery dry run passed." : "Identity seed export completed.");

function parseArguments(arguments_) {
  const parsed = {
    dryRun: false,
    part: "all",
    planOnly: false,
    runAt: null,
  };

  for (const argument of arguments_) {
    if (argument === "--dry-run") {
      parsed.dryRun = true;
      continue;
    }

    if (argument === "--plan") {
      parsed.planOnly = true;
      continue;
    }

    if (argument.startsWith("--part=")) {
      const value = argument.slice("--part=".length);

      if (!["all", "state", "mapping-evidence", "fact-evidence"].includes(value)) {
        fail(`Invalid --part value: ${value}`);
      }

      parsed.part = value;
      continue;
    }

    if (argument.startsWith("--run-at=")) {
      const value = argument.slice("--run-at=".length);
      const runAt = new Date(value);

      if (Number.isNaN(runAt.valueOf())) {
        fail(`Invalid --run-at value: ${value}`);
      }

      parsed.runAt = runAt;
      continue;
    }

    fail(`Unknown argument: ${argument}`);
  }

  return parsed;
}

function selectExports(part) {
  const definitions = [
    {
      part: "state",
      name: "state",
      templateName: "export-identity-state-seed.sql",
      gcsFolder: "identity_state",
    },
    {
      part: "mapping-evidence",
      name: "mapping evidence",
      templateName: "export-identity-mapping-evidence-seed.sql",
      gcsFolder: "identity_mapping_evidence",
    },
    {
      part: "fact-evidence",
      name: "fact evidence",
      templateName: "export-identity-fact-evidence-seed.sql",
      gcsFolder: "identity_fact_evidence",
    },
  ];

  if (part === "all") {
    return definitions;
  }

  return definitions.filter((definition) => definition.part === part);
}

function buildGcsUri(folder, snapshotSegment) {
  return [
    `gs://booming-data/tinybird/migration_seed/${folder}`,
    `snapshot_at=${snapshotSegment}`,
    "part-*.parquet",
  ].join("/");
}

function renderTemplate(template, replacements) {
  let rendered = template;

  for (const [placeholder, value] of Object.entries(replacements)) {
    rendered = rendered.replaceAll(placeholder, escapeSqlLiteral(value));
  }

  return rendered;
}

function runBigQuery(query, dryRun) {
  const arguments_ = [
    "query",
    "--project_id=able-folio-499722",
    "--location=US",
    "--use_legacy_sql=false",
    "--format=none",
  ];

  if (dryRun) {
    arguments_.push("--dry_run=true");
  }

  arguments_.push(query);

  return new Promise((resolve, reject) => {
    const child = spawn("bq", arguments_, {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";

    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });

    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      const details = [stdout.trim(), stderr.trim()].filter(Boolean).join("\n");
      reject(new Error(details || `bq exited with status ${code}`));
    });
  });
}

function formatSnapshotSegment(date) {
  return date.toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
}

function formatBigQueryTimestamp(date) {
  return date.toISOString().replace("T", " ").replace("Z", "+00");
}

function escapeSqlLiteral(value) {
  return value.replaceAll("'", "''");
}

function fail(message) {
  console.error(message);
  console.error(
    "Usage: node scripts/export-identity-state-seed-to-gcs.mjs " +
      "[--run-at=ISO] [--part=all|state|mapping-evidence|fact-evidence] " +
      "[--plan] [--dry-run]",
  );
  process.exit(1);
}
