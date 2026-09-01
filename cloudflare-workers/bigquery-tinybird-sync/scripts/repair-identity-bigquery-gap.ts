import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

import { bigQueryNormalizedFactsCtes } from "../../../tinybird-production/scripts/lib/identity-parity.mjs";
import {
  appendIdentityPendingRows,
  readCurrentIdentityFacts,
  readIdentityCompactionCursor,
  readPendingIdentityFacts,
  type TinybirdApiConfig,
} from "../src/tinybird-api";
import type { PendingIdentityFact } from "../src/identity-engine";

const projectId = "able-folio-499722";
const analyticsDataset = `${projectId}.booming_data_analytics`;
const seedSnapshot = "2026-08-26T23:05:00Z";
const defaultFinalSnapshot = "2026-08-28T03:46:42.355044Z";
const finalSnapshot = stringArgument("final-snapshot") ?? defaultFinalSnapshot;
const outputPath = stringArgument("output");
const repairTable = `${analyticsDataset}._identity_repair_seed_20260828`;
const tinybirdConfigPath = path.resolve(import.meta.dirname, "../../../tinybird-production/.tinyb");
const execute = process.argv.includes("--execute=true");

validateSnapshot(finalSnapshot);

const tinybird = tinybirdConfig();
createExpiringSeedTable();
const candidates = readBigQueryCandidates();
const reconciliation = await rowsMissingFromTinybird(candidates, tinybird);
const pending = reconciliation.rows;

const summary = {
  mode: execute ? "execute" : "dry_run",
  seed_snapshot: seedSnapshot,
  final_snapshot: finalSnapshot,
  candidate_rows: candidates.length,
  rows_already_current: candidates.length - pending.length,
  queued_rows_considered: reconciliation.queuedRows,
  rows_to_append: pending.length,
  by_action: countBy(pending, (row) => row.fact_deleted === 1 ? "delete" : "upsert"),
  input_hash: sha256(pending.map((row) => row.event_id).sort().join("|")),
};

if (execute) await appendIdentityPendingRows(pending, tinybird);
const summaryText = `${JSON.stringify(summary, null, 2)}\n`;
if (outputPath) writeFileSync(path.resolve(outputPath), summaryText, "utf8");
process.stdout.write(summaryText);

function createExpiringSeedTable(): void {
  const query = `
    CREATE OR REPLACE TABLE \`${repairTable}\`
    OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS
    WITH ${bigQueryNormalizedFactsCtes()}
    SELECT
      source_system,
      source_record_id,
      observed_at,
      UNIX_MICROS(observed_at) AS observed_at_micros,
      anonymous_id,
      user_id,
      email,
      phone,
      first_name,
      last_name,
      evidence_keys
    FROM normalized_identity_facts
  `;

  runBigQuery(query, seedSnapshot, "inherit");
}

function readBigQueryCandidates(): CandidateRow[] {
  const query = `
    WITH ${bigQueryNormalizedFactsCtes()},
    final_facts AS (
      SELECT
        source_system,
        source_record_id,
        observed_at,
        UNIX_MICROS(observed_at) AS observed_at_micros,
        anonymous_id,
        user_id,
        email,
        phone,
        first_name,
        last_name,
        evidence_keys
      FROM normalized_identity_facts
    ),
    upserts AS (
      SELECT final.*, 0 AS fact_deleted
      FROM final_facts AS final
      LEFT JOIN \`${repairTable}\` AS seed
        USING (source_system, source_record_id)
      WHERE
        seed.source_record_id IS NULL
        OR TO_JSON_STRING(STRUCT(
          final.observed_at,
          final.anonymous_id,
          final.user_id,
          final.email,
          final.phone,
          final.first_name,
          final.last_name,
          final.evidence_keys
        )) != TO_JSON_STRING(STRUCT(
          seed.observed_at,
          seed.anonymous_id,
          seed.user_id,
          seed.email,
          seed.phone,
          seed.first_name,
          seed.last_name,
          seed.evidence_keys
        ))
    ),
    deletions AS (
      SELECT seed.*, 1 AS fact_deleted
      FROM \`${repairTable}\` AS seed
      LEFT JOIN final_facts AS final
        USING (source_system, source_record_id)
      WHERE final.source_record_id IS NULL
    )
    SELECT
      source_system,
      source_record_id,
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', observed_at) AS observed_at,
      CAST(observed_at_micros AS STRING) AS observed_at_micros,
      IFNULL(anonymous_id, '') AS anonymous_id,
      IFNULL(user_id, '') AS user_id,
      IFNULL(email, '') AS email,
      IFNULL(phone, '') AS phone,
      IFNULL(first_name, '') AS first_name,
      IFNULL(last_name, '') AS last_name,
      evidence_keys,
      fact_deleted
    FROM upserts
    UNION ALL
    SELECT
      source_system,
      source_record_id,
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6SZ', observed_at) AS observed_at,
      CAST(observed_at_micros AS STRING) AS observed_at_micros,
      IFNULL(anonymous_id, '') AS anonymous_id,
      IFNULL(user_id, '') AS user_id,
      IFNULL(email, '') AS email,
      IFNULL(phone, '') AS phone,
      IFNULL(first_name, '') AS first_name,
      IFNULL(last_name, '') AS last_name,
      evidence_keys,
      fact_deleted
    FROM deletions
    ORDER BY source_system, source_record_id
  `;
  const output = runBigQuery(query, finalSnapshot, "pipe");
  const rows = JSON.parse(output) as BigQueryCandidateRow[];
  return rows.map((row) => ({
    ...row,
    fact_deleted: numberValue(row.fact_deleted, "fact_deleted"),
  }));
}

async function rowsMissingFromTinybird(
  candidates: CandidateRow[],
  config: TinybirdApiConfig,
): Promise<{ rows: IdentityCursorRow[]; queuedRows: number }> {
  const ingestedAt = new Date().toISOString();
  const sourceFactVersion = Math.floor(new Date(finalSnapshot).valueOf() * 1_000);
  const rows = candidates.map((candidate) => cursorRow(
    candidate,
    sourceFactVersion,
    ingestedAt,
  ));
  // Read pending work first. If a batch activates between these two reads, the
  // row is visible in either this queue snapshot or the later active-head read.
  const queuedFacts = await readAllQueuedFacts(config);
  const currentFacts = await readCurrentIdentityFacts(
    "boom",
    rows.map((row) => row.fact_key),
    config,
  );
  const heads = new Map(currentFacts.map((fact) => [fact.factKey, {
    sourceFactVersion: fact.sourceFactVersion,
    factDeleted: fact.factDeleted,
    factPayloadHash: fact.factPayloadHash,
  }]));
  for (const fact of queuedFacts) {
    const current = heads.get(fact.factKey);
    if (current && current.sourceFactVersion > fact.sourceFactVersion) continue;
    heads.set(fact.factKey, {
      sourceFactVersion: fact.sourceFactVersion,
      factDeleted: fact.factDeleted,
      factPayloadHash: fact.factPayloadHash,
    });
  }

  return {
    queuedRows: queuedFacts.length,
    rows: rows.filter((row) => {
      const current = heads.get(row.fact_key);
      if (!current) return true;
      if (current.sourceFactVersion > row.source_fact_version) return false;
      return current.factDeleted !== Boolean(row.fact_deleted)
        || current.factPayloadHash !== row.fact_payload_hash;
    }),
  };
}

async function readAllQueuedFacts(
  config: TinybirdApiConfig,
): Promise<PendingIdentityFact[]> {
  const active = await readIdentityCompactionCursor("boom", config);
  const cursor = {
    activeBatchVersion: active.activeBatchVersion,
    checkpointIngestedAt: active.checkpointIngestedAt,
    checkpointEventId: active.checkpointEventId,
  };
  const facts: PendingIdentityFact[] = [];

  for (;;) {
    const page = await readPendingIdentityFacts("boom", cursor, 5_000, config);
    facts.push(...page);
    const last = page.at(-1);
    if (!last || page.length < 5_000) return facts;
    cursor.checkpointIngestedAt = last.ingestedAt;
    cursor.checkpointEventId = last.eventId;
  }
}

function cursorRow(
  source: CandidateRow,
  sourceFactVersion: number,
  ingestedAt: string,
): IdentityCursorRow {
  const factKey = `${field(source.source_system)}:${field(source.source_record_id)}`;
  const deleted = String(source.fact_deleted);
  const canonicalPayload = [
    source.source_system,
    source.source_record_id,
    source.observed_at_micros,
    deleted,
    source.anonymous_id,
    source.user_id,
    source.email,
    source.phone,
    source.first_name,
    source.last_name,
  ].map(field).join("");
  const factPayloadHash = sha256(canonicalPayload);
  const eventId = sha256(
    `${factKey}:${sourceFactVersion}:${deleted}:${factPayloadHash}`,
  );
  const factPayload = JSON.stringify({
    source_system: source.source_system,
    source_record_id: source.source_record_id,
    fact_deleted: deleted,
    anonymous_id: source.anonymous_id,
    user_id: source.user_id,
    email: source.email,
    phone: source.phone,
    first_name: source.first_name,
    last_name: source.last_name,
  });

  return {
    tenant_id: "boom",
    event_id: eventId,
    producer_id: `source_identity:${source.source_system}`,
    event_kind: "identity_observation",
    observed_at: source.observed_at,
    ingested_at: ingestedAt,
    anonymous_id: source.anonymous_id,
    user_id: source.user_id,
    email: source.email,
    phone: source.phone,
    first_name: source.first_name,
    last_name: source.last_name,
    fact_kind: "identity_observation",
    fact_key: factKey,
    source_fact_version: sourceFactVersion,
    fact_deleted: source.fact_deleted,
    fact_payload_hash: factPayloadHash,
    fact_payload: factPayload,
    evidence_keys: source.evidence_keys,
  };
}

function tinybirdConfig(): TinybirdApiConfig {
  const config = JSON.parse(readFileSync(tinybirdConfigPath, "utf8")) as {
    host: string;
    token: string;
  };
  return { apiUrl: config.host, adminToken: config.token, fetchTimeoutMs: 30_000 };
}

function runBigQuery(
  query: string,
  snapshot: string,
  stdio: "inherit" | "pipe",
): string {
  const output = execFileSync("bq", [
    "query",
    "--location=US",
    "--use_legacy_sql=false",
    "--format=json",
    "--max_rows=200000",
    "--maximum_bytes_billed=20000000000",
    `--parameter=snapshot_at:STRING:${snapshot}`,
    query,
  ], {
    encoding: "utf8",
    maxBuffer: 512 * 1024 * 1024,
    stdio: stdio === "inherit" ? "inherit" : ["ignore", "pipe", "inherit"],
  });
  return output || "";
}

function field(value: string): string {
  return `${Buffer.byteLength(value, "utf8")}:${value}`;
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function numberValue(value: string | number, name: string): 0 | 1 {
  const parsed = Number(value);
  if (parsed === 0 || parsed === 1) return parsed;
  throw new Error(`${name} must be zero or one.`);
}

function stringArgument(name: string): string | null {
  const prefix = `--${name}=`;
  const values = process.argv.slice(2)
    .filter((argument) => argument.startsWith(prefix))
    .map((argument) => argument.slice(prefix.length));
  if (values.length > 1) throw new Error(`--${name} can only be supplied once.`);
  return values[0] || null;
}

function validateSnapshot(value: string): void {
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) throw new Error("--final-snapshot must be an ISO timestamp.");
  if (timestamp < Date.parse(seedSnapshot)) {
    throw new Error("--final-snapshot cannot be earlier than the seed snapshot.");
  }
}

function countBy<T>(rows: T[], key: (row: T) => string): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const row of rows) {
    const name = key(row);
    counts[name] = (counts[name] ?? 0) + 1;
  }
  return counts;
}

interface CandidateRow {
  source_system: string;
  source_record_id: string;
  observed_at: string;
  observed_at_micros: string;
  anonymous_id: string;
  user_id: string;
  email: string;
  phone: string;
  first_name: string;
  last_name: string;
  evidence_keys: string[];
  fact_deleted: 0 | 1;
}

type BigQueryCandidateRow = Omit<CandidateRow, "fact_deleted"> & {
  fact_deleted: string | number;
};

interface IdentityCursorRow {
  tenant_id: string;
  event_id: string;
  producer_id: string;
  event_kind: string;
  observed_at: string;
  ingested_at: string;
  anonymous_id: string;
  user_id: string;
  email: string;
  phone: string;
  first_name: string;
  last_name: string;
  fact_kind: string;
  fact_key: string;
  source_fact_version: number;
  fact_deleted: 0 | 1;
  fact_payload_hash: string;
  fact_payload: string;
  evidence_keys: string[];
}
