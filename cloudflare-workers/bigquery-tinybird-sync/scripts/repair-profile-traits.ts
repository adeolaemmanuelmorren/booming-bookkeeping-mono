import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

import type { PendingIdentityFact } from "../src/identity-engine";
import {
  appendIdentityPendingFacts,
  readCurrentIdentityEvidence,
  readCurrentIdentityProfiles,
  readIdentityCompactionCursor,
  readPendingIdentityFacts,
  type TinybirdApiConfig,
} from "../src/tinybird-api";

const evidencePath = path.resolve(stringArgument("evidence"));
const outputPath = optionalStringArgument("output");
const execute = process.argv.includes("--execute=true");
const expectedTraits = mismatchTraits(evidencePath);
const profileIds = [...expectedTraits.keys()].sort();
const config = tinybirdConfig();
// Snapshot pending rows before activated state. If a batch activates between
// these reads, the repair touch remains visible in at least one snapshot.
const queuedFacts = await readAllQueuedFacts(config);
const currentProfiles = await readCurrentIdentityProfiles("boom", profileIds, config);
const profiles = currentProfiles.filter((profile) => {
  const expected = expectedTraits.get(profile.profileId);
  return !expected
    || profile.firstName !== expected.firstName
    || profile.lastName !== expected.lastName;
});
if (profiles.length === 0) {
  printSummary({
    mode: execute ? "execute" : "dry_run",
    profile_count: profileIds.length,
    profiles_still_mismatched: 0,
    pending_touch_fact_count: 0,
    touch_fact_count: 0,
    input_hash: sha256(""),
  });
  process.exit(0);
}
const identifierKeys = profiles.flatMap((profile) => profile.memberIdentifierKeys);
const evidence = await readCurrentIdentityEvidence("boom", identifierKeys, config);
const factsByIdentifier = new Map<string, string[]>();

for (const row of evidence) {
  const facts = factsByIdentifier.get(row.identifierKey) ?? [];
  facts.push(row.factKey);
  factsByIdentifier.set(row.identifierKey, facts);
}

const selectedFactKeys = profiles.map((profile) => {
  for (const identifier of profile.memberIdentifierKeys) {
    const factKey = factsByIdentifier.get(identifier)?.sort()[0];
    if (factKey) return factKey;
  }
  throw new Error(`Profile ${profile.profileId} has no retained fact.`);
});
const currentFacts = readFactPayloads([...new Set(selectedFactKeys)].sort());
const currentByKey = new Map(currentFacts.map((fact) => [fact.factKey, fact]));
const queuedByKey = latestFactsByKey(queuedFacts);
const ingestedAt = new Date().toISOString();
const uniqueFactKeys = [...new Set(selectedFactKeys)].sort();
const deferredToPending = uniqueFactKeys.filter((factKey) => {
  const queued = queuedByKey.get(factKey);
  const current = currentByKey.get(factKey);
  if (!queued) return false;
  if (!current) return true;
  return queued.sourceFactVersion >= current.sourceFactVersion;
});
const touches = uniqueFactKeys.filter((factKey) => !deferredToPending.includes(factKey)).map((factKey) => {
  const active = currentByKey.get(factKey);
  if (!active) throw new Error(`Fact ${factKey} was not returned.`);
  const sourceFactVersion = active.sourceFactVersion + 1;
  return {
    ...active,
    sourceFactVersion,
    ingestedAt,
    eventId: sha256(`${factKey}:${sourceFactVersion}:0:${active.factPayloadHash}`),
  };
});

if (execute) await appendIdentityPendingFacts(touches, config);
printSummary({
  mode: execute ? "execute" : "dry_run",
  profile_count: profileIds.length,
  profiles_still_mismatched: profiles.length,
  pending_touch_fact_count: deferredToPending.length,
  touch_fact_count: touches.length,
  input_hash: sha256(touches.map((fact) => fact.eventId).sort().join("|")),
});

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

function latestFactsByKey(
  facts: PendingIdentityFact[],
): Map<string, PendingIdentityFact> {
  const latest = new Map<string, PendingIdentityFact>();
  for (const fact of facts) {
    const current = latest.get(fact.factKey);
    if (!current || fact.sourceFactVersion > current.sourceFactVersion) {
      latest.set(fact.factKey, fact);
    }
  }
  return latest;
}

function printSummary(summary: Record<string, unknown>): void {
  const text = `${JSON.stringify(summary, null, 2)}\n`;
  if (outputPath) writeFileSync(path.resolve(outputPath), text, "utf8");
  process.stdout.write(text);
}

function readFactPayloads(factKeys: string[]): PendingIdentityFact[] {
  const factList = factKeys.map(sqlString).join(",");
  const stateList = factKeys.map((key) => sqlString(`fact:${key}`)).join(",");
  const sql = `
    WITH candidates AS (
      SELECT fact_kind, fact_key, source_fact_version, fact_deleted,
        fact_observed_at, fact_payload_hash, fact_payload, evidence_keys,
        producer_id, is_deleted, batch_version, committed_at, row_hash
      FROM identity_state_seed_enriched
      WHERE tenant_id='boom' AND state_kind='fact' AND state_key IN (${stateList})
      UNION ALL
      SELECT journal.fact_kind, journal.fact_key, journal.source_fact_version,
        journal.fact_deleted, journal.fact_observed_at, journal.fact_payload_hash,
        journal.fact_payload, journal.evidence_keys, journal.producer_id,
        journal.is_deleted, journal.batch_version, journal.committed_at, journal.row_hash
      FROM identity_state_delta_versions AS journal
      INNER JOIN activated_identity_batches AS activated
        ON journal.tenant_id=activated.tenant_id
        AND journal.batch_version=activated.batch_version
        AND journal.batch_id=activated.batch_id
      WHERE journal.tenant_id='boom' AND journal.state_kind='fact'
        AND journal.lookup_key IN (${factList})
    )
    SELECT fact_kind, fact_key, toString(source_fact_version) AS source_fact_version,
      toString(fact_deleted) AS fact_deleted, toString(fact_observed_at) AS fact_observed_at,
      fact_payload_hash, fact_payload, evidence_keys, producer_id, toString(is_deleted) AS is_deleted
    FROM candidates
    QUALIFY row_number() OVER (
      PARTITION BY fact_kind, fact_key
      ORDER BY batch_version DESC, committed_at DESC, row_hash DESC
    )=1
  `;
  const output = execFileSync("tb", [
    "--cloud", "--output", "json", "sql", "--rows-limit", "500", sql,
  ], {
    cwd: path.resolve(import.meta.dirname, "../../../tinybird-production"),
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  const rows = parseJsonOutput(output).data as Record<string, unknown>[];
  return rows.map((row) => ({
    eventId: "",
    producerId: String(row.producer_id),
    observedAt: String(row.fact_observed_at),
    ingestedAt: "",
    factKind: String(row.fact_kind),
    factKey: String(row.fact_key),
    sourceFactVersion: Number(row.source_fact_version),
    factDeleted: Number(row.fact_deleted) === 1,
    factPayloadHash: String(row.fact_payload_hash),
    factPayload: String(row.fact_payload),
    evidenceKeys: row.evidence_keys as string[],
  })).filter((fact) => !fact.factDeleted);
}

function mismatchTraits(file: string): Map<string, { firstName: string; lastName: string }> {
  const evidence = JSON.parse(readFileSync(file, "utf8")) as {
    mismatches: Array<{
      profile_id: string;
      kind: string;
      bigquery_first_name?: string;
      bigquery_last_name?: string;
    }>;
  };
  return new Map(evidence.mismatches
    .filter((row) => row.kind === "trait")
    .map((row) => [row.profile_id, {
      firstName: row.bigquery_first_name ?? "",
      lastName: row.bigquery_last_name ?? "",
    }]));
}

function tinybirdConfig(): TinybirdApiConfig {
  const file = path.resolve(import.meta.dirname, "../../../tinybird-production/.tinyb");
  const value = JSON.parse(readFileSync(file, "utf8")) as { host: string; token: string };
  return { apiUrl: value.host, adminToken: value.token, fetchTimeoutMs: 30_000 };
}

function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function parseJsonOutput(output: string): { data: unknown[] } {
  for (const match of output.matchAll(/[\[{]/g)) {
    try {
      return JSON.parse(output.slice(match.index).trim()) as { data: unknown[] };
    } catch {
      continue;
    }
  }
  throw new Error("Command did not return JSON.");
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function stringArgument(name: string): string {
  const prefix = `--${name}=`;
  const values = process.argv.filter((argument) => argument.startsWith(prefix));
  if (values.length !== 1) throw new Error(`Supply --${name}=... exactly once.`);
  return values[0].slice(prefix.length);
}

function optionalStringArgument(name: string): string | null {
  const prefix = `--${name}=`;
  const values = process.argv.filter((argument) => argument.startsWith(prefix));
  if (values.length > 1) throw new Error(`Supply --${name}=... at most once.`);
  return values[0]?.slice(prefix.length) || null;
}
