import assert from "node:assert/strict";
import test from "node:test";

import {
  TinybirdBulkBootstrapPublisher,
  type BulkSourceRecord,
  type BulkTinybirdClient,
  type PendingIdentityFact,
} from "../../worker/fivetran/bootstrap-publisher.ts";
import type { JsonRecord, SourceReplacement } from "../../worker/fivetran/contracts.ts";
import { canonicalJson, sha256Text } from "../../worker/fivetran/json.ts";

const { sourceRecords } = await import(
  "../../worker/conversions/publication.ts"
);
const { conversionIdentityFacts } = await import(
  "../../worker/conversions/identity.ts"
);

const SNAPSHOT_ID = "fivetran-20260905T222800Z";
const SNAPSHOT_AT = "2026-09-05T22:28:00.000000Z";

test("bulk publisher retries exact source and identity writes, then seals one manifest", async () => {
  const client = new MemoryTinybird();
  const publisher = new TinybirdBulkBootstrapPublisher({
    tenantId: "boom",
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
    client,
    transforms: { sourceRecords, identityFacts: conversionIdentityFacts },
    sleep: async () => {},
  });
  const replacements = [replacement("ch_1"), replacement("ch_2")];
  client.failAfterIdentityAppend = true;

  await assert.rejects(
    publisher.publishBootstrapSourceReplacements({
      bootstrapId: SNAPSHOT_ID,
      replacements,
    }),
    /lost identity append response/,
  );
  assert.equal(client.commits.length, 0);
  assert.equal(client.identityFacts.length, 2);

  await publisher.publishBootstrapSourceReplacements({
    bootstrapId: SNAPSHOT_ID,
    replacements,
  });
  assert.equal(client.records.length, 10);
  assert.equal(client.identityFacts.length, 2);
  assert.equal(client.commits.length, 2);

  const canonicalFacts = client.identityFacts
    .map((row) => String(row.canonical_fact_json))
    .sort();
  const expectedHash = sha256Text(canonicalFacts.join("\n"));
  const manifest = await publisher.sealBootstrapIdentitySnapshot({
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
    expectedDistinctFactCount: 2,
    sealedAt: "2026-09-05T23:00:00.000000Z",
  });

  assert.equal(manifest.canonicalHash, expectedHash);
  assert.equal(manifest.expectedDistinctFactCount, 2);
  assert.equal(client.manifests.length, 1);

  await publisher.publishBootstrapSourceReplacements({
    bootstrapId: SNAPSHOT_ID,
    replacements,
  });
  await publisher.sealBootstrapIdentitySnapshot({
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
    expectedDistinctFactCount: 2,
    sealedAt: "2026-09-05T23:00:00.000000Z",
  });
  assert.equal(client.identityFacts.length, 2);
  assert.equal(client.commits.length, 2);
  assert.equal(client.manifests.length, 1);
});

test("manifest sealing rejects an incomplete identity snapshot", async () => {
  const client = new MemoryTinybird();
  const publisher = new TinybirdBulkBootstrapPublisher({
    tenantId: "boom",
    snapshotId: SNAPSHOT_ID,
    snapshotAt: SNAPSHOT_AT,
    client,
    transforms: { sourceRecords, identityFacts: conversionIdentityFacts },
    sleep: async () => {},
  });

  await assert.rejects(
    publisher.sealBootstrapIdentitySnapshot({
      snapshotId: SNAPSHOT_ID,
      snapshotAt: SNAPSHOT_AT,
      expectedDistinctFactCount: 1,
      sealedAt: "2026-09-05T23:00:00.000000Z",
    }),
    /fact count is incomplete/,
  );
  assert.equal(client.manifests.length, 0);
});

function replacement(chargeId: string): SourceReplacement {
  const scopeId = `stripe:main:charge:${chargeId}`;
  return {
    source: "stripe",
    source_account: "main",
    scope_id: scopeId,
    replacement_id: `${scopeId}:fivetran:bootstrap:${SNAPSHOT_ID}`,
    observed_at: SNAPSHOT_AT,
    observation_sequence: 1,
    rows: [{
      source: "stripe",
      source_account: "main",
      source_fact_id: `stripe:main:charge:${chargeId}`,
      charge_id: chargeId,
      email: `${chargeId}@example.com`,
      phone: null,
      name: "Ada Person",
      occurred_at: "2026-08-01T12:00:00.000000Z",
      is_deleted: false,
    }],
    evidence_inbox_ids: [`raw:charge:${chargeId}`],
    source_evidence: {
      raw_charge_versions: [{ id: chargeId }],
      raw_customer_versions: [],
      raw_payment_intent_versions: [],
    },
  };
}

class MemoryTinybird implements BulkTinybirdClient {
  readonly records: BulkSourceRecord[] = [];
  readonly commits: JsonRecord[] = [];
  readonly identityFacts: JsonRecord[] = [];
  readonly manifests: JsonRecord[] = [];
  failAfterIdentityAppend = false;

  async query<Row extends JsonRecord>(sql: string): Promise<Row[]> {
    if (sql.includes("FROM v1_source_records")) {
      return await recordVerificationRows(this.records) as Row[];
    }
    if (sql.includes("FROM v1_source_commits")) {
      return commitVerificationRows(this.commits) as Row[];
    }
    if (sql.includes("FROM v1_bootstrap_conversion_identity_manifests")) {
      return distinct(this.manifests.map((row) => ({
        snapshot_at: row.snapshot_at,
        expected_distinct_fact_count: row.expected_distinct_fact_count,
        canonical_hash: row.canonical_hash,
      }))) as Row[];
    }
    if (sql.includes("FROM v1_bootstrap_conversion_identity_facts")) {
      return distinct(this.identityFacts)
        .sort((left, right) => identityKey(left).localeCompare(identityKey(right))) as Row[];
    }
    throw new Error("Unexpected Tinybird query");
  }

  async append(): Promise<void> {
    throw new Error("Bulk append was expected");
  }

  async appendBulk(
    table: string,
    rows: readonly JsonRecord[],
    maximumBatchBytes: number,
  ): Promise<void> {
    assert.equal(maximumBatchBytes, 8 * 1024 * 1024);
    if (table === "v1_source_records") {
      this.records.push(...structuredClone(rows as BulkSourceRecord[]));
      return;
    }
    if (table === "v1_source_commits") {
      this.commits.push(...structuredClone(rows));
      return;
    }
    if (table === "v1_bootstrap_conversion_identity_facts") {
      this.identityFacts.push(...structuredClone(rows));
      if (this.failAfterIdentityAppend) {
        this.failAfterIdentityAppend = false;
        throw new Error("lost identity append response");
      }
      return;
    }
    if (table === "v1_bootstrap_conversion_identity_manifests") {
      this.manifests.push(...structuredClone(rows));
      return;
    }
    throw new Error(`Unexpected append table ${table}`);
  }
}

async function recordVerificationRows(
  rows: BulkSourceRecord[],
): Promise<JsonRecord[]> {
  const byReplacement = groupBy(rows, (row) => row.replacement_id);
  const result: JsonRecord[] = [];
  for (const [replacementId, physicalRows] of byReplacement) {
    const byRecord = groupBy(
      physicalRows,
      (row) => `${row.record_kind}\u001f${row.record_id}`,
    );
    const digests: string[] = [];
    let conflicts = 0;
    let corruptPayloads = 0;
    for (const versions of byRecord.values()) {
      const variants = new Set(versions.map((row) => canonicalJson([
        row.scope_id,
        row.observation_sequence,
        row.payload_hash,
      ])));
      if (variants.size !== 1) conflicts += 1;
      const row = versions[0];
      for (const version of versions) {
        if (sha256Text(version.payload_json) !== version.payload_hash) {
          corruptPayloads += 1;
        }
      }
      digests.push(sha256Text(
        `${row.record_kind}\u001f${row.record_id}\u001f${row.payload_hash}`,
      ));
    }
    result.push({
      replacement_id: replacementId,
      scope_id: physicalRows[0].scope_id,
      observation_sequence: physicalRows[0].observation_sequence,
      record_count: byRecord.size,
      conflicts,
      corrupt_payloads: corruptPayloads,
      verification_hash: sha256Text(digests.sort().join("")),
    });
  }
  return result;
}

function commitVerificationRows(rows: JsonRecord[]): JsonRecord[] {
  return [...groupBy(rows, (row) => String(row.replacement_id))]
    .map(([replacementId, physicalRows]) => ({
      replacement_id: replacementId,
      scope_id: physicalRows[0].scope_id,
      observation_sequence: physicalRows[0].observation_sequence,
      row_count: physicalRows[0].row_count,
      content_hash: physicalRows[0].content_hash,
      variants: new Set(physicalRows.map((row) => canonicalJson([
        row.scope_id,
        row.observation_sequence,
        row.row_count,
        row.content_hash,
      ]))).size,
    }));
}

function groupBy<Row>(
  rows: readonly Row[],
  key: (row: Row) => string,
): Map<string, Row[]> {
  const result = new Map<string, Row[]>();
  for (const row of rows) result.set(key(row), [...(result.get(key(row)) ?? []), row]);
  return result;
}

function distinct(rows: readonly JsonRecord[]): JsonRecord[] {
  return [...new Map(rows.map((row) => [canonicalJson(row), structuredClone(row)])).values()];
}

function identityKey(row: JsonRecord): string {
  return `${String(row.fact_kind)}\u001f${String(row.fact_key)}`;
}
