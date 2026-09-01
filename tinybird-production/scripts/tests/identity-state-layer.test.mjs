import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

const projectRoot = fileURLToPath(new URL("../..", import.meta.url));

test("the post-snapshot journal is separate, key-sorted, and never imports the seed", () => {
  const delta = readProjectFile(
    "datasources/state/identity_state_delta_versions.datasource",
  );

  assert.match(delta, /Post-snapshot identity changes only/);
  assert.match(delta, /`lookup_key` String/);
  assert.match(delta, /`sub_key` String/);
  assert.match(
    delta,
    /ENGINE_SORTING_KEY tenant_id, state_kind, lookup_key, sub_key, batch_version/,
  );
  assert.doesNotMatch(delta, /IMPORT_/);
  assert.equal(
    existsSync(`${projectRoot}/copies/identity/seed_identity_state.pipe`),
    false,
    "the immutable seed must not be copied into either journal",
  );
});

test("the current state overlays only activated delta rows onto the flat seed", () => {
  const base = readProjectFile("pipes/models/identity/identity_state_base.pipe");
  const activated = readProjectFile(
    "pipes/models/identity/activated_identity_batches.pipe",
  );
  const batches = readProjectFile(
    "pipes/models/identity/current_identity_batch_versions.pipe",
  );
  const delta = readProjectFile(
    "pipes/models/identity/current_identity_delta_state.pipe",
  );
  const current = readProjectFile(
    "pipes/models/identity/current_identity_state.pipe",
  );

  assert.match(base, /FROM identity_state_seed_enriched AS seed/);
  assert.doesNotMatch(base, /identity_mapping_evidence_seed/);
  assert.doesNotMatch(base, /identity_state_versions/);

  assert.match(activated, /FROM identity_state_seed_enriched/);
  assert.match(activated, /FROM identity_state_delta_versions AS journal/);
  assert.match(activated, /journal\.state_kind = 'activation_audit'/);
  assert.match(activated, /PARTITION BY tenant_id, batch_version/);
  assert.match(batches, /FROM activated_identity_batches/);
  assert.match(batches, /active\.batch_id AS active_batch_id/);

  assert.match(delta, /FROM identity_state_delta_versions AS journal/);
  assert.match(delta, /activated_identity_batches AS activated/);
  assert.match(delta, /journal\.tenant_id = activated\.tenant_id/);
  assert.match(delta, /journal\.batch_version = activated\.batch_version/);
  assert.match(delta, /journal\.batch_id = activated\.batch_id/);
  assert.doesNotMatch(delta, /journal\.batch_version <=/);
  assert.match(
    delta,
    /PARTITION BY tenant_id, state_kind, lookup_key, sub_key/,
  );
  assert.match(delta, /ORDER BY batch_version DESC, committed_at DESC, row_hash DESC/);
  assert.doesNotMatch(delta, /FROM identity_state_versions/);

  assert.match(current, /FROM identity_state_base AS base/);
  assert.match(current, /FROM current_identity_delta_state/);
  assert.match(current, /FULL OUTER JOIN/);
  assert.match(current, /WHERE delta\.state_key = '' OR delta\.is_deleted = 0/);
  assert.doesNotMatch(current, /identity_state_versions/);
});

test("mapping evidence timestamps are stored on identity seed rows", () => {
  const exportSql = readProjectFile(
    "scripts/sql/export-identity-state-seed.sql",
  );
  const mappings = readProjectFile(
    "pipes/models/identity/current_identity_mappings.pipe",
  );

  assert.match(exportSql, /mapping_observations AS/);
  assert.match(exportSql, /MIN\(facts\.observed_at\) AS first_seen_at/);
  assert.match(exportSql, /MAX\(facts\.observed_at\) AS last_seen_at/);
  assert.match(exportSql, /COALESCE\(observations\.first_seen_at/);
  assert.match(exportSql, /COALESCE\(observations\.last_seen_at/);
  assert.match(mappings, /seed\.first_seen_at AS first_seen_at/);
  assert.match(mappings, /seed\.last_seen_at AS last_seen_at/);
  assert.doesNotMatch(mappings, /identity_mapping_evidence_seed/);
});

test("the enriched identity seed is isolated and tuned for resolver point lookups", () => {
  const datasource = readProjectFile(
    "datasources/seed/identity_state_seed_enriched.datasource",
  );

  assert.match(
    datasource,
    /identity_state_enriched\/snapshot_at=20260826230500\/\*\.parquet/,
  );
  assert.match(
    datasource,
    /ENGINE_SORTING_KEY tenant_id, state_kind, state_key, batch_version/,
  );
  assert.match(datasource, /ENGINE_SETTINGS "index_granularity=2048"/);
  assert.doesNotMatch(
    datasource,
    /migration_seed\/identity_state\/snapshot_at=20260826230500/,
  );
});

test("the reverse evidence index uses the same normalized identifiers and fact keys", () => {
  const datasource = readProjectFile(
    "datasources/seed/identity_fact_evidence_seed.datasource",
  );
  const exportSql = readProjectFile(
    "scripts/sql/export-identity-fact-evidence-seed.sql",
  );
  const exportScript = readProjectFile(
    "scripts/export-identity-state-seed-to-gcs.mjs",
  );
  const current = readProjectFile(
    "pipes/models/identity/current_identity_evidence.pipe",
  );

  assert.match(
    datasource,
    /ENGINE_SORTING_KEY tenant_id, identifier_key, fact_key/,
  );
  assert.match(datasource, /migration_seed\/identity_fact_evidence/);
  assert.match(exportSql, /`able-folio-499722\.booming_data_analytics\.int_identity_events`/);
  assert.match(exportSql, /FOR SYSTEM_TIME AS OF TIMESTAMP '__COMMITTED_AT__'/);
  assert.match(exportSql, /'canonical_email' AS identifier_type/);
  assert.match(exportSql, /CAST\(BYTE_LENGTH\(source_system\) AS STRING\)/);
  assert.match(exportSql, /GROUP BY identifier_key, fact_key/);
  assert.match(exportScript, /part: "fact-evidence"/);
  assert.match(exportScript, /export-identity-fact-evidence-seed\.sql/);

  assert.match(current, /FROM identity_fact_evidence_seed AS base/);
  assert.match(current, /FROM identity_state_delta_versions AS journal/);
  assert.match(current, /journal\.state_kind = 'evidence'/);
  assert.match(current, /activated_identity_batches AS activated/);
  assert.match(current, /journal\.batch_version = activated\.batch_version/);
  assert.match(current, /journal\.batch_id = activated\.batch_id/);
  assert.doesNotMatch(current, /journal\.batch_version <=/);
  assert.match(current, /FULL OUTER JOIN/);
  assert.match(current, /WHERE delta\.state_key = '' OR delta\.is_deleted = 0/);
});

test("only rows with an exact activated batch triple can change visible state", () => {
  const base = [
    { key: "mapping:email:a@example.com", value: "profile-a", version: 1 },
    { key: "mapping:email:b@example.com", value: "profile-a", version: 1 },
  ];
  const delta = [
    {
      key: "mapping:email:a@example.com",
      value: "unactivated-lower-version",
      version: 2,
      batchId: "orphan-v2",
      deleted: false,
    },
    {
      key: "mapping:email:a@example.com",
      value: "wrong-batch-id",
      version: 3,
      batchId: "spoof-v3",
      deleted: false,
    },
    {
      key: "mapping:email:a@example.com",
      value: "profile-b",
      version: 3,
      batchId: "active-v3",
      deleted: false,
    },
    {
      key: "mapping:email:b@example.com",
      value: "",
      version: 3,
      batchId: "active-v3",
      deleted: true,
    },
  ];

  assert.deepEqual(overlay(base, delta, []), base);
  assert.deepEqual(overlay(base, delta, [{ version: 3, batchId: "active-v3" }]), [
    { key: "mapping:email:a@example.com", value: "profile-b", version: 3 },
  ]);
});

test("public current-state pipes keep their existing output contracts", () => {
  const expectedColumns = {
    "current_identity_facts.pipe": [
      "tenant_id",
      "fact_kind",
      "fact_key",
      "source_fact_version",
      "fact_deleted",
      "fact_observed_at",
      "fact_payload_hash",
      "fact_payload",
      "evidence_keys",
      "producer_id",
      "batch_version",
      "committed_at",
    ],
    "current_identity_mappings.pipe": [
      "tenant_id",
      "identifier_type",
      "identifier_value",
      "identifier_key",
      "profile_id",
      "first_seen_at",
      "last_seen_at",
      "batch_version",
      "committed_at",
    ],
    "current_identity_profiles.pipe": [
      "tenant_id",
      "profile_id",
      "profile_key",
      "winner_identifier_key",
      "member_identifier_keys",
      "anonymous_ids",
      "user_ids",
      "emails",
      "phones",
      "historical_profile_ids",
      "first_name",
      "last_name",
      "first_seen_at",
      "last_seen_at",
      "batch_version",
      "committed_at",
    ],
  };

  for (const [fileName, columns] of Object.entries(expectedColumns)) {
    const contents = readProjectFile(`pipes/models/identity/${fileName}`);

    assert.match(contents, /FROM identity_state_seed_enriched/);
    assert.match(contents, /FROM identity_state_delta_versions AS journal/);
    assert.match(contents, /activated_identity_batches AS activated/);
    assert.match(contents, /journal\.batch_version = activated\.batch_version/);
    assert.match(contents, /journal\.batch_id = activated\.batch_id/);
    assert.doesNotMatch(contents, /journal\.batch_version <=/);
    assert.match(contents, /PARTITION BY journal\.tenant_id, journal\.lookup_key, journal\.sub_key/);
    assert.match(contents, /FULL OUTER JOIN/);
    assert.doesNotMatch(contents, /FROM current_identity_state/);
    for (const column of columns) {
      assert.match(contents, new RegExp(`\\b${column}\\b`));
    }
  }
});

function overlay(baseRows, deltaRows, activationRows) {
  const latestByKey = new Map();
  const activated = new Set(
    activationRows.map((row) => `${row.version}:${row.batchId}`),
  );

  for (const row of deltaRows) {
    if (!activated.has(`${row.version}:${row.batchId}`)) continue;

    const current = latestByKey.get(row.key);
    if (!current || row.version > current.version) latestByKey.set(row.key, row);
  }

  const result = [];

  for (const row of baseRows) {
    if (!latestByKey.has(row.key)) result.push(row);
  }

  for (const row of latestByKey.values()) {
    if (row.deleted) continue;
    result.push({ key: row.key, value: row.value, version: row.version });
  }

  return result.sort((left, right) => left.key.localeCompare(right.key));
}

function readProjectFile(relativePath) {
  return readFileSync(`${projectRoot}/${relativePath}`, "utf8");
}
