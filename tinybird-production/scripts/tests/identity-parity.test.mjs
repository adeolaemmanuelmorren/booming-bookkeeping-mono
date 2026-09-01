import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  buildIdentityGenerationProofSql,
  buildIdentitySurfaceQueries,
  buildIdentitySurfaces,
  compareIdentitySurface,
  emptySha256,
  identityBucketCount,
  isCoordinatorGenerationId,
  validateIdentityGenerationProof,
} from "../lib/identity-parity.mjs";
import {
  buildIdentityParityConfig,
  parseIdentityParityArguments,
} from "../compare-identity-state.mjs";
import {
  buildBigQueryDigestSql,
  buildTinybirdDigestSql,
} from "../lib/parity-sql.mjs";

test("identity parity covers the four authoritative identity views", () => {
  const surfaces = buildIdentitySurfaces();

  assert.equal(identityBucketCount, 256);
  assert.deepEqual(
    surfaces.map((surface) => surface.name),
    ["mappings", "profiles", "logical_facts", "reverse_evidence"],
  );
  assert.deepEqual(
    surfaces.map((surface) => surface.key),
    [
      ["tenant_id", "identifier_key"],
      ["tenant_id", "profile_id"],
      ["tenant_id", "fact_kind", "fact_key"],
      ["tenant_id", "identifier_key", "fact_key"],
    ],
  );
});

test("each surface uses one bucketed digest query per system", () => {
  for (const surface of buildIdentitySurfaces()) {
    const queries = buildIdentitySurfaceQueries(surface);
    const usesKeyRollup = (surface.digestKey ?? surface.key).length > 0;

    assert.deepEqual(Object.keys(queries), ["bigqueryDigest", "tinybirdDigest"]);
    assert.match(queries.bigqueryDigest, /FARM_FINGERPRINT/);
    assert.match(queries.bigqueryDigest, /BIT_XOR/);
    assert.match(queries.bigqueryDigest, /_parity_invalid_key_row/);
    assert.match(queries.bigqueryDigest, /FOR SYSTEM_TIME AS OF TIMESTAMP\(@snapshot_at\)/);

    assert.match(queries.tinybirdDigest, /farmFingerprint64/);
    assert.match(queries.tinybirdDigest, /groupBitXor/);
    assert.match(queries.tinybirdDigest, /_parity_invalid_key_row/);
    assert.match(queries.tinybirdDigest, new RegExp(surface.tinybirdResource));

    if (usesKeyRollup) {
      assert.match(queries.bigqueryDigest, /MOD\(key_fingerprint, 256\)/);
      assert.match(queries.bigqueryDigest, /duplicate_key_count/);
      assert.match(queries.tinybirdDigest, /key_fingerprint % 256/);
      assert.match(queries.tinybirdDigest, /duplicate_key_count/);
    } else {
      assert.doesNotMatch(queries.bigqueryDigest, /key_rollups/);
      assert.doesNotMatch(queries.tinybirdDigest, /key_row_count/);
    }
  }
});

test("profile and fact projections compare the identity-defining fields", () => {
  const surfaces = new Map(
    buildIdentitySurfaces().map((surface) => [surface.name, surface]),
  );
  const profiles = surfaces.get("profiles");
  const facts = surfaces.get("logical_facts");
  const evidence = surfaces.get("reverse_evidence");
  const mappings = surfaces.get("mappings");

  assert.deepEqual(
    mappings.columns.slice(-2).map((column) => column.name),
    ["first_seen_at", "last_seen_at"],
  );
  assert.match(mappings.bigqueryQuery, /identifier_observations/);
  assert.match(mappings.bigqueryQuery, /MIN\(observed_at\) AS first_seen_at/);
  assert.ok(mappings.bigqueryRelations.includes(
    "able-folio-499722.booming_data_analytics.int_identity_events",
  ));
  assert.equal(profiles.arrayPolicies.member_identifier_keys, "set");
  assert.match(profiles.bigqueryQuery, /winner_identifier_key/);
  assert.match(profiles.bigqueryQuery, /canonical_email:/);
  assert.match(facts.bigqueryQuery, /fact_deleted/);
  assert.match(facts.bigqueryQuery, /fact_payload_hash/);
  assert.match(facts.bigqueryQuery, /evidence_keys/);
  assert.doesNotMatch(facts.tinybirdQuery, /JSONExtractString/);
  assert.match(facts.tinybirdQuery, /fact_deleted = 0/);
  assert.match(evidence.bigqueryQuery, /CROSS JOIN UNNEST\(evidence_keys\)/);
  assert.match(evidence.tinybirdQuery, /current_identity_evidence/);
});

test("surface comparison fails bucket drift and invalid logical keys", () => {
  const surface = buildIdentitySurfaces()[0];
  const validRow = {
    bucket: "7",
    row_count: "2",
    key_count: "2",
    duplicate_key_count: "0",
    duplicate_rows: "0",
    fingerprint_sum: "31",
    fingerprint_xor: "9",
    [`s_${surface.columns.length}`]: "0",
  };
  const passed = compareIdentitySurface(surface, [validRow], [validRow]);

  assert.equal(passed.status, "passed");
  assert.equal(passed.key_invariants.passes, true);
  assert.equal(passed.digest.bigquery.row_count, "2");

  const invalidRow = {
    ...validRow,
    duplicate_key_count: "1",
    duplicate_rows: "1",
    fingerprint_xor: "10",
    [`s_${surface.columns.length}`]: "1",
  };
  const failed = compareIdentitySurface(surface, [validRow], [invalidRow]);

  assert.equal(failed.status, "failed");
  assert.equal(failed.digest.matches, false);
  assert.equal(failed.key_invariants.passes, false);
  assert.equal(failed.key_invariants.tinybird.duplicate_key_count, "1");
  assert.equal(failed.key_invariants.tinybird.invalid_key_rows, "1");
});

test("the CLI plans by default and makes production execution explicit", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const planConfig = buildIdentityParityConfig(parseIdentityParityArguments([]));
  assert.equal(planConfig.execute, false);

  assert.throws(
    () => buildIdentityParityConfig(parseIdentityParityArguments([
      "--execute=true",
      "--snapshot=2026-08-26T23:05:00Z",
      `--generation=${generation}`,
    ])),
    /explicit --target/,
  );
  assert.throws(
    () => buildIdentityParityConfig(parseIdentityParityArguments([
      "--execute=true",
      "--target=cloud",
      "--snapshot=2026-08-26T23:05:00Z",
      `--generation=${generation}`,
    ])),
    /quiet-cut-confirmed/,
  );

  const productionConfig = buildIdentityParityConfig(
    parseIdentityParityArguments([
      "--execute=true",
      "--target=cloud",
      "--snapshot=2026-08-26T23:05:00Z",
      `--generation=${generation}`,
      "--quiet-cut-confirmed=true",
    ]),
  );
  assert.equal(productionConfig.execute, true);
  assert.equal(productionConfig.target.name, "cloud");
  assert.throws(
    () => buildIdentityParityConfig(parseIdentityParityArguments([
      "--execute=true",
      "--target=staging",
      "--snapshot=2026-08-26T23:05:00Z",
      "--generation=catchup-1",
    ])),
    /exact coordinator generation ID/,
  );
});

test("generation proof binds manifests and activations to one exact coordinator run", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation);
  const latestActive = {
    batch_version: "1787746440001",
    batch_id: `${generation}_identity_1787746440001`,
    checkpoint_ingested_at: "2026-08-26 23:05:02.000000",
    checkpoint_event_id: "2".repeat(64),
  };
  const proof = validateIdentityGenerationProof(
    generation,
    records,
    latestActive,
  );

  assert.equal(isCoordinatorGenerationId(generation), true);
  assert.equal(isCoordinatorGenerationId("catchup-1"), false);
  assert.equal(
    isCoordinatorGenerationId("generation_raw_20260230000000000_slot_1"),
    false,
  );
  assert.equal(
    isCoordinatorGenerationId("generation_raw_20260826121401000_slot_1"),
    false,
  );
  assert.equal(proof.status, "passed");
  assert.equal(proof.manifest_count, 3);
  assert.equal(proof.nonempty_manifest_count, 2);
  assert.equal(proof.predecessor_active_batch.batch_version, "1787746439999");
  assert.equal(proof.terminal_manifest.input_event_count, "0");
  assert.equal(proof.terminal_manifest.checkpoint_sequence, "0");

  const sql = buildIdentityGenerationProofSql(generation);
  assert.match(sql, new RegExp(`concat\\('${generation}', '_identity_'\\)`));
  assert.match(sql, /state_kind = 'batch_manifest'/);
  assert.match(sql, /state_kind = 'activation_audit'/);
  assert.match(sql, /predecessor_record/);
  assert.match(sql, /all_generation_batch_keys/);
  assert.match(sql, /actual_generation_outputs/);
  assert.match(sql, /LIMIT 100001/);
});

test("generation proof ignores a retry manifest superseded by activation and output", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation);
  const manifest = records.find((row) => (
    row.record_kind === "manifest"
    && row.batch_version === "1787746440000"
  ));
  assert.ok(manifest);
  records.push({
    ...manifest,
    row_hash: manifestHash(manifest.batch_id, manifest.input_hash, "f".repeat(64)),
    output_row_count: "4",
    output_hash: "f".repeat(64),
  });

  const proof = validateIdentityGenerationProof(
    generation,
    records,
    {
      batch_version: "1787746440001",
      batch_id: `${generation}_identity_1787746440001`,
      checkpoint_ingested_at: "2026-08-26 23:05:02.000000",
      checkpoint_event_id: "2".repeat(64),
    },
  );

  assert.equal(proof.status, "passed");
  assert.match(proof.warnings.join("\n"), /superseded retry rows/);
});

test("generation proof rejects gaps, missing activation, and a stale terminal cursor", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation)
    .filter((row) => !(
      row.record_kind === "activation"
      && row.batch_version === "1787746440001"
    ))
    .map((row) => row.batch_version === "1787746440002"
      ? {
          ...row,
          batch_version: "1787746440003",
          batch_id: `${generation}_identity_1787746440003`,
          row_hash: row.record_kind === "manifest"
            ? manifestHash(
                `${generation}_identity_1787746440003`,
                emptySha256,
                emptySha256,
              )
            : row.row_hash,
        }
      : row);
  const proof = validateIdentityGenerationProof(
    generation,
    records,
    {
      batch_version: "1787746440001",
      batch_id: `${generation}_identity_1787746440001`,
      checkpoint_ingested_at: "2026-08-26 23:05:01.000000",
      checkpoint_event_id: "1".repeat(64),
    },
  );

  assert.equal(proof.status, "failed");
  assert.match(proof.errors.join("\n"), /not consecutive/);
  assert.match(proof.errors.join("\n"), /has no activation/);
  assert.match(proof.errors.join("\n"), /terminal empty cursor/);
});

test("generation proof rejects activation drift and a forged terminal empty hash", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation).map((row) => {
    if (
      row.record_kind === "activation"
      && row.batch_version === "1787746440000"
    ) {
      return { ...row, output_hash: "f".repeat(64) };
    }
    if (
      row.record_kind === "manifest"
      && row.batch_version === "1787746440002"
    ) {
      return { ...row, input_hash: "0".repeat(64) };
    }
    return row;
  });
  const proof = validateIdentityGenerationProof(
    generation,
    records,
    {
      batch_version: "1787746440001",
      batch_id: `${generation}_identity_1787746440001`,
      checkpoint_ingested_at: "2026-08-26 23:05:02.000000",
      checkpoint_event_id: "2".repeat(64),
    },
  );

  assert.equal(proof.status, "failed");
  assert.match(proof.errors.join("\n"), /differs from its manifest: output_hash/);
  assert.match(proof.errors.join("\n"), /wrong input hash/);
});

test("generation proof rejects activation count and checkpoint drift", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation).map((row) => {
    if (
      row.record_kind !== "activation"
      || row.batch_version !== "1787746440001"
    ) {
      return row;
    }

    return {
      ...row,
      checkpoint_sequence: "1",
      checkpoint_event_id: "9".repeat(64),
      input_event_count: "42",
      output_row_count: "10",
    };
  });
  const proof = validateIdentityGenerationProof(
    generation,
    records,
    {
      batch_version: "1787746440001",
      batch_id: `${generation}_identity_1787746440001`,
      checkpoint_ingested_at: "2026-08-26 23:05:02.000000",
      checkpoint_event_id: "2".repeat(64),
    },
  );

  assert.equal(proof.status, "failed");
  assert.match(
    proof.errors.join("\n"),
    /checkpoint_sequence, input_event_count, checkpoint_event_id, output_row_count/,
  );
});

test("generation proof rejects a missing leading batch", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation).filter(
    (row) => row.batch_version !== "1787746440000",
  );
  const proof = validateIdentityGenerationProof(
    generation,
    records,
    {
      batch_version: "1787746440001",
      batch_id: `${generation}_identity_1787746440001`,
      checkpoint_ingested_at: "2026-08-26 23:05:02.000000",
      checkpoint_event_id: "2".repeat(64),
    },
  );

  assert.equal(proof.status, "failed");
  assert.match(proof.errors.join("\n"), /does not equal anchored version/);
});

test("generation proof rejects nonzero producer checkpoint sequences", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation).map((row) => {
    if (["manifest", "activation"].includes(row.record_kind)) {
      return { ...row, checkpoint_sequence: "999" };
    }
    return row;
  });
  const proof = validateIdentityGenerationProof(
    generation,
    records,
    {
      batch_version: "1787746440001",
      batch_id: `${generation}_identity_1787746440001`,
      checkpoint_ingested_at: "2026-08-26 23:05:02.000000",
      checkpoint_event_id: "2".repeat(64),
    },
  );

  assert.equal(proof.status, "failed");
  assert.match(proof.errors.join("\n"), /invalid checkpoint sequence/);
});

test("generation proof rejects output rows without a manifest", () => {
  const generation = "generation_raw_20260826121400000_slot_1";
  const records = validGenerationRecords(generation);
  const orphanActual = records.find((row) => row.record_kind === "actual");
  assert.ok(orphanActual);
  records.push({
    ...orphanActual,
    batch_version: "1787746440003",
    batch_id: `${generation}_identity_1787746440003`,
    output_row_count: "1",
    output_hash: "8".repeat(64),
  });
  const proof = validateIdentityGenerationProof(
    generation,
    records,
    {
      batch_version: "1787746440001",
      batch_id: `${generation}_identity_1787746440001`,
      checkpoint_ingested_at: "2026-08-26 23:05:02.000000",
      checkpoint_event_id: "2".repeat(64),
    },
  );

  assert.equal(proof.status, "failed");
  assert.match(proof.errors.join("\n"), /actual output .* has no manifest/);
});

test("inline parity projections reject non-read-only statements", () => {
  const columns = [{ name: "id", data_type: "STRING" }];

  assert.throws(
    () => buildBigQueryDigestSql({
      columns,
      source: { query: "DELETE FROM dataset.table WHERE true" },
      bucketCount: 256,
      floatScale: 9,
    }),
    /must start with SELECT or WITH/,
  );
  assert.throws(
    () => buildTinybirdDigestSql({
      columns,
      resource: { query: "SELECT id FROM source; DROP TABLE source" },
      bucketCount: 256,
      floatScale: 9,
    }),
    /cannot contain a statement separator/,
  );
});

function validGenerationRecords(generation) {
  const batches = [
    {
      version: "1787746440000",
      inputCount: "5000",
      inputHash: "a".repeat(64),
      checkpoint: "2026-08-26 23:05:01.000000",
      checkpointEvent: "1".repeat(64),
      outputCount: "17",
      outputHash: "b".repeat(64),
    },
    {
      version: "1787746440001",
      inputCount: "41",
      inputHash: "c".repeat(64),
      checkpoint: "2026-08-26 23:05:02.000000",
      checkpointEvent: "2".repeat(64),
      outputCount: "9",
      outputHash: "d".repeat(64),
    },
    {
      version: "1787746440002",
      inputCount: "0",
      inputHash: emptySha256,
      checkpoint: "2026-08-26 23:05:02.000000",
      checkpointEvent: "2".repeat(64),
      outputCount: "0",
      outputHash: emptySha256,
    },
  ];
  const records = [
    {
      record_kind: "predecessor",
      tenant_id: "boom",
      batch_version: "1787746439999",
      batch_id:
        "generation_raw_20260826120900000_slot_0_identity_1787746439999",
      committed_at: "2026-08-26 23:04:59.000000",
      row_hash: "e".repeat(64),
      is_deleted: "0",
      producer_id: "identity_compactor",
      checkpoint_sequence: "0",
      input_event_count: "3",
      input_hash: "e".repeat(64),
      checkpoint_ingested_at: "2026-08-26 23:05:00.000000",
      checkpoint_event_id: "0".repeat(64),
      output_row_count: "2",
      output_hash: "e".repeat(64),
    },
  ];

  for (const batch of batches) {
    const batchId = `${generation}_identity_${batch.version}`;
    const shared = {
      tenant_id: "boom",
      batch_version: batch.version,
      batch_id: batchId,
      committed_at: "2026-08-26 23:06:00.000000",
      is_deleted: "0",
      producer_id: "identity_compactor",
      checkpoint_sequence: "0",
      input_event_count: batch.inputCount,
      input_hash: batch.inputHash,
      checkpoint_ingested_at: batch.checkpoint,
      checkpoint_event_id: batch.checkpointEvent,
      output_row_count: batch.outputCount,
      output_hash: batch.outputHash,
    };

    records.push({
      ...shared,
      record_kind: "manifest",
      row_hash: manifestHash(batchId, batch.inputHash, batch.outputHash),
    });
    records.push({
      ...shared,
      record_kind: "actual",
      committed_at: "",
      row_hash: "",
      producer_id: "",
      input_event_count: "0",
      input_hash: "",
      checkpoint_ingested_at: "",
      checkpoint_event_id: "",
    });

    if (batch.inputCount !== "0") {
      records.push({
        ...shared,
        record_kind: "activation",
        row_hash: batch.outputHash,
      });
    }
  }

  return records;
}

function manifestHash(batchId, inputHash, outputHash) {
  return createHash("sha256")
    .update(`${batchId}:${inputHash}:${outputHash}`)
    .digest("hex");
}
