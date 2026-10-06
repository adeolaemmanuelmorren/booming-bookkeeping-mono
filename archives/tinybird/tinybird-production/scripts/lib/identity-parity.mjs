import { createHash } from "node:crypto";

import {
  buildBigQueryDigestSql,
  buildTinybirdDigestSql,
} from "./parity-sql.mjs";

export const identityBucketCount = 256;
export const identityFloatScale = 9;
export const emptySha256 = sha256("");

const coordinatorGenerationPattern =
  /^generation_raw_([0-9]{17})_slot_([0-9])$/;

const analyticsDataset =
  "able-folio-499722.booming_data_analytics";

export function buildIdentitySurfaces() {
  return [
    mappingsSurface(),
    profilesSurface(),
    logicalFactsSurface(),
    reverseEvidenceSurface(),
  ];
}

export function buildIdentitySurfaceQueries(surface) {
  const columns = comparisonColumns(surface);
  const common = {
    columns,
    key: surface.digestKey ?? surface.key,
    bucketCount: identityBucketCount,
    floatScale: identityFloatScale,
    arrayPolicies: surface.arrayPolicies,
  };

  return {
    bigqueryDigest: buildBigQueryDigestSql({
      ...common,
      source: { query: withBigQueryKeyInvariants(surface) },
    }),
    tinybirdDigest: buildTinybirdDigestSql({
      ...common,
      resource: { query: withTinybirdKeyInvariants(surface) },
    }),
  };
}

export function isCoordinatorGenerationId(value) {
  return coordinatorGenerationStartVersion(value) !== null;
}

export function buildIdentityGenerationProofSql(generationId) {
  if (!isCoordinatorGenerationId(generationId)) {
    throw new Error("Invalid coordinator generation ID");
  }

  return `
    WITH
      generation_request AS (
        SELECT
          '${generationId}' AS generation_id,
          concat('${generationId}', '_identity_') AS batch_prefix
      ),
      generation_manifest_rows AS (
        SELECT journal.*
        FROM identity_state_delta_versions AS journal
        CROSS JOIN generation_request AS request
        WHERE
          journal.tenant_id = 'boom'
          AND journal.state_kind = 'batch_manifest'
          AND startsWith(journal.batch_id, request.batch_prefix)
      ),
      generation_activation_rows AS (
        SELECT journal.*
        FROM identity_state_delta_versions AS journal
        CROSS JOIN generation_request AS request
        WHERE
          journal.tenant_id = 'boom'
          AND journal.state_kind = 'activation_audit'
          AND startsWith(journal.batch_id, request.batch_prefix)
      ),
      manifest_batch_keys AS (
        SELECT DISTINCT tenant_id, batch_version, batch_id
        FROM generation_manifest_rows
      ),
      first_manifest_batch AS (
        SELECT min(batch_version) AS first_batch_version
        FROM generation_manifest_rows
      ),
      predecessor_record AS (
        SELECT
          'predecessor' AS record_kind,
          toString(active.tenant_id) AS tenant_id,
          toString(active.batch_version) AS batch_version,
          toString(active.batch_id) AS batch_id,
          toString(active.committed_at) AS committed_at,
          toString(active.row_hash) AS row_hash,
          '0' AS is_deleted,
          toString(active.producer_id) AS producer_id,
          toString(active.checkpoint_sequence) AS checkpoint_sequence,
          toString(active.input_event_count) AS input_event_count,
          toString(active.input_hash) AS input_hash,
          toString(active.checkpoint_ingested_at) AS checkpoint_ingested_at,
          toString(active.checkpoint_event_id) AS checkpoint_event_id,
          toString(active.output_row_count) AS output_row_count,
          toString(active.output_hash) AS output_hash
        FROM activated_identity_batches AS active
        CROSS JOIN first_manifest_batch AS first
        CROSS JOIN generation_request AS request
        WHERE
          active.tenant_id = 'boom'
          AND active.batch_version < first.first_batch_version
          AND NOT startsWith(active.batch_id, request.batch_prefix)
        ORDER BY
          active.batch_version DESC,
          active.committed_at DESC,
          active.row_hash DESC,
          active.batch_id DESC
        LIMIT 1
      ),
      distinct_generation_output_rows AS (
        SELECT DISTINCT
          journal.tenant_id AS tenant_id,
          journal.batch_version AS batch_version,
          journal.batch_id AS batch_id,
          journal.state_kind AS state_kind,
          journal.lookup_key AS lookup_key,
          journal.sub_key AS sub_key,
          journal.row_hash AS row_hash
        FROM identity_state_delta_versions AS journal
        CROSS JOIN generation_request AS request
        WHERE
          journal.tenant_id = 'boom'
          AND journal.state_kind IN ('fact', 'evidence', 'mapping', 'profile')
          AND startsWith(journal.batch_id, request.batch_prefix)
      ),
      actual_generation_outputs AS (
        SELECT
          tenant_id,
          batch_version,
          batch_id,
          count() AS actual_output_row_count,
          lower(
            hex(
              SHA256(
                arrayStringConcat(
                  arraySort(
                    groupArray(
                      concat(state_kind, ':', lookup_key, ':', sub_key, ':', row_hash)
                    )
                  ),
                  '|'
                )
              )
            )
          ) AS actual_output_hash
        FROM distinct_generation_output_rows
        GROUP BY tenant_id, batch_version, batch_id
      ),
      all_generation_batch_keys AS (
        SELECT DISTINCT tenant_id, batch_version, batch_id
        FROM (
          SELECT tenant_id, batch_version, batch_id
          FROM manifest_batch_keys
          UNION ALL
          SELECT tenant_id, batch_version, batch_id
          FROM actual_generation_outputs
        )
      ),
      manifest_records AS (
        SELECT
          'manifest' AS record_kind,
          toString(tenant_id) AS tenant_id,
          toString(batch_version) AS batch_version,
          toString(batch_id) AS batch_id,
          toString(committed_at) AS committed_at,
          toString(row_hash) AS row_hash,
          toString(is_deleted) AS is_deleted,
          toString(producer_id) AS producer_id,
          toString(checkpoint_sequence) AS checkpoint_sequence,
          toString(input_event_count) AS input_event_count,
          toString(input_hash) AS input_hash,
          toString(checkpoint_ingested_at) AS checkpoint_ingested_at,
          toString(checkpoint_event_id) AS checkpoint_event_id,
          toString(output_row_count) AS output_row_count,
          toString(output_hash) AS output_hash
        FROM generation_manifest_rows
      ),
      activation_records AS (
        SELECT
          'activation' AS record_kind,
          toString(tenant_id) AS tenant_id,
          toString(batch_version) AS batch_version,
          toString(batch_id) AS batch_id,
          toString(committed_at) AS committed_at,
          toString(row_hash) AS row_hash,
          toString(is_deleted) AS is_deleted,
          toString(producer_id) AS producer_id,
          toString(checkpoint_sequence) AS checkpoint_sequence,
          toString(input_event_count) AS input_event_count,
          toString(input_hash) AS input_hash,
          toString(checkpoint_ingested_at) AS checkpoint_ingested_at,
          toString(checkpoint_event_id) AS checkpoint_event_id,
          toString(output_row_count) AS output_row_count,
          toString(output_hash) AS output_hash
        FROM generation_activation_rows
      ),
      actual_records AS (
        SELECT
          'actual' AS record_kind,
          toString(batch.tenant_id) AS tenant_id,
          toString(batch.batch_version) AS batch_version,
          toString(batch.batch_id) AS batch_id,
          '' AS committed_at,
          '' AS row_hash,
          '0' AS is_deleted,
          '' AS producer_id,
          '0' AS checkpoint_sequence,
          '0' AS input_event_count,
          '' AS input_hash,
          '' AS checkpoint_ingested_at,
          '' AS checkpoint_event_id,
          toString(
            if(outputs.batch_id = '', toUInt64(0), outputs.actual_output_row_count)
          ) AS output_row_count,
          if(
            outputs.batch_id = '',
            lower(hex(SHA256(''))),
            toString(outputs.actual_output_hash)
          ) AS output_hash
        FROM all_generation_batch_keys AS batch
        LEFT JOIN actual_generation_outputs AS outputs
          ON batch.tenant_id = outputs.tenant_id
          AND batch.batch_version = outputs.batch_version
          AND batch.batch_id = outputs.batch_id
      )
    SELECT *
    FROM (
      SELECT * FROM manifest_records
      UNION ALL
      SELECT * FROM activation_records
      UNION ALL
      SELECT * FROM predecessor_record
      UNION ALL
      SELECT * FROM actual_records
    )
    ORDER BY toUInt64(batch_version), record_kind, committed_at, row_hash
    LIMIT 100001
  `;
}

export function validateIdentityGenerationProof(
  generationId,
  records,
  latestActive,
) {
  const errors = [];
  const warnings = [];
  const normalizedRecords = records.map(normalizeGenerationRecord);
  const manifests = normalizedRecords.filter((row) => row.record_kind === "manifest");
  const activations = normalizedRecords.filter((row) => row.record_kind === "activation");
  const actuals = normalizedRecords.filter((row) => row.record_kind === "actual");
  const predecessors = normalizedRecords.filter(
    (row) => row.record_kind === "predecessor",
  );
  const expectedPrefix = `${generationId}_identity_`;
  const generationStartVersion = coordinatorGenerationStartVersion(generationId);

  if (!isCoordinatorGenerationId(generationId)) {
    errors.push("generation_id is not an exact coordinator generation ID");
  }
  if (normalizedRecords.length >= 100001) {
    errors.push("generation proof exceeded the 100000-row safety limit");
  }
  if (manifests.length === 0) {
    errors.push("generation has no identity manifests");
  }
  if (predecessors.length !== 1) {
    errors.push("generation proof must contain exactly one predecessor activation");
  }

  const manifestGroups = groupByBatch(manifests);
  const activationGroups = groupByBatch(activations);
  const actualGroups = groupByBatch(actuals);
  const manifestRows = [];

  for (const [batchKey, group] of manifestGroups) {
    const actualGroup = actualGroups.get(batchKey) ?? [];
    if (actualGroup.length !== 1) {
      errors.push(`manifest ${batchKey} does not have one actual-output record`);
      continue;
    }
    const actual = actualGroup[0];
    const candidates = group.filter((manifest) => (
      manifest.output_row_count === actual.output_row_count
      && manifest.output_hash === actual.output_hash
    ));
    if (distinctSignatureCount(candidates, manifestSignatureFields()) !== 1) {
      errors.push(`manifest ${batchKey} has no unique publication-matching row`);
      continue;
    }
    if (distinctSignatureCount(group, manifestSignatureFields()) > 1) {
      warnings.push(`manifest ${batchKey} has superseded retry rows`);
    }

    const manifest = candidates[0];
    const expectedBatchId = `${expectedPrefix}${manifest.batch_version}`;
    if (manifest.batch_id !== expectedBatchId) {
      errors.push(`manifest ${batchKey} is not bound to generation ${generationId}`);
    }
    if (manifest.tenant_id !== "boom") {
      errors.push(`manifest ${batchKey} has the wrong tenant`);
    }
    if (manifest.is_deleted !== "0" || manifest.producer_id !== "identity_compactor") {
      errors.push(`manifest ${batchKey} has invalid publication metadata`);
    }
    if (manifest.checkpoint_sequence !== "0") {
      errors.push(`manifest ${batchKey} has an invalid checkpoint sequence`);
    }
    if (manifest.row_hash !== manifestRowHash(manifest)) {
      errors.push(`manifest ${batchKey} row hash does not match its payload`);
    }

    if (
      manifest.output_row_count !== actual.output_row_count
      || manifest.output_hash !== actual.output_hash
    ) {
      errors.push(`manifest ${batchKey} does not match its actual output`);
    }

    manifestRows.push({ ...manifest, actual });
  }

  for (const batchKey of activationGroups.keys()) {
    if (!manifestGroups.has(batchKey)) {
      errors.push(`activation ${batchKey} has no manifest in this generation`);
    }
  }
  for (const batchKey of actualGroups.keys()) {
    if (!manifestGroups.has(batchKey)) {
      errors.push(`actual output ${batchKey} has no manifest in this generation`);
    }
  }

  manifestRows.sort(compareBatchVersions);
  validateManifestSequence(
    manifestRows,
    predecessors[0] ?? null,
    generationStartVersion,
    generationId,
    errors,
  );
  const terminal = manifestRows.at(-1) ?? null;
  const nonempty = manifestRows.filter((row) => row.input_event_count !== "0");

  if (terminal) {
    validateTerminalManifest(terminal, activationGroups, latestActive, errors);
  }

  for (const manifest of nonempty) {
    validateManifestActivation(manifest, activationGroups, errors);
  }

  if (terminal && nonempty.length > 0) {
    const latestNonempty = nonempty.at(-1);
    if (
      latestActive?.batch_version !== latestNonempty.batch_version
      || latestActive?.batch_id !== latestNonempty.batch_id
    ) {
      errors.push("latest active batch is not the generation's final nonempty manifest");
    }
  }

  return {
    status: errors.length === 0 ? "passed" : "failed",
    generation_id: generationId,
    manifest_count: manifestRows.length,
    nonempty_manifest_count: nonempty.length,
    predecessor_active_batch: predecessors.length === 1
      ? summarizePredecessor(predecessors[0])
      : null,
    terminal_manifest: terminal ? summarizeManifest(terminal) : null,
    batches: manifestRows.map(summarizeManifest),
    errors,
    warnings,
  };
}

export function compareIdentitySurface(
  surface,
  bigqueryDigestRows,
  tinybirdDigestRows,
) {
  const bigqueryBuckets = normalizeBuckets(bigqueryDigestRows);
  const tinybirdBuckets = normalizeBuckets(tinybirdDigestRows);
  const bucketMismatches = compareBuckets(bigqueryBuckets, tinybirdBuckets);
  const bigqueryKeyInvariants = summarizeKeyInvariants(surface, bigqueryBuckets);
  const tinybirdKeyInvariants = summarizeKeyInvariants(surface, tinybirdBuckets);
  const keyInvariantsPass = bigqueryKeyInvariants.passes
    && tinybirdKeyInvariants.passes;

  return {
    name: surface.name,
    status: bucketMismatches.length === 0 && keyInvariantsPass
      ? "passed"
      : "failed",
    unique_key: surface.key,
    digest: {
      matches: bucketMismatches.length === 0,
      bucket_count: identityBucketCount,
      bigquery: summarizeBuckets(bigqueryBuckets),
      tinybird: summarizeBuckets(tinybirdBuckets),
      bucket_mismatches: bucketMismatches,
    },
    key_invariants: {
      passes: keyInvariantsPass,
      requirement: (surface.digestKey ?? surface.key).length > 0
        ? "Every logical key is present, non-empty, and occurs exactly once."
        : "Every logical key is present and non-empty; the digest compares the exact row multiset.",
      bigquery: bigqueryKeyInvariants,
      tinybird: tinybirdKeyInvariants,
    },
  };
}

function mappingsSurface() {
  const columns = [
    stringColumn("tenant_id"),
    stringColumn("identifier_type"),
    stringColumn("identifier_value"),
    stringColumn("identifier_key"),
    stringColumn("profile_id"),
    timestampColumn("first_seen_at"),
    timestampColumn("last_seen_at"),
  ];

  return {
    name: "mappings",
    description: "One current profile per normalized identifier, including canonical Gmail aliases.",
    key: ["tenant_id", "identifier_key"],
    columns,
    arrayPolicies: {},
    bigqueryRelations: [
      `${analyticsDataset}.int_identifier_profile_lookup`,
      `${analyticsDataset}.int_resolved_identifiers`,
      `${analyticsDataset}.int_identity_events`,
    ],
    tinybirdResource: "current_identity_mappings",
    bigqueryQuery: `
      WITH ${bigQueryNormalizedFactsCtes()},
      identifier_observations AS (
        SELECT
          identifier_key,
          MIN(observed_at) AS first_seen_at,
          MAX(observed_at) AS last_seen_at
        FROM normalized_identity_facts
        CROSS JOIN UNNEST(evidence_keys) AS identifier_key
        GROUP BY identifier_key
      ),
      public_mappings AS (
        SELECT identifier_type, identifier_value, profile_id
        FROM \`${analyticsDataset}.int_identifier_profile_lookup\`
          FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
      ),
      canonical_email_mappings AS (
        SELECT
          'canonical_email' AS identifier_type,
          ${bigQueryCanonicalEmail("email")} AS identifier_value,
          profile_id
        FROM \`${analyticsDataset}.int_resolved_identifiers\`
          FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
        CROSS JOIN UNNEST(emails) AS email
      ),
      all_mappings AS (
        SELECT * FROM public_mappings
        UNION DISTINCT
        SELECT * FROM canonical_email_mappings
      )
      SELECT
        'boom' AS tenant_id,
        identifier_type,
        identifier_value,
        CONCAT(identifier_type, ':', identifier_value) AS identifier_key,
        profile_id,
        COALESCE(observations.first_seen_at, TIMESTAMP '1970-01-01 00:00:00+00')
          AS first_seen_at,
        COALESCE(observations.last_seen_at, TIMESTAMP '1970-01-01 00:00:00+00')
          AS last_seen_at
      FROM all_mappings AS mappings
      LEFT JOIN identifier_observations AS observations
        ON observations.identifier_key = CONCAT(
          mappings.identifier_type,
          ':',
          mappings.identifier_value
        )
    `,
    tinybirdQuery: `
      SELECT
        tenant_id,
        identifier_type,
        identifier_value,
        identifier_key,
        profile_id,
        first_seen_at,
        last_seen_at
      FROM current_identity_mappings
      WHERE tenant_id = 'boom'
    `,
  };
}

function profilesSurface() {
  const columns = [
    stringColumn("tenant_id"),
    stringColumn("profile_id"),
    stringColumn("profile_key"),
    stringColumn("winner_identifier_key"),
    stringArrayColumn("member_identifier_keys"),
    stringArrayColumn("anonymous_ids"),
    stringArrayColumn("user_ids"),
    stringArrayColumn("emails"),
    stringArrayColumn("phones"),
    stringColumn("first_name"),
    stringColumn("last_name"),
    timestampColumn("first_seen_at"),
    timestampColumn("last_seen_at"),
  ];

  return {
    name: "profiles",
    description: "Resolved connected components, deterministic winners, members, and profile traits.",
    key: ["tenant_id", "profile_id"],
    columns,
    arrayPolicies: {
      member_identifier_keys: "set",
      anonymous_ids: "set",
      user_ids: "set",
      emails: "set",
      phones: "set",
    },
    bigqueryRelations: [`${analyticsDataset}.int_resolved_identifiers`],
    tinybirdResource: "current_identity_profiles",
    bigqueryQuery: `
      WITH profiles AS (
        SELECT *
        FROM \`${analyticsDataset}.int_resolved_identifiers\`
          FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
      )
      SELECT
        'boom' AS tenant_id,
        profile_id,
        profile_key,
        CASE
          WHEN profile_key IN UNNEST(emails) THEN CONCAT('email:', profile_key)
          WHEN profile_key IN UNNEST(ARRAY(
            SELECT ${bigQueryCanonicalEmail("candidate_email")}
            FROM UNNEST(emails) AS candidate_email
          )) THEN CONCAT('canonical_email:', profile_key)
          WHEN profile_key IN UNNEST(user_ids) THEN CONCAT('user_id:', profile_key)
          WHEN profile_key IN UNNEST(anonymous_ids) THEN CONCAT('anonymous_id:', profile_key)
          WHEN profile_key IN UNNEST(phones) THEN CONCAT('phone:', profile_key)
          ELSE CONCAT('unknown:', profile_key)
        END AS winner_identifier_key,
        ARRAY(
          SELECT DISTINCT identifier_key
          FROM UNNEST(ARRAY_CONCAT(
            ARRAY(
              SELECT CONCAT('anonymous_id:', value)
              FROM UNNEST(anonymous_ids) AS value
            ),
            ARRAY(
              SELECT CONCAT('user_id:', value)
              FROM UNNEST(user_ids) AS value
            ),
            ARRAY(
              SELECT CONCAT('email:', value)
              FROM UNNEST(emails) AS value
            ),
            ARRAY(
              SELECT CONCAT('canonical_email:', ${bigQueryCanonicalEmail("value")})
              FROM UNNEST(emails) AS value
            ),
            ARRAY(
              SELECT CONCAT('phone:', value)
              FROM UNNEST(phones) AS value
            )
          )) AS identifier_key
          ORDER BY identifier_key
        ) AS member_identifier_keys,
        anonymous_ids,
        user_ids,
        emails,
        phones,
        COALESCE(first_name, '') AS first_name,
        COALESCE(last_name, '') AS last_name,
        COALESCE(first_seen_at, TIMESTAMP '1970-01-01 00:00:00+00') AS first_seen_at,
        COALESCE(last_seen_at, TIMESTAMP '1970-01-01 00:00:00+00') AS last_seen_at
      FROM profiles
    `,
    tinybirdQuery: `
      SELECT
        tenant_id,
        profile_id,
        profile_key,
        winner_identifier_key,
        member_identifier_keys,
        anonymous_ids,
        user_ids,
        emails,
        phones,
        first_name,
        last_name,
        first_seen_at,
        last_seen_at
      FROM current_identity_profiles
      WHERE tenant_id = 'boom'
    `,
  };
}

function logicalFactsSurface() {
  const columns = [
    stringColumn("tenant_id"),
    stringColumn("fact_kind"),
    stringColumn("fact_key"),
    integerColumn("fact_deleted"),
    timestampColumn("fact_observed_at"),
    stringColumn("fact_payload_hash"),
    stringArrayColumn("evidence_keys"),
  ];

  return {
    name: "logical_facts",
    description: "Current identity-bearing logical facts after normalization and deletion handling.",
    key: ["tenant_id", "fact_kind", "fact_key"],
    digestKey: [],
    columns,
    arrayPolicies: { evidence_keys: "set" },
    bigqueryRelations: [`${analyticsDataset}.int_identity_events`],
    tinybirdResource: "current_identity_facts",
    bigqueryQuery: `
      WITH ${bigQueryNormalizedFactsCtes()}
      SELECT
        'boom' AS tenant_id,
        'identity_observation' AS fact_kind,
        ${bigQueryFactKey()} AS fact_key,
        CAST(0 AS INT64) AS fact_deleted,
        observed_at AS fact_observed_at,
        ${bigQueryFactPayloadHash()} AS fact_payload_hash,
        evidence_keys
      FROM normalized_identity_facts
    `,
    tinybirdQuery: `
      SELECT
        tenant_id,
        fact_kind,
        fact_key,
        toInt64(fact_deleted) AS fact_deleted,
        fact_observed_at,
        fact_payload_hash,
        evidence_keys
      FROM current_identity_facts
      WHERE tenant_id = 'boom' AND fact_deleted = 0
    `,
  };
}

function reverseEvidenceSurface() {
  const columns = [
    stringColumn("tenant_id"),
    stringColumn("identifier_key"),
    stringColumn("fact_key"),
  ];

  return {
    name: "reverse_evidence",
    description: "Every active identifier-to-fact pair used for exact component splits and joins.",
    key: ["tenant_id", "identifier_key", "fact_key"],
    digestKey: [],
    columns,
    arrayPolicies: {},
    bigqueryRelations: [`${analyticsDataset}.int_identity_events`],
    tinybirdResource: "current_identity_evidence",
    bigqueryQuery: `
      WITH ${bigQueryNormalizedFactsCtes()}
      SELECT DISTINCT
        'boom' AS tenant_id,
        identifier_key,
        ${bigQueryFactKey()} AS fact_key
      FROM normalized_identity_facts
      CROSS JOIN UNNEST(evidence_keys) AS identifier_key
    `,
    tinybirdQuery: `
      SELECT tenant_id, identifier_key, fact_key
      FROM current_identity_evidence
      WHERE tenant_id = 'boom'
    `,
  };
}

export function bigQueryNormalizedFactsCtes() {
  return `
        identity_events AS (
          SELECT *
          FROM \`${analyticsDataset}.int_identity_events\`
            FOR SYSTEM_TIME AS OF TIMESTAMP(@snapshot_at)
        ),
        phone_digits AS (
          SELECT
            events.*,
            REGEXP_REPLACE(TRIM(CAST(phone AS STRING)), r'[^0-9]', '') AS digits
          FROM identity_events AS events
        ),
        national_phones AS (
          SELECT
            events.*,
            CASE
              WHEN LENGTH(digits) = 10 THEN digits
              WHEN LENGTH(digits) = 11 AND STARTS_WITH(digits, '1') THEN SUBSTR(digits, 2)
              ELSE CAST(NULL AS STRING)
            END AS national_phone
          FROM phone_digits AS events
        ),
        normalized_values AS (
          SELECT
            source_system,
            source_record_id,
            COALESCE(observed_at, TIMESTAMP '1970-01-01 00:00:00+00') AS observed_at,
            NULLIF(TRIM(anonymous_id), '') AS anonymous_id,
            NULLIF(LOWER(TRIM(user_id)), '') AS user_id,
            NULLIF(LOWER(TRIM(email)), '') AS email,
            CASE
              WHEN NULLIF(LOWER(TRIM(email)), '') IS NULL THEN CAST(NULL AS STRING)
              ELSE ${bigQueryCanonicalEmail("email")}
            END AS canonical_email,
            CASE
              WHEN phone IS NULL THEN CAST(NULL AS STRING)
              WHEN NOT REGEXP_CONTAINS(national_phone, r'^[2-9][0-9]{2}[2-9][0-9]{6}$') THEN CAST(NULL AS STRING)
              WHEN SUBSTR(national_phone, 4, 3) = '555' THEN CAST(NULL AS STRING)
              WHEN REGEXP_CONTAINS(
                national_phone,
                r'(0000000|1111111|2222222|3333333|4444444|5555555|6666666|7777777|8888888|9999999)$'
              ) THEN CAST(NULL AS STRING)
              WHEN REGEXP_CONTAINS(
                national_phone,
                r'^(0123456789|1234567890|234567890[0-9]|9876543210)$'
              ) THEN CAST(NULL AS STRING)
              ELSE CONCAT('+1', national_phone)
            END AS phone,
            first_name,
            last_name
          FROM national_phones
        ),
        normalized_identity_facts AS (
          SELECT
            *,
            ARRAY(
              SELECT DISTINCT identifier_key
              FROM UNNEST([
                IF(anonymous_id IS NULL, NULL, CONCAT('anonymous_id:', anonymous_id)),
                IF(user_id IS NULL, NULL, CONCAT('user_id:', user_id)),
                IF(email IS NULL, NULL, CONCAT('email:', email)),
                IF(canonical_email IS NULL, NULL, CONCAT('canonical_email:', canonical_email)),
                IF(phone IS NULL, NULL, CONCAT('phone:', phone))
              ]) AS identifier_key
              WHERE identifier_key IS NOT NULL
              ORDER BY identifier_key
            ) AS evidence_keys
          FROM normalized_values
        )
  `.trim();
}

function bigQueryCanonicalEmail(expression) {
  return `CASE
            WHEN SPLIT(LOWER(TRIM(${expression})), '@')[SAFE_OFFSET(1)]
              IN ('gmail.com', 'googlemail.com') THEN
              CONCAT(
                REPLACE(
                  REGEXP_REPLACE(
                    SPLIT(LOWER(TRIM(${expression})), '@')[SAFE_OFFSET(0)],
                    r'[+].*$',
                    ''
                  ),
                  '.',
                  ''
                ),
                '@gmail.com'
              )
            ELSE LOWER(TRIM(${expression}))
          END`;
}

function bigQueryFactKey() {
  return `CONCAT(
          CAST(BYTE_LENGTH(source_system) AS STRING),
          ':',
          source_system,
          ':',
          CAST(BYTE_LENGTH(source_record_id) AS STRING),
          ':',
          source_record_id
        )`;
}

function bigQueryFactPayloadHash() {
  const fields = [
    "source_system",
    "source_record_id",
    "CAST(UNIX_MICROS(observed_at) AS STRING)",
    "'0'",
    "COALESCE(anonymous_id, '')",
    "COALESCE(user_id, '')",
    "COALESCE(email, '')",
    "COALESCE(phone, '')",
    "COALESCE(first_name, '')",
    "COALESCE(last_name, '')",
  ];
  const canonical = fields.map((expression) => `CONCAT(
            CAST(BYTE_LENGTH(${expression}) AS STRING),
            ':',
            ${expression}
          )`).join(",\n          ");
  return `LOWER(TO_HEX(SHA256(CONCAT(
          ${canonical}
        ))))`;
}

function normalizeBuckets(rows) {
  return rows.map((row) => Object.fromEntries(
    Object.entries(row)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([name, value]) => [name, String(value)]),
  )).sort((left, right) => Number(left.bucket) - Number(right.bucket));
}

function compareBuckets(expectedRows, actualRows) {
  const expected = new Map(expectedRows.map((row) => [row.bucket, row]));
  const actual = new Map(actualRows.map((row) => [row.bucket, row]));
  const bucketNumbers = [...new Set([...expected.keys(), ...actual.keys()])]
    .sort((left, right) => Number(left) - Number(right));

  return bucketNumbers.flatMap((bucket) => {
    const bigquery = expected.get(bucket) ?? null;
    const tinybird = actual.get(bucket) ?? null;
    if (JSON.stringify(bigquery) === JSON.stringify(tinybird)) return [];
    return [{ bucket, bigquery, tinybird }];
  });
}

function summarizeBuckets(rows) {
  return {
    row_count: sumField(rows, "row_count"),
    populated_bucket_count: rows.length,
    bucket_digest_sha256: sha256(rows.map((row) => JSON.stringify(row)).join("\n")),
    buckets: rows,
  };
}

function comparisonColumns(surface) {
  return [
    ...surface.columns,
    integerColumn("_parity_invalid_key_row"),
  ];
}

function withBigQueryKeyInvariants(surface) {
  const columns = surface.columns.map((column) => `\`${column.name}\``).join(", ");
  const invalidKey = surface.key
    .map((key) => `\`${key}\` IS NULL OR \`${key}\` = ''`)
    .join(" OR ");

  return `
    SELECT
      ${columns},
      IF(${invalidKey}, 1, 0) AS _parity_invalid_key_row
    FROM (${surface.bigqueryQuery})
  `;
}

function withTinybirdKeyInvariants(surface) {
  const columns = surface.columns.map((column) => `\`${column.name}\``).join(", ");
  const invalidKey = surface.key
    .map((key) => `isNull(\`${key}\`) OR \`${key}\` = ''`)
    .join(" OR ");

  return `
    SELECT
      ${columns},
      toInt64(${invalidKey}) AS _parity_invalid_key_row
    FROM (${surface.tinybirdQuery})
  `;
}

function summarizeKeyInvariants(surface, rows) {
  const invalidMetric = `s_${surface.columns.length}`;
  const uniquenessChecked = (surface.digestKey ?? surface.key).length > 0;
  const duplicateKeyCount = sumField(rows, "duplicate_key_count");
  const duplicateRows = sumField(rows, "duplicate_rows");
  const invalidRows = sumField(rows, invalidMetric);

  return {
    passes: (!uniquenessChecked || (
      duplicateKeyCount === "0" && duplicateRows === "0"
    ))
      && invalidRows === "0",
    uniqueness_checked_by_digest: uniquenessChecked,
    duplicate_key_count: duplicateKeyCount,
    duplicate_key_rows: duplicateRows,
    invalid_key_rows: invalidRows,
  };
}

function normalizeGenerationRecord(row) {
  return Object.fromEntries(
    Object.entries(row).map(([name, value]) => [name, String(value ?? "")]),
  );
}

function groupByBatch(rows) {
  const groups = new Map();

  for (const row of rows) {
    const key = `${row.batch_version}:${row.batch_id}`;
    const group = groups.get(key) ?? [];
    group.push(row);
    groups.set(key, group);
  }

  return groups;
}

function distinctSignatureCount(rows, fields) {
  return new Set(
    rows.map((row) => fields.map((field) => row[field]).join("\u001f")),
  ).size;
}

function manifestSignatureFields() {
  return [
    "tenant_id",
    "batch_version",
    "batch_id",
    "row_hash",
    "is_deleted",
    "producer_id",
    "checkpoint_sequence",
    "input_event_count",
    "input_hash",
    "checkpoint_ingested_at",
    "checkpoint_event_id",
    "output_row_count",
    "output_hash",
  ];
}

function compareBatchVersions(left, right) {
  const leftVersion = BigInt(left.batch_version);
  const rightVersion = BigInt(right.batch_version);
  if (leftVersion < rightVersion) return -1;
  if (leftVersion > rightVersion) return 1;
  return left.batch_id.localeCompare(right.batch_id);
}

function manifestRowHash(manifest) {
  return sha256(
    `${manifest.batch_id}:${manifest.input_hash}:${manifest.output_hash}`,
  );
}

function validateManifestSequence(
  manifests,
  predecessor,
  generationStartVersion,
  generationId,
  errors,
) {
  if (manifests.length === 0) return;

  if (predecessor && generationStartVersion !== null) {
    const firstVersion = BigInt(manifests[0].batch_version);
    const predecessorVersion = BigInt(predecessor.batch_version);
    const expectedFirstVersion = maxBigInt(
      predecessorVersion + 1n,
      generationStartVersion,
    );

    if (predecessor.tenant_id !== "boom") {
      errors.push("predecessor activation has the wrong tenant");
    }
    if (predecessor.batch_id.startsWith(`${generationId}_identity_`)) {
      errors.push("predecessor activation belongs to the requested generation");
    }
    if (firstVersion !== expectedFirstVersion) {
      errors.push(
        `first identity manifest version ${firstVersion} does not equal anchored version ${expectedFirstVersion}`,
      );
    }
  }

  for (let index = 1; index < manifests.length; index += 1) {
    const previous = BigInt(manifests[index - 1].batch_version);
    const current = BigInt(manifests[index].batch_version);
    if (current !== previous + 1n) {
      errors.push(
        `identity manifest versions are not consecutive at ${manifests[index].batch_version}`,
      );
    }
  }

  for (let index = 0; index < manifests.length - 1; index += 1) {
    if (manifests[index].input_event_count === "0") {
      errors.push(`manifest ${manifests[index].batch_id} is empty before the terminal batch`);
    }
  }
}

function validateTerminalManifest(
  terminal,
  activationGroups,
  latestActive,
  errors,
) {
  const batchKey = `${terminal.batch_version}:${terminal.batch_id}`;

  if (terminal.input_event_count !== "0") {
    errors.push("generation does not end in an empty manifest");
  }
  if (terminal.input_hash !== emptySha256) {
    errors.push("terminal empty manifest has the wrong input hash");
  }
  if (
    terminal.output_row_count !== "0"
    || terminal.output_hash !== emptySha256
    || terminal.actual.output_row_count !== "0"
    || terminal.actual.output_hash !== emptySha256
  ) {
    errors.push("terminal empty manifest has nonempty or mismatched output");
  }
  if ((activationGroups.get(batchKey) ?? []).length > 0) {
    errors.push("terminal empty manifest must not be activated");
  }
  if (!latestActive) {
    errors.push("latest active identity cursor is missing");
    return;
  }
  if (
    terminal.checkpoint_ingested_at !== latestActive.checkpoint_ingested_at
    || terminal.checkpoint_event_id !== latestActive.checkpoint_event_id
  ) {
    errors.push("terminal empty cursor does not equal the latest active cursor");
  }
}

function validateManifestActivation(manifest, activationGroups, errors) {
  const batchKey = `${manifest.batch_version}:${manifest.batch_id}`;
  const group = activationGroups.get(batchKey) ?? [];

  if (group.length === 0) {
    errors.push(`nonempty manifest ${batchKey} has no activation`);
    return;
  }
  if (distinctSignatureCount(group, manifestSignatureFields()) !== 1) {
    errors.push(`activation ${batchKey} has conflicting rows`);
    return;
  }

  const activation = group[0];
  const exactFields = [
    "tenant_id",
    "batch_version",
    "batch_id",
    "checkpoint_sequence",
    "input_event_count",
    "input_hash",
    "checkpoint_ingested_at",
    "checkpoint_event_id",
    "output_row_count",
    "output_hash",
  ];
  const mismatched = exactFields.filter((field) =>
    activation[field] !== manifest[field]
  );

  if (mismatched.length > 0) {
    errors.push(
      `activation ${batchKey} differs from its manifest: ${mismatched.join(", ")}`,
    );
  }
  if (
    activation.row_hash !== manifest.output_hash
    || activation.is_deleted !== "0"
    || activation.producer_id !== "identity_compactor"
  ) {
    errors.push(`activation ${batchKey} has invalid publication metadata`);
  }
}

function summarizeManifest(manifest) {
  return {
    batch_version: manifest.batch_version,
    batch_id: manifest.batch_id,
    checkpoint_sequence: manifest.checkpoint_sequence,
    input_event_count: manifest.input_event_count,
    input_hash: manifest.input_hash,
    checkpoint_ingested_at: manifest.checkpoint_ingested_at,
    checkpoint_event_id: manifest.checkpoint_event_id,
    expected_output_row_count: manifest.output_row_count,
    expected_output_hash: manifest.output_hash,
    actual_output_row_count: manifest.actual.output_row_count,
    actual_output_hash: manifest.actual.output_hash,
  };
}

function summarizePredecessor(predecessor) {
  return {
    batch_version: predecessor.batch_version,
    batch_id: predecessor.batch_id,
    checkpoint_sequence: predecessor.checkpoint_sequence,
    checkpoint_ingested_at: predecessor.checkpoint_ingested_at,
    checkpoint_event_id: predecessor.checkpoint_event_id,
  };
}

function coordinatorGenerationStartVersion(value) {
  if (typeof value !== "string") return null;

  const match = coordinatorGenerationPattern.exec(value);
  if (!match) return null;

  const compactTimestamp = match[1];
  if (compactTimestamp.slice(12) !== "00000") return null;

  const isoTimestamp = [
    compactTimestamp.slice(0, 4),
    "-",
    compactTimestamp.slice(4, 6),
    "-",
    compactTimestamp.slice(6, 8),
    "T",
    compactTimestamp.slice(8, 10),
    ":",
    compactTimestamp.slice(10, 12),
    ":",
    compactTimestamp.slice(12, 14),
    ".",
    compactTimestamp.slice(14, 17),
    "Z",
  ].join("");
  const milliseconds = Date.parse(isoTimestamp);
  if (Number.isNaN(milliseconds)) return null;
  if (new Date(milliseconds).toISOString() !== isoTimestamp) return null;

  return BigInt(milliseconds);
}

function maxBigInt(left, right) {
  return left > right ? left : right;
}

function sumField(rows, field) {
  return rows.reduce(
    (total, row) => total + BigInt(row[field] ?? "0"),
    0n,
  ).toString();
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function stringColumn(name) {
  return { name, data_type: "STRING" };
}

function stringArrayColumn(name) {
  return { name, data_type: "ARRAY<STRING>" };
}

function integerColumn(name) {
  return { name, data_type: "INT64" };
}

function timestampColumn(name) {
  return { name, data_type: "TIMESTAMP" };
}
