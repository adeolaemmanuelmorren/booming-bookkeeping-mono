EXPORT DATA OPTIONS(
  uri='__GCS_URI__',
  format='PARQUET',
  overwrite=true
) AS
WITH
seed_metadata AS (
  SELECT
    '__SEED_ID__' AS batch_id,
    TIMESTAMP '__COMMITTED_AT__' AS committed_at
),

profiles AS (
  SELECT *
  FROM `able-folio-499722.booming_data_analytics.int_resolved_identifiers`
),

public_mappings AS (
  SELECT *
  FROM `able-folio-499722.booming_data_analytics.int_identifier_profile_lookup`
),

canonical_email_mappings AS (
  SELECT
    'canonical_email' AS identifier_type,
    CASE
      WHEN SPLIT(LOWER(TRIM(email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN
        CONCAT(
          REPLACE(
            REGEXP_REPLACE(SPLIT(LOWER(TRIM(email)), '@')[SAFE_OFFSET(0)], r'[+].*$', ''),
            '.',
            ''
          ),
          '@gmail.com'
        )
      ELSE LOWER(TRIM(email))
    END AS identifier_value,
    profile_id
  FROM profiles
  CROSS JOIN UNNEST(emails) AS email
),

all_mappings AS (
  SELECT identifier_type, identifier_value, profile_id
  FROM public_mappings

  UNION DISTINCT

  SELECT identifier_type, identifier_value, profile_id
  FROM canonical_email_mappings
),

profile_members AS (
  SELECT
    profiles.*,
    CASE
      WHEN LOWER(TO_HEX(MD5(CONCAT('email:', profile_key)))) = profile_id THEN 'email'
      WHEN LOWER(TO_HEX(MD5(CONCAT('canonical_email:', profile_key)))) = profile_id THEN 'canonical_email'
      WHEN LOWER(TO_HEX(MD5(CONCAT('user_id:', profile_key)))) = profile_id THEN 'user_id'
      WHEN LOWER(TO_HEX(MD5(CONCAT('anonymous_id:', profile_key)))) = profile_id THEN 'anonymous_id'
      WHEN LOWER(TO_HEX(MD5(CONCAT('phone:', profile_key)))) = profile_id THEN 'phone'
      ELSE 'unknown'
    END AS winner_identifier_type,
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
          SELECT CONCAT(
            'canonical_email:',
            CASE
              WHEN SPLIT(LOWER(TRIM(value)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN
                CONCAT(
                  REPLACE(
                    REGEXP_REPLACE(SPLIT(LOWER(TRIM(value)), '@')[SAFE_OFFSET(0)], r'[+].*$', ''),
                    '.',
                    ''
                  ),
                  '@gmail.com'
                )
              ELSE LOWER(TRIM(value))
            END
          )
          FROM UNNEST(emails) AS value
        ),
        ARRAY(
          SELECT CONCAT('phone:', value)
          FROM UNNEST(phones) AS value
        )
      )) AS identifier_key
      ORDER BY identifier_key
    ) AS member_identifier_keys
  FROM profiles
),

identity_events AS (
  SELECT *
  FROM `able-folio-499722.booming_data_analytics.int_identity_events`
),

fact_phone_digits AS (
  SELECT
    events.*,
    REGEXP_REPLACE(TRIM(CAST(phone AS STRING)), r'[^0-9]', '') AS phone_digits
  FROM identity_events AS events
),

fact_national_phones AS (
  SELECT
    events.*,
    CASE
      WHEN LENGTH(phone_digits) = 10 THEN phone_digits
      WHEN LENGTH(phone_digits) = 11 AND STARTS_WITH(phone_digits, '1') THEN SUBSTR(phone_digits, 2)
      ELSE CAST(NULL AS STRING)
    END AS national_phone
  FROM fact_phone_digits AS events
),

normalized_facts AS (
  SELECT
    source_system,
    source_record_id,
    COALESCE(observed_at, TIMESTAMP '1970-01-01 00:00:00+00') AS observed_at,
    NULLIF(TRIM(anonymous_id), '') AS anonymous_id,
    NULLIF(LOWER(TRIM(user_id)), '') AS user_id,
    NULLIF(LOWER(TRIM(email)), '') AS email,
    CASE
      WHEN NULLIF(LOWER(TRIM(email)), '') IS NULL THEN CAST(NULL AS STRING)
      WHEN SPLIT(LOWER(TRIM(email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN
        CONCAT(
          REPLACE(
            REGEXP_REPLACE(SPLIT(LOWER(TRIM(email)), '@')[SAFE_OFFSET(0)], r'[+].*$', ''),
            '.',
            ''
          ),
          '@gmail.com'
        )
      ELSE LOWER(TRIM(email))
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
  FROM fact_national_phones
),

fact_payloads AS (
  SELECT
    facts.*,
    CONCAT(
      BYTE_LENGTH(COALESCE(source_system, '')), ':', COALESCE(source_system, ''),
      BYTE_LENGTH(COALESCE(source_record_id, '')), ':', COALESCE(source_record_id, ''),
      BYTE_LENGTH(CAST(UNIX_MICROS(observed_at) AS STRING)), ':', CAST(UNIX_MICROS(observed_at) AS STRING),
      BYTE_LENGTH(COALESCE(anonymous_id, '')), ':', COALESCE(anonymous_id, ''),
      BYTE_LENGTH(COALESCE(user_id, '')), ':', COALESCE(user_id, ''),
      BYTE_LENGTH(COALESCE(email, '')), ':', COALESCE(email, ''),
      BYTE_LENGTH(COALESCE(phone, '')), ':', COALESCE(phone, ''),
      BYTE_LENGTH(COALESCE(first_name, '')), ':', COALESCE(first_name, ''),
      BYTE_LENGTH(COALESCE(last_name, '')), ':', COALESCE(last_name, '')
    ) AS canonical_payload,
    TO_JSON_STRING(STRUCT(
      source_system,
      source_record_id,
      observed_at,
      anonymous_id,
      user_id,
      email,
      phone,
      first_name,
      last_name
    )) AS fact_payload,
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
  FROM normalized_facts AS facts
),

mapping_observations AS (
  SELECT
    identifier_key,
    MIN(facts.observed_at) AS first_seen_at,
    MAX(facts.observed_at) AS last_seen_at
  FROM fact_payloads AS facts
  CROSS JOIN UNNEST(facts.evidence_keys) AS identifier_key
  GROUP BY identifier_key
),

profile_state_rows AS (
  SELECT
    'boom' AS tenant_id,
    'profile' AS state_kind,
    CONCAT('profile:', CAST(BYTE_LENGTH(profile_id) AS STRING), ':', profile_id) AS state_key,
    CAST(1 AS INT64) AS batch_version,
    metadata.batch_id,
    metadata.committed_at,
    CAST(0 AS INT64) AS is_deleted,
    LOWER(TO_HEX(SHA256(CONCAT(
      'profile:',
      profile_id,
      ':',
      LOWER(TO_HEX(SHA256(TO_JSON_STRING(member_identifier_keys))))
    )))) AS row_hash,
    '' AS identifier_type,
    '' AS identifier_value,
    '' AS identifier_key,
    profile_id,
    profile_key,
    CONCAT(winner_identifier_type, ':', profile_key) AS winner_identifier_key,
    member_identifier_keys,
    anonymous_ids,
    user_ids,
    emails,
    phones,
    ARRAY<STRING>[] AS historical_profile_ids,
    COALESCE(first_name, '') AS first_name,
    COALESCE(last_name, '') AS last_name,
    COALESCE(first_seen_at, TIMESTAMP '1970-01-01 00:00:00+00') AS first_seen_at,
    COALESCE(last_seen_at, TIMESTAMP '1970-01-01 00:00:00+00') AS last_seen_at,
    '' AS fact_kind,
    '' AS fact_key,
    CAST(0 AS INT64) AS source_fact_version,
    CAST(0 AS INT64) AS fact_deleted,
    TIMESTAMP '1970-01-01 00:00:00+00' AS fact_observed_at,
    REPEAT('0', 64) AS fact_payload_hash,
    '' AS fact_payload,
    ARRAY<STRING>[] AS evidence_keys,
    '' AS producer_id,
    CAST(0 AS INT64) AS checkpoint_sequence,
    ARRAY<STRING>[] AS prior_profile_ids,
    ARRAY<STRING>[] AS dirty_profile_ids,
    CAST(0 AS INT64) AS input_event_count,
    REPEAT('0', 64) AS input_hash
  FROM profile_members
  CROSS JOIN seed_metadata AS metadata
),

mapping_state_rows AS (
  SELECT
    'boom' AS tenant_id,
    'mapping' AS state_kind,
    CONCAT(
      'mapping:',
      CAST(BYTE_LENGTH(CONCAT(identifier_type, ':', identifier_value)) AS STRING),
      ':',
      identifier_type,
      ':',
      identifier_value
    ) AS state_key,
    CAST(1 AS INT64) AS batch_version,
    metadata.batch_id,
    metadata.committed_at,
    CAST(0 AS INT64) AS is_deleted,
    LOWER(TO_HEX(SHA256(CONCAT(
      'mapping:', identifier_type, ':', identifier_value, ':', profile_id
    )))) AS row_hash,
    mappings.identifier_type,
    mappings.identifier_value,
    CONCAT(mappings.identifier_type, ':', mappings.identifier_value) AS identifier_key,
    mappings.profile_id,
    '' AS profile_key,
    '' AS winner_identifier_key,
    ARRAY<STRING>[] AS member_identifier_keys,
    ARRAY<STRING>[] AS anonymous_ids,
    ARRAY<STRING>[] AS user_ids,
    ARRAY<STRING>[] AS emails,
    ARRAY<STRING>[] AS phones,
    ARRAY<STRING>[] AS historical_profile_ids,
    '' AS first_name,
    '' AS last_name,
    COALESCE(observations.first_seen_at, TIMESTAMP '1970-01-01 00:00:00+00') AS first_seen_at,
    COALESCE(observations.last_seen_at, TIMESTAMP '1970-01-01 00:00:00+00') AS last_seen_at,
    '' AS fact_kind,
    '' AS fact_key,
    CAST(0 AS INT64) AS source_fact_version,
    CAST(0 AS INT64) AS fact_deleted,
    TIMESTAMP '1970-01-01 00:00:00+00' AS fact_observed_at,
    REPEAT('0', 64) AS fact_payload_hash,
    '' AS fact_payload,
    ARRAY<STRING>[] AS evidence_keys,
    '' AS producer_id,
    CAST(0 AS INT64) AS checkpoint_sequence,
    ARRAY<STRING>[] AS prior_profile_ids,
    ARRAY<STRING>[] AS dirty_profile_ids,
    CAST(0 AS INT64) AS input_event_count,
    REPEAT('0', 64) AS input_hash
  FROM all_mappings AS mappings
  LEFT JOIN mapping_observations AS observations
    ON observations.identifier_key = CONCAT(
      mappings.identifier_type,
      ':',
      mappings.identifier_value
    )
  CROSS JOIN seed_metadata AS metadata
),

fact_state_rows AS (
  SELECT
    'boom' AS tenant_id,
    'fact' AS state_kind,
    CONCAT(
      'fact:',
      CAST(BYTE_LENGTH(source_system) AS STRING),
      ':',
      source_system,
      ':',
      CAST(BYTE_LENGTH(source_record_id) AS STRING),
      ':',
      source_record_id
    ) AS state_key,
    CAST(1 AS INT64) AS batch_version,
    metadata.batch_id,
    metadata.committed_at,
    CAST(0 AS INT64) AS is_deleted,
    LOWER(TO_HEX(SHA256(CONCAT(
      'fact:', source_system, ':', source_record_id, ':', canonical_payload
    )))) AS row_hash,
    '' AS identifier_type,
    '' AS identifier_value,
    '' AS identifier_key,
    '' AS profile_id,
    '' AS profile_key,
    '' AS winner_identifier_key,
    ARRAY<STRING>[] AS member_identifier_keys,
    ARRAY<STRING>[] AS anonymous_ids,
    ARRAY<STRING>[] AS user_ids,
    ARRAY<STRING>[] AS emails,
    ARRAY<STRING>[] AS phones,
    ARRAY<STRING>[] AS historical_profile_ids,
    '' AS first_name,
    '' AS last_name,
    TIMESTAMP '1970-01-01 00:00:00+00' AS first_seen_at,
    TIMESTAMP '1970-01-01 00:00:00+00' AS last_seen_at,
    'identity_observation' AS fact_kind,
    CONCAT(
      CAST(BYTE_LENGTH(source_system) AS STRING),
      ':',
      source_system,
      ':',
      CAST(BYTE_LENGTH(source_record_id) AS STRING),
      ':',
      source_record_id
    ) AS fact_key,
    UNIX_MICROS(observed_at) AS source_fact_version,
    CAST(0 AS INT64) AS fact_deleted,
    observed_at AS fact_observed_at,
    LOWER(TO_HEX(SHA256(canonical_payload))) AS fact_payload_hash,
    fact_payload,
    evidence_keys,
    CONCAT('bigquery_identity_seed:', source_system) AS producer_id,
    CAST(0 AS INT64) AS checkpoint_sequence,
    ARRAY<STRING>[] AS prior_profile_ids,
    ARRAY<STRING>[] AS dirty_profile_ids,
    CAST(0 AS INT64) AS input_event_count,
    REPEAT('0', 64) AS input_hash
  FROM fact_payloads
  CROSS JOIN seed_metadata AS metadata
),

audit_state_rows AS (
  SELECT
    'boom' AS tenant_id,
    state_kind,
    CONCAT(state_kind, ':', metadata.batch_id) AS state_key,
    CAST(1 AS INT64) AS batch_version,
    metadata.batch_id,
    metadata.committed_at,
    CAST(0 AS INT64) AS is_deleted,
    LOWER(TO_HEX(SHA256(CONCAT(state_kind, ':', metadata.batch_id)))) AS row_hash,
    '' AS identifier_type,
    '' AS identifier_value,
    '' AS identifier_key,
    '' AS profile_id,
    '' AS profile_key,
    '' AS winner_identifier_key,
    ARRAY<STRING>[] AS member_identifier_keys,
    ARRAY<STRING>[] AS anonymous_ids,
    ARRAY<STRING>[] AS user_ids,
    ARRAY<STRING>[] AS emails,
    ARRAY<STRING>[] AS phones,
    ARRAY<STRING>[] AS historical_profile_ids,
    '' AS first_name,
    '' AS last_name,
    TIMESTAMP '1970-01-01 00:00:00+00' AS first_seen_at,
    TIMESTAMP '1970-01-01 00:00:00+00' AS last_seen_at,
    '' AS fact_kind,
    '' AS fact_key,
    CAST(0 AS INT64) AS source_fact_version,
    CAST(0 AS INT64) AS fact_deleted,
    TIMESTAMP '1970-01-01 00:00:00+00' AS fact_observed_at,
    REPEAT('0', 64) AS fact_payload_hash,
    '' AS fact_payload,
    ARRAY<STRING>[] AS evidence_keys,
    'bigquery_identity_seed' AS producer_id,
    CAST(0 AS INT64) AS checkpoint_sequence,
    ARRAY<STRING>[] AS prior_profile_ids,
    ARRAY<STRING>[] AS dirty_profile_ids,
    CAST((SELECT COUNT(*) FROM fact_payloads) AS INT64) AS input_event_count,
    LOWER(TO_HEX(SHA256(metadata.batch_id))) AS input_hash
  FROM UNNEST(['batch_audit', 'activation_audit']) AS state_kind
  CROSS JOIN seed_metadata AS metadata
)

SELECT * FROM profile_state_rows
UNION ALL
SELECT * FROM mapping_state_rows
UNION ALL
SELECT * FROM fact_state_rows
UNION ALL
SELECT * FROM audit_state_rows
