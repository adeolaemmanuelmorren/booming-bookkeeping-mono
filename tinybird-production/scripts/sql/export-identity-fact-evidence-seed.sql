EXPORT DATA OPTIONS(
  uri='__GCS_URI__',
  format='PARQUET',
  overwrite=true
) AS
WITH
identity_events AS (
  SELECT *
  FROM `able-folio-499722.booming_data_analytics.int_identity_events`
  FOR SYSTEM_TIME AS OF TIMESTAMP '__COMMITTED_AT__'
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

normalized_facts AS (
  SELECT
    source_system,
    source_record_id,
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
    END AS phone
  FROM national_phones
),

fact_evidence AS (
  SELECT
    CONCAT(identifier_type, ':', identifier_value) AS identifier_key,
    CONCAT(
      CAST(BYTE_LENGTH(source_system) AS STRING),
      ':',
      source_system,
      ':',
      CAST(BYTE_LENGTH(source_record_id) AS STRING),
      ':',
      source_record_id
    ) AS fact_key
  FROM normalized_facts
  CROSS JOIN UNNEST([
    STRUCT('anonymous_id' AS identifier_type, anonymous_id AS identifier_value),
    STRUCT('user_id' AS identifier_type, user_id AS identifier_value),
    STRUCT('email' AS identifier_type, email AS identifier_value),
    STRUCT('canonical_email' AS identifier_type, canonical_email AS identifier_value),
    STRUCT('phone' AS identifier_type, phone AS identifier_value)
  ]) AS identifier
  WHERE identifier_value IS NOT NULL
)

SELECT
  'boom' AS tenant_id,
  identifier_key,
  fact_key,
  CAST(1 AS INT64) AS base_batch_version,
  TIMESTAMP '__COMMITTED_AT__' AS committed_at,
  LOWER(TO_HEX(SHA256(CONCAT(identifier_key, ':', fact_key)))) AS row_hash
FROM fact_evidence
GROUP BY identifier_key, fact_key
