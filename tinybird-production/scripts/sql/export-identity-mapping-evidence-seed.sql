EXPORT DATA OPTIONS(
  uri='__GCS_URI__',
  format='PARQUET',
  overwrite=true
) AS
WITH
identity_events AS (
  SELECT *
  FROM `able-folio-499722.booming_data_analytics.int_identity_events`
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

normalized_events AS (
  SELECT
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
    END AS phone
  FROM national_phones
),

identifier_observations AS (
  SELECT
    identifier_key,
    observed_at
  FROM normalized_events
  CROSS JOIN UNNEST([
    IF(anonymous_id IS NULL, NULL, CONCAT('anonymous_id:', anonymous_id)),
    IF(user_id IS NULL, NULL, CONCAT('user_id:', user_id)),
    IF(email IS NULL, NULL, CONCAT('email:', email)),
    IF(canonical_email IS NULL, NULL, CONCAT('canonical_email:', canonical_email)),
    IF(phone IS NULL, NULL, CONCAT('phone:', phone))
  ]) AS identifier_key
  WHERE identifier_key IS NOT NULL
)

SELECT
  identifier_key,
  MIN(observed_at) AS first_seen_at,
  MAX(observed_at) AS last_seen_at
FROM identifier_observations
GROUP BY identifier_key
