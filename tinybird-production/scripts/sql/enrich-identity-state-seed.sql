CREATE EXTERNAL TABLE IF NOT EXISTS
  `able-folio-499722.booming_data_analytics.identity_state_seed_enrichment_source_20260828_0540`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://booming-data/tinybird/migration_seed/identity_state/snapshot_at=20260826230500/*.parquet'
  ],
  expiration_timestamp = TIMESTAMP '2026-08-28 07:00:00 UTC'
);

CREATE EXTERNAL TABLE IF NOT EXISTS
  `able-folio-499722.booming_data_analytics.identity_mapping_evidence_enrichment_source_20260828_0540`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://booming-data/tinybird/migration_seed/identity_mapping_evidence/snapshot_at=20260826230500/*.parquet'
  ],
  expiration_timestamp = TIMESTAMP '2026-08-28 07:00:00 UTC'
);

EXPORT DATA OPTIONS (
  uri = 'gs://booming-data/tinybird/migration_seed/identity_state_enriched/snapshot_at=20260826230500/part-*.parquet',
  format = 'PARQUET',
  overwrite = true
) AS
SELECT
  seed.* REPLACE (
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.member_identifier_keys.list) AS item
    ) AS member_identifier_keys,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.anonymous_ids.list) AS item
    ) AS anonymous_ids,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.user_ids.list) AS item
    ) AS user_ids,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.emails.list) AS item
    ) AS emails,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.phones.list) AS item
    ) AS phones,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.historical_profile_ids.list) AS item
    ) AS historical_profile_ids,
    IF(
      seed.state_kind = 'mapping',
      COALESCE(evidence.first_seen_at, seed.first_seen_at),
      seed.first_seen_at
    ) AS first_seen_at,
    IF(
      seed.state_kind = 'mapping',
      COALESCE(evidence.last_seen_at, seed.last_seen_at),
      seed.last_seen_at
    ) AS last_seen_at,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.evidence_keys.list) AS item
    ) AS evidence_keys,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.prior_profile_ids.list) AS item
    ) AS prior_profile_ids,
    ARRAY(
      SELECT item.element
      FROM UNNEST(seed.dirty_profile_ids.list) AS item
    ) AS dirty_profile_ids
  )
FROM
  `able-folio-499722.booming_data_analytics.identity_state_seed_enrichment_source_20260828_0540`
  AS seed
LEFT JOIN
  `able-folio-499722.booming_data_analytics.identity_mapping_evidence_enrichment_source_20260828_0540`
  AS evidence
  ON seed.state_kind = 'mapping'
  AND seed.identifier_key = evidence.identifier_key;
