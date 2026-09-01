CREATE OR REPLACE EXTERNAL TABLE
  `able-folio-499722.booming_data_analytics.identity_mapping_serving_seed_source_20260830`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://booming-data/tinybird/migration_seed/identity_state_enriched/snapshot_at=20260826230500/*.parquet'
  ],
  expiration_timestamp = TIMESTAMP '2026-09-01 00:00:00 UTC'
);

EXPORT DATA OPTIONS (
  uri = 'gs://booming-data/tinybird/migration_seed/identity_mapping_serving/snapshot_at=20260826230500/part-*.parquet',
  format = 'PARQUET',
  overwrite = true
) AS
SELECT
  tenant_id,
  identifier_key,
  profile_id
FROM
  `able-folio-499722.booming_data_analytics.identity_mapping_serving_seed_source_20260830`
WHERE state_kind = 'mapping' AND is_deleted = 0;
