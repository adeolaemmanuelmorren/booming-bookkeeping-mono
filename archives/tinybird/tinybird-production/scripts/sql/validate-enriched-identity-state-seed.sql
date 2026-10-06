CREATE EXTERNAL TABLE IF NOT EXISTS
  `able-folio-499722.booming_data_analytics.identity_state_seed_enriched_validation_20260828_0605`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://booming-data/tinybird/migration_seed/identity_state_enriched/snapshot_at=20260826230500/*.parquet'
  ],
  expiration_timestamp = TIMESTAMP '2026-08-28 08:00:00 UTC'
);

WITH original_counts AS (
  SELECT state_kind, COUNT(*) AS row_count
  FROM
    `able-folio-499722.booming_data_analytics.identity_state_seed_enrichment_source_20260828_0540`
  GROUP BY state_kind
),
enriched_counts AS (
  SELECT state_kind, COUNT(*) AS row_count
  FROM
    `able-folio-499722.booming_data_analytics.identity_state_seed_enriched_validation_20260828_0605`
  GROUP BY state_kind
)
SELECT
  COALESCE(original.state_kind, enriched.state_kind) AS state_kind,
  COALESCE(original.row_count, 0) AS original_row_count,
  COALESCE(enriched.row_count, 0) AS enriched_row_count,
  COALESCE(enriched.row_count, 0) - COALESCE(original.row_count, 0) AS row_count_delta
FROM original_counts AS original
FULL OUTER JOIN enriched_counts AS enriched USING (state_kind)
ORDER BY state_kind;

SELECT
  COUNT(*) AS mapping_row_count,
  COUNTIF(
    enriched.first_seen_at
      != COALESCE(evidence.first_seen_at, original.first_seen_at)
    OR enriched.last_seen_at
      != COALESCE(evidence.last_seen_at, original.last_seen_at)
  ) AS timestamp_mismatch_count,
  COUNTIF(evidence.identifier_key IS NULL) AS missing_evidence_count,
  COUNTIF(enriched.first_seen_at = TIMESTAMP '1970-01-01 00:00:00 UTC') AS zero_first_seen_count,
  COUNTIF(enriched.last_seen_at = TIMESTAMP '1970-01-01 00:00:00 UTC') AS zero_last_seen_count
FROM
  `able-folio-499722.booming_data_analytics.identity_state_seed_enrichment_source_20260828_0540`
  AS original
INNER JOIN
  `able-folio-499722.booming_data_analytics.identity_state_seed_enriched_validation_20260828_0605`
  AS enriched
  USING (tenant_id, state_kind, state_key, batch_version, batch_id, committed_at, is_deleted, row_hash)
LEFT JOIN
  `able-folio-499722.booming_data_analytics.identity_mapping_evidence_enrichment_source_20260828_0540`
  AS evidence
  ON original.identifier_key = evidence.identifier_key
WHERE original.state_kind = 'mapping';

SELECT
  COUNTIF(original.state_key IS NULL) AS only_in_enriched_count,
  COUNTIF(enriched.state_key IS NULL) AS only_in_original_count
FROM
  `able-folio-499722.booming_data_analytics.identity_state_seed_enrichment_source_20260828_0540`
  AS original
FULL OUTER JOIN
  `able-folio-499722.booming_data_analytics.identity_state_seed_enriched_validation_20260828_0605`
  AS enriched
  USING (tenant_id, state_kind, state_key, batch_version, batch_id, committed_at, is_deleted, row_hash);
