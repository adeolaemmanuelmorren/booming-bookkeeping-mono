-- One-time immutable baseline for the current historical page-view contract.
-- Future Jitsu observations continue through the Events API version pipeline.
EXPORT DATA OPTIONS (
  uri = 'gs://booming-data/tinybird/migration_seed/jitsu_page_view_versions/snapshot_at=20260828005300/part-*.parquet',
  format = 'PARQUET',
  overwrite = true
) AS
WITH raw_versions AS (
  SELECT
    'boom_domains' AS source_system,
    id AS page_view_id,
    MAX(
      COALESCE(
        loaded_at,
        received_at,
        sent_at,
        timestamp,
        TIMESTAMP '1970-01-01 00:00:00+00'
      )
    ) AS source_version
  FROM `able-folio-499722.boom_domains.pages`
  GROUP BY id

  UNION ALL

  SELECT
    'jitsu_data' AS source_system,
    message_id AS page_view_id,
    MAX(
      COALESCE(
        received_at,
        sent_at,
        timestamp,
        TIMESTAMP '1970-01-01 00:00:00+00'
      )
    ) AS source_version
  FROM `able-folio-499722.jitsu_data.pages`
  GROUP BY message_id
)
SELECT
  'boom' AS tenant_id,
  page_views.source_system,
  IF(page_views.source_system = 'jitsu_data', 2, 1) AS source_priority,
  0 AS source_deleted,
  GREATEST(UNIX_MICROS(versions.source_version), 0) + 1 AS source_fact_version,
  versions.source_version AS source_ingested_at,
  TO_HEX(
    SHA256(
      CONCAT(
        page_views.source_system,
        '|',
        COALESCE(page_views.page_view_id, ''),
        '|',
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', versions.source_version, 'UTC')
      )
    )
  ) AS payload_hash,
  page_views.* EXCEPT (source_system)
    REPLACE (
      CAST(page_views.is_identified_meta_ad_click AS INT64)
        AS is_identified_meta_ad_click
    ),
  versions.source_version AS source_version,
  COALESCE(page_views.page_view_id, '') AS __tb_state_key
FROM `able-folio-499722.booming_data_analytics.stg_page_views` AS page_views
JOIN raw_versions AS versions
  USING (source_system, page_view_id)
WHERE versions.source_version < TIMESTAMP '2026-08-28 00:53:00+00'
