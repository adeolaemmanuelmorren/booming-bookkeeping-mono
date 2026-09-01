-- Completes the immutable historical page-view baseline without replacing
-- versions that Tinybird already holds. Query-time version resolution makes
-- replaying the small overlap safe.
EXPORT DATA OPTIONS (
  uri = 'gs://booming-data/tinybird/migration_seed/jitsu_page_view_versions/delta_from=20260826000000_to=20260828005300/part-*.parquet',
  format = 'PARQUET',
  overwrite = true
) AS
WITH raw_versions AS (
  SELECT
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
  2 AS source_priority,
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
  USING (page_view_id)
WHERE
  page_views.source_system = 'jitsu_data'
  AND versions.source_version >= TIMESTAMP '2026-08-26 00:00:00+00'
  AND versions.source_version < TIMESTAMP '2026-08-28 00:53:00+00'
