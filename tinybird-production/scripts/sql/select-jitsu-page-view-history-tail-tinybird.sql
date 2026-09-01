SELECT * EXCEPT (
  tenant_id,
  source_priority,
  source_deleted,
  source_fact_version,
  source_ingested_at,
  payload_hash,
  source_version,
  __tb_state_key,
  current_version_rank
)
FROM (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY tenant_id, page_view_id
      ORDER BY
        source_priority DESC,
        source_fact_version DESC,
        source_ingested_at DESC,
        payload_hash DESC
    ) AS current_version_rank
  FROM jitsu_page_view_versions
  WHERE source_system = 'jitsu_data'
    AND source_version >= toDateTime64('2026-08-26 00:00:00', 6, 'UTC')
    AND source_version < toDateTime64('2026-08-28 00:53:00', 6, 'UTC')
    AND farmFingerprint64(ifNull(page_view_id, '')) % 16 = 0
)
WHERE current_version_rank = 1 AND source_deleted = 0
