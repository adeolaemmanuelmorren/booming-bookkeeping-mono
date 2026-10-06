SELECT source, source_account, record_kind, count() AS rows,
       min(JSONExtractString(payload_json,'occurred_at')) AS first_conversion,
       max(JSONExtractString(payload_json,'occurred_at')) AS last_conversion,
       countIf(JSONExtractString(payload_json,'email') != '') AS rows_with_email,
       countIf(JSONExtractBool(payload_json,'is_deleted')) AS deleted_rows
FROM v1_source_records WHERE tenant_id='boom' AND record_kind='conversion'
GROUP BY source, source_account, record_kind
SETTINGS max_execution_time=20, max_memory_usage=536870912, max_threads=2
