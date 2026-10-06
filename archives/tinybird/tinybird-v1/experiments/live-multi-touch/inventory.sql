SELECT 'source_records' AS dataset, source AS kind, record_kind AS detail, count() AS rows,
       arrayStringConcat(arraySort(any(JSONExtractKeys(payload_json))), ',') AS fields,
       '' AS earliest, '' AS latest
FROM v1_source_records
WHERE tenant_id = 'boom'
GROUP BY source, record_kind
UNION ALL
SELECT 'pages', 'jitsu', '', count(), '', toString(min(timestamp)), toString(max(timestamp))
FROM v1_history_jitsu_data_pages
UNION ALL
SELECT 'forms', 'jitsu', '', count(), '', toString(min(timestamp)), toString(max(timestamp))
FROM v1_history_jitsu_data_form_submitted
UNION ALL
SELECT 'orders', 'jitsu', '', count(), '', toString(min(timestamp)), toString(max(timestamp))
FROM v1_history_jitsu_data_order_completed
UNION ALL
SELECT 'identifiers', id_type, '', count(), '', '', ''
FROM identifiers WHERE tenant_id = 'boom' GROUP BY id_type
SETTINGS max_execution_time=20, max_memory_usage=536870912, max_threads=2
