import { writeFile } from 'node:fs/promises';
import { query } from './tinybird.mjs';

const sql = `
  WITH arrayElement(Options.Values, indexOf(Options.Names, 'source')) AS raw_source,
    arrayElement(splitByChar('?', raw_source), 1) AS source_path
  SELECT datasource_name, source_path,
    min(timestamp) AS first_operation, max(timestamp) AS last_operation,
    count() AS operations, sum(rows) AS rows,
    sum(rows_quarantine) AS quarantine_rows
  FROM tinybird.datasources_ops_log
  WHERE datasource_name IN ('raw_activecampaign_contact', 'raw_activecampaign_contact_tag', 'raw_activecampaign_tags')
    AND event_type = 'append' AND result = 'ok'
    AND position(raw_source, 'storage.googleapis.com') > 0
  GROUP BY datasource_name, source_path
  ORDER BY datasource_name, source_path`;

const response = await query(sql);
const evidence = { checked_at: new Date().toISOString(), sql, data: response.data };
await writeFile(new URL('../evidence/activecampaign-history/file-operations.json', import.meta.url), JSON.stringify(evidence, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify({ files: response.data.length, first_source_path: response.data[0]?.source_path }));
