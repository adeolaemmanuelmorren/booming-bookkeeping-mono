import { writeFile } from 'node:fs/promises';
import { query } from './tinybird.mjs';

const sql = `
  WITH arrayElement(Options.Values, indexOf(Options.Names, 'source')) AS raw_source,
    multiIf(startsWith(raw_source, 'gs://'), 'gcs',
      position(raw_source, 'storage.googleapis.com') > 0, 'gcs_http',
      raw_source = 'body', 'body', raw_source = 'stream', 'stream',
      raw_source = '', 'unspecified', 'other') AS source_kind
  SELECT datasource_name, event_type, result, source_kind,
    min(timestamp) AS first_operation, max(timestamp) AS last_operation,
    count() AS operations, sum(rows) AS affected_rows,
    sum(written_rows) AS written_rows, sum(rows_quarantine) AS quarantine_rows
  FROM tinybird.datasources_ops_log
  WHERE datasource_name IN ('raw_activecampaign_contact', 'raw_activecampaign_contact_tag', 'raw_activecampaign_tags')
  GROUP BY datasource_name, event_type, result, source_kind
  ORDER BY datasource_name, event_type, result, source_kind`;

let response;
try {
  response = await query(sql);
} catch (error) {
  console.error('ActiveCampaign source metadata query failed');
  process.exit(1);
}
const evidence = { checked_at: new Date().toISOString(), sql, data: response.data };
await writeFile(new URL('../evidence/activecampaign-history/operations-summary.json', import.meta.url), JSON.stringify(evidence, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify({ rows: response.data.length }));
