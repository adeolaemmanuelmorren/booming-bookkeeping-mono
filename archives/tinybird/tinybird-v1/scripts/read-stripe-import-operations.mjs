import { readFile, writeFile } from 'node:fs/promises';
import { query } from './tinybird.mjs';

const directory = new URL('../evidence/stripe-history/', import.meta.url);
const manifest = JSON.parse(await readFile(new URL('manifest.json', directory), 'utf8'));
const sources = Object.keys(manifest.sources);
if (!sources.every(name => /^raw_stripe_[a-z_]+$/.test(name))) throw new Error('Unexpected source name');
const predicate = sources.map(name => `'${name}'`).join(',');
const common = `WITH arrayElement(Options.Values, indexOf(Options.Names, 'source')) AS raw_source`;
const sql = `${common}, arrayElement(splitByChar('?', raw_source), 1) AS source_path
  SELECT datasource_name, source_path,
    min(timestamp) AS first_operation, max(timestamp) AS last_operation,
    count() AS operations, sum(rows) AS rows, sum(rows_quarantine) AS quarantine_rows
  FROM tinybird.datasources_ops_log
  WHERE datasource_name IN (${predicate}) AND event_type = 'append' AND result = 'ok'
    AND position(raw_source, 'storage.googleapis.com') > 0
  GROUP BY datasource_name, source_path ORDER BY datasource_name, source_path`;
const response = await query(sql);
await writeFile(new URL('file-operations.json', directory), JSON.stringify({ checked_at: new Date().toISOString(), sql, data: response.data }, null, 2) + '\n', { mode: 0o600 });
const summarySql = `${common}, multiIf(startsWith(raw_source, 'gs://'), 'gcs',
  position(raw_source, 'storage.googleapis.com') > 0, 'gcs_http',
  raw_source = 'body', 'body', raw_source = 'stream', 'stream', raw_source = '', 'unspecified', 'other') AS source_kind
  SELECT datasource_name, event_type, result, source_kind, min(timestamp) AS first_operation,
    max(timestamp) AS last_operation, count() AS operations, sum(rows) AS affected_rows,
    sum(written_rows) AS written_rows, sum(rows_quarantine) AS quarantine_rows
  FROM tinybird.datasources_ops_log WHERE datasource_name IN (${predicate})
  GROUP BY datasource_name, event_type, result, source_kind
  ORDER BY datasource_name, event_type, result, source_kind`;
const summary = await query(summarySql);
await writeFile(new URL('operations-summary.json', directory), JSON.stringify({ checked_at: new Date().toISOString(), sql: summarySql, data: summary.data }, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify({ sources: sources.length, files: response.data.length, summary_rows: summary.data.length }));
