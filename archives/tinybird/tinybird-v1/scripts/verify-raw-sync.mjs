import { readFile, writeFile } from 'node:fs/promises';
import { query } from './tinybird.mjs';

const root = new URL('../', import.meta.url);
const metadata = JSON.parse(await readFile(new URL('restore/fivetran-change-history/plan.json', root), 'utf8'));
const names = metadata.tables.map(table => table.name).sort();
const { ADMIN_TOKEN } = JSON.parse(await readFile(new URL('.dev.vars.transport.json', root), 'utf8'));
const response = await fetch('https://bigquery-tinybird-sync.bill-3e3.workers.dev/admin/source-export/status', {
  headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
  signal: AbortSignal.timeout(20_000), redirect: 'error',
});
if (!response.ok) throw new Error(`Raw sync status failed with HTTP ${response.status}`);
const status = await response.json();

const receipts = await query(`
  SELECT receipt_key,
    argMax(toString(window_started_at), verified_at) AS window_start,
    argMax(toString(complete_through_exclusive), verified_at) AS window_end,
    argMax(source_count, verified_at) AS sources,
    argMax(manifest_json, verified_at) AS manifest,
    uniqExact(tuple(window_started_at, complete_through_exclusive, source_count, manifest_json)) AS variants
  FROM v1_fivetran_source_receipts
  WHERE tenant_id = 'boom'
  GROUP BY receipt_key
  ORDER BY window_end DESC
  LIMIT 3`);

const windows = [];
for (const receipt of receipts.data) {
  if (Number(receipt.variants) !== 1 || Number(receipt.sources) !== 9) {
    throw new Error('Raw sync receipt is incomplete or conflicting');
  }
  const manifest = JSON.parse(receipt.manifest);
  const actualNames = manifest.map(row => row.source_name).sort();
  if (JSON.stringify(actualNames) !== JSON.stringify(names)) throw new Error('Raw sync receipt covers the wrong sources');
  const timestamp = receipt.window_end;
  if (!/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{1,6})?$/.test(timestamp)) {
    throw new Error('Unexpected raw sync cutoff');
  }
  const countQueries = manifest.map(row => {
    const table = row.source_name.replace(/^raw_/, 'v1_fivetran_');
    return `SELECT '${row.source_name}' AS source, count() AS rows, uniqExact(id) AS ids,
      countIf(_v1_deleted) AS deleted FROM ${table}
      WHERE _v1_observed_at = toDateTime64('${timestamp}', 6)`;
  });
  const counts = (await query(countQueries.join('\nUNION ALL\n'))).data;
  for (const row of counts) {
    const expected = manifest.find(source => source.source_name === row.source);
    if (Number(row.rows) !== Number(expected.row_count) || Number(row.ids) !== Number(row.rows)) {
      throw new Error(`Imported raw rows differ from the verified export for ${row.source}`);
    }
  }
  windows.push({ from: receipt.window_start, through: timestamp, sources: 9,
    files: manifest.reduce((total, row) => total + Number(row.file_count), 0),
    rows: counts.reduce((total, row) => total + Number(row.rows), 0), counts });
}
for (let index = 0; index + 1 < windows.length; index++) {
  if (windows[index].from !== windows[index + 1].through) {
    throw new Error('Verified raw sync windows contain a gap');
  }
}
const completedAt = Date.parse(status.completedThrough);
if (!Number.isFinite(completedAt)) throw new Error('Invalid raw sync position');
const evidence = { checkedAt: new Date().toISOString(), status,
  lagSeconds: Math.max(0, Math.round((Date.now() - completedAt) / 1000)), verifiedWindows: windows };
await writeFile(new URL('evidence/cutover/raw-sync-progress.json', root), JSON.stringify(evidence, null, 2) + '\n');
console.log(JSON.stringify({ ...evidence, verifiedWindows: windows.map(({ counts, ...window }) => window) }));
if (status.phase === 'attention') throw new Error('Raw sync requires attention');
