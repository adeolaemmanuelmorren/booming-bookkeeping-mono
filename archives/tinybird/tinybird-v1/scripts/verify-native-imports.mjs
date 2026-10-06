import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { query, tinybirdRequest } from './tinybird.mjs';
import { readVerifiedSupplement, SUPPLEMENT_TABLE } from './restore-activecampaign-supplement.mjs';

const root = new URL('../', import.meta.url);
const readJson = async path => JSON.parse(await readFile(new URL(path, root), 'utf8'));

// These manifests contain object metadata and counts, never customer payloads.
const history = await readJson('restore/history-import-plan.json');
const snapshot = await readJson('restore/fivetran-snapshot/manifest.json');
const tables = new Map();
for (const source of Object.values(history.sources)) {
  tables.set(source.landing_table, {
    rows: source.expected_physical_rows,
    files: new Map(source.objects.map(object => [object.destination, object.rows])),
  });
}
for (const source of snapshot.tables) {
  tables.set(source.landing, {
    rows: source.rows,
    files: new Map(source.objects.map(object => [object.name, null])),
  });
}
const browser = { rows: 0, files: new Map() };
for (const name of ['live-jitsu', 'live-jitsu-tail']) {
  const manifest = await readJson(`backups/${name}/manifest.json`);
  if (!manifest.verified) throw new Error('Browser backup is not verified');
  browser.rows += manifest.rows;
  for (const object of Object.values(manifest.buckets)) {
    browser.files.set(`tinybird/v1-preserved/${name}/${object.filename}`, object.rows);
  }
}
tables.set('v1_history_live_jitsu', browser);

const workspace = await (await tinybirdRequest('/v1/workspace')).json();
if (workspace.id !== '00c04079-d0b4-4d8b-8de6-6fa8072b85af') {
  throw new Error('Import verification requires the approved main workspace');
}
const inventory = await (await tinybirdRequest('/v0/datasources')).json();
const existing = new Set(inventory.datasources.map(row => row.name));
const results = [];
const supplement = await readVerifiedSupplement();
for (const [table, expected] of tables) {
  if (!existing.has(table)) {
    results.push({ table, verified: false, missingTable: true });
    continue;
  }
  if (!/^v1_(history|snapshot)_[a-z0-9_]+$/.test(table)) {
    throw new Error('Unexpected import table');
  }
  const [count, operations] = await Promise.all([
    query(`SELECT count() AS rows FROM ${table}`),
    query(`
      WITH arrayElement(Options.Values, indexOf(Options.Names, 'source')) AS raw_source,
        arrayElement(splitByChar('?', raw_source), 1) AS source_path
      SELECT source_path, result, count() AS operations,
        sum(rows) AS rows, sum(rows_quarantine) AS quarantine_rows
      FROM tinybird.datasources_ops_log
      WHERE datasource_name = '${table}' AND event_type = 'append'
        AND source_path != ''
      GROUP BY source_path, result
      ORDER BY source_path, result`),
  ]);
  const missing = new Set(expected.files.keys());
  const problems = [];
  let acceptedRows = 0;
  let quarantineRows = 0;
  let successfulFiles = 0;
  for (const operation of operations.data) {
    const name = objectName(operation.source_path);
    const rows = Number(operation.rows);
    const quarantined = Number(operation.quarantine_rows);
    if (!name || !expected.files.has(name)) {
      problems.push({ reason: 'Unexpected import object', path: operation.source_path });
      continue;
    }
    if (operation.result !== 'ok') {
      problems.push({ reason: 'Unsuccessful file operation', name, result: operation.result });
      continue;
    }
    acceptedRows += rows;
    quarantineRows += quarantined;
    successfulFiles++;
    missing.delete(name);
    const expectedRows = expected.files.get(name);
    if (expectedRows !== null && rows !== expectedRows) {
      problems.push({ reason: 'File row count differs', name, expectedRows, rows });
    }
    if (Number(operation.operations) !== 1) {
      problems.push({ reason: 'Repeated file import', name, operations: operation.operations });
    }
  }
  const rows = Number(count.data[0].rows);
  const supplementalReceipt = table === SUPPLEMENT_TABLE ? supplement : null;
  const supplementalRows = supplementalReceipt?.rows ?? 0;
  const expectedRows = expected.rows + supplementalRows;
  const verified = !missing.size && !problems.length && !quarantineRows
    && successfulFiles === expected.files.size && acceptedRows === expected.rows && rows === expectedRows
    && (table !== SUPPLEMENT_TABLE || supplementalRows === 1);
  const result = {
    table, verified, expectedRows, expectedNativeRows: expected.rows, supplementalRows,
    ...(supplementalReceipt ? { supplementalReceipt } : {}), rows, acceptedRows, quarantineRows,
    expectedFiles: expected.files.size, successfulFiles,
    missingFiles: [...missing], problems,
  };
  results.push(result);
  process.stdout.write(JSON.stringify({ table, verified, rows, expectedRows, missingFiles: missing.size }) + '\n');
}
const verified = results.every(result => result.verified);
const evidence = { checkedAt: new Date().toISOString(), verified, snapshotAt: snapshot.snapshot_at, tables: results };
const bytes = JSON.stringify(evidence, null, 2) + '\n';
const checkpoint = `restore/native-import-checks/${evidence.checkedAt.replaceAll(':', '-').replaceAll('.', '-')}.json`;
await mkdir(new URL('restore/native-import-checks/', root), { recursive: true });
await writeFile(new URL(checkpoint, root), bytes, { flag: 'wx' });
await writeFile(new URL('restore/native-import-verification.json', root), bytes);
process.stdout.write(JSON.stringify({ verified, tables: results.length, completed: results.filter(result => result.verified).length, checkpoint }) + '\n');
if (!verified) process.exitCode = 1;

function objectName(path) {
  if (path.startsWith('gs://booming-data/')) return path.slice('gs://booming-data/'.length);
  try {
    const url = new URL(path);
    if (url.hostname === 'storage.googleapis.com' && url.pathname.startsWith('/booming-data/')) {
      return decodeURIComponent(url.pathname.slice('/booming-data/'.length));
    }
    if (url.hostname === 'booming-data.storage.googleapis.com') {
      return decodeURIComponent(url.pathname.slice(1));
    }
  } catch {
    return null;
  }
  return null;
}
