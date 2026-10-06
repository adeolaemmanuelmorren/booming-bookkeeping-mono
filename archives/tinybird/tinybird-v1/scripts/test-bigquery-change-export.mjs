// Uses only synthetic rows in an expiring validation dataset.
import { execFileSync } from 'node:child_process';
import { writeFile } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';

const PROJECT = 'able-folio-499722';
const DATASET = 'tinybird_v1_validation';
const NODE_BUILDER = process.argv[2];
if (!NODE_BUILDER) throw new Error('Pass the reviewed bigquery.ts builder path');
const { buildExportPlan, buildBridgeExportPlan, checkBigQueryExport } = await import(pathToFileURL(NODE_BUILDER));
const token = execFileSync('/Users/adeola/google-cloud-sdk/bin/gcloud',
  ['auth', 'print-access-token', '--project', PROJECT], { encoding: 'utf8' }).trim();
const stamp = new Date().toISOString().replaceAll(/[^0-9]/g, '');
const tableId = `commit_fixture_${stamp}`;
const fullTable = `\`${PROJECT}.${DATASET}.${tableId}\``;
const base = `https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT}/`;
const jobs = [];
await writeFile(new URL('../evidence/tests/bigquery-change-export-attempt.json', import.meta.url),
  JSON.stringify({ stamp, tableId, dataset: DATASET, syntheticOnly: true }, null, 2) + '\n');

async function api(path, body, allowMissing = false) {
  const response = await fetch(base + path, {
    method: body ? 'POST' : 'GET',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(30_000),
  });
  if (allowMissing && response.status === 404) return null;
  const result = await response.json();
  if (!response.ok) throw new Error(`Synthetic validation API ${response.status}: ${result.error?.message ?? 'request failed'}`);
  return result;
}

async function run(query, label) {
  const jobId = `tinybird_v1_${stamp}_${label}`;
  let job = await api('jobs', {
    jobReference: { projectId: PROJECT, location: 'US', jobId },
    configuration: { query: { query, useLegacySql: false, maximumBytesBilled: String(1024 ** 3) } },
    labels: { purpose: 'tinybird_v1_synthetic_validation' },
  });
  for (let attempt = 0; job.status?.state !== 'DONE' && attempt < 120; attempt++) {
    await new Promise(resolve => setTimeout(resolve, 500));
    job = await api(`jobs/${jobId}?location=US`);
  }
  if (job.status?.state !== 'DONE' || job.status.errorResult) {
    throw new Error(`Synthetic ${label} failed: ${job.status?.errorResult?.message ?? 'timeout'}`);
  }
  jobs.push({ label, jobId, statistics: job.statistics });
  return job;
}

async function queryRows(query, label) {
  const job = await run(query, label);
  const result = await api(`queries/${job.jobReference.jobId}?location=US`);
  assert.equal(result.jobComplete, true);
  return (result.rows ?? []).map(row => Object.fromEntries(row.f.map((field, index) => [result.schema.fields[index].name, field.v])));
}

let dataset = await api(`datasets/${DATASET}`, undefined, true);
if (!dataset) dataset = await api('datasets', {
  datasetReference: { projectId: PROJECT, datasetId: DATASET }, location: 'US',
  defaultTableExpirationMs: String(60 * 60 * 1000),
  labels: { purpose: 'tinybird_v1_synthetic_validation' },
});
assert.equal(dataset.location, 'US');

const created = await run(`CREATE TABLE ${fullTable} (id STRING, payload STRING, _fivetran_synced TIMESTAMP)
OPTIONS(enable_change_history=TRUE, expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));
INSERT INTO ${fullTable} VALUES
('updated', 'old', TIMESTAMP '2020-01-01'),
('deleted', 'old', TIMESTAMP '2020-01-01'),
('unchanged', 'same', TIMESTAMP '2020-01-01'),
('resurrected', 'old', TIMESTAMP '2020-01-01');`, 'create');
const start = new Date(Number(created.statistics.endTime));
await run(`UPDATE ${fullTable} SET payload='new', _fivetran_synced=TIMESTAMP '2019-01-01' WHERE id='updated';
DELETE FROM ${fullTable} WHERE id IN ('deleted', 'resurrected');
INSERT INTO ${fullTable} VALUES
('resurrected', 'new', TIMESTAMP '2019-01-01'),
('inserted', 'new', TIMESTAMP '2019-01-01');`, 'mutate');
const end = new Date();
const metadata = await api(`datasets/${DATASET}/tables/${tableId}`);
const sourceMetadata = { enabledAt: start, sourceCreatedAt: new Date(Number(metadata.creationTime)),
  checkedAt: end, retentionHours: 168 };
const source = {
  resourceName: `synthetic_commit_${stamp}`, exportKind: 'table',
  source: { project: PROJECT, dataset: DATASET, table: tableId },
  unionSources: [], sourcePartitioning: null,
  versionColumns: [{ name: '_fivetran_synced', bigqueryType: 'TIMESTAMP' }],
  watermarkColumns: [{ name: '_fivetran_synced', bigqueryType: 'TIMESTAMP' }],
  columns: ['id', 'payload', '_fivetran_synced'],
  columnTypes: { id: 'STRING', payload: 'STRING', _fivetran_synced: 'TIMESTAMP' },
  jsonColumns: [], geographyColumns: [],
};
const transport = { bucket: 'booming-data', prefix: `tinybird/v1-validation/${stamp}` };
const plans = [
  ['changes', buildExportPlan(source, end, {
    ...transport, overlapMinutes: (end - start) / 60_000,
    changeHistory: sourceMetadata,
  })],
  ['bridge', buildBridgeExportPlan(source, end, { ...transport, bridgeStart: start, sourceMetadata })],
];
const results = [];
for (const [label, plan] of plans) {
  const exported = await run(plan.query, label);
  const checked = await checkBigQueryExport({ ...plan, jobId: exported.jobReference.jobId }, {
    projectId: PROJECT, location: 'US', pollIntervalMs: 500, jobTimeoutMs: 30_000,
  }, token);
  assert.equal(checked.status, 'done');
  assert.equal(Number(checked.job.statistics.query.exportDataStatistics.rowCount), 4);
  const children = await api(`jobs?parentJobId=${exported.jobReference.jobId}&projection=full`);
  const exportStats = [];
  for (const child of children.jobs ?? []) {
    const full = await api(`jobs/${child.jobReference.jobId}?location=US`);
    if (full.statistics?.query?.statementType === 'EXPORT_DATA') {
      exportStats.push(full.statistics.query.exportDataStatistics);
    }
  }
  assert.equal(exportStats.length, 1);
  assert.equal(Number(exportStats[0].rowCount), 4);
  assert.ok(Number(exportStats[0].fileCount) <= 4, 'Small exports must not generate hundreds of empty files');
  const externalId = `${tableId}_${label}`;
  await api(`datasets/${DATASET}/tables`, {
    tableReference: { projectId: PROJECT, datasetId: DATASET, tableId: externalId },
    expirationTime: String(Date.now() + 60 * 60 * 1000),
    externalDataConfiguration: { sourceFormat: 'PARQUET', sourceUris: [plan.uri], autodetect: true },
  });
  const rows = await queryRows(`SELECT id,payload,_v1_deleted,
    CAST(_fivetran_synced AS STRING) AS source_time
    FROM \`${PROJECT}.${DATASET}.${externalId}\` ORDER BY id`, `${label}_readback`);
  assert.deepEqual(rows.map(row => [row.id, row.payload, row._v1_deleted]), [
    ['deleted', 'old', 'true'], ['inserted', 'new', 'false'],
    ['resurrected', 'new', 'false'], ['updated', 'new', 'false'],
  ]);
  assert.ok(rows.filter(row => row.id !== 'deleted').every(row => row.source_time.startsWith('2019-01-01')));
  results.push({ label, verified: true, rows: rows.length, uri: plan.uri,
    parentExportStatistics: exported.statistics?.query?.exportDataStatistics ?? null,
    childExportStatistics: exportStats });
  await writeFile(new URL(`../evidence/tests/bigquery-${stamp}-${label}.json`, import.meta.url),
    JSON.stringify(results.at(-1), null, 2) + '\n');
}
const emptyEnd = new Date();
const emptyPlan = buildExportPlan(source, emptyEnd, {
  ...transport, overlapMinutes: (emptyEnd - end) / 60_000,
  changeHistory: { ...sourceMetadata, checkedAt: emptyEnd },
});
const emptyJob = await run(emptyPlan.query, 'empty');
const emptyChildren = await api(`jobs?parentJobId=${emptyJob.jobReference.jobId}&projection=full`);
const emptyExports = [];
for (const child of emptyChildren.jobs ?? []) {
  const full = await api(`jobs/${child.jobReference.jobId}?location=US`);
  if (full.statistics?.query?.statementType === 'EXPORT_DATA') emptyExports.push(full.statistics.query.exportDataStatistics);
}
assert.equal(emptyExports.length, 1);
assert.equal(Number(emptyExports[0].rowCount), 0);
results.push({ label: 'empty', verified: true, rows: 0, uri: emptyPlan.uri,
  parentExportStatistics: emptyJob.statistics?.query?.exportDataStatistics ?? null,
  childExportStatistics: emptyExports });
const evidence = { checkedAt: new Date().toISOString(), syntheticOnly: true,
  dataset: DATASET, tableId, start: start.toISOString(), end: end.toISOString(), results, jobs };
await writeFile(new URL('../evidence/tests/bigquery-change-export.json', import.meta.url), JSON.stringify(evidence, null, 2) + '\n');
console.log(JSON.stringify({ verified: true, syntheticOnly: true, results }));
