const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const root = path.resolve(__dirname, '../../..');
const libraries = root + '/cloud-run/activecampaign-backup-receiver/node_modules';
const { OAuth2Client } = require(libraries + '/google-auth-library');
const { BigQuery } = require(libraries + '/@google-cloud/bigquery');

async function main() {
  if (!process.argv.includes('--replay')) throw new Error('Pass --replay to append two exact copies of one published record');
  const token = execFileSync(process.env.GCLOUD_BIN || 'gcloud', ['auth', 'print-access-token'], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'],
  }).trim();
  const auth = new OAuth2Client();
  auth.setCredentials({ access_token: token });
  const bigquery = new BigQuery({ projectId: 'able-folio-499722', authClient: auth });
  const [rows] = await bigquery.query({
    location: 'US', maximumBytesBilled: '1000000000',
    query: `SELECT records.* EXCEPT(payload), TO_JSON_STRING(payload) AS payload_json
      FROM \`able-folio-499722.activecampaign_backup.records\` records
      JOIN \`able-folio-499722.activecampaign_backup.completed_windows\` USING(workflow_id)
      WHERE stream = 'contacts' ORDER BY observed_at DESC LIMIT 1`,
  });
  if (!rows.length) throw new Error('No completed contact snapshot to replay');
  const row = rows[0];
  const receiverToken = fs.readFileSync(path.resolve(__dirname, '../.dev.vars'), 'utf8')
    .match(/^WAREHOUSE_TOKEN=([a-f0-9]{64})$/m)?.[1];
  if (!receiverToken) throw new Error('Missing receiver token');
  const body = {
    account: 'activecampaign', workflowId: row.workflow_id, batchId: row.batch_id,
    observedAt: row.observed_at.value,
    records: [{ stream: row.stream, id: row.record_id, payload: JSON.parse(row.payload_json) }],
  };
  for (let copy = 0; copy < 2; copy++) {
    const response = await fetch('https://boom-activecampaign-backup-receiver-943586541190.us-central1.run.app/records', {
      method: 'POST', headers: { Authorization: `Bearer ${receiverToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body), signal: AbortSignal.timeout(120_000),
    });
    if (!response.ok) throw new Error(`Replay returned ${response.status}`);
  }
  const [counts] = await bigquery.query({
    location: 'US', maximumBytesBilled: '1000000000',
    query: `SELECT COUNT(*) AS physical_copies, COUNT(DISTINCT record_id) AS logical_contacts
      FROM \`able-folio-499722.activecampaign_backup.records\`
      WHERE workflow_id = @workflowId AND batch_id = @batchId AND record_id = @recordId AND stream = 'contacts'`,
    params: { workflowId: row.workflow_id, batchId: row.batch_id, recordId: row.record_id },
  });
  console.log(JSON.stringify(counts[0]));
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
