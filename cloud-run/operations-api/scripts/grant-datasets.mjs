// Add a reader to each configured dataset while preserving all existing access entries.
import { execFileSync } from 'node:child_process';
import { BigQuery } from '@google-cloud/bigquery';
import { OAuth2Client } from 'google-auth-library';
const projectId = 'able-folio-499722';
const email = `boom-operations-api@${projectId}.iam.gserviceaccount.com`;
const client = new OAuth2Client();
client.setCredentials({ access_token: execFileSync('gcloud', ['auth', 'print-access-token'], { encoding: 'utf8' }).trim() });
const bigquery = new BigQuery({ projectId, authClient: client });
const datasets = ['booming_data_analytics', 'booming_data_analytics_assertions', 'boom_domains', 'jitsu_data',
  'facebook_ads_airbyte', 'google_ads_airbyte', 'activecampaign_airbyte', 'stripe_booming_airbyte',
  'stripe_kajabi_airbyte', 'manual_attribution', 'bill_reports_realtime'];
for (const id of datasets) {
  const dataset = bigquery.dataset(id);
  const [metadata] = await dataset.getMetadata();
  const access = metadata.access ?? [];
  if (access.some(entry => entry.userByEmail === email && entry.role === 'READER')) continue;
  await client.request({ url: `https://bigquery.googleapis.com/bigquery/v2/projects/${projectId}/datasets/${id}`,
    method: 'PATCH', headers: { 'If-Match': metadata.etag },
    data: { access: [...access, { role: 'READER', userByEmail: email }], etag: metadata.etag } });
  console.log(`Added dataset reader: ${id}`);
}
