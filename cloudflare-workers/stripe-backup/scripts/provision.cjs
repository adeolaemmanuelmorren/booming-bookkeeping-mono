// Run from the monorepo root. Uses the operator's existing gcloud login.
const path = require('node:path');
const {execFileSync} = require('node:child_process');
const root = path.resolve(__dirname, '../../..');
const {OAuth2Client} = require(root + '/cloud-run/operations-api/node_modules/google-auth-library');
const {BigQuery} = require(root + '/cloud-run/operations-api/node_modules/@google-cloud/bigquery');
const project = 'able-folio-499722';
const email = `stripe-backup-writer@${project}.iam.gserviceaccount.com`;
const gcloud = '/Users/adeola/google-cloud-sdk/bin/gcloud';
async function main() {
  const token = execFileSync(gcloud, ['auth', 'print-access-token'], {encoding: 'utf8', stdio: ['ignore','pipe','pipe']}).trim();
  const auth = new OAuth2Client();
  auth.setCredentials({access_token: token});
  const bq = new BigQuery({projectId: project, authClient: auth});
  for (const name of ['stripe_backup', 'stripe_backup_validation', 'stripe_backup_validation_assertions']) {
    const dataset = bq.dataset(name);
    const [exists] = await dataset.exists();
    if (!exists) await bq.createDataset(name, {location: 'US'});
    console.log(`Dataset ready: ${name}`);
  }
  const definitions = {
    records: {
      schema: [
        {name:'account',type:'STRING',mode:'REQUIRED'}, {name:'stream',type:'STRING',mode:'REQUIRED'},
        {name:'record_id',type:'STRING',mode:'REQUIRED'}, {name:'payload',type:'JSON',mode:'REQUIRED'},
        {name:'observed_at',type:'TIMESTAMP',mode:'REQUIRED'}, {name:'batch_id',type:'STRING',mode:'REQUIRED'},
      ], timePartitioning:{type:'DAY',field:'observed_at'}, clustering:{fields:['account','stream','record_id']},
    },
    completed_windows: {
      schema: [
        {name:'account',type:'STRING',mode:'REQUIRED'}, {name:'window_start',type:'TIMESTAMP',mode:'REQUIRED'},
        {name:'window_end',type:'TIMESTAMP',mode:'REQUIRED'}, {name:'completed_at',type:'TIMESTAMP',mode:'REQUIRED'},
        {name:'workflow_id',type:'STRING',mode:'REQUIRED'},
      ],
    },
  };
  for (const [name, options] of Object.entries(definitions)) {
    const table = bq.dataset('stripe_backup').table(name);
    if (!(await table.exists())[0]) await bq.dataset('stripe_backup').createTable(name, options);
    console.log(`Table ready: ${name}`);
  }
  const serviceUrl = `https://iam.googleapis.com/v1/projects/${project}/serviceAccounts`;
  const headers = {Authorization: `Bearer ${token}`, 'Content-Type':'application/json'};
  let response = await fetch(`${serviceUrl}/${email}`, {headers});
  if (response.status === 404) {
    response = await fetch(serviceUrl, {method:'POST',headers,body:JSON.stringify({accountId:'stripe-backup-writer',serviceAccount:{displayName:'Stripe backup warehouse writer'}})});
  }
  if (!response.ok) throw new Error(`Service account setup failed: ${response.status}`);
  // This account can run jobs, but can write only the separate backup dataset.
  execFileSync(gcloud, ['projects','add-iam-policy-binding',project,`--member=serviceAccount:${email}`,'--role=roles/bigquery.jobUser','--condition=None','--quiet'], {stdio:['ignore','pipe','pipe']});
  const dataset = bq.dataset('stripe_backup');
  const [metadata] = await dataset.getMetadata();
  const access = metadata.access ?? [];
  for (const [role, userByEmail] of [['WRITER',email],['READER',`service-943586541190@gcp-sa-dataform.iam.gserviceaccount.com`]]) {
    if (!access.some(entry => entry.userByEmail === userByEmail && entry.role === role)) access.push({role,userByEmail});
  }
  await dataset.setMetadata({access});
  console.log('Restricted service identity ready. Cloud Run uses ADC; no service-account key is needed.');

}
main().catch(error => {console.error(error.message);process.exitCode=1;});
