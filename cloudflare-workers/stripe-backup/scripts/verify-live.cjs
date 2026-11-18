const fs = require('node:fs');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {execFileSync} = require('node:child_process');

async function main() {
  const {hydrate} = await import(pathToFileURL(path.join(__dirname, '../src/stripe.js')));
  const {publish} = await import(pathToFileURL(path.join(__dirname, '../src/warehouse.js')));
  const sourceToken = execFileSync('/Users/adeola/google-cloud-sdk/bin/gcloud', [
    'secrets', 'versions', 'access', 'latest',
    '--project=able-folio-499722', '--secret=bill-ondemand-source-token',
  ], {encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe']}).trim();

  const source = {
    async read(account, sourcePath, parameters = {}) {
      const response = await fetch('https://bill-realtime-ondemand-source-d7ef.bill-3e3.workers.dev/read', {
        method: 'POST',
        headers: {Authorization: `Bearer ${sourceToken}`, 'Content-Type': 'application/json'},
        body: JSON.stringify({
          source: 'stripe', account: account === 'stripe' ? 'main' : 'kajabi',
          path: sourcePath, parameters,
        }),
        signal: AbortSignal.timeout(30000),
      });
      if (!response.ok) throw new Error(`Source gateway ${response.status}`);
      return response.json();
    },
  };
  const config = fs.readFileSync(path.join(__dirname, '../.dev.vars'), 'utf8');
  const token = config.match(/^WAREHOUSE_TOKEN=([a-f0-9]{64})$/m)?.[1];
  if (!token) throw new Error('Missing ingestion-only warehouse token.');
  const env = {
    WAREHOUSE_URL: 'https://boom-stripe-backup-receiver-943586541190.us-central1.run.app',
    WAREHOUSE_TOKEN: token,
  };
  const samples = [
    ['overlap', {limit: '5', 'created[lt]': String(Date.parse('2026-11-17T08:00:00Z') / 1000)}],
    ['recent', {limit: '5'}],
  ];
  for (const account of ['stripe', 'stripe_kajabi']) {
    for (const [label, parameters] of samples) {
      const result = await source.read(account, '/charges', parameters);
      if (result.status !== 200) throw new Error('Charge list unavailable');
      const observedAt = new Date().toISOString();
      const references = result.body.data.map(charge => ({stream: 'charges', id: charge.id}));
      const records = await hydrate(source, account, references);
      const summary = {account, label, charges: references.length, records: records.length};
      if (process.argv.includes('--read-only')) {
        console.log(JSON.stringify({...summary, readOnly: true}));
        continue;
      }
      const batchId = `live-validation-${account}-${label}-${Date.now()}`;
      await publish(env, account, batchId, observedAt, records);
      // Replay the exact batch to check ambiguous-acknowledgement recovery.
      await publish(env, account, batchId, observedAt, records);
      console.log(JSON.stringify({...summary, publishedTwice: true}));
    }
  }
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
