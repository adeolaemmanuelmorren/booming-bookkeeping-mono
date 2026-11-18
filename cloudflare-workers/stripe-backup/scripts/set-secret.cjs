const fs = require('node:fs');
const path = require('node:path');
const {execFileSync} = require('node:child_process');

const worker = path.resolve(__dirname, '..');
const config = fs.readFileSync(path.join(worker, '.dev.vars'), 'utf8');
const token = config.match(/^WAREHOUSE_TOKEN=([a-f0-9]{64})$/m)?.[1];
if (!token) throw new Error('Missing ingestion-only warehouse token.');

execFileSync(process.execPath, [
  path.join(worker, 'node_modules/wrangler/bin/wrangler.js'),
  'secret', 'put', 'WAREHOUSE_TOKEN',
], {
  cwd: worker,
  input: token,
  stdio: ['pipe', 'inherit', 'inherit'],
});
