const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const worker = path.resolve(__dirname, '..');
const wrangler = path.join(worker, 'node_modules/wrangler/bin/wrangler.js');

function main() {
  const config = fs.readFileSync(path.join(worker, '.dev.vars'), 'utf8');
  const warehouseToken = config.match(/^WAREHOUSE_TOKEN=([a-f0-9]{64})$/m)?.[1];
  if (!warehouseToken) throw new Error('Missing receiver token in the ignored .dev.vars file');
  const sourceToken = execFileSync('doppler', [
    'secrets', 'get', 'ACTIVE_CAMPAIGN_TOKEN', '--plain',
    '--project', 'booming-bookkeeping', '--config', 'dev',
  ], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  if (!sourceToken) throw new Error('Doppler returned an empty AC token');
  for (const [name, value] of [['WAREHOUSE_TOKEN', warehouseToken], ['ACTIVE_CAMPAIGN_TOKEN', sourceToken]]) {
    execFileSync(process.execPath, [wrangler, 'secret', 'put', name], {
      cwd: worker, input: value, stdio: ['pipe', 'inherit', 'inherit'],
    });
  }
}

main();
