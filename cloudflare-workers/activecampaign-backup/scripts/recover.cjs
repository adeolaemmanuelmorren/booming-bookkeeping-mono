const fs = require('node:fs');
const path = require('node:path');
const {execFile} = require('node:child_process');
const {promisify} = require('node:util');
const run = promisify(execFile);
const worker = path.resolve(__dirname, '..');
const wrangler = path.join(worker, 'node_modules/wrangler/bin/wrangler.js');
const token = fs.readFileSync(path.join(worker, '.dev.vars'), 'utf8').match(/^WAREHOUSE_TOKEN=([a-f0-9]{64})$/m)?.[1];
if (!token) throw new Error('Missing receiver token');

async function command(args) {
  const {stdout} = await run(process.execPath, [wrangler, ...args, '--json'], {cwd: worker, maxBuffer: 8_000_000});
  return JSON.parse(stdout);
}

async function recover(account, initialId) {
  let id = initialId;
  for (let window = 0; window < 20; window++) {
    if (!id) {
      id = `recovery-${account}-${Date.now()}`;
      await command(['workflows', 'trigger', 'boom-activecampaign-backup', JSON.stringify({account}), '--id', id]);
    }
    for (;;) {
      const result = await command(['workflows', 'instances', 'describe', 'boom-activecampaign-backup', id]);
      console.log(JSON.stringify({account, id, status: result.status, steps: result.step_count, error: result.error}));
      if (result.status === 'complete') break;
      if (['errored', 'terminated', 'paused'].includes(result.status)) throw new Error(`${account} recovery stopped: ${result.status}`);
      await new Promise(resolve => setTimeout(resolve, 20000));
    }
    const response = await fetch('https://boom-activecampaign-backup-receiver-943586541190.us-central1.run.app/window', {
      method: 'POST', headers: {Authorization: `Bearer ${token}`, 'Content-Type': 'application/json'}, body: JSON.stringify({account}),
    });
    if (!response.ok) throw new Error(`Cursor read failed: ${response.status}`);
    const {windowEnd} = await response.json();
    const lagSeconds = (Date.now() - Date.parse(windowEnd)) / 1000;
    console.log(JSON.stringify({account, windowEnd, lagSeconds}));
    if (lagSeconds < 600) return;
    id = null;
  }
  throw new Error(`${account} exceeded recovery window limit`);
}

recover('activecampaign', process.argv[2] || null).catch(error => { console.error(error.message); process.exitCode = 1; });
