import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { cloudflareRequest } from './cloudflare.mjs';

const directory = new URL('../evidence/cutover/', import.meta.url);
await mkdir(directory, { recursive: true, mode: 0o700 });
const phase = process.argv[2];
if (!['before', 'after', 'probe'].includes(phase)) throw new Error('Choose before, after, or probe');

const settings = await cloudflareRequest('/workers/scripts/jitsu-tinybird-ingest/settings');
const deployments = await cloudflareRequest('/workers/scripts/jitsu-tinybird-ingest/deployments');
const safeSettings = { ...settings, bindings: settings.bindings.map(binding => binding.type === 'secret_text' ? { name: binding.name, type: binding.type } : binding) };
await writeFile(new URL(`${phase}-jitsu-settings.json`, directory), JSON.stringify(safeSettings, null, 2));
await writeFile(new URL(`${phase}-jitsu-deployments.json`, directory), JSON.stringify(deployments, null, 2));
const versions = deployments.deployments?.[0]?.versions ?? [];
console.log(JSON.stringify({ phase, jitsu_versions: versions, bindings: settings.bindings.map(row => ({ name: row.name, type: row.type })) }));

if (phase === 'probe') {
  const { ADMIN_TOKEN } = JSON.parse(await readFile(new URL('../.dev.vars.admin.json', import.meta.url), 'utf8'));
  const response = await fetch('https://boom-tinybird-facts-v1.bill-3e3.workers.dev/admin/browser/probe', {
    method: 'POST', headers: { Authorization: `Bearer ${ADMIN_TOKEN}` },
    redirect: 'manual', signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`Browser buffer probe failed with HTTP ${response.status}`);
  const proof = await response.json();
  if (!proof.verified || !proof.probe_removed || proof.mode !== 'buffer') throw new Error('Browser buffer probe failed');
  await writeFile(new URL('buffer-probe.json', directory), JSON.stringify({ checked_at: new Date().toISOString(), ...proof }, null, 2));
  console.log(JSON.stringify(proof));
}
