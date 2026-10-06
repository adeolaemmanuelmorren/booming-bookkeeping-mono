import { readFile } from 'node:fs/promises';
import { getHeapStatistics } from 'node:v8';
import { Tinybird } from '../worker/storage/tinybird.ts';
import { TinybirdBootstrapStorage } from '../worker/bootstrap/tinybird-storage.ts';
import { executeBootstrap, type BootstrapConfig } from '../worker/bootstrap/executor.ts';

const location = process.env.BOOTSTRAP_CONFIG_PATH;
if (!location) throw new Error('BOOTSTRAP_CONFIG_PATH must point to the metadata-only frozen run configuration');
const config = JSON.parse(await readFile(location, 'utf8')) as BootstrapConfig;
const token = process.env.TINYBIRD_TOKEN;
const host = process.env.TINYBIRD_URL;
if (!token || host !== 'https://api.us-east.tinybird.co') throw new Error('Explicit Tinybird credentials and the approved region are required');
if (process.env.BOOTSTRAP_RUNNER_REGION !== config.region || process.env.BOOTSTRAP_SINGLE_TASK !== '1') {
  throw new Error('Use one supervised task in the configured region near Tinybird');
}
if (getHeapStatistics().heap_size_limit < 4 * 1024 ** 3) throw new Error('Start the one-time executor with at least a 4 GiB Node heap');
const workspaceResponse = await fetch(new URL('/v1/workspace', host), {
  headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(20000), redirect: 'error',
});
if (!workspaceResponse.ok) throw new Error('Cannot verify the configured bootstrap workspace');
const workspace = await workspaceResponse.json() as { id?: string; name?: string };
if (workspace.id !== config.workspaceId || workspace.name !== config.workspaceName) throw new Error('Bootstrap token belongs to another workspace');

// Credentials stay in memory. The program never writes raw rows or payload files locally.
const started = Date.now();
const requests = { sql: 0, appends: 0, uploadBytes: 0, failures: 0 };
const client = new Tinybird({ TINYBIRD_URL: host, TINYBIRD_TOKEN: token }, async (input, init) => {
  const append = new URL(String(input)).pathname === '/v0/events';
  if (append) {
    requests.appends++;
    requests.uploadBytes += Buffer.byteLength(String(init?.body ?? ''));
  } else requests.sql++;
  try {
    const response = await fetch(input, init);
    if (!response.ok) requests.failures++;
    return response;
  } catch (error) {
    requests.failures++;
    throw error;
  }
},
  config.bulk ? { requestTimeoutMs: 60_000, appendMaxBytes: 8_000_000, appendConcurrency: 4 } : {});
const store = new TinybirdBootstrapStorage(client, {
  baselineId: config.baselineId, tenantId: config.tenantId, sourceSeal: config.sourceSeal,
}, { bulk: config.bulk });
try {
  const result = await executeBootstrap(config, store, (stage, counts) => {
    process.stdout.write(JSON.stringify({ timestamp: new Date().toISOString(), stage, ...counts,
      elapsedSeconds: Math.round((Date.now() - started) / 1000), requests, rssBytes: process.memoryUsage().rss }) + '\n');
  });
  process.stdout.write(JSON.stringify({ sealed: true, result }) + '\n');
} catch (error) {
  // Raw engine errors can contain a fact key. Logs contain only the failure class.
  process.stderr.write(JSON.stringify({ sealed: false, error: 'Bootstrap stopped before its next verified checkpoint', errorType: error instanceof Error ? error.name : 'UnknownError' }) + '\n');
  process.exitCode = 1;
}
