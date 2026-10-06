import { appendFile, readFile, writeFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';

// Read-only benchmark runner. Credentials stay in memory; output is local.
const path = process.argv[2];
if (!path) throw new Error('Pass a SQL file.');
const config = JSON.parse(await readFile(new URL('../../.tinyb', import.meta.url), 'utf8'));
if (config.host !== 'https://api.us-east.tinybird.co') throw new Error('Unexpected host.');
const sql = (await readFile(path, 'utf8')).trim().replace(/;$/, '');
if (!/^(SELECT|WITH|EXPLAIN|DESCRIBE)\b/i.test(sql)) throw new Error('Read-only SQL required.');
const started = performance.now();
const evidence = { checkedAt: new Date().toISOString(), workspace: config.name, sqlFile: path };
try {
  const response = await fetch(new URL('/v0/sql', config.host), {
    method: 'POST',
    headers: { Authorization: `Bearer ${config.token}` },
    body: new URLSearchParams({ q: `${sql}\nFORMAT JSON` }),
    signal: AbortSignal.timeout(45_000),
  });
  const result = await response.json();
  evidence.wallMs = Math.round(performance.now() - started);
  evidence.httpStatus = response.status;
  if (!response.ok) {
    evidence.error = String(result.error ?? 'Query failed').slice(0, 3500);
  } else {
    evidence.statistics = result.statistics;
    evidence.rows = result.rows;
    evidence.data = result.data;
  }
} catch (error) {
  evidence.wallMs = Math.round(performance.now() - started);
  evidence.error = `${error.name}: request failed`;
}
await writeFile(path.replace(/\.sql$/, '') + '.result.json', JSON.stringify(evidence, null, 2) + '\n');
await appendFile(new URL('./runs.jsonl',import.meta.url),JSON.stringify(evidence)+'\n');
console.log(JSON.stringify(evidence));
if (evidence.error) process.exitCode = 1;
