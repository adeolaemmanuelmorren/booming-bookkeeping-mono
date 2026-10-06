import { mkdir, readFile, writeFile, rename } from "node:fs/promises";
import { createWriteStream, createReadStream } from "node:fs";
import { createGzip, createGunzip } from "node:zlib";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { query, tinybirdRequest } from "./tinybird.mjs";

const directory = new URL("../backups/live-jitsu/", import.meta.url);
await mkdir(directory, { recursive: true, mode: 0o700 });
const manifestFile = new URL("manifest.json", directory);
let manifest;
try {
  manifest = JSON.parse(await readFile(manifestFile, "utf8"));
} catch (error) {
  if (error.code !== "ENOENT") throw error;
  const result = await query("SELECT now64(6) AS cutoff");
  manifest = { source: "jitsu_events_api_observations", cutoff: result.data[0].cutoff, bucketCount: 256, buckets: {} };
  await saveManifest();
}

if (manifest.bucketCount < 256 && Object.keys(manifest.buckets).length === 0) {
  manifest.bucketCount = 256;
  await saveManifest();
}

if (!manifest.columns) {
  const schema = await query('SELECT * FROM jitsu_events_api_observations LIMIT 0');
  manifest.columns = schema.meta.map(column => column.name);
  await saveManifest();
}

for (let bucket = 0; bucket < manifest.bucketCount; bucket++) {
  if (manifest.buckets[bucket]?.verified) continue;
  const where = `ingested_at < toDateTime64('${manifest.cutoff}', 6) AND cityHash64(message_id) % ${manifest.bucketCount} = ${bucket}`;
  const expected = await query(`SELECT count() AS rows FROM jitsu_events_api_observations WHERE ${where}`);
  const expectedRows = Number(expected.data[0].rows);
  const filename = `bucket-${String(bucket).padStart(2, "0")}.ndjson.gz`;
  const target = new URL(filename, directory);
  const temporary = new URL(`${filename}.partial`, directory);
  // Serializing before returning avoids the API's wide-column result-memory limit.
  // The column manifest lets us restore the exact named record without losing fields.
  const body = new URLSearchParams({ q: `SELECT toJSONString(tuple(*)) AS row FROM jitsu_events_api_observations WHERE ${where} FORMAT JSONEachRow` });
  const response = await tinybirdRequest("/v0/sql", { method: "POST", body, timeoutMs: 60_000 });
  await pipeline(Readable.from(namedRows(response.body)), createGzip(), createWriteStream(temporary, { mode: 0o600 }));

  let rows = 0;
  const digest = createHash("sha256");
  const input = createReadStream(temporary).pipe(createGunzip());
  for await (const line of createInterface({ input, crlfDelay: Infinity })) {
    if (!line) continue;
    const event = JSON.parse(line);
    if (!event.message_id || !event.delivery_event_id || !event.fact_payload_hash) {
      throw new Error(`Invalid event in bucket ${bucket}`);
    }
    digest.update(line + "\n");
    rows++;
  }
  if (rows !== expectedRows) throw new Error(`Bucket ${bucket}: expected ${expectedRows} rows, received ${rows}`);
  await rename(temporary, target);
  manifest.buckets[bucket] = { filename, rows, sha256: digest.digest("hex"), verified: true };
  await saveManifest();
  console.log(JSON.stringify({ bucket, rows, verified: true }));
}

manifest.verified = Object.keys(manifest.buckets).length === manifest.bucketCount;
manifest.rows = Object.values(manifest.buckets).reduce((total, bucket) => total + bucket.rows, 0);
await saveManifest();
console.log(JSON.stringify({ complete: manifest.verified, rows: manifest.rows, cutoff: manifest.cutoff }));

async function saveManifest() {
  const temporary = new URL("manifest.json.partial", directory);
  await writeFile(temporary, JSON.stringify(manifest, null, 2) + "\n", { mode: 0o600 });
  await rename(temporary, manifestFile);
}

async function* namedRows(body) {
  const input = Readable.fromWeb(body);
  for await (const line of createInterface({ input, crlfDelay: Infinity })) {
    if (!line) continue;
    const values = JSON.parse(JSON.parse(line).row);
    if (!Array.isArray(values) || values.length !== manifest.columns.length) {
      throw new Error('Backup row does not match the saved source schema');
    }
    yield JSON.stringify(Object.fromEntries(manifest.columns.map((name, index) => [name, values[index]]))) + '\n';
  }
}
