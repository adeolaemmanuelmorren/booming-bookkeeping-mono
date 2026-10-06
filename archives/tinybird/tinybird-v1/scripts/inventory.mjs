import { mkdir, writeFile } from "node:fs/promises";
import { tinybirdRequest, query } from "./tinybird.mjs";

const directory = new URL("../evidence/before-reset/", import.meta.url);
await mkdir(directory, { recursive: true, mode: 0o700 });

for (const resource of ["datasources", "pipes"]) {
  const response = await tinybirdRequest(`/v0/${resource}`);
  const result = await response.json();
  await writeFile(new URL(`${resource}.json`, directory), JSON.stringify(result, null, 2));
  console.log(JSON.stringify({ resource, count: result[resource].length }));
}

const result = await query(`
  SELECT event_kind, count() AS deliveries, uniqExact(message_id) AS events,
    min(observed_at) AS earliest_event, max(observed_at) AS latest_event,
    max(ingested_at) AS latest_delivery
  FROM jitsu_events_api_observations
  GROUP BY event_kind
`);
await writeFile(new URL("jitsu-source-counts.json", directory), JSON.stringify(result, null, 2));
console.log(JSON.stringify({ jitsu: result.data, statistics: result.statistics }));
