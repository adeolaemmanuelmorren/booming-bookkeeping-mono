import { readFile } from "node:fs/promises";

const PROFILE_CHUNK_SIZE = 200;
const MAX_ATTEMPTS = 5;

const [beforeBatchVersionText, repairPrefix] = process.argv.slice(2);
const beforeBatchVersion = Number(beforeBatchVersionText);
if (!Number.isSafeInteger(beforeBatchVersion) || beforeBatchVersion < 1) {
  throw new Error(
    "Usage: node scripts/enqueue-journey-profile-repairs.mjs <before-batch-version> <repair-prefix>",
  );
}
if (!repairPrefix || !/^[A-Za-z0-9._:-]{1,120}$/.test(repairPrefix)) {
  throw new Error("repair-prefix must contain 1 to 120 safe identifier characters.");
}

const syncAdminToken = process.env.SYNC_ADMIN_TOKEN;
if (!syncAdminToken) throw new Error("SYNC_ADMIN_TOKEN is required.");
const syncWorkerUrl = process.env.SYNC_WORKER_URL
  ?? "https://bigquery-tinybird-sync.bill-3e3.workers.dev";

const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const profileIds = await readRepairProfileIds(tinybirdConfig, beforeBatchVersion);
const chunks = chunk(profileIds, PROFILE_CHUNK_SIZE);

for (const [index, profileChunk] of chunks.entries()) {
  const repairId = `${repairPrefix}-${String(index + 1).padStart(4, "0")}`;
  await enqueueRepair(syncWorkerUrl, syncAdminToken, repairId, profileChunk);
  console.log(JSON.stringify({
    repairId,
    queuedProfiles: profileChunk.length,
    completedChunks: index + 1,
    totalChunks: chunks.length,
  }));
}

console.log(JSON.stringify({
  result: "queued",
  profiles: profileIds.length,
  chunks: chunks.length,
  beforeBatchVersion,
}));

async function readRepairProfileIds(config, cutoff) {
  const sql = `
    SELECT DISTINCT profile_id
    FROM reporting_journey_versions
    WHERE tenant_id = 'boom'
      AND batch_version > 0
      AND batch_version < ${cutoff}
      AND profile_id IS NOT NULL
      AND profile_id != ''
    ORDER BY profile_id
    FORMAT JSON
  `;
  const response = await fetch(`${config.host}/v0/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ q: sql }),
  });
  if (!response.ok) {
    throw new Error(`Tinybird repair profile query failed with HTTP ${response.status}.`);
  }

  const payload = await response.json();
  if (!Array.isArray(payload.data)) {
    throw new Error("Tinybird repair profile query returned an invalid response.");
  }
  return payload.data.map((row) => {
    if (typeof row.profile_id !== "string" || row.profile_id === "") {
      throw new Error("Tinybird repair profile query returned an invalid profile_id.");
    }
    return row.profile_id;
  });
}

async function enqueueRepair(workerUrl, token, repairId, profileIds) {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    const response = await fetch(`${workerUrl}/journey/repair`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ repairId, profileIds }),
    });
    if (response.ok) return;

    const retryable = response.status === 429 || response.status >= 500;
    if (!retryable || attempt === MAX_ATTEMPTS) {
      throw new Error(`Journey repair ${repairId} failed with HTTP ${response.status}.`);
    }

    const retryAfterSeconds = Number(response.headers.get("Retry-After") ?? "1");
    const delayMs = Number.isFinite(retryAfterSeconds)
      ? Math.max(1, retryAfterSeconds) * 1_000
      : 1_000;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
}

function chunk(values, size) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}
