import { readFile, readdir } from "node:fs/promises";
import { basename, join } from "node:path";

const [datasourceName, directoryPath, firstFileName] = process.argv.slice(2);

if (!datasourceName || !directoryPath || !firstFileName) {
  throw new Error(
    "Usage: node scripts/append-parquet-directory.mjs <datasource> <directory> <first-file>",
  );
}

const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const allFileNames = await readdir(directoryPath);
const fileNames = allFileNames
  .filter((fileName) => /^part-\d+\.parquet$/.test(fileName))
  .filter((fileName) => fileName >= basename(firstFileName))
  .sort();

if (fileNames.length === 0) {
  throw new Error("No Parquet files matched the requested range");
}

const authorization = `Bearer ${tinybirdConfig.token}`;
const jobs = [];

for (const fileBatch of inBatches(fileNames, 8)) {
  const batchJobs = await Promise.all(fileBatch.map(queueFile));
  jobs.push(...batchJobs);
  console.error(`Queued ${jobs.length}/${fileNames.length} files`);
}

const pendingJobIds = new Set(jobs.map(({ jobId }) => jobId));
const failedJobs = [];
const deadline = Date.now() + 10 * 60 * 1000;

while (pendingJobIds.size > 0) {
  if (Date.now() > deadline) {
    throw new Error(
      `Timed out with ${pendingJobIds.size} Tinybird imports still running`,
    );
  }

  for (const jobBatch of inBatches([...pendingJobIds], 10)) {
    const results = await Promise.all(jobBatch.map(readJob));

    for (const job of results) {
      if (job.status === "done") {
        pendingJobIds.delete(job.id);
      }

      if (job.status === "error") {
        pendingJobIds.delete(job.id);
        failedJobs.push({ id: job.id, errors: job.errors ?? [job.error] });
      }
    }
  }

  if (pendingJobIds.size > 0) {
    console.error(`${pendingJobIds.size} imports still running`);
    await wait(2_000);
  }
}

if (failedJobs.length > 0) {
  throw new Error(`Tinybird imports failed: ${JSON.stringify(failedJobs)}`);
}

console.log(JSON.stringify({ queued: jobs.length, completed: jobs.length }));

async function queueFile(fileName) {
  const filePath = join(directoryPath, fileName);
  const fileContents = await readFile(filePath);
  const datasourcePath = encodeURIComponent(datasourceName);
  const response = await fetch(
    `${tinybirdConfig.host}/v1/datasources/${datasourcePath}/append?format=parquet`,
    {
      method: "POST",
      headers: {
        Authorization: authorization,
        "Content-Type": "application/vnd.apache.parquet",
      },
      body: fileContents,
    },
  );
  const result = await readJsonResponse(response, `queue ${fileName}`);
  const jobId = result.id ?? result.import_id ?? result.job_id;

  if (!jobId) {
    throw new Error(`Tinybird returned no job ID for ${fileName}`);
  }

  return { fileName, jobId };
}

async function readJob(jobId) {
  const response = await fetch(`${tinybirdConfig.host}/v0/jobs/${jobId}`, {
    headers: { Authorization: authorization },
  });
  const result = await readJsonResponse(response, `read job ${jobId}`);

  return { ...result, id: jobId };
}

async function readJsonResponse(response, operation) {
  const responseText = await response.text();
  const safeResponseText = responseText.replaceAll(
    tinybirdConfig.token,
    "[REDACTED]",
  );

  if (!response.ok) {
    throw new Error(`${operation} failed: ${safeResponseText}`);
  }

  return JSON.parse(responseText);
}

function inBatches(values, batchSize) {
  const batches = [];

  for (let index = 0; index < values.length; index += batchSize) {
    batches.push(values.slice(index, index + batchSize));
  }

  return batches;
}

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
