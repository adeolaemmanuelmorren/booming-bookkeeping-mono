import { execFileSync } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
const run = JSON.parse(
  await readFile(new URL("./execution.json", import.meta.url), "utf8"),
);
const gcloud = (args) =>
  JSON.parse(
    execFileSync("/Users/adeola/google-cloud-sdk/bin/gcloud", args, {
      encoding: "utf8",
      maxBuffer: 2000000,
      stdio: ["ignore", "pipe", "pipe"],
    }),
  );
const state = gcloud([
  "run",
  "jobs",
  "executions",
  "describe",
  run.execution,
  "--project",
  run.project,
  "--region",
  run.region,
  "--format=json(status)",
]);
const logs = gcloud([
  "logging",
  "read",
  `resource.type="cloud_run_job" AND resource.labels.job_name="${run.job}" AND labels."run.googleapis.com/execution_name"="${run.execution}" AND jsonPayload.stage:*`,
  "--project",
  run.project,
  "--limit=3",
  "--order=desc",
  "--format=json(timestamp,jsonPayload)",
]);
const result = {
  checkedAt: new Date().toISOString(),
  execution: run.execution,
  status: state.status,
  progress: logs.map((row) => row.jsonPayload),
};
await writeFile(
  new URL("./status.json", import.meta.url),
  JSON.stringify(result, null, 2),
);
console.log(JSON.stringify(result));
