import { readFile } from "node:fs/promises";

const [datasourceName, connectorId, bucketUri, datasourcePath] =
  process.argv.slice(2);

if (!datasourceName || !connectorId || !bucketUri || !datasourcePath) {
  throw new Error(
    "Usage: node scripts/import-gcs-prefix.mjs <datasource> <connector-id> <gs://prefix/*.parquet> <datasource-file>",
  );
}

if (!bucketUri.startsWith("gs://")) {
  throw new Error("The bucket URI must start with gs://");
}

const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const datasourceDefinition = await readFile(datasourcePath, "utf8");
const schemaMatch = datasourceDefinition.match(/SCHEMA >\n([\s\S]*?)\n\nENGINE /);

if (!schemaMatch) {
  throw new Error(`Could not read SCHEMA from ${datasourcePath}`);
}

const requestBody = new URLSearchParams({
  mode: "append",
  name: datasourceName,
  service: "gcs",
  connector: connectorId,
  bucket_uri: bucketUri,
  format: "parquet",
  schema: schemaMatch[1].trim(),
});

const response = await fetch(`${tinybirdConfig.host}/v0/datasources`, {
  method: "POST",
  headers: {
    Authorization: `Bearer ${tinybirdConfig.token}`,
    "Content-Type": "application/x-www-form-urlencoded",
  },
  body: requestBody,
});

const responseText = await response.text();
const safeResponseText = responseText.replaceAll(
  tinybirdConfig.token,
  "[REDACTED]",
);

if (!response.ok) {
  throw new Error(`Tinybird import failed: ${safeResponseText}`);
}

const result = JSON.parse(responseText);
const jobId =
  result.import_id ?? result.job_id ?? result.id ?? result.job?.job_id;

if (!jobId) {
  throw new Error(`Tinybird did not return an import job: ${safeResponseText}`);
}

console.log(jobId);
