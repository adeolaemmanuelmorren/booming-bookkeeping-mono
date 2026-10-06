import { readFile, readdir } from "node:fs/promises";
import { basename, join } from "node:path";

const [datasourceName, directoryPath, firstFileName] = process.argv.slice(2);

if (!datasourceName || !directoryPath || !firstFileName) {
  throw new Error(
    "Usage: node scripts/send-events-directory.mjs <datasource> <directory> <first-file>",
  );
}

const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const fileNames = (await readdir(directoryPath))
  .filter((fileName) => fileName.endsWith(".gz"))
  .filter((fileName) => fileName >= basename(firstFileName))
  .sort();

if (fileNames.length === 0) {
  throw new Error("No compressed NDJSON files matched the requested range");
}

for (const [index, fileName] of fileNames.entries()) {
  await sendFile(fileName);
  console.error(`Sent ${index + 1}/${fileNames.length}: ${fileName}`);
}

console.log(JSON.stringify({ sent: fileNames.length }));

async function sendFile(fileName) {
  const fileContents = await readFile(join(directoryPath, fileName));
  const eventsUrl = new URL("/v0/events", tinybirdConfig.host);
  eventsUrl.searchParams.set("name", datasourceName);
  eventsUrl.searchParams.set("wait", "true");

  for (let attempt = 1; attempt <= 5; attempt += 1) {
    const response = await fetch(eventsUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tinybirdConfig.token}`,
        "Content-Encoding": "gzip",
        "Content-Type": "application/x-ndjson",
      },
      body: fileContents,
    });
    const responseText = await response.text();

    if (response.status === 200) {
      return;
    }

    if (response.status !== 429 && response.status !== 503) {
      throw new Error(`${fileName} failed with HTTP ${response.status}`);
    }

    const retryAfterSeconds = Number(response.headers.get("retry-after")) || attempt;
    console.error(
      `${fileName} received HTTP ${response.status}; retrying in ${retryAfterSeconds}s`,
    );
    await wait(retryAfterSeconds * 1_000);
  }

  throw new Error(`${fileName} exceeded the safe retry limit`);
}

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
