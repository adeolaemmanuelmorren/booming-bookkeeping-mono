import { readFile, writeFile } from "node:fs/promises";

const [sqlPath, outputPath, outputFormat = "Parquet"] = process.argv.slice(2);

if (!sqlPath || !outputPath) {
  throw new Error(
    "Usage: node scripts/export-tinybird-query.mjs <sql-file> <output-file> [format]",
  );
}

const tinybirdConfigUrl = new URL("../.tinyb", import.meta.url);
const tinybirdConfig = JSON.parse(await readFile(tinybirdConfigUrl, "utf8"));
const sql = (await readFile(sqlPath, "utf8")).trim();
const response = await fetch(`${tinybirdConfig.host}/v0/sql`, {
  method: "POST",
  headers: {
    Authorization: `Bearer ${tinybirdConfig.token}`,
    "Content-Type": "application/json",
  },
  body: JSON.stringify({ q: `${sql} FORMAT ${outputFormat}` }),
});

if (!response.ok) {
  throw new Error(`Tinybird export failed with HTTP ${response.status}`);
}

const output = Buffer.from(await response.arrayBuffer());
await writeFile(outputPath, output);
console.log(JSON.stringify({ bytes: output.length }));
