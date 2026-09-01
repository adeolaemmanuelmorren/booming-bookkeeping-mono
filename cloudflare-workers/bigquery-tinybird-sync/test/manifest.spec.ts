import { readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { buildExportPlan } from "../src/bigquery";
import { TABLE_MANIFEST } from "../src/table-manifest.generated";

const workerDirectory = resolve(import.meta.dirname, "..");
const rawDatasourceDirectory = resolve(
  workerDirectory,
  "..",
  "..",
  "tinybird-production",
  "datasources",
  "raw",
);
const runAt = new Date("2026-08-26T12:34:00.000Z");

describe("generated table manifest", () => {
  it("builds 49 physical exports and one derived export under non-overlapping prefixes", () => {
    const datasourceFiles = readdirSync(rawDatasourceDirectory)
      .filter((file) => file.endsWith(".datasource"))
      .sort();
    const resources = TABLE_MANIFEST.map((table) => table.resourceName);

    expect(TABLE_MANIFEST).toHaveLength(50);
    expect(resources).toContain("raw_stripe_auxiliary");

    expect(datasourceFiles).toEqual(
      resources.map((resource) => `${resource}.datasource`).sort(),
    );

    for (const table of TABLE_MANIFEST) {
      const plan = buildExportPlan(table, runAt, {
        bucket: "booming-data",
        prefix: "tinybird",
        overlapMinutes: 20,
      });
      const datasource = readFileSync(
        join(rawDatasourceDirectory, `${table.resourceName}.datasource`),
        "utf8",
      );
      const importUri = datasource.match(/^IMPORT_BUCKET_URI (.+)$/m)?.[1];
      const recursivePrefix = importUri?.replace("**/*.parquet", "");

      expect(table.watermarkColumns.length).toBeGreaterThan(0);
      expect(plan.uri.startsWith(recursivePrefix || "missing-prefix")).toBe(true);
      expect(plan.query).toContain("format='PARQUET'");
      expect(plan.query).toContain("overwrite=true");

      if (table.sourcePartitioning) {
        const partitionColumn = table.sourcePartitioning.field
          || table.sourcePartitioning.pseudoColumn;
        expect(plan.query).toContain(`WHERE \`${partitionColumn}\` >=`);
      }

      for (const column of table.jsonColumns) {
        expect(plan.query).toContain(
          `TO_JSON_STRING(\`${column}\`) AS \`${column}\``,
        );
      }

      for (const column of table.geographyColumns) {
        expect(plan.query).toContain(
          `ST_ASWKT(\`${column}\`) AS \`${column}\``,
        );
      }
    }
  });
});
